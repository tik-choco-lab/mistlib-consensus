//! In-process, multi-node integration tests for the chained-HotStuff
//! engine, mirroring `tests/cluster.rs` (the Raft harness) but wired to
//! `HotStuffNode` / `HotStuffTransport`. Self-contained: it carries its
//! own Tokio-backed `AsyncRuntime` and an in-memory, partition-capable
//! router rather than extending `tests/common` (which is Raft-typed), so
//! the Raft tests are untouched.
//!
//! Not compiled for wasm32: needs Tokio's multi-thread scheduler + timers.
#![cfg(not(target_arch = "wasm32"))]

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use mistlib_consensus_core::hotstuff::{
    BlockStore, CommittedCommand, CryptoProvider, Ed25519Crypto, HotStuffMessage, HotStuffNode,
    HotStuffTransport, MemoryBlockStore, NoopCrypto, PacemakerConfig,
};
use mistlib_consensus_core::{
    ConsensusMode, ConsensusNode, MemoryStorage, NodeId, RaftConfig, RaftError, RaftMessage,
    RaftNode, RaftTransport,
};
use rand::rngs::OsRng;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;
use tokio::sync::{broadcast, mpsc};
use web_time::Duration;

// ------------------------------------------------------------------
// Runtime: a trivial `AsyncRuntime` over the ambient Tokio runtime,
// exactly like the Raft integration harness.
// ------------------------------------------------------------------

struct TestRuntime(tokio::runtime::Handle);

#[async_trait]
impl mistlib_core::runtime::AsyncRuntime for TestRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.0.spawn(future);
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(StdDuration::from_millis(duration.as_millis() as u64)).await;
    }
}

fn test_runtime() -> Arc<dyn mistlib_core::runtime::AsyncRuntime> {
    Arc::new(TestRuntime(tokio::runtime::Handle::current()))
}

// ------------------------------------------------------------------
// In-memory HotStuff router: routes `(from, msg)` between nodes'
// `attach_inbound` channels, and can isolate a node (both directions)
// to model a silent / partitioned validator.
// ------------------------------------------------------------------

type Inbound = mpsc::UnboundedReceiver<(NodeId, HotStuffMessage)>;

#[derive(Clone)]
struct Router(Arc<RouterInner>);

struct RouterInner {
    routes: Mutex<HashMap<NodeId, mpsc::UnboundedSender<(NodeId, HotStuffMessage)>>>,
    dropped: Mutex<HashSet<NodeId>>,
    /// A small, uniform per-hop latency. Without it the in-memory pipeline
    /// free-runs at thousands of views/sec and, because a quorum is only
    /// 3-of-4, the consistently-slowest node is left out of every QC and
    /// falls permanently behind (v1 has no block sync to recover it). A
    /// few ms makes the vote round-trip (~2 hops) dominate task-scheduling
    /// jitter, so a proposal for view v+1 never outruns the votes for view
    /// v and all four nodes advance in lockstep.
    hop_latency: Duration,
}

impl Router {
    fn new(ids: &[NodeId]) -> (Self, HashMap<NodeId, Inbound>) {
        let mut routes = HashMap::new();
        let mut rxs = HashMap::new();
        for id in ids {
            let (tx, rx) = mpsc::unbounded_channel();
            routes.insert(id.clone(), tx);
            rxs.insert(id.clone(), rx);
        }
        (
            Router(Arc::new(RouterInner {
                routes: Mutex::new(routes),
                dropped: Mutex::new(HashSet::new()),
                hop_latency: Duration::from_millis(3),
            })),
            rxs,
        )
    }

    /// Isolates `id`: nothing is delivered to or from it. Its own view
    /// timers keep firing but reach no peer, modelling a silent leader.
    fn drop_node(&self, id: &NodeId) {
        self.0.dropped.lock().unwrap().insert(id.clone());
    }

    fn transport_for(&self, from: NodeId) -> Arc<dyn HotStuffTransport> {
        Arc::new(RouterTransport { router: self.clone(), from })
    }
}

struct RouterTransport {
    router: Router,
    from: NodeId,
}

#[async_trait]
impl HotStuffTransport for RouterTransport {
    async fn send(&self, to: &NodeId, msg: HotStuffMessage) -> mistlib_consensus_core::Result<()> {
        {
            let dropped = self.router.0.dropped.lock().unwrap();
            if dropped.contains(&self.from) || dropped.contains(to) {
                return Ok(());
            }
        }
        let latency = self.router.0.hop_latency;
        tokio::time::sleep(StdDuration::from_millis(latency.as_millis() as u64)).await;
        // Re-check the partition after the delay so a node dropped mid-flight
        // still has its in-flight message suppressed.
        {
            let dropped = self.router.0.dropped.lock().unwrap();
            if dropped.contains(&self.from) || dropped.contains(to) {
                return Ok(());
            }
        }
        if let Some(tx) = self.router.0.routes.lock().unwrap().get(to) {
            let _ = tx.send((self.from.clone(), msg));
        }
        Ok(())
    }
}

// ------------------------------------------------------------------
// Cluster + polling helpers (mirroring cluster.rs's shape).
// ------------------------------------------------------------------

type HsNode = HotStuffNode<MemoryBlockStore, NoopCrypto>;

fn nid(s: &str) -> NodeId {
    NodeId(s.to_string())
}

/// Short pacemaker timeouts keep the tests quick while leaving enough
/// slack for the in-memory round-trips.
fn pacemaker() -> PacemakerConfig {
    PacemakerConfig { base_timeout_ms: 150, max_timeout_ms: 2_000 }
}

/// Builds and starts `ids` as a fully-connected NoopCrypto cluster.
fn spawn_cluster(
    ids: &[NodeId],
    router: &Router,
    mut rxs: HashMap<NodeId, Inbound>,
    runtime: &Arc<dyn mistlib_core::runtime::AsyncRuntime>,
) -> HashMap<NodeId, Arc<HsNode>> {
    let mut nodes = HashMap::new();
    for id in ids {
        let peers: Vec<NodeId> = ids.iter().filter(|p| *p != id).cloned().collect();
        let node = HotStuffNode::new(
            id.clone(),
            peers,
            pacemaker(),
            Arc::clone(runtime),
            router.transport_for(id.clone()),
            MemoryBlockStore::new(),
            NoopCrypto,
        );
        node.attach_inbound(rxs.remove(id).unwrap());
        node.start();
        nodes.insert(id.clone(), node);
    }
    nodes
}

/// Polls `check` every 5ms up to `timeout`, returning its first `Some`.
async fn wait_for<T>(timeout: StdDuration, mut check: impl FnMut() -> Option<T>) -> T {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(v) = check() {
                return v;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition not met before timeout")
}

/// Returns the id of a currently-leading node among `candidates`, if any.
fn current_leader<B: BlockStore + 'static, C: CryptoProvider + 'static>(
    nodes: &HashMap<NodeId, Arc<HotStuffNode<B, C>>>,
    candidates: &[NodeId],
) -> Option<NodeId> {
    candidates.iter().find(|id| nodes[*id].is_leader()).cloned()
}

/// A committed-command sink that a background task keeps filled from one
/// node's committed stream, so injectors can tell when a specific command
/// has actually reached commit (not merely been accepted).
type Seen = Arc<Mutex<Vec<Vec<u8>>>>;

fn watch_committed<B: BlockStore + 'static, C: CryptoProvider + 'static>(
    node: &Arc<HotStuffNode<B, C>>,
    runtime: &Arc<dyn mistlib_core::runtime::AsyncRuntime>,
) -> Seen {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let mut rx = node.subscribe_committed();
    runtime.spawn(Box::pin(async move {
        while let Ok(cmd) = rx.recv().await {
            sink.lock().unwrap().push(cmd.command);
        }
    }));
    seen
}

/// Best-effort single attempt to hand `command` to the believed leader,
/// following `NotLeader` hints a bounded number of hops. `true` if some
/// node accepted it this attempt.
///
/// A single attempt is racy on purpose: a healthy leader self-votes on
/// its own proposal and leaves leadership within the same step, so most
/// attempts see `NotLeader`. Callers retry.
async fn try_land<B: BlockStore + 'static, C: CryptoProvider + 'static>(
    nodes: &HashMap<NodeId, Arc<HotStuffNode<B, C>>>,
    candidates: &[NodeId],
    command: &[u8],
) -> bool {
    let mut target = candidates
        .iter()
        .find_map(|id| nodes[id].leader_hint())
        .or_else(|| candidates.first().cloned());
    for _ in 0..(candidates.len() * 2).max(4) {
        let Some(t) = target else { return false };
        if !nodes.contains_key(&t) {
            return false;
        }
        match nodes[&t].propose(command.to_vec()).await {
            Ok(_) => return true,
            Err(RaftError::NotLeader { leader_hint }) => target = leader_hint,
            Err(_) => return false,
        }
    }
    false
}

/// Injects `command` and blocks until it is observed committed via `seen`,
/// re-injecting if a landed command fails to commit within `commit_grace`
/// (it was queued on a node that then lost the block, or orphaned). Unique
/// commands + a grace window well above the healthy commit latency keep
/// re-injection from double-committing a merely-slow command.
async fn commit_command<B: BlockStore + 'static, C: CryptoProvider + 'static>(
    nodes: &HashMap<NodeId, Arc<HotStuffNode<B, C>>>,
    candidates: &[NodeId],
    command: Vec<u8>,
    seen: &Seen,
    overall: StdDuration,
) {
    let commit_grace = StdDuration::from_millis(1_500);
    let committed = |c: &[u8]| seen.lock().unwrap().iter().any(|x| x == c);
    tokio::time::timeout(overall, async {
        loop {
            // Land the command (retry the racy hand-off for up to a second).
            let _ = tokio::time::timeout(StdDuration::from_secs(1), async {
                while !try_land(nodes, candidates, &command).await {
                    tokio::time::sleep(StdDuration::from_millis(5)).await;
                }
            })
            .await;
            // Wait for it to actually commit; if it does, we're done.
            let landed_committed = tokio::time::timeout(commit_grace, async {
                while !committed(&command) {
                    tokio::time::sleep(StdDuration::from_millis(10)).await;
                }
            })
            .await;
            if landed_committed.is_ok() {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("command {command:?} never committed within {overall:?}"));
}

/// Drains exactly `n` committed commands from `rx`, panicking on timeout.
async fn collect_committed(
    id: &NodeId,
    rx: &mut broadcast::Receiver<CommittedCommand>,
    n: usize,
    timeout: StdDuration,
) -> Vec<CommittedCommand> {
    let mut got = Vec::new();
    while got.len() < n {
        let cmd = tokio::time::timeout(timeout, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("node {id:?} timed out waiting for {n} commits (had {})", got.len())
            })
            .expect("committed channel closed unexpectedly");
        got.push(cmd);
    }
    got
}

// ==================================================================
// Test 1 — happy path: a 4-node cluster commits a batch of proposals
// identically on every node.
// ==================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_node_cluster_commits_proposals() {
    let ids: Vec<NodeId> = ["n1", "n2", "n3", "n4"].iter().map(|s| nid(s)).collect();
    let (router, rxs) = Router::new(&ids);
    let runtime = test_runtime();
    let nodes = spawn_cluster(&ids, &router, rxs, &runtime);

    // A view leader must emerge before we can steer proposals.
    let _ = wait_for(StdDuration::from_secs(5), || current_leader(&nodes, &ids)).await;

    // Subscribe before proposing so no early commit is missed. Only real
    // commands ride the committed stream (empty pipeline blocks carry
    // none), so the invariant is cross-node agreement on the order and
    // gapless sequences — not agreement with proposal order, since a
    // leader that has already proposed its view queues our command for a
    // later block it leads.
    let mut committed: HashMap<NodeId, broadcast::Receiver<CommittedCommand>> =
        nodes.iter().map(|(id, n)| (id.clone(), n.subscribe_committed())).collect();
    let seen = watch_committed(&nodes[&ids[0]], &runtime);

    // Inject three distinct commands, each confirmed committed before the
    // next so we know exactly three real commands enter the log.
    let commands: Vec<Vec<u8>> = vec![vec![0xA0], vec![0xB1], vec![0xC2]];
    for cmd in &commands {
        commit_command(&nodes, &ids, cmd.clone(), &seen, StdDuration::from_secs(20)).await;
    }

    // Collect three commits per node.
    let mut per_node: HashMap<NodeId, Vec<CommittedCommand>> = HashMap::new();
    for id in &ids {
        let rx = committed.get_mut(id).unwrap();
        per_node.insert(id.clone(), collect_committed(id, rx, 3, StdDuration::from_secs(15)).await);
    }

    // Every node agrees on the exact committed sequence.
    let reference = &per_node[&ids[0]];
    let seqs: Vec<u64> = reference.iter().map(|c| c.sequence).collect();
    assert_eq!(seqs, vec![1, 2, 3], "sequences must be gapless 1..=3");
    let mut payloads: Vec<Vec<u8>> = reference.iter().map(|c| c.command.clone()).collect();
    payloads.sort();
    let mut expected = commands.clone();
    expected.sort();
    assert_eq!(payloads, expected, "all three commands present exactly once");
    for id in &ids[1..] {
        assert_eq!(&per_node[id], reference, "node {id:?} disagrees on the committed order");
    }

    for n in nodes.values() {
        n.shutdown();
    }
}

// ==================================================================
// Test 3 — a proposal on a non-leader is rejected synchronously with a
// hint that names an actual leader.
// ==================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_leader_propose_rejected_with_hint() {
    let ids: Vec<NodeId> = ["n1", "n2", "n3", "n4"].iter().map(|s| nid(s)).collect();
    let (router, rxs) = Router::new(&ids);
    let runtime = test_runtime();
    let nodes = spawn_cluster(&ids, &router, rxs, &runtime);

    // Let the pipeline start (some node has an opinion on who leads).
    let _ = wait_for(StdDuration::from_secs(5), || {
        ids.iter().find_map(|id| nodes[id].leader_hint())
    })
    .await;

    // Propose on a follower (a node that is not currently leading) with no
    // retries. Leadership rotates fast, so if we happen to pick a momentary
    // leader that accepts, we re-pick a follower and try again — but each
    // proposal itself is a single shot.
    let hint = tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            let follower = ids.iter().find(|id| !nodes[*id].is_leader()).cloned();
            if let Some(follower) = follower {
                match nodes[&follower].propose(b"nope".to_vec()).await {
                    Err(RaftError::NotLeader { leader_hint: Some(h) }) => {
                        assert!(ids.contains(&h), "hint {h:?} is not a validator");
                        assert_ne!(h, follower, "a follower must not name itself as leader");
                        return h;
                    }
                    // Hint momentarily unknown, or we caught a leader that
                    // accepted — re-pick and retry.
                    Err(RaftError::NotLeader { leader_hint: None }) => {}
                    Ok(_) => {}
                    Err(e) => panic!("unexpected propose error: {e:?}"),
                }
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("never observed a NotLeader rejection with a hint");

    // The hint identifies a real leader: that node is observed leading a
    // view within a rotation (round-robin cycles leadership to it).
    let _ = wait_for(StdDuration::from_secs(5), || nodes[&hint].is_leader().then_some(())).await;

    for n in nodes.values() {
        n.shutdown();
    }
}

// ==================================================================
// Test 2 — leader silence: partitioning the current view leader out at
// the router drives the survivors through repeated view changes
// (NewView + timeout), and they keep committing without it.
//
// NOTE: this uses n=5 (not 4). v1 chained HotStuff cannot commit while a
// validator is permanently silent in an n=4 cluster — committing a block
// needs a 3-chain of consecutive-view blocks whose top is certified by a
// fourth consecutive live leader, but the silent node leads every 4th
// view, so the top of every honest run loses its certifier (verified: a
// 4-node variant froze for 15s). n=5 (f=1, quorum=4, four survivors) is
// the smallest cluster with a 4-consecutive-live-leader window each
// period, so the survivors keep committing. See the test report.
// ==================================================================

/// Samples `node`'s believed leader over `window`, returning the set of
/// distinct hints seen. More than one distinct value means the view is
/// advancing (leadership is rotating), independent of whether anything
/// commits.
async fn observe_leader_hints(node: &Arc<HsNode>, window: StdDuration) -> HashSet<NodeId> {
    let mut seen = HashSet::new();
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        if let Some(h) = node.leader_hint() {
            seen.insert(h);
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_survives_leader_silence() {
    let ids: Vec<NodeId> = ["n1", "n2", "n3", "n4", "n5"].iter().map(|s| nid(s)).collect();
    let (router, rxs) = Router::new(&ids);
    let runtime = test_runtime();
    let nodes = spawn_cluster(&ids, &router, rxs, &runtime);

    // Wait for healthy progress, then isolate whoever currently leads.
    let _ = wait_for(StdDuration::from_secs(5), || {
        (nodes[&ids[0]].committed_height() > 0).then_some(())
    })
    .await;
    let victim = wait_for(StdDuration::from_secs(5), || current_leader(&nodes, &ids)).await;
    let survivors: Vec<NodeId> = ids.iter().filter(|id| **id != victim).cloned().collect();
    router.drop_node(&victim);

    // View-change liveness: with the leader silent, the survivors must keep
    // advancing views (timeout -> NewView -> next leader). We observe that
    // as the believed leader rotating through more than one node — and, in
    // particular, moving off the silenced victim onto a survivor.
    let hints = observe_leader_hints(&nodes[&survivors[0]], StdDuration::from_secs(3)).await;
    assert!(
        hints.len() > 1,
        "views did not advance during leader silence (hints seen: {hints:?})"
    );
    assert!(
        hints.iter().any(|h| survivors.contains(h)),
        "leadership never moved to a survivor during silence (hints: {hints:?})"
    );

    // A command proposed via the survivors still commits — the view change
    // recovered the pipeline onto the live validators — and every survivor
    // observes it. The victim is partitioned and never sees it.
    let seen = watch_committed(&nodes[&survivors[0]], &runtime);
    let mut committed: HashMap<NodeId, broadcast::Receiver<CommittedCommand>> = survivors
        .iter()
        .map(|id| (id.clone(), nodes[id].subscribe_committed()))
        .collect();
    let cmd = b"after-silence".to_vec();
    commit_command(&nodes, &survivors, cmd.clone(), &seen, StdDuration::from_secs(30)).await;

    for id in &survivors {
        let rx = committed.get_mut(id).unwrap();
        let got = collect_committed(id, rx, 1, StdDuration::from_secs(20)).await;
        assert_eq!(got[0].command, cmd, "survivor {id:?} committed the wrong command");
    }

    for n in nodes.values() {
        n.shutdown();
    }
}

/// A `RaftTransport` that discards everything — a single-node Raft cluster
/// never has a peer to message, so nothing is ever sent through it.
struct NullRaftTransport;

#[async_trait]
impl RaftTransport for NullRaftTransport {
    async fn send(&self, _to: &NodeId, _msg: RaftMessage) -> mistlib_consensus_core::Result<()> {
        Ok(())
    }
}

// ==================================================================
// Test 4 — mode facade: a single-validator HotStuff node
// (ConsensusMode::Untrusted) and a single-node Raft node
// (ConsensusMode::Trusted), driven only through the unified
// `ConsensusNode` API, each commit a proposed command with the right
// payload, and report distinct modes.
// ==================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mode_facade_smoke() {
    let runtime = test_runtime();

    // --- Untrusted: a lone HotStuff validator (n=1, its own quorum). A
    // long pacemaker timeout keeps view-timeouts from interleaving with
    // our proposals, so successive proposals build a consecutive-view
    // chain; a single validator needs a block plus two more on top to
    // close the first command's 3-chain, so we feed follow-on fillers
    // until the target commits.
    let hs_id = nid("solo-hs");
    let (hs_router, mut hs_rxs) = Router::new(std::slice::from_ref(&hs_id));
    let hs_node = HotStuffNode::new(
        hs_id.clone(),
        vec![],
        PacemakerConfig { base_timeout_ms: 5_000, max_timeout_ms: 8_000 },
        Arc::clone(&runtime),
        hs_router.transport_for(hs_id.clone()),
        MemoryBlockStore::new(),
        NoopCrypto,
    );
    hs_node.attach_inbound(hs_rxs.remove(&hs_id).unwrap());
    hs_node.start();
    let untrusted: ConsensusNode<MemoryStorage, MemoryBlockStore, NoopCrypto> =
        ConsensusNode::untrusted(hs_node);

    // --- Trusted: a lone Raft node (elects itself, commits immediately).
    let raft_id = nid("solo-raft");
    let (_raft_tx, raft_rx) = mpsc::unbounded_channel();
    let raft_node = RaftNode::new(
        raft_id.clone(),
        vec![],
        RaftConfig { election_timeout_min_ms: 60, election_timeout_max_ms: 120, heartbeat_interval_ms: 20 },
        Arc::clone(&runtime),
        Arc::new(NullRaftTransport),
        MemoryStorage::new(),
    );
    raft_node.attach_inbound(raft_rx);
    raft_node.start();
    let trusted: ConsensusNode<MemoryStorage, MemoryBlockStore, NoopCrypto> =
        ConsensusNode::trusted(raft_node);

    // Distinct modes through the facade.
    assert_eq!(untrusted.mode(), ConsensusMode::Untrusted);
    assert_eq!(trusted.mode(), ConsensusMode::Trusted);
    assert_ne!(untrusted.mode(), trusted.mode());

    // Subscribe both unified streams before proposing.
    let mut hs_rx = untrusted.subscribe_committed(&runtime);
    let mut raft_rx = trusted.subscribe_committed(&runtime);

    // Trusted path: one propose commits (single-node Raft).
    let _ = wait_for(StdDuration::from_secs(5), || trusted.is_leader().then_some(())).await;
    let raft_cmd = b"raft-payload".to_vec();
    trusted.propose(raft_cmd.clone()).await.expect("single-node raft propose should succeed");
    let raft_committed = tokio::time::timeout(StdDuration::from_secs(5), raft_rx.recv())
        .await
        .expect("raft commit timed out")
        .expect("raft committed stream closed");
    assert_eq!(raft_committed.command, raft_cmd, "trusted facade committed wrong payload");

    // Untrusted path: propose the target, then feed fillers until it
    // commits (the first committed command must be our target).
    let hs_cmd = b"hotstuff-payload".to_vec();
    untrusted.propose(hs_cmd.clone()).await.expect("solo hotstuff propose should succeed");
    let hs_committed = tokio::time::timeout(StdDuration::from_secs(15), async {
        loop {
            // Try to observe the commit; feed a filler if it hasn't landed.
            if let Ok(Some(c)) =
                tokio::time::timeout(StdDuration::from_millis(200), hs_rx.recv()).await
            {
                return c;
            }
            let _ = untrusted.propose(b"filler".to_vec()).await;
        }
    })
    .await
    .expect("hotstuff commit timed out");
    assert_eq!(hs_committed.command, hs_cmd, "untrusted facade committed wrong payload");
    assert_eq!(hs_committed.sequence, 1, "target is the first committed command");

    untrusted.shutdown();
    trusted.shutdown();
}

// ==================================================================
// Test 5 — untrusted mode: a 4-node cluster running real Ed25519
// signing (every vote, proposal and QC is genuinely signed and
// verified) commits a proposed command on every node. End-to-end proof
// that the signed path works through the async driver, not just the
// sans-IO unit tests.
// ==================================================================

type Ed25519Node = HotStuffNode<MemoryBlockStore, Ed25519Crypto>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untrusted_mode_with_real_signatures() {
    let ids: Vec<NodeId> = ["n1", "n2", "n3", "n4"].iter().map(|s| nid(s)).collect();

    // One keypair per validator; the registry of verifying keys is shared
    // by all nodes (key distribution is out of scope for the engine).
    let signing_keys: HashMap<NodeId, SigningKey> =
        ids.iter().map(|id| (id.clone(), SigningKey::generate(&mut OsRng))).collect();
    let registry: HashMap<NodeId, _> =
        signing_keys.iter().map(|(id, sk)| (id.clone(), sk.verifying_key())).collect();

    let (router, mut rxs) = Router::new(&ids);
    let runtime = test_runtime();
    let mut nodes: HashMap<NodeId, Arc<Ed25519Node>> = HashMap::new();
    for id in &ids {
        let peers: Vec<NodeId> = ids.iter().filter(|p| *p != id).cloned().collect();
        let crypto = Ed25519Crypto::new(signing_keys[id].clone(), registry.clone());
        let node = HotStuffNode::new(
            id.clone(),
            peers,
            pacemaker(),
            Arc::clone(&runtime),
            router.transport_for(id.clone()),
            MemoryBlockStore::new(),
            crypto,
        );
        node.attach_inbound(rxs.remove(id).unwrap());
        node.start();
        nodes.insert(id.clone(), node);
    }

    let _ = wait_for(StdDuration::from_secs(5), || current_leader(&nodes, &ids)).await;

    let seen = watch_committed(&nodes[&ids[0]], &runtime);
    let mut committed: HashMap<NodeId, broadcast::Receiver<CommittedCommand>> =
        ids.iter().map(|id| (id.clone(), nodes[id].subscribe_committed())).collect();

    let cmd = b"signed-command".to_vec();
    commit_command(&nodes, &ids, cmd.clone(), &seen, StdDuration::from_secs(20)).await;

    for id in &ids {
        let rx = committed.get_mut(id).unwrap();
        let got = collect_committed(id, rx, 1, StdDuration::from_secs(15)).await;
        assert_eq!(got[0].command, cmd, "node {id:?} committed the wrong command");
    }

    for n in nodes.values() {
        n.shutdown();
    }
}
