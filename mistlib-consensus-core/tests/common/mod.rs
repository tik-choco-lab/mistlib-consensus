//! Shared harness for multi-node integration tests: a Tokio-backed
//! `AsyncRuntime`, a partition-capable in-memory transport, and small
//! polling helpers. Kept in `tests/common/mod.rs` (not `tests/common.rs`)
//! so Rust doesn't treat it as its own test binary.
#![allow(dead_code)]

use async_trait::async_trait;
use mistlib_consensus_core::{
    LogEntry, MemoryStorage, NodeId, RaftConfig, RaftMessage, RaftNode, RaftStorage, RaftTransport,
    Role,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;
use tokio::sync::mpsc;
use web_time::Duration;

/// Trivial `AsyncRuntime` wrapping the ambient Tokio runtime.
pub struct TestRuntime(pub tokio::runtime::Handle);

#[async_trait]
impl mistlib_core::runtime::AsyncRuntime for TestRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.0.spawn(future);
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(StdDuration::from_millis(duration.as_millis() as u64)).await;
    }
}

pub fn test_runtime() -> Arc<dyn mistlib_core::runtime::AsyncRuntime> {
    Arc::new(TestRuntime(tokio::runtime::Handle::current()))
}

pub fn fast_config() -> RaftConfig {
    RaftConfig {
        election_timeout_min_ms: 60,
        election_timeout_max_ms: 120,
        heartbeat_interval_ms: 20,
    }
}

/// A shared in-process network that routes messages between nodes and
/// can simulate partitions by assigning nodes to numbered groups: a
/// message is only delivered if the sender and recipient are currently
/// in the same group. All nodes start in group 0 (fully connected).
#[derive(Clone)]
pub struct Network(Arc<NetworkInner>);

struct NetworkInner {
    routes: Mutex<HashMap<NodeId, mpsc::UnboundedSender<(NodeId, RaftMessage)>>>,
    groups: Mutex<HashMap<NodeId, u32>>,
}

impl Network {
    /// Creates a network for the given node ids and returns it along with
    /// each node's inbound receiver (to be passed to `attach_inbound`).
    pub fn new(
        ids: &[NodeId],
    ) -> (
        Self,
        HashMap<NodeId, mpsc::UnboundedReceiver<(NodeId, RaftMessage)>>,
    ) {
        let mut routes = HashMap::new();
        let mut rxs = HashMap::new();
        let mut groups = HashMap::new();
        for id in ids {
            let (tx, rx) = mpsc::unbounded_channel();
            routes.insert(id.clone(), tx);
            rxs.insert(id.clone(), rx);
            groups.insert(id.clone(), 0);
        }
        (
            Network(Arc::new(NetworkInner {
                routes: Mutex::new(routes),
                groups: Mutex::new(groups),
            })),
            rxs,
        )
    }

    /// Places `id` into `group`. Two nodes can only exchange messages
    /// while in the same group; use distinct groups to simulate a
    /// partition and call `heal` (or set everyone back to the same
    /// group) to reconnect them.
    pub fn set_group(&self, id: &NodeId, group: u32) {
        self.0.groups.lock().unwrap().insert(id.clone(), group);
    }

    /// Reconnects every listed node into group 0.
    pub fn heal(&self, ids: &[NodeId]) {
        for id in ids {
            self.set_group(id, 0);
        }
    }

    /// Fully removes a node from the routing table so no other node can
    /// reach it, simulating a hard crash/disconnect. The node's own
    /// outbound sends still no-op harmlessly (nothing routes back to
    /// it), and callers should also call `RaftNode::shutdown()`.
    pub fn kill(&self, id: &NodeId) {
        self.0.routes.lock().unwrap().remove(id);
    }

    /// Re-adds a previously killed node's inbound route, returning the
    /// new receiver to attach to a freshly constructed `RaftNode`.
    pub fn revive(&self, id: &NodeId) -> mpsc::UnboundedReceiver<(NodeId, RaftMessage)> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.routes.lock().unwrap().insert(id.clone(), tx);
        rx
    }

    pub fn transport_for(&self, from: NodeId) -> Arc<dyn RaftTransport> {
        Arc::new(NetTransport {
            net: self.clone(),
            from,
        })
    }
}

struct NetTransport {
    net: Network,
    from: NodeId,
}

#[async_trait]
impl RaftTransport for NetTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) -> mistlib_consensus_core::Result<()> {
        let same_group = {
            let groups = self.net.0.groups.lock().unwrap();
            groups.get(&self.from) == groups.get(to)
        };
        if same_group {
            if let Some(tx) = self.net.0.routes.lock().unwrap().get(to) {
                let _ = tx.send((self.from.clone(), msg));
            }
        }
        Ok(())
    }
}

/// Spawns a `RaftNode` wired up to `net`, returning it plus a shared
/// vector that a background task keeps filled with every entry the node
/// applies (in commit order), for cross-node invariant checks.
pub fn spawn_node<S: RaftStorage + 'static>(
    id: NodeId,
    peers: Vec<NodeId>,
    config: RaftConfig,
    runtime: &Arc<dyn mistlib_core::runtime::AsyncRuntime>,
    net: &Network,
    inbound_rx: mpsc::UnboundedReceiver<(NodeId, RaftMessage)>,
    storage: S,
) -> (Arc<RaftNode<S>>, Arc<Mutex<Vec<LogEntry>>>) {
    let node = RaftNode::new(
        id.clone(),
        peers,
        config,
        Arc::clone(runtime),
        net.transport_for(id),
        storage,
    );
    node.attach_inbound(inbound_rx);

    let applied = Arc::new(Mutex::new(Vec::new()));
    let mut committed_rx = node.subscribe_committed();
    let applied_clone = Arc::clone(&applied);
    runtime.spawn(Box::pin(async move {
        while let Ok(entry) = committed_rx.recv().await {
            applied_clone.lock().unwrap().push(entry);
        }
    }));

    node.start();
    (node, applied)
}

/// Waits (polling every 10ms, up to `timeout`) until `check` returns
/// `Some(value)`, then returns that value. Panics on timeout.
pub async fn wait_for<T>(timeout: StdDuration, mut check: impl FnMut() -> Option<T>) -> T {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(v) = check() {
                return v;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition not met before timeout")
}

pub async fn wait_for_leader_among<S: RaftStorage + 'static>(
    nodes: &HashMap<NodeId, Arc<RaftNode<S>>>,
    ids: &[NodeId],
    timeout: StdDuration,
) -> NodeId {
    wait_for(timeout, || {
        ids.iter()
            .find(|id| nodes.get(*id).and_then(|n| n.role()) == Some(Role::Leader))
            .cloned()
    })
    .await
}

/// Wraps a `MemoryStorage` behind an `Arc<Mutex<_>>` so the exact same
/// durable-state instance can be handed to a brand-new `RaftNode` after
/// the old one is shut down, simulating a process restart against the
/// same disk. `log()` must return `&[LogEntry]` tied to `&self`, which
/// isn't possible while holding a `Mutex` guard, so it leaks a fresh
/// snapshot slice on each call; that's fine for a short-lived test but
/// would be wasteful for a real on-disk backend.
#[derive(Clone)]
pub struct SharedMemoryStorage(Arc<Mutex<MemoryStorage>>);

impl SharedMemoryStorage {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MemoryStorage::new())))
    }
}

impl RaftStorage for SharedMemoryStorage {
    fn current_term(&self) -> u64 {
        self.0.lock().unwrap().current_term()
    }

    fn voted_for(&self) -> Option<NodeId> {
        self.0.lock().unwrap().voted_for()
    }

    fn set_term_and_vote(&mut self, term: u64, voted_for: Option<NodeId>) {
        self.0.lock().unwrap().set_term_and_vote(term, voted_for);
    }

    fn log(&self) -> &[LogEntry] {
        let snapshot: Vec<LogEntry> = self.0.lock().unwrap().log().to_vec();
        Box::leak(snapshot.into_boxed_slice())
    }

    fn append_entries(&mut self, start_index: u64, entries: Vec<LogEntry>) {
        self.0.lock().unwrap().append_entries(start_index, entries);
    }

    fn last_log_index(&self) -> u64 {
        self.0.lock().unwrap().last_log_index()
    }

    fn last_log_term(&self) -> u64 {
        self.0.lock().unwrap().last_log_term()
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        self.0.lock().unwrap().term_at(index)
    }

    fn entries_from(&self, from: u64) -> Vec<LogEntry> {
        self.0.lock().unwrap().entries_from(from)
    }

    fn truncate_from(&mut self, index: u64) {
        self.0.lock().unwrap().truncate_from(index);
    }
}

/// Log Matching / State Machine Safety check: for every pair of nodes,
/// their applied-entry sequences must agree on every index they both
/// have reached (no divergence in the committed prefix), regardless of
/// which one has applied further so far.
pub fn assert_logs_consistent(applied: &HashMap<NodeId, Arc<Mutex<Vec<LogEntry>>>>) {
    let snapshots: HashMap<NodeId, Vec<LogEntry>> = applied
        .iter()
        .map(|(id, v)| (id.clone(), v.lock().unwrap().clone()))
        .collect();
    let ids: Vec<&NodeId> = snapshots.keys().collect();
    for i in 0..ids.len() {
        for j in (i + 1)..ids.len() {
            let a = &snapshots[ids[i]];
            let b = &snapshots[ids[j]];
            let common_len = a.len().min(b.len());
            for k in 0..common_len {
                assert_eq!(
                    a[k], b[k],
                    "log divergence between {:?} and {:?} at applied position {}",
                    ids[i], ids[j], k
                );
            }
        }
    }
}
