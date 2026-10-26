//! Async driver for [`HotStuffState`], mirroring [`crate::node::RaftNode`].
//!
//! The [`HotStuffState`] machine is sans-IO: it turns [`Event`]s into
//! [`Output`]s and never touches the clock, the network, or a task
//! scheduler. This driver supplies exactly those three: it owns the view
//! timer, forwards messages through a [`HotStuffTransport`], and runs the
//! single-threaded event loop that serializes every mutation of the state
//! machine. See `docs/hotstuff-design.md`.
//!
//! # Minimal usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use mistlib_consensus_core::hotstuff::{
//!     HotStuffNode, MemoryBlockStore, NoopCrypto, PacemakerConfig,
//! };
//!
//! // `runtime` and `transport` come from the platform crate
//! // (mistlib-consensus-native / -wasm); `validators` excludes `id`,
//! // which HotStuffState adds itself.
//! let node = HotStuffNode::new(
//!     id,
//!     validators,
//!     PacemakerConfig::default(),
//!     runtime,
//!     transport,
//!     MemoryBlockStore::new(),
//!     NoopCrypto,
//! );
//! node.attach_inbound(inbound_rx); // decoded (NodeId, HotStuffMessage) pairs
//! node.start();
//!
//! let mut committed = node.subscribe_committed();
//! let _view = node.propose(b"cmd".to_vec()).await?; // Err(NotLeader) off-leader
//! while let Ok(cmd) = committed.recv().await {
//!     // apply cmd.command at execution sequence cmd.sequence
//! }
//! ```

use super::crypto::CryptoProvider;
use super::message::HotStuffMessage;
use super::state::{CommittedCommand, Event, HotStuffState, Output, PacemakerConfig};
use super::storage::BlockStore;
use super::transport::HotStuffTransport;
use crate::error::{RaftError, Result};
use mistlib_core::runtime::AsyncRuntime;
use mistlib_core::types::NodeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc, oneshot};
use web_time::Duration;

enum NodeEvent {
    HotStuff(Event),
    Propose {
        command: Vec<u8>,
        reply: oneshot::Sender<Result<u64>>,
    },
    Shutdown,
}

/// A snapshot of leadership/progress kept in sync after every `step` so
/// the public accessors never have to lock the state machine itself.
#[derive(Debug, Clone, Default)]
struct Status {
    is_leader: bool,
    leader_hint: Option<NodeId>,
    running: bool,
    committed_height: u64,
}

/// Drives a [`HotStuffState`] machine over time using an [`AsyncRuntime`]
/// and a [`HotStuffTransport`]. Owns the single view timer (rearmed with
/// a fresh, possibly backed-off duration each view) and exposes the
/// public API client code interacts with. See the module docs for a
/// minimal usage sketch.
pub struct HotStuffNode<B: BlockStore + 'static, C: CryptoProvider + 'static> {
    id: NodeId,
    runtime: Arc<dyn AsyncRuntime>,
    transport: Arc<dyn HotStuffTransport>,
    events_tx: mpsc::UnboundedSender<NodeEvent>,
    events_rx: Mutex<Option<mpsc::UnboundedReceiver<NodeEvent>>>,
    state: Mutex<Option<HotStuffState<B, C>>>,
    status: Arc<Mutex<Status>>,
    committed_tx: broadcast::Sender<CommittedCommand>,
    /// Bumped on every `ResetViewTimer`; a spawned sleeper only fires its
    /// `ViewTimeout` if the epoch it captured is still current.
    view_epoch: Arc<AtomicU64>,
}

impl<B: BlockStore + 'static, C: CryptoProvider + 'static> HotStuffNode<B, C> {
    /// Builds a node and its inner [`HotStuffState`]. `validators` need
    /// not include `id` — the state machine adds, sorts, and dedups it.
    /// Nothing runs until [`start`](Self::start) is called.
    pub fn new(
        id: NodeId,
        validators: Vec<NodeId>,
        config: PacemakerConfig,
        runtime: Arc<dyn AsyncRuntime>,
        transport: Arc<dyn HotStuffTransport>,
        store: B,
        crypto: C,
    ) -> Arc<Self> {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (committed_tx, _) = broadcast::channel(1024);
        let state = HotStuffState::new(id.clone(), validators, store, crypto, config);

        Arc::new(Self {
            id,
            runtime,
            transport,
            events_tx,
            events_rx: Mutex::new(Some(events_rx)),
            state: Mutex::new(Some(state)),
            status: Arc::new(Mutex::new(Status::default())),
            committed_tx,
            view_epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Bootstraps the state machine (enters view 1, arms the first view
    /// timer, proposes if this node leads view 1) and starts the event
    /// loop. Safe to call only once; subsequent calls are no-ops.
    pub fn start(self: &Arc<Self>) {
        let mut events_rx_guard = self.events_rx.lock().unwrap();
        let events_rx = match events_rx_guard.take() {
            Some(rx) => rx,
            None => return, // already started
        };
        drop(events_rx_guard);

        let mut state_guard = self.state.lock().unwrap();
        let state = state_guard.take().expect("state already taken");
        drop(state_guard);

        self.status.lock().unwrap().running = true;

        self.spawn_event_loop(state, events_rx);
    }

    fn spawn_event_loop(
        self: &Arc<Self>,
        mut state: HotStuffState<B, C>,
        mut events_rx: mpsc::UnboundedReceiver<NodeEvent>,
    ) {
        let this = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        runtime.spawn(Box::pin(async move {
            // Bootstrap outputs (first ResetViewTimer, maybe a proposal)
            // are handled before we start draining external events.
            let outputs = state.bootstrap();
            this.handle_outputs(&state, outputs, None).await;

            loop {
                let event = match events_rx.recv().await {
                    Some(e) => e,
                    None => break,
                };
                match event {
                    NodeEvent::Shutdown => break,
                    NodeEvent::Propose { command, reply } => {
                        let outputs = state.step(Event::ClientPropose { command });
                        this.handle_outputs(&state, outputs, Some(reply)).await;
                    }
                    NodeEvent::HotStuff(ev) => {
                        let outputs = state.step(ev);
                        this.handle_outputs(&state, outputs, None).await;
                    }
                }
            }
            this.status.lock().unwrap().running = false;
        }));
    }

    async fn handle_outputs(
        self: &Arc<Self>,
        state: &HotStuffState<B, C>,
        outputs: Vec<Output>,
        propose_reply: Option<oneshot::Sender<Result<u64>>>,
    ) {
        let mut propose_reply = propose_reply;
        for out in outputs {
            match out {
                Output::SendMessage { to, msg } => {
                    let transport = Arc::clone(&self.transport);
                    self.runtime.spawn(Box::pin(async move {
                        if let Err(err) = transport.send(&to, msg).await {
                            tracing::warn!(target: "mistlib_consensus", %to, ?err, "failed to send hotstuff message");
                        }
                    }));
                }
                Output::Broadcast { msg } => {
                    // Fan out to every other validator; the state machine
                    // has already applied the message locally where it
                    // matters (e.g. a leader's own proposal).
                    for peer in state.validators() {
                        if peer == &self.id {
                            continue;
                        }
                        let to = peer.clone();
                        let msg = msg.clone();
                        let transport = Arc::clone(&self.transport);
                        self.runtime.spawn(Box::pin(async move {
                            if let Err(err) = transport.send(&to, msg).await {
                                tracing::warn!(target: "mistlib_consensus", %to, ?err, "failed to broadcast hotstuff message");
                            }
                        }));
                    }
                }
                Output::ApplyCommitted { commands } => {
                    for cmd in commands {
                        let _ = self.committed_tx.send(cmd);
                    }
                }
                Output::BecameLeader => {
                    let mut status = self.status.lock().unwrap();
                    status.is_leader = true;
                    status.leader_hint = Some(self.id.clone());
                }
                Output::BecameFollower => {
                    self.status.lock().unwrap().is_leader = false;
                }
                Output::ResetViewTimer { duration_ms } => {
                    self.arm_view_timer(duration_ms);
                }
                Output::Proposed { view } => {
                    if let Some(reply) = propose_reply.take() {
                        let _ = reply.send(Ok(view));
                    }
                }
                Output::ProposeRejected { leader_hint } => {
                    if let Some(reply) = propose_reply.take() {
                        let _ = reply.send(Err(RaftError::NotLeader { leader_hint }));
                    }
                }
            }
        }
        // Keep the status snapshot fresh even when no BecameLeader /
        // BecameFollower fired this step (e.g. a plain vote or a view
        // advance driven by an incoming QC).
        let mut status = self.status.lock().unwrap();
        status.is_leader = state.is_leader();
        status.leader_hint = state.current_leader();
        status.committed_height = state.committed_height();
    }

    /// (Re)arms the single view timer. Unlike Raft's fixed-cadence
    /// timers, the duration changes per view, so each reset bumps the
    /// epoch and spawns a fresh one-shot sleeper; stale sleepers observe
    /// the changed epoch and silently drop their `ViewTimeout`.
    fn arm_view_timer(self: &Arc<Self>, duration_ms: u64) {
        let epoch = self.view_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        let this = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        runtime.spawn(Box::pin(async move {
            this.runtime.sleep(Duration::from_millis(duration_ms)).await;
            if !this.status.lock().unwrap().running {
                return;
            }
            if this.view_epoch.load(Ordering::SeqCst) == epoch {
                let _ = this.events_tx.send(NodeEvent::HotStuff(Event::ViewTimeout));
            }
        }));
    }

    /// Feeds a message received from the network into this node.
    pub fn deliver(&self, from: NodeId, msg: HotStuffMessage) {
        let _ = self
            .events_tx
            .send(NodeEvent::HotStuff(Event::Message { from, msg }));
    }

    /// Spawns a background task that forwards every `(from, msg)` pair
    /// from `rx` into this node, e.g. the receiver half returned by
    /// [`crate::hotstuff::transport::HotStuffMistTransport::new`].
    pub fn attach_inbound(
        self: &Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<(NodeId, HotStuffMessage)>,
    ) {
        let this = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        runtime.spawn(Box::pin(async move {
            while let Some((from, msg)) = rx.recv().await {
                this.deliver(from, msg);
            }
        }));
    }

    /// Proposes a new command. Succeeds only while this node leads the
    /// current view; otherwise returns [`RaftError::NotLeader`] with a
    /// hint of the believed leader. The returned `u64` is the view the
    /// command was accepted into — commit is observed separately via
    /// [`subscribe_committed`](Self::subscribe_committed).
    pub async fn propose(&self, command: Vec<u8>) -> Result<u64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.events_tx
            .send(NodeEvent::Propose {
                command,
                reply: reply_tx,
            })
            .map_err(|_| RaftError::ShuttingDown)?;
        reply_rx.await.map_err(|_| RaftError::ChannelClosed)?
    }

    /// Subscribes to committed commands in execution order. Each
    /// [`CommittedCommand`] carries a gapless `sequence` starting at 1.
    pub fn subscribe_committed(&self) -> broadcast::Receiver<CommittedCommand> {
        self.committed_tx.subscribe()
    }

    /// Whether this node leads the current view, per the last `step`.
    pub fn is_leader(&self) -> bool {
        self.status.lock().unwrap().is_leader
    }

    /// The believed leader of the current view, per the last `step`.
    pub fn leader_hint(&self) -> Option<NodeId> {
        self.status.lock().unwrap().leader_hint.clone()
    }

    /// Height of the last committed block, per the last `step`.
    pub fn committed_height(&self) -> u64 {
        self.status.lock().unwrap().committed_height
    }

    pub fn add_peer(&self, peer: NodeId) {
        let _ = self
            .events_tx
            .send(NodeEvent::HotStuff(Event::AddPeer { peer }));
    }

    pub fn remove_peer(&self, peer: NodeId) {
        let _ = self
            .events_tx
            .send(NodeEvent::HotStuff(Event::RemovePeer { peer }));
    }

    pub fn shutdown(&self) {
        let _ = self.events_tx.send(NodeEvent::Shutdown);
    }
}
