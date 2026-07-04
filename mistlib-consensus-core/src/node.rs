use crate::config::RaftConfig;
use crate::error::{RaftError, Result};
use crate::message::{LogEntry, RaftMessage};
use crate::state::{Event, Output, RaftState, Role};
use crate::storage::RaftStorage;
use crate::transport::RaftTransport;
use mistlib_core::runtime::AsyncRuntime;
use mistlib_core::types::NodeId;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc, oneshot};
use web_time::Duration;

enum NodeEvent {
    Raft(Event),
    Propose {
        command: Vec<u8>,
        reply: oneshot::Sender<Result<u64>>,
    },
    Shutdown,
}

#[derive(Debug, Clone, Default)]
struct Status {
    role: Option<Role>,
    leader_hint: Option<NodeId>,
    running: bool,
}

/// Drives a [`RaftState`] machine over time using an [`AsyncRuntime`]
/// and a [`RaftTransport`]. Owns randomized election timeouts and
/// periodic heartbeats, and exposes the public API client code
/// interacts with.
pub struct RaftNode<S: RaftStorage + 'static> {
    id: NodeId,
    config: RaftConfig,
    runtime: Arc<dyn AsyncRuntime>,
    transport: Arc<dyn RaftTransport>,
    events_tx: mpsc::UnboundedSender<NodeEvent>,
    events_rx: Mutex<Option<mpsc::UnboundedReceiver<NodeEvent>>>,
    state: Mutex<Option<RaftState<S>>>,
    status: Arc<Mutex<Status>>,
    committed_tx: broadcast::Sender<LogEntry>,
    election_epoch: Arc<AtomicU64>,
}

impl<S: RaftStorage + 'static> RaftNode<S> {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        config: RaftConfig,
        runtime: Arc<dyn AsyncRuntime>,
        transport: Arc<dyn RaftTransport>,
        storage: S,
    ) -> Arc<Self> {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (committed_tx, _) = broadcast::channel(1024);
        let state = RaftState::new(id.clone(), peers, storage);

        Arc::new(Self {
            id,
            config,
            runtime,
            transport,
            events_tx,
            events_rx: Mutex::new(Some(events_rx)),
            state: Mutex::new(Some(state)),
            status: Arc::new(Mutex::new(Status::default())),
            committed_tx,
            election_epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Starts the event loop and the election/heartbeat timers. Safe to
    /// call only once; subsequent calls are no-ops.
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
        self.spawn_election_timer();
        self.spawn_heartbeat_timer();
    }

    fn spawn_event_loop(
        self: &Arc<Self>,
        mut state: RaftState<S>,
        mut events_rx: mpsc::UnboundedReceiver<NodeEvent>,
    ) {
        let this = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        runtime.spawn(Box::pin(async move {
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
                    NodeEvent::Raft(ev) => {
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
        state: &RaftState<S>,
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
                            tracing::warn!(target: "mistlib_consensus", %to, ?err, "failed to send raft message");
                        }
                    }));
                }
                Output::ApplyCommitted { entries } => {
                    for entry in entries {
                        let _ = self.committed_tx.send(entry);
                    }
                }
                Output::BecameLeader => {
                    let mut status = self.status.lock().unwrap();
                    status.role = Some(Role::Leader);
                    status.leader_hint = Some(self.id.clone());
                }
                Output::BecameFollower => {
                    let mut status = self.status.lock().unwrap();
                    status.role = Some(Role::Follower);
                }
                Output::ResetElectionTimer => {
                    self.election_epoch.fetch_add(1, Ordering::SeqCst);
                }
                Output::ResetHeartbeatTimer => {}
                Output::Proposed { index } => {
                    if let Some(reply) = propose_reply.take() {
                        let _ = reply.send(Ok(index));
                    }
                }
                Output::ProposeRejected { leader_hint } => {
                    if let Some(reply) = propose_reply.take() {
                        let _ = reply.send(Err(RaftError::NotLeader { leader_hint }));
                    }
                }
            }
        }
        // Keep status.role/leader_hint fresh even when no BecameLeader /
        // BecameFollower output fired this step (e.g. plain vote grants).
        let mut status = self.status.lock().unwrap();
        status.role = Some(state.role);
        if state.role == Role::Leader {
            status.leader_hint = Some(self.id.clone());
        } else if let Some(leader) = &state.leader_id {
            status.leader_hint = Some(leader.clone());
        }
    }

    fn spawn_election_timer(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        runtime.spawn(Box::pin(async move {
            loop {
                let epoch_before = this.election_epoch.load(Ordering::SeqCst);
                let timeout_ms = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(
                        this.config.election_timeout_min_ms..=this.config.election_timeout_max_ms,
                    )
                };
                this.runtime.sleep(Duration::from_millis(timeout_ms)).await;
                if !this.status.lock().unwrap().running {
                    break;
                }
                if this.election_epoch.load(Ordering::SeqCst) == epoch_before
                    && this.events_tx.send(NodeEvent::Raft(Event::ElectionTimeout)).is_err()
                {
                    break;
                }
            }
        }));
    }

    fn spawn_heartbeat_timer(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        let interval = Duration::from_millis(self.config.heartbeat_interval_ms);
        runtime.spawn(Box::pin(async move {
            loop {
                this.runtime.sleep(interval).await;
                if !this.status.lock().unwrap().running {
                    break;
                }
                if this.events_tx.send(NodeEvent::Raft(Event::HeartbeatTimeout)).is_err() {
                    break;
                }
            }
        }));
    }

    /// Feeds a message received from the network into this node.
    pub fn deliver(&self, from: NodeId, msg: RaftMessage) {
        let _ = self.events_tx.send(NodeEvent::Raft(Event::Message { from, msg }));
    }

    /// Spawns a background task that forwards every `(from, msg)` pair
    /// from `rx` into this node, e.g. the receiver half returned by
    /// [`crate::transport::MistTransport::new`].
    pub fn attach_inbound(
        self: &Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<(NodeId, RaftMessage)>,
    ) {
        let this = Arc::clone(self);
        let runtime = Arc::clone(&self.runtime);
        runtime.spawn(Box::pin(async move {
            while let Some((from, msg)) = rx.recv().await {
                this.deliver(from, msg);
            }
        }));
    }

    /// Proposes a new command. Only succeeds while this node is the
    /// leader; otherwise returns `RaftError::NotLeader` with a hint of
    /// who the current leader is believed to be, if known.
    pub async fn propose(&self, command: Vec<u8>) -> Result<u64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.events_tx
            .send(NodeEvent::Propose { command, reply: reply_tx })
            .map_err(|_| RaftError::ShuttingDown)?;
        reply_rx.await.map_err(|_| RaftError::ChannelClosed)?
    }

    pub fn subscribe_committed(&self) -> broadcast::Receiver<LogEntry> {
        self.committed_tx.subscribe()
    }

    pub fn role(&self) -> Option<Role> {
        self.status.lock().unwrap().role
    }

    pub fn leader_hint(&self) -> Option<NodeId> {
        self.status.lock().unwrap().leader_hint.clone()
    }

    pub fn add_peer(&self, peer: NodeId) {
        let _ = self.events_tx.send(NodeEvent::Raft(Event::AddPeer { peer }));
    }

    pub fn remove_peer(&self, peer: NodeId) {
        let _ = self.events_tx.send(NodeEvent::Raft(Event::RemovePeer { peer }));
    }

    pub fn shutdown(&self) {
        let _ = self.events_tx.send(NodeEvent::Shutdown);
    }
}
