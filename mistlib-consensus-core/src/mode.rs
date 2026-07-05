//! Consensus-mode selection: one switch between the two engines this
//! crate ships, chosen by the trust model of the underlying P2P mesh.
//!
//! [`ConsensusNode`] is a thin wrapper that erases the engine choice
//! behind a unified API (`propose`, `subscribe_committed`, `is_leader`,
//! …). Both engines are driven identically once constructed; only the
//! fault model and wire protocol differ:
//!
//! | Mode | Fault model | Engine | Cost |
//! |------|-------------|--------|------|
//! | [`ConsensusMode::Trusted`] | crash faults (CFT) | Raft | cheaper: no signatures, majority quorum, lower latency |
//! | [`ConsensusMode::Untrusted`] | Byzantine faults, `f = (n-1)/3` | chained HotStuff | signed quorum certificates, `n - f` quorum |
//!
//! The wire protocols are incompatible, so **every node in a cluster
//! must be built in the same mode**; there is no runtime negotiation.

use crate::error::Result;
use crate::hotstuff::crypto::CryptoProvider;
use crate::hotstuff::node::HotStuffNode;
use crate::hotstuff::storage::BlockStore;
use crate::node::RaftNode;
use crate::state::Role;
use crate::storage::RaftStorage;
use mistlib_core::runtime::AsyncRuntime;
use mistlib_core::types::NodeId;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

/// The unified committed-command type both engines feed into
/// [`ConsensusNode::subscribe_committed`]. Re-exported from the HotStuff
/// core so callers can name it without reaching into `hotstuff::`.
pub use crate::hotstuff::state::CommittedCommand;

/// Which consensus engine a node runs. The wire protocols are
/// incompatible; every node in a cluster must use the same mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusMode {
    /// Trusted P2P mesh: peers may crash but never lie. Runs Raft —
    /// cheaper (no signatures, majority quorum) and lower latency.
    Trusted,
    /// Untrusted P2P mesh: up to `f = (n - 1) / 3` peers may be
    /// Byzantine. Runs chained HotStuff with signed quorum
    /// certificates.
    Untrusted,
}

/// A consensus node with the engine choice erased. Construct with
/// [`trusted`](Self::trusted) (Raft) or [`untrusted`](Self::untrusted)
/// (HotStuff) and drive both through the same unified methods.
///
/// The three type parameters are the two engines' storage/crypto
/// backends; a given value only ever uses the pair for its variant, but
/// both must be nameable at the type level. In practice the constructor
/// you call pins them by inference.
pub enum ConsensusNode<
    S: RaftStorage + 'static,
    B: BlockStore + 'static,
    C: CryptoProvider + 'static,
> {
    /// [`ConsensusMode::Trusted`] — a running [`RaftNode`].
    Raft(Arc<RaftNode<S>>),
    /// [`ConsensusMode::Untrusted`] — a running [`HotStuffNode`].
    HotStuff(Arc<HotStuffNode<B, C>>),
}

impl<S, B, C> ConsensusNode<S, B, C>
where
    S: RaftStorage + 'static,
    B: BlockStore + 'static,
    C: CryptoProvider + 'static,
{
    /// Wraps a Raft node ([`ConsensusMode::Trusted`]).
    pub fn trusted(node: Arc<RaftNode<S>>) -> Self {
        Self::Raft(node)
    }

    /// Wraps a HotStuff node ([`ConsensusMode::Untrusted`]).
    pub fn untrusted(node: Arc<HotStuffNode<B, C>>) -> Self {
        Self::HotStuff(node)
    }

    /// The mode this node was built in.
    pub fn mode(&self) -> ConsensusMode {
        match self {
            Self::Raft(_) => ConsensusMode::Trusted,
            Self::HotStuff(_) => ConsensusMode::Untrusted,
        }
    }

    /// Proposes a command. Succeeds only on the current leader; both
    /// engines return [`crate::error::RaftError::NotLeader`] otherwise.
    /// The `u64` is the Raft log index / HotStuff view the command was
    /// accepted at, not proof of commit — watch
    /// [`subscribe_committed`](Self::subscribe_committed) for that.
    pub async fn propose(&self, command: Vec<u8>) -> Result<u64> {
        match self {
            Self::Raft(node) => node.propose(command).await,
            Self::HotStuff(node) => node.propose(command).await,
        }
    }

    /// Whether this node currently leads. For Raft this means role
    /// `Leader`; for HotStuff, leadership of the current view.
    pub fn is_leader(&self) -> bool {
        match self {
            Self::Raft(node) => node.role() == Some(Role::Leader),
            Self::HotStuff(node) => node.is_leader(),
        }
    }

    /// The believed current leader, if known.
    pub fn leader_hint(&self) -> Option<NodeId> {
        match self {
            Self::Raft(node) => node.leader_hint(),
            Self::HotStuff(node) => node.leader_hint(),
        }
    }

    pub fn add_peer(&self, peer: NodeId) {
        match self {
            Self::Raft(node) => node.add_peer(peer),
            Self::HotStuff(node) => node.add_peer(peer),
        }
    }

    pub fn remove_peer(&self, peer: NodeId) {
        match self {
            Self::Raft(node) => node.remove_peer(peer),
            Self::HotStuff(node) => node.remove_peer(peer),
        }
    }

    pub fn shutdown(&self) {
        match self {
            Self::Raft(node) => node.shutdown(),
            Self::HotStuff(node) => node.shutdown(),
        }
    }

    /// Subscribes to committed commands as a unified
    /// [`CommittedCommand`] stream, in execution order.
    ///
    /// The two engines expose different native channels — Raft a
    /// `broadcast::Receiver<LogEntry>`, HotStuff a
    /// `broadcast::Receiver<CommittedCommand>` — so this spawns a small
    /// forwarding task on `runtime` that normalizes both into one
    /// `mpsc` receiver. For Raft, `LogEntry { term, index, command }`
    /// maps to `CommittedCommand { sequence: index, view: term,
    /// command }`. An `mpsc` (rather than another broadcast) is returned
    /// because there is a single owned forwarding source per call and no
    /// need to fan out again.
    ///
    /// Lifecycle: the forwarding task terminates as soon as the returned
    /// receiver is dropped (detected via the sender's `closed()`) or the
    /// engine's committed channel closes (e.g. on shutdown), so a dropped
    /// subscriber never leaves a task parked on the next upstream commit.
    pub fn subscribe_committed(
        &self,
        runtime: &Arc<dyn AsyncRuntime>,
    ) -> mpsc::UnboundedReceiver<CommittedCommand> {
        let (tx, rx) = mpsc::unbounded_channel();
        match self {
            Self::Raft(node) => {
                let mut source = node.subscribe_committed();
                runtime.spawn(Box::pin(async move {
                    loop {
                        tokio::select! {
                            // Returned receiver was dropped: stop forwarding
                            // instead of parking on the next upstream commit.
                            _ = tx.closed() => break,
                            recv = source.recv() => match recv {
                                Ok(entry) => {
                                    let cmd = CommittedCommand {
                                        sequence: entry.index,
                                        view: entry.term,
                                        command: entry.command,
                                    };
                                    if tx.send(cmd).is_err() {
                                        break; // consumer dropped
                                    }
                                }
                                // A slow consumer that fell behind the
                                // broadcast ring; skip the gap and continue.
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            },
                        }
                    }
                }));
            }
            Self::HotStuff(node) => {
                let mut source = node.subscribe_committed();
                runtime.spawn(Box::pin(async move {
                    loop {
                        tokio::select! {
                            _ = tx.closed() => break,
                            recv = source.recv() => match recv {
                                Ok(cmd) => {
                                    if tx.send(cmd).is_err() {
                                        break; // consumer dropped
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            },
                        }
                    }
                }));
            }
        }
        rx
    }
}
