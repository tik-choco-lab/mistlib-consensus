use mistlib_core::types::NodeId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("this node is not the leader (leader hint: {leader_hint:?})")]
    NotLeader { leader_hint: Option<NodeId> },

    #[error("node is shutting down")]
    ShuttingDown,

    #[error("storage error: {0}")]
    Storage(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("internal channel closed")]
    ChannelClosed,

    #[error("invalid quorum certificate: {0}")]
    InvalidCertificate(String),

    #[error("invalid block: {0}")]
    InvalidBlock(String),

    #[error("node is not in the validator set")]
    NotValidator,
}

/// Alias for readers coming from the consensus-mode facade; both the
/// Raft and HotStuff engines share this error type.
pub type ConsensusError = RaftError;

pub type Result<T> = std::result::Result<T, RaftError>;
