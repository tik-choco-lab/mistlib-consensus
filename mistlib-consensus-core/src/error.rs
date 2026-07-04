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
}

pub type Result<T> = std::result::Result<T, RaftError>;
