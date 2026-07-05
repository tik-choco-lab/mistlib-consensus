//! Platform-agnostic consensus implementations — Raft for trusted P2P
//! meshes ([`mode::ConsensusMode::Trusted`]) and chained HotStuff for
//! untrusted, Byzantine-prone meshes ([`mode::ConsensusMode::Untrusted`])
//! — built on top of
//! `mistlib-core`'s [`AsyncRuntime`](mistlib_core::runtime::AsyncRuntime)
//! and `L1Transport` abstractions. Compiles for native targets and for
//! `wasm32-unknown-unknown`.
//!
//! See the crate README for an architecture overview.

pub mod config;
pub mod error;
pub mod hotstuff;
pub mod message;
pub mod mode;
pub mod node;
pub mod state;
pub mod storage;
pub mod transport;

pub use config::RaftConfig;
pub use error::{ConsensusError, RaftError, Result};
pub use hotstuff::{CommittedCommand, HotStuffNode};
pub use mode::{ConsensusMode, ConsensusNode};
pub use message::{LogEntry, RaftMessage};
pub use node::RaftNode;
pub use state::{Event, Output, RaftState, Role};
pub use storage::{MemoryStorage, RaftStorage};
pub use transport::{MistTransport, RaftTransport};

// Re-exported so downstream crates don't need a direct mistlib-core
// dependency just to name a `NodeId`.
pub use mistlib_core::types::NodeId;
