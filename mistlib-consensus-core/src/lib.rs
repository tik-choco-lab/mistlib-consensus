//! Platform-agnostic consensus implementations — Raft for trusted P2P
//! meshes ([`mode::ConsensusMode::Trusted`]) and chained HotStuff for
//! untrusted, Byzantine-prone meshes ([`mode::ConsensusMode::Untrusted`])
//! — built on top of
//! `mistlib-core`'s [`AsyncRuntime`](mistlib_core::runtime::AsyncRuntime)
//! and `L1Transport` abstractions. Compiles for native targets and for
//! `wasm32-unknown-unknown`.
//!
//! See `docs/architecture.md` for an architecture overview and
//! `docs/hotstuff-design.md` for the HotStuff protocol design.

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
pub use message::{LogEntry, RaftMessage};
pub use mode::{ConsensusMode, ConsensusNode};
pub use node::{RaftApplied, RaftNode};
pub use state::{Event, Output, RaftState, Role};
pub use storage::{MemoryStorage, RaftMembership, RaftSnapshot, RaftStorage};
pub use transport::{MistTransport, RaftTransport};

// Re-exported so downstream crates don't need a direct mistlib-core
// dependency just to name a `NodeId`.
pub use mistlib_core::types::NodeId;
