//! Chained HotStuff BFT consensus for untrusted P2P meshes, tolerating
//! `f = (n - 1) / 3` Byzantine validators. Architecture mirrors the
//! Raft implementation at the crate root: a sans-IO state machine
//! ([`HotStuffState`]) driven by an async node ([`HotStuffNode`]) over a
//! pluggable transport. See `docs/hotstuff-design.md`.

pub mod crypto;
pub mod message;
pub mod node;
pub mod state;
pub mod storage;
pub mod transport;

pub use crypto::{CryptoProvider, Ed25519Crypto, NoopCrypto};
pub use message::{Block, BlockHash, HotStuffMessage, PartialSig, QuorumCert, ViewNumber};
pub use node::HotStuffNode;
pub use state::{
    CommittedCommand, Event, HotStuffState, Output, PacemakerConfig, SyncConfig,
    MAX_BLOCKS_PER_RESPONSE, MAX_BLOCK_RESPONSE_BYTES,
};
pub use storage::{BlockStore, MemoryBlockStore};
pub use transport::{HotStuffMistTransport, HotStuffTransport};
