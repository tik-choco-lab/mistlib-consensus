//! Persistence for HotStuff. Mirrors the sync-trait style of
//! [`crate::storage::RaftStorage`].

use super::message::{Block, BlockHash, QuorumCert, ViewNumber};
use mistlib_core::types::HostSendSync;
use std::collections::HashMap;

/// Block tree plus the safety-critical replica fields. Implementations
/// must make `set_last_voted_view` / `set_locked_qc` durable before the
/// corresponding vote is sent — a replica that forgets either can be
/// tricked into equivocating after a restart. `MemoryBlockStore` is for
/// tests/single-process use and does not persist anything.
pub trait BlockStore: HostSendSync {
    fn put_block(&mut self, block: Block);
    fn get_block(&self, hash: &BlockHash) -> Option<&Block>;
    fn contains_block(&self, hash: &BlockHash) -> bool {
        self.get_block(hash).is_some()
    }

    fn last_voted_view(&self) -> ViewNumber;
    fn set_last_voted_view(&mut self, view: ViewNumber);

    fn locked_qc(&self) -> QuorumCert;
    fn set_locked_qc(&mut self, qc: QuorumCert);

    fn high_qc(&self) -> QuorumCert;
    fn set_high_qc(&mut self, qc: QuorumCert);
}

/// In-memory [`BlockStore`] seeded with the genesis block and QC.
#[derive(Debug)]
pub struct MemoryBlockStore {
    blocks: HashMap<BlockHash, Block>,
    last_voted_view: ViewNumber,
    locked_qc: QuorumCert,
    high_qc: QuorumCert,
}

impl MemoryBlockStore {
    pub fn new() -> Self {
        let genesis = Block::genesis();
        let mut blocks = HashMap::new();
        blocks.insert(genesis.hash(), genesis);
        Self {
            blocks,
            last_voted_view: 0,
            locked_qc: QuorumCert::genesis(),
            high_qc: QuorumCert::genesis(),
        }
    }
}

impl Default for MemoryBlockStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockStore for MemoryBlockStore {
    fn put_block(&mut self, block: Block) {
        self.blocks.insert(block.hash(), block);
    }

    fn get_block(&self, hash: &BlockHash) -> Option<&Block> {
        self.blocks.get(hash)
    }

    fn last_voted_view(&self) -> ViewNumber {
        self.last_voted_view
    }

    fn set_last_voted_view(&mut self, view: ViewNumber) {
        self.last_voted_view = view;
    }

    fn locked_qc(&self) -> QuorumCert {
        self.locked_qc.clone()
    }

    fn set_locked_qc(&mut self, qc: QuorumCert) {
        self.locked_qc = qc;
    }

    fn high_qc(&self) -> QuorumCert {
        self.high_qc.clone()
    }

    fn set_high_qc(&mut self, qc: QuorumCert) {
        self.high_qc = qc;
    }
}
