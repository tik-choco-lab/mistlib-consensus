//! Wire types for the chained HotStuff protocol. See
//! `docs/hotstuff-design.md` for the protocol description.

use mistlib_core::types::NodeId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Monotonically increasing view (round) number. Each view has exactly
/// one designated leader: `validators_sorted[view % n]`.
pub type ViewNumber = u64;

/// SHA-256 digest identifying a [`Block`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlockHash(pub [u8; 32]);

impl BlockHash {
    pub const ZERO: BlockHash = BlockHash([0u8; 32]);
}

impl std::fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0[..4] {
            write!(f, "{b:02x}")?;
        }
        write!(f, "…")
    }
}

/// A single replica's signature over a vote/proposal digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialSig {
    pub signer: NodeId,
    pub signature: Vec<u8>,
}

/// Certificate that a quorum (`n - f`) of distinct validators voted for
/// `block_hash` in `view`. The genesis QC (view 0, genesis hash, no
/// signatures) is valid by definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumCert {
    pub view: ViewNumber,
    pub block_hash: BlockHash,
    pub signatures: Vec<PartialSig>,
}

impl QuorumCert {
    /// The genesis certificate justifying the genesis block.
    pub fn genesis() -> Self {
        Self {
            view: 0,
            block_hash: Block::genesis().hash(),
            signatures: Vec::new(),
        }
    }
}

/// One block in the chain. Exactly one block is proposed per view; the
/// `justify` QC certifies the parent, pipelining the classic HotStuff
/// phases across successive blocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub parent: BlockHash,
    pub view: ViewNumber,
    /// Chain height: `parent.height + 1`; genesis is 0.
    pub height: u64,
    /// Client commands carried by this block. May be empty — empty
    /// blocks keep the commit pipeline moving.
    pub commands: Vec<Vec<u8>>,
    /// QC for the parent block.
    pub justify: QuorumCert,
}

impl Block {
    /// The fixed genesis block every replica starts from.
    pub fn genesis() -> Self {
        Self {
            parent: BlockHash::ZERO,
            view: 0,
            height: 0,
            commands: Vec::new(),
            justify: QuorumCert {
                view: 0,
                block_hash: BlockHash::ZERO,
                signatures: Vec::new(),
            },
        }
    }

    /// Content-addressed hash over block fields and justify view/hash, domain-separated and
    /// length-prefixed. QC signature witnesses do not affect block identity.
    pub fn hash(&self) -> BlockHash {
        let mut h = Sha256::new();
        h.update(b"mistlib-hs-block");
        h.update(self.parent.0);
        h.update(self.view.to_le_bytes());
        h.update(self.height.to_le_bytes());
        h.update((self.commands.len() as u64).to_le_bytes());
        for cmd in &self.commands {
            h.update((cmd.len() as u64).to_le_bytes());
            h.update(cmd);
        }
        h.update(self.justify.view.to_le_bytes());
        h.update(self.justify.block_hash.0);
        BlockHash(h.finalize().into())
    }
}

/// Digest a replica signs when voting for `block_hash` in `view`.
pub fn vote_digest(view: ViewNumber, block_hash: &BlockHash) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"mistlib-hs-vote");
    h.update(view.to_le_bytes());
    h.update(block_hash.0);
    h.finalize().into()
}

/// Digest a leader signs when proposing `block_hash` in `view`.
pub fn proposal_digest(view: ViewNumber, block_hash: &BlockHash) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"mistlib-hs-prop");
    h.update(view.to_le_bytes());
    h.update(block_hash.0);
    h.finalize().into()
}

/// Wire messages exchanged between HotStuff validators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HotStuffMessage {
    /// Leader's block proposal for `block.view`, signed over
    /// [`proposal_digest`].
    Proposal { block: Block, sig: PartialSig },
    /// A replica's vote, signed over [`vote_digest`], sent to the
    /// leader of `view + 1` (who aggregates it into the next QC).
    Vote {
        view: ViewNumber,
        block_hash: BlockHash,
        sig: PartialSig,
    },
    /// Sent to the leader of `view` on timeout so it can propose while
    /// extending the highest QC known to the quorum. Unsigned: the QC
    /// inside self-certifies, and NewView cannot affect safety.
    NewView {
        view: ViewNumber,
        high_qc: QuorumCert,
    },
    /// Fetch a content-addressed block and a bounded chain of ancestors.
    BlockRequest {
        hash: BlockHash,
        request_id: u64,
        max_blocks: u32,
    },
    /// Descending chain, starting at `hash`. Echoes the request token.
    BlockResponse {
        hash: BlockHash,
        request_id: u64,
        blocks: Vec<Block>,
    },
}

impl HotStuffMessage {
    /// Consensus view, or zero for block-sync control messages.
    pub fn view(&self) -> ViewNumber {
        match self {
            HotStuffMessage::Proposal { block, .. } => block.view,
            HotStuffMessage::Vote { view, .. } => *view,
            HotStuffMessage::NewView { view, .. } => *view,
            HotStuffMessage::BlockRequest { .. } | HotStuffMessage::BlockResponse { .. } => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::Options;

    #[test]
    fn sync_wire_tags_and_encoding_are_stable() {
        let request = HotStuffMessage::BlockRequest {
            hash: BlockHash([7; 32]),
            request_id: 9,
            max_blocks: 32,
        };
        let mut expected = 3u32.to_le_bytes().to_vec();
        expected.extend([7; 32]);
        expected.extend(9u64.to_le_bytes());
        expected.extend(32u32.to_le_bytes());
        assert_eq!(bincode::serialize(&request).unwrap(), expected);
        let response = HotStuffMessage::BlockResponse {
            hash: BlockHash([7; 32]),
            request_id: 9,
            blocks: vec![Block::genesis()],
        };
        let bytes = bincode::serialize(&response).unwrap();
        assert_eq!(&bytes[..4], &4u32.to_le_bytes());
        let decoded: HotStuffMessage = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(4 * 1024 * 1024)
            .deserialize(&bytes)
            .unwrap();
        assert_eq!(decoded, response);
    }
}
