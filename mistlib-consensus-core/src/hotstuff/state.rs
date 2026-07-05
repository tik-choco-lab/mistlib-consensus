//! Pure, synchronous chained-HotStuff core. Like [`crate::state`], this
//! module never touches async, I/O, or wall-clock time — it reacts to
//! [`Event`]s and produces [`Output`]s. Protocol rules are specified in
//! `docs/hotstuff-design.md`.

use super::crypto::CryptoProvider;
use super::message::{
    proposal_digest, vote_digest, Block, BlockHash, HotStuffMessage, PartialSig, QuorumCert,
    ViewNumber,
};
use super::storage::BlockStore;
use mistlib_core::types::NodeId;
use std::collections::{HashMap, HashSet};

/// Timing knobs for the pacemaker (driven by the node, decided here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacemakerConfig {
    /// Base view timeout. Doubles per consecutive timeout (exponential
    /// backoff) until a QC advances the view again.
    pub base_timeout_ms: u64,
    /// Upper bound for the backed-off timeout.
    pub max_timeout_ms: u64,
}

impl Default for PacemakerConfig {
    fn default() -> Self {
        Self { base_timeout_ms: 400, max_timeout_ms: 8_000 }
    }
}

/// Inputs the driver feeds into the state machine.
#[derive(Debug, Clone)]
pub enum Event {
    Message { from: NodeId, msg: HotStuffMessage },
    ViewTimeout,
    ClientPropose { command: Vec<u8> },
    AddPeer { peer: NodeId },
    RemovePeer { peer: NodeId },
}

/// A command that reached the 3-chain commit point, in execution order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedCommand {
    /// Global, gapless execution sequence starting at 1.
    pub sequence: u64,
    /// View of the block that carried the command.
    pub view: ViewNumber,
    pub command: Vec<u8>,
}

/// Effects the driver must carry out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    SendMessage { to: NodeId, msg: HotStuffMessage },
    /// Send to every other validator (the state machine has already
    /// processed the message locally where relevant).
    Broadcast { msg: HotStuffMessage },
    ApplyCommitted { commands: Vec<CommittedCommand> },
    BecameLeader,
    BecameFollower,
    /// (Re)arm the single view timer with this duration.
    ResetViewTimer { duration_ms: u64 },
    /// Command accepted by this leader; it will ride in a block at or
    /// after `view`. Completion is observed via `ApplyCommitted`.
    Proposed { view: ViewNumber },
    ProposeRejected { leader_hint: Option<NodeId> },
}

/// Chained HotStuff replica state. `B` persists the block tree and the
/// safety-critical fields; `C` signs votes/proposals.
pub struct HotStuffState<B: BlockStore, C: CryptoProvider> {
    pub id: NodeId,
    /// Validator set (self included), kept sorted by the inner string
    /// and deduplicated; leader of view `v` is the `v % n`-th element.
    validators: Vec<NodeId>,
    store: B,
    crypto: C,
    config: PacemakerConfig,

    pub current_view: ViewNumber,
    /// Consecutive timeouts since the last QC-driven view advance.
    consecutive_timeouts: u32,
    /// Highest view this node proposed in (at most one proposal/view).
    last_proposed_view: ViewNumber,
    /// Commands queued while leading (drained into the next proposal).
    pending_commands: Vec<Vec<u8>>,
    /// Votes gathered while this node is the leader that will certify a
    /// block, keyed by the `(view, block_hash)` being voted on. Each inner
    /// map dedups by signer so a Byzantine double-vote can't inflate the
    /// tally; the stored bytes are the verified signatures later assembled
    /// into the resulting [`QuorumCert`].
    votes: HashMap<(ViewNumber, BlockHash), HashMap<NodeId, Vec<u8>>>,
    /// Distinct `NewView` senders per view, used on the timeout path to
    /// decide when this leader may propose (needs a quorum of them).
    new_views: HashMap<ViewNumber, HashSet<NodeId>>,
    /// Whether this node led the view it most recently entered, so
    /// `BecameLeader`/`BecameFollower` fire only on genuine transitions.
    was_leader: bool,
    /// Re-entrancy guard: a proposal is replayed locally as a replica,
    /// which can synchronously assemble a QC that wants to propose again.
    /// This bounds that to one self-proposal per driving event (otherwise
    /// a single-node cluster, always its own next leader, would loop).
    in_propose: bool,
    /// Gapless execution sequence of the last committed command.
    exec_sequence: u64,
    /// Height of the last committed block.
    committed_height: u64,
}

impl<B: BlockStore, C: CryptoProvider> HotStuffState<B, C> {
    pub fn new(
        id: NodeId,
        validators: Vec<NodeId>,
        store: B,
        crypto: C,
        config: PacemakerConfig,
    ) -> Self {
        let mut validators = validators;
        validators.push(id.clone());
        validators.sort_by(|a, b| a.0.cmp(&b.0));
        validators.dedup();
        Self {
            id,
            validators,
            store,
            crypto,
            config,
            current_view: 0,
            consecutive_timeouts: 0,
            last_proposed_view: 0,
            pending_commands: Vec::new(),
            votes: HashMap::new(),
            new_views: HashMap::new(),
            was_leader: false,
            in_propose: false,
            exec_sequence: 0,
            committed_height: 0,
        }
    }

    /// Called exactly once by the driver before any `step`. Enters view
    /// 1, arms the first view timer, and proposes if this node leads
    /// view 1 (the genesis QC drives the entry, so it is a happy-path
    /// proposal extending the genesis block).
    pub fn bootstrap(&mut self) -> Vec<Output> {
        let mut out = Vec::new();
        if self.enter_view(1, true, &mut out) {
            self.propose(&mut out);
        }
        out
    }

    /// Advances the state machine by one event; pure and deterministic.
    pub fn step(&mut self, event: Event) -> Vec<Output> {
        let mut out = Vec::new();
        match event {
            Event::Message { from, msg } => match msg {
                HotStuffMessage::Proposal { block, sig } => {
                    self.handle_proposal(from, block, sig, &mut out)
                }
                HotStuffMessage::Vote { view, block_hash, sig } => {
                    self.handle_vote(view, block_hash, sig, &mut out)
                }
                HotStuffMessage::NewView { view, high_qc } => {
                    self.handle_new_view(from, view, high_qc, &mut out)
                }
            },
            Event::ViewTimeout => self.handle_timeout(&mut out),
            Event::ClientPropose { command } => {
                if self.is_leader() {
                    // The command rides the block for `view`; if we already
                    // proposed this view it queues for the next block we
                    // lead, but we still acknowledge it synchronously.
                    let view = self.current_view;
                    self.pending_commands.push(command);
                    if self.last_proposed_view < view {
                        self.propose(&mut out);
                    }
                    out.push(Output::Proposed { view });
                } else {
                    out.push(Output::ProposeRejected {
                        leader_hint: self.current_leader(),
                    });
                }
            }
            // Membership changes are operator-trusted: BFT reconfiguration
            // is out of scope for v1, so we simply resize the validator set
            // (keeping it sorted + deduped). `quorum()` tracks `n`, so the
            // fault threshold moves at the next `step`.
            Event::AddPeer { peer } => {
                if !self.validators.iter().any(|v| v == &peer) {
                    self.validators.push(peer);
                    self.validators.sort_by(|a, b| a.0.cmp(&b.0));
                    self.validators.dedup();
                }
            }
            Event::RemovePeer { peer } => {
                // Operator-trusted, but never let a request strand consensus:
                // refuse to remove ourselves or to empty the validator set.
                if peer == self.id {
                    tracing::warn!("hotstuff: refusing to remove self from the validator set");
                } else if self.validators.len() <= 1 {
                    tracing::warn!("hotstuff: refusing to shrink the validator set below one");
                } else {
                    self.validators.retain(|v| v != &peer);
                }
            }
        }
        out
    }

    // ----- private protocol helpers -----

    /// Enter `new_view` if it is strictly ahead of the current view.
    /// Returns whether the view actually advanced. Emits the pacemaker
    /// timer reset and any leader/follower transition. `reset_backoff` is
    /// set on QC-driven advances (healthy pipeline) and cleared on the
    /// timeout path so the exponential backoff keeps growing.
    fn enter_view(&mut self, new_view: ViewNumber, reset_backoff: bool, out: &mut Vec<Output>) -> bool {
        if new_view <= self.current_view {
            return false;
        }
        self.current_view = new_view;
        if reset_backoff {
            self.consecutive_timeouts = 0;
        }
        // Evict aggregation state that can no longer influence a proposal,
        // so a Byzantine peer cannot make these maps grow without bound by
        // flooding validly-formed votes/NewViews. A vote for block-view `v`
        // only helps the leader of `v + 1` propose, so it is dead once we
        // are past `v + 1`; a NewView for view `V` only helps us propose in
        // `V`, dead once we are past `V`. Future-view entries are retained —
        // we may lead those views later.
        let cv = self.current_view;
        self.votes.retain(|&(v, _), _| v + 1 >= cv);
        self.new_views.retain(|&v, _| v >= cv);
        out.push(Output::ResetViewTimer { duration_ms: self.timer_duration() });
        let now_leader = self.is_leader();
        if now_leader && !self.was_leader {
            out.push(Output::BecameLeader);
        } else if !now_leader && self.was_leader {
            out.push(Output::BecameFollower);
        }
        self.was_leader = now_leader;
        true
    }

    /// Exponential backoff duration: `base * 2^consecutive_timeouts`,
    /// saturating and capped at `max_timeout_ms`.
    fn timer_duration(&self) -> u64 {
        let factor = 1u64.checked_shl(self.consecutive_timeouts).unwrap_or(u64::MAX);
        self.config
            .base_timeout_ms
            .saturating_mul(factor)
            .min(self.config.max_timeout_ms)
    }

    /// Leader-only, once per view: build a block extending `high_qc`,
    /// draining the pending command queue into it (empty is fine and
    /// required for pipeline progress), then process it locally as a
    /// replica before broadcasting it to the other validators.
    fn propose(&mut self, out: &mut Vec<Output>) {
        if self.in_propose || !self.is_leader() || self.last_proposed_view >= self.current_view {
            return;
        }
        let high_qc = self.store.high_qc();
        let parent_height = match self.store.get_block(&high_qc.block_hash) {
            Some(p) => p.height,
            // Parent missing (no block sync in v1): we cannot extend it.
            None => return,
        };
        let commands = std::mem::take(&mut self.pending_commands);
        let block = Block {
            parent: high_qc.block_hash,
            view: self.current_view,
            height: parent_height + 1,
            commands,
            justify: high_qc,
        };
        let digest = proposal_digest(block.view, &block.hash());
        let sig = PartialSig { signer: self.id.clone(), signature: self.crypto.sign(&digest) };
        // Mark proposed before the local replica pass, which votes and
        // advances our view and would otherwise re-enter propose().
        self.last_proposed_view = self.current_view;
        self.in_propose = true;
        self.handle_proposal(self.id.clone(), block.clone(), sig.clone(), out);
        self.in_propose = false;
        out.push(Output::Broadcast { msg: HotStuffMessage::Proposal { block, sig } });
    }

    /// Validate a proposal and, if the SafeNode rule allows, vote on it.
    /// Invalid or unverifiable proposals are silently ignored (logged at
    /// debug). If the parent block is unknown we drop the proposal — there
    /// is no block-sync protocol in v1, so we cannot fetch it.
    fn handle_proposal(&mut self, from: NodeId, block: Block, sig: PartialSig, out: &mut Vec<Output>) {
        let hash = block.hash();
        if self.leader_of(block.view).as_ref() != Some(&from) {
            tracing::debug!(view = block.view, "hotstuff: proposal from non-leader, ignoring");
            return;
        }
        if block.view != self.current_view {
            tracing::debug!(
                view = block.view,
                current = self.current_view,
                "hotstuff: proposal for another view, ignoring"
            );
            return;
        }
        if sig.signer != from
            || !self.crypto.verify(&from, &proposal_digest(block.view, &hash), &sig.signature)
        {
            tracing::debug!("hotstuff: bad proposal signature, ignoring");
            return;
        }
        if !self.verify_qc(&block.justify) {
            tracing::debug!("hotstuff: proposal carries an invalid justify QC, ignoring");
            return;
        }
        if block.parent != block.justify.block_hash {
            tracing::debug!("hotstuff: proposal parent does not match its justify, ignoring");
            return;
        }
        let parent_height = match self.store.get_block(&block.parent) {
            Some(p) => p.height,
            None => {
                tracing::debug!("hotstuff: proposal parent unknown, ignoring (no sync in v1)");
                return;
            }
        };
        if block.height != parent_height + 1 {
            tracing::debug!("hotstuff: proposal height is not parent + 1, ignoring");
            return;
        }

        // Fold the justify QC into our state (may lock/commit; never
        // advances past the current view since it certifies an ancestor).
        self.handle_qc(&block.justify, out);

        // SafeNode: vote iff the view is fresh and the block either extends
        // our locked block or carries a justify newer than our lock.
        let locked = self.store.locked_qc();
        let safe = block.view > self.store.last_voted_view()
            && (self.extends(&block, &locked.block_hash) || block.justify.view > locked.view);
        if !safe {
            return;
        }

        // Persist the safety-critical vote before emitting it, then store
        // the block so we can extend and later commit it.
        self.store.set_last_voted_view(block.view);
        self.store.put_block(block.clone());
        let vote_view = block.view;
        let vote_sig = PartialSig {
            signer: self.id.clone(),
            signature: self.crypto.sign(&vote_digest(vote_view, &hash)),
        };

        // Voting is progress: advance the view (reset the backoff).
        self.enter_view(vote_view + 1, true, out);

        // Route the vote to the next leader; a self-addressed vote is
        // collected internally rather than emitted.
        match self.leader_of(vote_view + 1) {
            Some(leader) if leader == self.id => {
                self.collect_vote(self.id.clone(), vote_view, hash, vote_sig.signature, out);
            }
            Some(leader) => out.push(Output::SendMessage {
                to: leader,
                msg: HotStuffMessage::Vote { view: vote_view, block_hash: hash, sig: vote_sig },
            }),
            None => {}
        }
    }

    fn handle_vote(&mut self, view: ViewNumber, block_hash: BlockHash, sig: PartialSig, out: &mut Vec<Output>) {
        // The signature (not the transport sender) identifies the voter.
        self.collect_vote(sig.signer.clone(), view, block_hash, sig.signature, out);
    }

    /// Aggregate a vote we are responsible for (leader of `view + 1`).
    /// Signatures are verified before counting and deduped by signer so a
    /// Byzantine double-vote cannot inflate the tally. On reaching a quorum
    /// we assemble the QC (with exactly the verified sigs) and fold it in,
    /// which advances us into `view + 1` and proposes if we lead it.
    fn collect_vote(&mut self, signer: NodeId, view: ViewNumber, block_hash: BlockHash, sig: Vec<u8>, out: &mut Vec<Output>) {
        if self.leader_of(view + 1).as_ref() != Some(&self.id) || !self.is_validator(&signer) {
            return;
        }
        if !self.crypto.verify(&signer, &vote_digest(view, &block_hash), &sig) {
            tracing::debug!("hotstuff: vote with invalid signature, not counting");
            return;
        }
        let quorum = self.quorum();
        let tally = self.votes.entry((view, block_hash)).or_default();
        tally.insert(signer, sig);
        if tally.len() != quorum {
            return;
        }
        let mut signatures: Vec<PartialSig> = tally
            .iter()
            .map(|(s, sg)| PartialSig { signer: s.clone(), signature: sg.clone() })
            .collect();
        signatures.sort_by(|a, b| a.signer.0.cmp(&b.signer.0));
        let qc = QuorumCert { view, block_hash, signatures };
        self.handle_qc(&qc, out);
    }

    /// Timeout-path liveness: fold the sender's high QC into our state,
    /// then (if we lead `view` and it is not stale) count the distinct
    /// senders; a quorum lets us propose extending the highest QC seen.
    fn handle_new_view(&mut self, from: NodeId, view: ViewNumber, high_qc: QuorumCert, out: &mut Vec<Output>) {
        // The carried QC self-certifies the NewView; an unverifiable one
        // must not count toward the recovery quorum, or a Byzantine peer
        // could pad it for free with garbage QCs.
        if !self.verify_qc(&high_qc) {
            tracing::debug!(view, "hotstuff: NewView with an invalid high QC, ignoring");
            return;
        }
        self.handle_qc(&high_qc, out);
        self.record_new_view(view, from, out);
    }

    fn record_new_view(&mut self, view: ViewNumber, sender: NodeId, out: &mut Vec<Output>) {
        if self.leader_of(view).as_ref() != Some(&self.id)
            || view < self.current_view
            || !self.is_validator(&sender)
        {
            return;
        }
        let quorum = self.quorum();
        let senders = self.new_views.entry(view).or_default();
        senders.insert(sender);
        if senders.len() != quorum {
            return;
        }
        // Enter the view on the timeout path (backoff not reset) and
        // propose; propose() self-guards against a second proposal.
        self.enter_view(view, false, out);
        self.propose(out);
    }

    /// Pacemaker fired: abandon the current view, move to the next, and ask
    /// its leader to propose by sending a NewView carrying our high QC.
    fn handle_timeout(&mut self, out: &mut Vec<Output>) {
        self.consecutive_timeouts += 1;
        let new_view = self.current_view + 1;
        self.enter_view(new_view, false, out);
        let high_qc = self.store.high_qc();
        match self.leader_of(new_view) {
            // We lead the recovery view: count ourselves toward the NewView
            // quorum rather than messaging ourselves.
            Some(leader) if leader == self.id => {
                self.record_new_view(new_view, self.id.clone(), out);
            }
            Some(leader) => out.push(Output::SendMessage {
                to: leader,
                msg: HotStuffMessage::NewView { view: new_view, high_qc },
            }),
            None => {}
        }
    }

    /// The "On QC" rule set: track the highest QC, apply the 2-chain lock,
    /// apply the 3-chain (consecutive-view) commit, then advance the view.
    /// The QC is assumed already validated by the caller.
    fn handle_qc(&mut self, qc: &QuorumCert, out: &mut Vec<Output>) {
        // A QC that advances our high QC gives the leader a fresh block to
        // extend — that (not merely advancing the view, which a vote also
        // does) is what admits an immediate happy-path proposal.
        let fresher = qc.view > self.store.high_qc().view;
        if fresher {
            self.store.set_high_qc(qc.clone());
        }
        // The chain rules need the certified block (b'') and its ancestors
        // to be known locally; if any is missing we simply skip them.
        if let Some(b2) = self.store.get_block(&qc.block_hash).cloned() {
            if let Some(b1) = self.store.get_block(&b2.justify.block_hash).cloned() {
                if b1.view > self.store.locked_qc().view {
                    self.store.set_locked_qc(b2.justify.clone());
                }
                if let Some(b0) = self.store.get_block(&b1.justify.block_hash).cloned() {
                    let chained = b2.parent == b1.hash() && b1.parent == b0.hash();
                    let consecutive = b2.view == b1.view + 1 && b1.view == b0.view + 1;
                    if chained && consecutive {
                        self.commit_block(&b0, out);
                    }
                }
            }
        }
        // Learning a QC for `qc.view` admits entering `qc.view + 1`.
        self.enter_view(qc.view + 1, true, out);
        if fresher {
            self.propose(out);
        }
    }

    /// Commit `block` and every still-uncommitted ancestor, oldest first,
    /// assigning gapless execution sequences starting at 1. A height is
    /// never committed twice (guarded by `committed_height`).
    fn commit_block(&mut self, block: &Block, out: &mut Vec<Output>) {
        if block.height <= self.committed_height {
            return;
        }
        let mut chain = Vec::new();
        let mut cursor = Some(block.clone());
        while let Some(b) = cursor {
            if b.height <= self.committed_height {
                break;
            }
            cursor = self.store.get_block(&b.parent).cloned();
            chain.push(b);
        }
        chain.reverse(); // oldest (lowest height) first
        let mut commands = Vec::new();
        for b in &chain {
            for cmd in &b.commands {
                self.exec_sequence += 1;
                commands.push(CommittedCommand {
                    sequence: self.exec_sequence,
                    view: b.view,
                    command: cmd.clone(),
                });
            }
        }
        self.committed_height = block.height;
        if !commands.is_empty() {
            out.push(Output::ApplyCommitted { commands });
        }
    }

    /// A QC is valid iff it is the genesis QC, or it carries signatures
    /// from `quorum` distinct validators that each verify over the vote
    /// digest. Any signature that fails verification voids the whole QC.
    fn verify_qc(&self, qc: &QuorumCert) -> bool {
        if qc.view == 0 && qc.block_hash == Block::genesis().hash() {
            return true;
        }
        let digest = vote_digest(qc.view, &qc.block_hash);
        let mut distinct = HashSet::new();
        for ps in &qc.signatures {
            if !self.is_validator(&ps.signer) {
                return false;
            }
            if !self.crypto.verify(&ps.signer, &digest, &ps.signature) {
                return false;
            }
            distinct.insert(ps.signer.clone());
        }
        distinct.len() >= self.quorum()
    }

    /// Whether `block` extends the chain rooted at `ancestor` (i.e.
    /// `ancestor` is `block`'s parent or a transitive ancestor).
    fn extends(&self, block: &Block, ancestor: &BlockHash) -> bool {
        let mut cursor = block.parent;
        loop {
            if cursor == *ancestor {
                return true;
            }
            match self.store.get_block(&cursor) {
                Some(b) if b.parent != cursor => cursor = b.parent,
                _ => return false,
            }
        }
    }

    fn is_validator(&self, id: &NodeId) -> bool {
        self.validators.iter().any(|v| v == id)
    }

    // ----- read-side accessors used by the driver and tests -----

    pub fn quorum(&self) -> usize {
        let n = self.validators.len();
        if n == 0 {
            return 0; // defensive: `new` always seeds self, but never panic
        }
        n - (n - 1) / 3
    }

    /// Leader of `view`: the `view % n`-th validator in sorted order.
    pub fn leader_of(&self, view: ViewNumber) -> Option<NodeId> {
        let n = self.validators.len() as u64;
        if n == 0 {
            return None;
        }
        self.validators.get((view % n) as usize).cloned()
    }

    pub fn is_leader(&self) -> bool {
        self.leader_of(self.current_view).as_ref() == Some(&self.id)
    }

    pub fn current_leader(&self) -> Option<NodeId> {
        self.leader_of(self.current_view)
    }

    pub fn committed_height(&self) -> u64 {
        self.committed_height
    }

    pub fn high_qc(&self) -> QuorumCert {
        self.store.high_qc()
    }

    pub fn locked_qc(&self) -> QuorumCert {
        self.store.locked_qc()
    }

    pub fn validators(&self) -> impl Iterator<Item = &NodeId> {
        self.validators.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hotstuff::crypto::{Ed25519Crypto, NoopCrypto};
    use crate::hotstuff::storage::MemoryBlockStore;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    fn nid(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    fn ids(names: &[&str]) -> Vec<NodeId> {
        names.iter().map(|s| nid(s)).collect()
    }

    /// A node over the full validator set `all` (self included; `new`
    /// re-adds self, sorts and dedups so passing the whole set is fine).
    fn node(id: &str, all: &[&str]) -> HotStuffState<MemoryBlockStore, NoopCrypto> {
        HotStuffState::new(
            nid(id),
            ids(all),
            MemoryBlockStore::new(),
            NoopCrypto,
            PacemakerConfig::default(),
        )
    }

    // ----- helpers to craft protocol messages for single-node drive-through -----

    /// A QC over `block` signed by `signers` (NoopCrypto: bytes ignored).
    fn qc(block: &Block, signers: &[&str]) -> QuorumCert {
        QuorumCert {
            view: block.view,
            block_hash: block.hash(),
            signatures: signers
                .iter()
                .map(|s| PartialSig { signer: nid(s), signature: Vec::new() })
                .collect(),
        }
    }

    /// A child block extending `parent` at `view` with `justify`.
    fn child(parent: &Block, view: ViewNumber, commands: Vec<Vec<u8>>, justify: QuorumCert) -> Block {
        Block { parent: parent.hash(), view, height: parent.height + 1, commands, justify }
    }

    fn proposal_msg(block: &Block, leader: &str) -> HotStuffMessage {
        HotStuffMessage::Proposal {
            block: block.clone(),
            sig: PartialSig { signer: nid(leader), signature: Vec::new() },
        }
    }

    fn deliver(
        n: &mut HotStuffState<MemoryBlockStore, NoopCrypto>,
        from: &str,
        msg: HotStuffMessage,
    ) -> Vec<Output> {
        n.step(Event::Message { from: nid(from), msg })
    }

    fn committed(outs: &[Output]) -> Vec<CommittedCommand> {
        outs.iter()
            .flat_map(|o| match o {
                Output::ApplyCommitted { commands } => commands.clone(),
                _ => Vec::new(),
            })
            .collect()
    }

    /// Test 1 — four... (five) validators, happy path. We drive a single
    /// follower `a` through a valid 3-chain of proposals (b1..b4 by the
    /// rotating leaders b,c,d,e) and assert the command carried by the
    /// first block commits with execution sequence 1 once b4's justify
    /// (a QC over b3) completes the 3-chain b1<-b2<-b3.
    #[test]
    fn happy_path_three_chain_commits_first_command_with_sequence_one() {
        let all = ["a", "b", "c", "d", "e"];
        let mut a = node("a", &all); // a leads none of views 1..4
        a.bootstrap();
        assert_eq!(a.current_view, 1);

        let genesis = Block::genesis();
        let b1 = child(&genesis, 1, vec![b"cmd".to_vec()], QuorumCert::genesis());
        let b2 = child(&b1, 2, vec![], qc(&b1, &["b", "c", "d", "e"]));
        let b3 = child(&b2, 3, vec![], qc(&b2, &["b", "c", "d", "e"]));
        let b4 = child(&b3, 4, vec![], qc(&b3, &["b", "c", "d", "e"]));

        // Each proposal is accepted, voted, and advances a's view.
        let _ = deliver(&mut a, "b", proposal_msg(&b1, "b"));
        assert_eq!(a.current_view, 2);
        let _ = deliver(&mut a, "c", proposal_msg(&b2, "c"));
        assert_eq!(a.current_view, 3);
        let _ = deliver(&mut a, "d", proposal_msg(&b3, "d"));
        assert_eq!(a.current_view, 4);
        assert_eq!(a.committed_height(), 0, "no 3-chain closed yet");

        // b4.justify certifies b3; folding it closes the 3-chain and
        // commits b1 (the oldest, command-bearing block).
        let out = deliver(&mut a, "e", proposal_msg(&b4, "e"));
        let applied = committed(&out);
        assert_eq!(
            applied,
            vec![CommittedCommand { sequence: 1, view: 1, command: b"cmd".to_vec() }]
        );
        assert_eq!(a.committed_height(), 1);
    }

    /// Test 2 — a single validator is its own quorum. bootstrap proposes
    /// the first (empty) block; each ClientPropose then proposes the next
    /// block carrying that command. A command committed with sequence 1
    /// proves the node commits its own chain (b_cmd needs two more blocks
    /// on top to close its 3-chain).
    #[test]
    fn single_validator_commits_its_own_chain() {
        let mut n = node("solo", &["solo"]);
        assert_eq!(n.quorum(), 1);
        let boot = n.bootstrap();
        assert!(boot.contains(&Output::BecameLeader));
        assert_eq!(n.current_view, 2, "self-vote on b1 formed QC and advanced");

        // b2 carries the command; b3 and b4 pile on to close b2's 3-chain.
        let out = n.step(Event::ClientPropose { command: b"pay".to_vec() });
        assert!(out.contains(&Output::Proposed { view: 2 }));
        n.step(Event::ClientPropose { command: b"noop1".to_vec() });
        let out = n.step(Event::ClientPropose { command: b"noop2".to_vec() });

        let applied = committed(&out);
        assert_eq!(
            applied,
            vec![CommittedCommand { sequence: 1, view: 2, command: b"pay".to_vec() }],
            "first real command commits with sequence 1"
        );
        assert_eq!(n.committed_height(), 2);
    }

    fn is_vote(out: &[Output]) -> bool {
        out.iter().any(|o| {
            matches!(o, Output::SendMessage { msg: HotStuffMessage::Vote { .. }, .. })
        })
    }

    /// Test 3a — the SafeNode view rule: a proposal whose view is not
    /// strictly greater than `last_voted_view` must not be voted on, even
    /// if it is otherwise valid (models a node restarted after voting in a
    /// higher view — the persisted `last_voted_view` forbids equivocation).
    #[test]
    fn stale_view_proposal_is_not_voted() {
        let all = ["a", "b", "c", "d", "e"];
        let mut a = node("a", &all);
        a.bootstrap();
        // Pretend we already voted through view 5 in a previous life.
        a.store.set_last_voted_view(5);

        let genesis = Block::genesis();
        let b1 = child(&genesis, 1, vec![], QuorumCert::genesis());
        let out = deliver(&mut a, "b", proposal_msg(&b1, "b"));

        assert!(!is_vote(&out), "must not vote when block.view <= last_voted_view");
        assert_eq!(a.store.last_voted_view(), 5, "vote record unchanged");
        assert_eq!(a.current_view, 1, "no vote means no view advance");
    }

    /// Test 3b — the SafeNode lock rule: after locking on b1 (via a 2-chain)
    /// a proposal that neither extends the locked block nor carries a
    /// justify newer than the lock must be refused.
    #[test]
    fn lock_conflicting_proposal_is_not_voted() {
        let all = ["a", "b", "c", "d", "e"];
        let mut a = node("a", &all);
        a.bootstrap();

        let genesis = Block::genesis();
        let b1 = child(&genesis, 1, vec![], QuorumCert::genesis());
        let b2 = child(&b1, 2, vec![], qc(&b1, &["b", "c", "d", "e"]));
        let b3 = child(&b2, 3, vec![], qc(&b2, &["b", "c", "d", "e"]));
        deliver(&mut a, "b", proposal_msg(&b1, "b"));
        deliver(&mut a, "c", proposal_msg(&b2, "c")); // locks nothing yet
        deliver(&mut a, "d", proposal_msg(&b3, "d")); // folding QC(b2) locks b1
        assert_eq!(a.locked_qc().block_hash, b1.hash(), "locked on b1");
        assert_eq!(a.current_view, 4);

        // A fork off genesis at view 4: does not extend b1, and its justify
        // (genesis, view 0) is not newer than the lock (view 1).
        let fork = child(&genesis, 4, vec![], QuorumCert::genesis());
        let out = deliver(&mut a, "e", proposal_msg(&fork, "e"));
        assert!(!is_vote(&out), "must not vote on a lock-conflicting proposal");
        assert_eq!(a.store.last_voted_view(), 3, "vote record unchanged");
    }

    /// Test 4a — QC assembly counts DISTINCT signers. A repeated signer
    /// cannot push the tally to quorum; only a genuinely new signer does,
    /// at which point the leader assembles the QC and proposes.
    #[test]
    fn duplicate_signer_does_not_reach_quorum() {
        let all = ["a", "b", "c", "d"]; // quorum = 3; c leads view 2
        let mut c = node("c", &all);
        c.bootstrap();

        let genesis = Block::genesis();
        let b1 = child(&genesis, 1, vec![], QuorumCert::genesis());
        // c votes b1 and, as leader of view 2, collects its own vote.
        deliver(&mut c, "b", proposal_msg(&b1, "b"));
        assert_eq!(c.current_view, 2);

        let vote_from = |s: &str| HotStuffMessage::Vote {
            view: 1,
            block_hash: b1.hash(),
            sig: PartialSig { signer: nid(s), signature: Vec::new() },
        };

        // Two votes from the SAME signer: distinct tally is {c, a} = 2 < 3.
        let out1 = deliver(&mut c, "a", vote_from("a"));
        let out2 = deliver(&mut c, "a", vote_from("a"));
        assert!(!has_broadcast(&out1) && !has_broadcast(&out2), "no QC from a duplicate");

        // A genuinely distinct third signer reaches quorum -> propose b2.
        let out3 = deliver(&mut c, "d", vote_from("d"));
        assert!(has_broadcast(&out3), "distinct quorum assembles the QC and proposes");
    }

    fn has_broadcast(out: &[Output]) -> bool {
        out.iter().any(|o| matches!(o, Output::Broadcast { .. }))
    }

    /// Test 4b — with real Ed25519, a vote signed by the wrong key is
    /// rejected (not counted), so the forged signer cannot help reach
    /// quorum; a genuine signature from the same node does.
    #[test]
    fn forged_signature_is_rejected_with_real_crypto() {
        let names = ["a", "b", "c", "d"]; // quorum = 3; c leads view 2
        let sks: Vec<SigningKey> = (0..names.len())
            .map(|_| SigningKey::generate(&mut OsRng))
            .collect();
        let registry: HashMap<NodeId, _> = names
            .iter()
            .zip(&sks)
            .map(|(n, k)| (nid(n), k.verifying_key()))
            .collect();
        let crypto = Ed25519Crypto::new(sks[2].clone(), registry);
        let mut c = HotStuffState::new(
            nid("c"),
            ids(&names),
            MemoryBlockStore::new(),
            crypto,
            PacemakerConfig::default(),
        );
        c.bootstrap();

        let genesis = Block::genesis();
        let b1 = child(&genesis, 1, vec![], QuorumCert::genesis());
        // Leader b's real signature over the proposal digest.
        let prop_sig = sks[1].sign(&proposal_digest(1, &b1.hash())).to_bytes().to_vec();
        c.step(Event::Message {
            from: nid("b"),
            msg: HotStuffMessage::Proposal {
                block: b1.clone(),
                sig: PartialSig { signer: nid("b"), signature: prop_sig },
            },
        });
        assert_eq!(c.current_view, 2, "c voted b1 and collected its own vote");

        let vote_digest_b1 = vote_digest(1, &b1.hash());
        let signed_vote = |signer: &str, key: &SigningKey| HotStuffMessage::Vote {
            view: 1,
            block_hash: b1.hash(),
            sig: PartialSig { signer: nid(signer), signature: key.sign(&vote_digest_b1).to_bytes().to_vec() },
        };

        // Genuine vote from a (tally {c, a} = 2).
        c.step(Event::Message { from: nid("a"), msg: signed_vote("a", &sks[0]) });
        // Forged: claims to be d but signed with a's key -> verify fails.
        let forged = HotStuffMessage::Vote {
            view: 1,
            block_hash: b1.hash(),
            sig: PartialSig { signer: nid("d"), signature: sks[0].sign(&vote_digest_b1).to_bytes().to_vec() },
        };
        let out = c.step(Event::Message { from: nid("d"), msg: forged });
        assert!(!has_broadcast(&out), "forged vote must not count toward quorum");

        // Genuine vote from d finishes the quorum -> propose.
        let out = c.step(Event::Message { from: nid("d"), msg: signed_vote("d", &sks[3]) });
        assert!(has_broadcast(&out), "genuine quorum assembles the QC and proposes");
    }

    fn newview_target(out: &[Output]) -> Option<(NodeId, ViewNumber)> {
        out.iter().find_map(|o| match o {
            Output::SendMessage { to, msg: HotStuffMessage::NewView { view, .. } } => {
                Some((to.clone(), *view))
            }
            _ => None,
        })
    }

    fn reset_duration(out: &[Output]) -> Option<u64> {
        out.iter().find_map(|o| match o {
            Output::ResetViewTimer { duration_ms } => Some(*duration_ms),
            _ => None,
        })
    }

    /// Test 5a — on timeout the node abandons its view, sends a NewView to
    /// the next view's leader, and re-arms the timer with exponential
    /// backoff (base 400ms -> 800 -> 1600 across consecutive timeouts).
    #[test]
    fn view_timeout_sends_newview_with_exponential_backoff() {
        let all = ["a", "b", "c", "d", "e"];
        let mut a = node("a", &all);
        a.bootstrap(); // arms view-1 timer at the base 400ms

        let out = a.step(Event::ViewTimeout);
        assert_eq!(a.current_view, 2);
        assert_eq!(newview_target(&out), Some((nid("c"), 2)), "NewView -> leader(2)=c");
        assert_eq!(reset_duration(&out), Some(800), "first backoff = base * 2");

        let out = a.step(Event::ViewTimeout);
        assert_eq!(a.current_view, 3);
        assert_eq!(newview_target(&out), Some((nid("d"), 3)), "NewView -> leader(3)=d");
        assert_eq!(reset_duration(&out), Some(1600), "second backoff = base * 4");
    }

    /// Test 5b — timeout recovery: the leader of a view that gathers a
    /// quorum of distinct NewViews proposes, extending the highest QC it
    /// has seen, so the chain makes progress after a stall.
    #[test]
    fn quorum_newviews_lets_new_leader_propose() {
        let all = ["a", "b", "c", "d", "e"]; // quorum = 4; c leads view 2
        let mut c = node("c", &all);
        c.bootstrap();
        assert_eq!(c.current_view, 1, "c is a follower in view 1");

        let new_view = |from: &str| Event::Message {
            from: nid(from),
            msg: HotStuffMessage::NewView { view: 2, high_qc: QuorumCert::genesis() },
        };

        // Three distinct NewViews are not yet a quorum of four.
        assert!(!has_broadcast(&c.step(new_view("a"))));
        assert!(!has_broadcast(&c.step(new_view("b"))));
        assert!(!has_broadcast(&c.step(new_view("d"))));
        assert_eq!(c.current_view, 1, "still waiting for a quorum");

        // The fourth distinct NewView reaches quorum: enter view 2 and
        // propose (extending the genesis high QC). Processing its own
        // proposal as a replica then votes and carries it into view 3.
        let out = c.step(new_view("e"));
        assert!(c.current_view >= 2, "entered the recovery view");
        assert!(out.contains(&Output::BecameLeader));
        let proposed_view = out.iter().find_map(|o| match o {
            Output::Broadcast { msg: HotStuffMessage::Proposal { block, .. } } => Some(block.view),
            _ => None,
        });
        assert_eq!(proposed_view, Some(2), "recovery leader proposes in view 2");
    }

    /// Test 6 — a ClientPropose on a node that does not lead the current
    /// view is rejected synchronously, pointing the client at the actual
    /// leader.
    #[test]
    fn client_propose_on_non_leader_is_rejected_with_hint() {
        let mut a = node("a", &["a", "b", "c", "d"]);
        a.bootstrap(); // leader(1) = b, so a is a follower
        assert!(!a.is_leader());

        let out = a.step(Event::ClientPropose { command: b"x".to_vec() });
        assert_eq!(out, vec![Output::ProposeRejected { leader_hint: Some(nid("b")) }]);
    }

    /// Robustness #1 — a NewView whose carried high QC does not verify must
    /// not count toward the recovery quorum. Here the QC has one signer for
    /// a 5-node set (quorum 4), so it is invalid; four such NewViews still
    /// leave the leader stalled with nothing recorded.
    #[test]
    fn newview_with_unverifiable_qc_is_not_counted() {
        let all = ["a", "b", "c", "d", "e"]; // quorum 4; c leads view 2
        let mut c = node("c", &all);
        c.bootstrap();

        let bogus = QuorumCert {
            view: 2,
            block_hash: BlockHash([3u8; 32]),
            signatures: vec![PartialSig { signer: nid("a"), signature: Vec::new() }],
        };
        for from in ["a", "b", "d", "e"] {
            let out = c.step(Event::Message {
                from: nid(from),
                msg: HotStuffMessage::NewView { view: 2, high_qc: bogus.clone() },
            });
            assert!(!has_broadcast(&out), "invalid NewView must not drive a proposal");
        }
        assert_eq!(c.new_views.len(), 0, "unverifiable NewViews are never recorded");
        assert_eq!(c.current_view, 1, "no valid quorum -> no recovery");
    }

    /// Robustness #2 — vote/NewView aggregation maps do not retain rounds
    /// that can no longer matter once the view advances past them.
    #[test]
    fn stale_aggregation_entries_are_pruned_on_advance() {
        let all = ["a", "b", "c", "d", "e"]; // a leads view 5 (5 % 5 == 0)
        let mut a = node("a", &all);
        a.bootstrap();

        // As leader of view 5, `a` aggregates votes for view 4 and NewViews
        // for view 5. One of each (below quorum, so just parked).
        a.step(Event::Message {
            from: nid("b"),
            msg: HotStuffMessage::Vote {
                view: 4,
                block_hash: BlockHash([9u8; 32]),
                sig: PartialSig { signer: nid("b"), signature: Vec::new() },
            },
        });
        a.step(Event::Message {
            from: nid("b"),
            msg: HotStuffMessage::NewView { view: 5, high_qc: QuorumCert::genesis() },
        });
        assert_eq!(a.votes.len(), 1);
        assert_eq!(a.new_views.len(), 1);

        // Jumping well past view 5 evicts both now-useless entries.
        a.enter_view(10, true, &mut Vec::new());
        assert_eq!(a.votes.len(), 0, "vote round 4 is dead once past view 5");
        assert_eq!(a.new_views.len(), 0, "NewView round 5 is dead once past view 5");
    }

    /// Robustness #3 — RemovePeer must never strand consensus: removing
    /// ourselves, or the last remaining validator, is a no-op, and quorum()
    /// stays well-defined.
    #[test]
    fn remove_peer_cannot_empty_the_validator_set() {
        let mut a = node("a", &["a", "b"]);
        assert_eq!(a.validators().count(), 2);
        a.step(Event::RemovePeer { peer: nid("a") }); // remove self -> ignored
        assert!(a.validators().any(|v| v == &nid("a")), "self must remain");
        assert_eq!(a.validators().count(), 2);

        let mut solo = node("solo", &["solo"]);
        solo.step(Event::RemovePeer { peer: nid("solo") }); // last validator -> ignored
        assert_eq!(solo.validators().count(), 1);
        assert_eq!(solo.quorum(), 1, "quorum stays well-defined");
    }
}
