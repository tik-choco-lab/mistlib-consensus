# HotStuff / Consensus-Mode Design

Goal: `mistlib-consensus` supports two consensus modes selected at node
construction time:

| Mode | Fault model | Engine | Use case |
|------|-------------|--------|----------|
| `ConsensusMode::Trusted` | crash faults (CFT) | existing Raft | trusted P2P mesh |
| `ConsensusMode::Untrusted` | Byzantine faults (BFT), f = (n-1)/3 | chained HotStuff | untrusted P2P mesh |

The existing Raft modules are **not modified** except for additive error
variants. HotStuff lives in `mistlib-consensus-core/src/hotstuff/` and
mirrors the established architecture:

- **sans-IO state machine** (`hotstuff/state.rs`): pure `step(Event) -> Vec<Output>`,
  no async / clock / network. Mirrors `RaftState`.
- **async driver** (`hotstuff/node.rs`): owns timers + event loop over an
  `AsyncRuntime`, mirrors `RaftNode` (see `src/node.rs` for the pattern:
  unbounded event channel, epoch-guarded timers, broadcast channel for
  committed output).
- **transport adapter** (`hotstuff/transport.rs`): mirrors `src/transport.rs`
  (bincode over `L1Transport`, ReliableOrdered).
- **mode facade** (`src/mode.rs`): `ConsensusMode` enum + `ConsensusNode`
  enum wrapper delegating to `RaftNode` / `HotStuffNode`, with a unified
  `CommittedCommand` stream.

## Algorithm: chained (event-driven) HotStuff

One block per view, pipelined phases. Round-robin leader:
`leader(v) = validators_sorted[v % n]` over the *sorted* validator set.

Quorum: `n = validators.len()` (includes self), `f = (n - 1) / 3`,
`quorum = n - f`.

### Data types (contract — see `hotstuff/message.rs`)

- `BlockHash` = sha256, 32 bytes. Genesis block hash = all zeros' digest
  (computed like any block, from the fixed genesis fields).
- `Block { parent, view, height, commands: Vec<Vec<u8>>, justify: QuorumCert }`,
  hash computed over all fields (domain-separated, length-prefixed).
- `QuorumCert { view, block_hash, signatures: Vec<PartialSig> }`.
  Valid iff signatures are from `quorum` **distinct** validators and each
  verifies over `vote_digest(view, block_hash)`. The genesis QC
  (view 0, genesis hash, no sigs) is valid by definition.
- `PartialSig { signer: NodeId, signature: Vec<u8> }`.
- Messages: `Proposal { block, sig }` (leader signs `proposal_digest`),
  `Vote { view, block_hash, sig }` (sig over `vote_digest`),
  `NewView { view, high_qc }` (unsigned; the QC inside self-certifies).

### Replica protocol (state.rs)

Per-replica state: `current_view`, `high_qc` (highest QC seen),
`locked_qc` (2-chain lock), `last_voted_view`, block tree (storage),
`committed_height`, pending command queue (leader side).

On `Proposal { block, sig }` from `from`:
1. Verify `from == leader(block.view)`, `block.view == current_view`,
   leader sig over `proposal_digest(view, block.hash())`, and
   `block.justify` QC validity. Verify `block.parent == block.justify.block_hash`
   and `block.height == parent.height + 1`. Reject (ignore) otherwise.
2. Process `block.justify` as a QC (see "On QC" below).
3. **Voting rule** (SafeNode): vote iff `block.view > last_voted_view` AND
   (`block` extends `locked_qc.block_hash` OR
   `block.justify.view > locked_qc.view`).
   If voting: set `last_voted_view = block.view`, store block, send
   `Vote` to `leader(block.view + 1)` (pipelined: next leader collects).
4. Advance view to `block.view + 1` after voting (QC-driven advance
   happens via step 2 as well; view only moves forward, never back).

On `Vote` (as leader of `vote.view + 1`): collect distinct-signer votes
for `(view, block_hash)`; at `quorum`, assemble `QuorumCert`, update
`high_qc`, enter view `vote.view + 1` if not there yet, and propose.

On QC `qc` (from a justify or assembled from votes):
1. If `qc.view > high_qc.view`: `high_qc = qc`.
2. **Lock rule (2-chain)**: let `b'' = block(qc)`, `b' = block(b''.justify)`.
   If `b'.view > locked_qc.view`: `locked_qc = b''.justify`.
3. **Commit rule (3-chain with consecutive views)**: let
   `b = block(b'.justify)`. If `b''.parent == b'.hash`, `b'.parent == b.hash`,
   and views are consecutive (`b''.view == b'.view + 1`, `b'.view == b.view + 1`):
   commit `b` and all uncommitted ancestors, emit `ApplyCommitted`.

On `NewView { view, high_qc }`: process the QC; if we are `leader(view)`
and `view >= current_view`, count distinct senders; on `quorum` NewViews
(or a QC that already advances us) enter the view and propose extending
`high_qc.block_hash`. (Timeout path liveness; no TimeoutCert in v1.)

On `ViewTimeout`: `current_view += 1`, send `NewView(current_view, high_qc)`
to `leader(current_view)`, emit `ResetViewTimer` with exponential backoff
(`base * 2^consecutive_timeouts`, capped). Reset backoff whenever a QC
advances the view.

Proposing (leader of `current_view`, once per view): block extends
`high_qc.block_hash` with `justify = high_qc`, drains the pending command
queue (empty blocks are allowed and REQUIRED for pipeline progress —
leader proposes even with no pending commands). Broadcast `Proposal` to
all validators (including processing it locally as a replica).

`ClientPropose`: if leader of current view and not yet proposed → queue
and propose. If not leader → `ProposeRejected { leader_hint: leader(current_view) }`.
Queued commands ride in the next block this node leads.
`Proposed { sequence }` is emitted when the command is included in a
block (sequence = tentative; commit is signalled via the committed stream).

### Crypto (`hotstuff/crypto.rs`)

```rust
pub trait CryptoProvider: HostSendSync {
    fn sign(&self, digest: &[u8; 32]) -> Vec<u8>;
    fn verify(&self, signer: &NodeId, digest: &[u8; 32], sig: &[u8]) -> bool;
}
```
- `NoopCrypto`: sign → `vec![]`, verify → `true`. For tests / semi-trusted.
- `Ed25519Crypto`: `ed25519-dalek` SigningKey + `HashMap<NodeId, VerifyingKey>`
  registry supplied by the caller (key distribution out of scope).
- Digests are domain-separated: `vote_digest = sha256("mistlib-hs-vote" ‖ view_le ‖ hash)`,
  `proposal_digest = sha256("mistlib-hs-prop" ‖ view_le ‖ hash)`.

### Storage (`hotstuff/storage.rs`)

`BlockStore` trait: put/get blocks by hash, walk parents, persist
safety-critical fields (`last_voted_view`, `locked_qc`, `high_qc`) so a
restarted node cannot equivocate. `MemoryBlockStore` provided.

### Membership

`AddPeer`/`RemovePeer` update the validator set at a view boundary and
are operator-trusted actions (documented limitation; BFT reconfiguration
is out of scope for v1). Changing `n` changes `quorum` immediately.

### Facade (`src/mode.rs`)

```rust
pub enum ConsensusMode { Trusted, Untrusted }
pub struct CommittedCommand { pub sequence: u64, pub view: u64, pub command: Vec<u8> }
pub enum ConsensusNode<S: RaftStorage, B: BlockStore, C: CryptoProvider> { ... }
```
Unified API: `propose`, `subscribe_committed` (forwarding task maps
Raft `LogEntry { term→view, index→sequence }`; HotStuff assigns a
monotonically increasing execution sequence), `is_leader`, `leader_hint`,
`add_peer`, `remove_peer`, `shutdown`.

### Errors

Additive variants on `RaftError` (kept name for compatibility, alias
`ConsensusError` exported): `InvalidCertificate(String)`,
`InvalidBlock(String)`, `NotValidator`.

### Known limitations (v1)

- **No block-sync protocol**: a replica that misses a proposal whose
  parent it never stored ignores descendants until the pacemaker path
  catches it up via NewView/high_qc. Fine over ReliableOrdered links;
  a fetch-missing-ancestors protocol is future work.
- **Membership is operator-trusted** (see above); no BFT reconfiguration.
- **QC size is O(n)** (vector of Ed25519 sigs); threshold/BLS
  aggregation is future work.
- **Inbound flow control**: wire messages are size-capped
  (`MAX_WIRE_MESSAGE_BYTES`), but inbound channels are unbounded;
  per-peer rate limiting / bounded channels with drop policy are
  future work.

### Testing

- Unit tests in `hotstuff/state.rs` (mirror `state.rs` Raft tests):
  happy path 3-chain commit across 4 validators, voting-rule refusals
  (stale view, lock conflict), QC quorum counting with duplicate signer
  rejection, view timeout → NewView → recovery, forged-signature rejection
  with a real `Ed25519Crypto`, empty-block pipeline progress.
- Integration test `tests/hotstuff_cluster.rs` mirroring `tests/cluster.rs`
  using the in-memory router from `tests/common/mod.rs` (extend it
  generically or add a HotStuff router variant).
