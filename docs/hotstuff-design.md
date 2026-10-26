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

- `BlockHash` = sha256, 32 bytes. Genesis is hashed from its fixed fields;
  `BlockHash::ZERO` is only its parent/QC sentinel, not the genesis hash.
- `Block { parent, view, height, commands: Vec<Vec<u8>>, justify: QuorumCert }`,
  hash computed over parent, view, height, commands, and justify view/hash
  (domain-separated, length-prefixed). QC signature witnesses are excluded
  so equivalent certificates identify the same block.
- `QuorumCert { view, block_hash, signatures: Vec<PartialSig> }`.
  Valid iff signatures are from `quorum` **distinct** validators and each
  verifies over `vote_digest(view, block_hash)`. The genesis QC
  (view 0, genesis hash, no sigs) is valid by definition.
- `PartialSig { signer: NodeId, signature: Vec<u8> }`.
- Messages: `Proposal { block, sig }` (leader signs `proposal_digest`),
  `Vote { view, block_hash, sig }` (sig over `vote_digest`),
  `NewView { view, high_qc }` (unsigned; the QC inside self-certifies),
  `BlockRequest { hash, request_id, max_blocks }`, and
  `BlockResponse { hash, request_id, blocks }` (descending parent chain).

### Replica protocol (state.rs)

Per-replica state: `current_view`, `high_qc` (highest QC seen),
`locked_qc` (2-chain lock), `last_voted_view`, block tree (storage),
`committed_height`, pending command queue (leader side).

On `Proposal { block, sig }` from `from`:
1. Verify `from == leader(block.view)`, the leader signature, and
   `block.justify` QC validity. Require `block.parent == block.justify.block_hash`
   and `block.justify.view < block.view`.
2. Retain the first authenticated proposal per view/leader; ignore
   equivocations at debug level. If the parent has not arrived, defer the
   proposal and request its parent plus ancestors from the sender. Then require `block.height == parent.height + 1` and
   `block.justify.view == parent.view`, and store the block, including
   proposals whose views have already timed out. Replay deferred children
   whenever their parent becomes available.
3. Process its justify as a QC, advancing to `justify.view + 1` if ahead.
   If we already learned a QC for this block before receiving it, retry
   that QC's lock/commit rules and any proposal waiting to extend it.
   Retain recovery proposals for a future view until the local pacemaker
   or a QC reaches that view. A leader signature alone cannot skip views.
4. **Voting rule** (SafeNode): vote iff `block.view == current_view`,
   `block.view > last_voted_view`, AND (`block` extends the locked block OR
   `block.justify.view > locked_qc.view`). Persist `last_voted_view` before
   sending `Vote` to `leader(block.view + 1)`. Late storage never causes
   a vote in a view the replica has left.
5. Advance to `block.view + 1` after voting. View numbers only increase.

On `Vote` (as leader of `vote.view + 1`): collect distinct-signer votes
for `(view, block_hash)` within the view window, keeping only each
validator's first vote per view; at `quorum`, assemble `QuorumCert`, update
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
(`base * 2^consecutive_timeouts`, capped). Retain backoff across votes
and QCs; reset it when the three-chain commits a newer block. Partial
progress must not repeatedly shrink the timeout below the time needed
for a complete commit chain. The async driver tags timeout events with
an epoch and checks it both before enqueueing and when consuming them,
so a timeout queued during verification cannot expire a newer view.

Proposing (leader of `current_view`, once per view): block extends
`high_qc.block_hash` with `justify = high_qc`, drains the pending command
queue (empty blocks are allowed and REQUIRED for pipeline progress —
leader proposes even with no pending commands). A proposal requires either
`high_qc.view + 1 == current_view` or a quorum of NewViews for this view;
a client request only queues its command until that condition holds.
Broadcast `Proposal` to all validators (including processing it locally as a replica).

`ClientPropose`: if leader of the current view, queue the command and
propose once authorized by a QC or NewView quorum, at most once per view.
Otherwise emit `ProposeRejected { leader_hint: leader(current_view) }`.
`Proposed { view }` acknowledges acceptance into the queue for a block at
or after `view`. The execution sequence is assigned at commit and reported
via the committed stream.

### Pending-state bounds and ancestor sync

`PacemakerConfig.sync: SyncConfig` configures temporary-state limits:

| Knob | Default | Scope |
|------|---------|-------|
| `max_future_views` | 128 | Proposals/vote aggregation within current view ±128; NewView aggregation up to current +128 |
| `max_deferred_blocks` | 256 | Combined missing-parent and future proposal count |
| `max_deferred_bytes` | 16 MiB | Combined serialized block bytes in those queues |
| `max_inflight_requests` | 32 | Recovery sessions, including exhausted sessions awaiting a new view |
| `max_fetch_attempts` | 8 | Initial send plus retries per requested cursor, per recovery round |
| `retry_base_ms` | 300 ms | Doubles on each retry, capped at 8 seconds; rounded up to 100 ms ticks |
| `serve_interval_ms` | 100 ms | At most one response per validator per interval, rounded up to 100 ms |

Pending/future queues are keyed by view (one designated leader per view).
When their shared count or byte cap is exceeded, evict the highest view
first. Bounded first-proposal hashes remain as tombstones within the view
window, so eviction does not let equivocations refill that view. Invalid
parent height/view releases the entry when checked. Prune views at or below
the lock/committed view, outside the window, and blocks at/below committed
height or whose parent conflicts at the committed boundary. Known parents
must also extend the committed branch. These limits never remove persisted
votes, locks, high QCs, or stored safety history. Certified blocks can be
fetched independently of a leader's first-proposal choice.

Vote aggregation retains at most one signature per validator per view,
regardless of the number of equivocated hashes. NewView sender sets are
bounded by membership and the lookahead. There is no growable pending-QC
queue: the persisted high QC is retried when its block becomes available.
A higher QC subsumes the chain recovery needed by lower QCs.

When a proposal parent, an assembled QC, or a NewView QC references an
unknown block, start a fetch. Prefer the message sender, then rotate through
the other validators in deterministic order on timeout. The high QC gets
priority if the request table is full; its persisted certificate is never
evicted. Deduplicate both recovery roots and active ancestor cursors by
hash. Tokens correlate responses with the currently selected peer and
request; stale responses do not advance recovery. Exhausted sessions stop
sending until a later view timeout starts another bounded recovery round,
so healing after a long partition remains possible.

The sans-IO machine emits `ScheduleSyncTick` and accepts `SyncTick`; the
async driver owns a single independent 100 ms maintenance timer chain.
View changes do not cancel fetch timers or replenish serving budgets.
Maintenance ticks drive retry deadlines and per-validator serving budgets;
there is no task or timer per untrusted hash/requester. Serving is restricted
to validators and reads only stored blocks. Excess requests are dropped,
not queued. Requester counts are capped at **32 total blocks**, and each
response is capped at **4 MiB including envelope**, matching the wire cap.

Accept a response only for an outstanding request token from its selected
peer. Before any mutation, check block count and serialized bytes, hash of
the requested head, certified view, each parent hash/link and height, and
all justify QC quorums/signatures (duplicates are invalid). The final block
must link to a known parent before the batch can be stored. If that parent
is missing, discard the batch and move the cursor backward using its verified
hash/view/height. Once an anchored suffix is fetched, store oldest first
and restart at the recovery root. This permits arbitrarily long gaps with
bounded temporary memory, at the cost of repeated downloads (quadratic
worst-case bandwidth for long gaps). Reject a batch conflicting with the
committed branch. Invalid, oversized, unsolicited, wrong-peer and stale
responses do not store blocks or advance the cursor.

Fetched blocks enter the same parent/height/branch and QC/commit rules,
but **never vote**. They replay authenticated deferred children and retry
the high QC/proposing rules. A separately received current-view leader
proposal may still vote after its block was fetched, subject to the usual
persisted vote and lock checks.

### Wire encoding and wasm

Consensus messages use serde/bincode 1.x fixed-width integers, little endian,
with enum discriminants in declaration order: Proposal=0, Vote=1, NewView=2,
BlockRequest=3, BlockResponse=4. Struct fields follow declaration order;
vector lengths are u64. Request fields are hash (32 bytes), request_id
(u64), max_blocks (u32); response fields are hash, request_id, blocks.
Responses order blocks head-to-ancestor. Locally assembled QC witnesses
are sorted by signer. Decoding retains the existing 4 MiB limit and
trailing-byte tolerance. This experimental wire extension requires peers
to upgrade together.

The wasm crate's `CallbackTransport` supports both `RaftTransport` and
`HotStuffTransport`, handing deterministic serialized bytes to JavaScript
opaquely. Rust wasm embedders can use the HotStuff core and runtime with
this adapter; the existing `WasmRaftNode` JavaScript facade remains
Raft-specific. No HotStuff JavaScript node facade is introduced here.

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

- **Stored block history is not pruned**: pending state and fetch bookkeeping
  are bounded, but `MemoryBlockStore` retains accepted/certified history.
  Checkpointing, persistent recovery metadata, durable stores, and a safe
  stored-history retention policy remain future work. Restart persistence
  must also restore committed height/execution sequence before resuming
  command delivery; the bundled store is for a single process lifetime.
- **Recovery bandwidth**: backward cursor recovery avoids unbounded buffering
  but may repeatedly download batches across a large gap (see above).
- **Membership is operator-trusted** (see above); no BFT reconfiguration.
- **QC size is O(n)** (vector of Ed25519 sigs); threshold/BLS
  aggregation is future work.
- **Inbound flow control**: wire messages are size-capped
  (`MAX_WIRE_MESSAGE_BYTES`), but inbound channels are unbounded;
  rate limiting applies only to serving block requests from validators.
  General inbound per-peer rate limiting / bounded channels with drop
  policy remain future work. Application-submitted commands also require
  admission control at the embedding boundary.

### Testing

- Unit tests in `hotstuff/state.rs` (mirror `state.rs` Raft tests):
  happy path 3-chain commit across 4 validators, voting-rule refusals
  (stale view, lock conflict), QC quorum counting with duplicate signer
  rejection, view timeout → NewView → recovery, forged-signature rejection
  with a real `Ed25519Crypto`, empty-block pipeline progress.
- Integration test `tests/hotstuff_cluster.rs` mirroring `tests/cluster.rs`
  using the in-memory router from `tests/common/mod.rs` (extend it
  generically or add a HotStuff router variant).

- Sync/bounds regressions in `hotstuff/state/sync_tests.rs`: real signed
  equivocation floods, deterministic cap/byte eviction, bounded vote and
  NewView aggregation, retry backoff/peer rotation, invalid/forged/oversized/
  unsolicited responses, serving budgets, lock/commit pruning, no fetched
  votes, and recovery across a 100-block gap with a two-block pending cap.
- The cluster partition regression permanently drops proposals, heals the
  network without replaying them, checks actual block responses and equal
  committed command logs, then verifies further commits.
