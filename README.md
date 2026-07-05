# mistlib-consensus

Consensus engines built on top of `mistlib-dev`, with a mode switch for
the trust model of the underlying P2P mesh:

| Mode | Fault model | Engine | Use case |
|------|-------------|--------|----------|
| `ConsensusMode::Trusted` | crash faults (CFT) | Raft | trusted P2P mesh |
| `ConsensusMode::Untrusted` | Byzantine faults, `f = (n-1)/3` | chained HotStuff | untrusted P2P mesh |

Both engines share the same architecture — a platform-agnostic core
(`mistlib-consensus-core`) that builds natively and for
`wasm32-unknown-unknown`, plus per-platform adapters
(`mistlib-consensus-native` / `mistlib-consensus-wasm`). The
`ConsensusNode` facade in `mode.rs` erases the engine choice behind a
unified `propose` / `subscribe_committed` API. The wire protocols are
incompatible, so every node in a cluster must be built in the same mode.

The HotStuff protocol design, safety rules, and v1 limitations are
documented in [`docs/hotstuff-design.md`](docs/hotstuff-design.md).

## Depending on `mistlib-core`

`mistlib-core` is referenced as a git dependency pinned to a `rev`
(`https://github.com/tik-choco-lab/mistlib-dev.git`). It used to be a
relative-path dependency on `../../mistlib-dev/mistlib-core`, which made
this repository unbuildable when vendored/cloned standalone unless
`mistlib-dev` sat in a specific sibling directory
([#1](https://github.com/tik-choco-lab/mistlib-consensus/issues/1)).
The git dependency removes that constraint.

To pick up changes from `mistlib-core`, bump the `rev` in each
`Cargo.toml`'s `mistlib-core = { git = "...", rev = "..." }` to the
latest `mistlib-dev` commit hash and run `cargo build` to refresh
`Cargo.lock`.

## Crate layout

```
mistlib-consensus/
├── mistlib-consensus-core    # Raft + HotStuff cores (pure, async-driven, platform-agnostic)
│   └── src/hotstuff/         # chained HotStuff BFT engine (state, node, crypto, storage, transport)
├── mistlib-consensus-native  # TokioRuntime + native startup helpers
└── mistlib-consensus-wasm    # WasmRuntime + wasm-bindgen FFI wrappers
```

## Architecture

Both engines follow the same sans-IO pattern; the Raft variants are
described here, and the HotStuff counterparts mirror them one-for-one
(`hotstuff/state.rs`, `hotstuff/node.rs`, `hotstuff/transport.rs`,
`hotstuff/storage.rs`, plus `hotstuff/crypto.rs` for signatures).

### 1. Pure state machine (`state.rs`)

`RaftState<S: RaftStorage>` implements the core Raft logic
**synchronously, without async**. It exposes a purely functional
`step(event: Event) -> Vec<Output>` interface over:

- `Event`: `Message` (RPCs from other nodes), `ElectionTimeout`,
  `HeartbeatTimeout`, `ClientPropose`, `AddPeer`/`RemovePeer`
- `Output`: `SendMessage`, `ApplyCommitted`, `BecameLeader`/`BecameFollower`,
  `ResetElectionTimer`/`ResetHeartbeatTimer`, `Proposed`/`ProposeRejected`

Because it never touches I/O or the clock, it can be tested
deterministically without mocks — see the `#[cfg(test)]` unit tests in
`mistlib-consensus-core/src/state.rs` (election majorities, stepping
down on a higher term, log replication and commit advancement,
truncating conflicting entries, refusing votes to candidates with stale
logs). `hotstuff/state.rs` gets the same treatment for the BFT rules
(3-chain commits, lock conflicts, quorum counting, forged-signature
rejection, timeout recovery).

### 2. Transport (`transport.rs`)

The `RaftTransport` trait follows the same
`#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]` pattern as
`mistlib-core`'s `L1Transport` and only requires
`send(&self, to, msg) -> Result<()>`.

- `MistTransport` wraps an `Arc<dyn L1Transport>`, serializes with
  `bincode`, and sends with `DeliveryMethod::ReliableOrdered`. On the
  receive side, feed raw bytes into `handle_incoming(from, bytes)` and
  they reach the `RaftNode` through an internal `mpsc` channel
  (subscribe with `RaftNode::attach_inbound`).
- `mistlib-consensus-wasm::CallbackTransport` delegates actual sending
  to a JavaScript callback (see below).
- The HotStuff twin (`hotstuff/transport.rs`) additionally caps decoded
  message size (`MAX_WIRE_MESSAGE_BYTES`) because it faces explicitly
  untrusted peers.

### 3. Driver loop (`node.rs`)

`RaftNode<S>` drives `RaftState<S>` from an async event loop:

- `AsyncRuntime::spawn` launches three tasks: the event loop itself, the
  election timer, and the heartbeat timer.
- Timer resets are detected with an `AtomicU64` epoch counter rather
  than by cancelling sleeps: a timer that wakes up checks whether the
  epoch moved while it slept. (`HotStuffNode` reuses the same trick for
  its single view timer, whose duration changes per view due to
  exponential backoff.)
- Election timeouts are drawn randomly from
  `election_timeout_min_ms..=election_timeout_max_ms` (default
  150–300 ms); the heartbeat interval defaults to 50 ms.
- All state-machine mutation happens on a single task, so no locking is
  needed for the protocol itself. A single `std::sync::Mutex<Status>`
  serves external read-only accessors like `role()`/`leader_hint()`.

### 4. Storage (`storage.rs`)

The `RaftStorage` trait owns persistence of the term, the vote, and the
log. `MemoryStorage` is a non-durable implementation for tests and
single-process use. The HotStuff counterpart is `BlockStore`
(`hotstuff/storage.rs`), which persists the block tree plus the
safety-critical `last_voted_view` / `locked_qc` / `high_qc` fields.

## Public API (`mistlib-consensus-core`)

```rust
let node: Arc<RaftNode<MemoryStorage>> = RaftNode::new(
    NodeId("a".into()),
    vec![NodeId("b".into()), NodeId("c".into())],
    RaftConfig::default(),
    runtime,      // Arc<dyn AsyncRuntime>
    transport,    // Arc<dyn RaftTransport>
    MemoryStorage::new(),
);
node.start();

// Succeeds only on the leader; returns the log index.
let index = node.propose(b"command".to_vec()).await?;

// Subscribe to committed entries.
let mut rx = node.subscribe_committed();
while let Ok(entry) = rx.recv().await { /* apply entry.command */ }

node.role();         // Option<Role>
node.leader_hint();  // Option<NodeId>
node.add_peer(id);
node.remove_peer(id);
node.shutdown();
```

### Consensus modes

`HotStuffNode` exposes the same shape of API (see the module docs of
`hotstuff/node.rs`); the `ConsensusNode` facade unifies the two:

```rust
use mistlib_consensus_core::{ConsensusMode, ConsensusNode};
use mistlib_consensus_core::hotstuff::{
    Ed25519Crypto, HotStuffNode, MemoryBlockStore, PacemakerConfig,
};

// Trusted P2P: Raft.
let node = ConsensusNode::trusted(raft_node);

// Untrusted P2P: chained HotStuff with Ed25519 quorum certificates.
// The caller supplies the signing key and the validator key registry.
let hs = HotStuffNode::new(
    id, validators, PacemakerConfig::default(),
    runtime.clone(), transport,
    MemoryBlockStore::new(),
    Ed25519Crypto::new(signing_key, registry),
);
hs.start();
let node = ConsensusNode::untrusted(hs);

// Same API in either mode.
let seq = node.propose(b"command".to_vec()).await?;
let mut committed = node.subscribe_committed(&runtime);
while let Some(cmd) = committed.recv().await { /* apply cmd.command */ }
```

For tests or semi-trusted meshes, `NoopCrypto` disables signing while
keeping HotStuff's deterministic leader rotation.

## `mistlib-consensus-native`

```rust
let node = mistlib_consensus_native::spawn_native(
    NodeId("a".into()), peers, RaftConfig::default(), transport, MemoryStorage::new(),
);
```

A convenience function that builds a `RaftNode` on
`TokioRuntime::current()` (a handle to the ambient Tokio runtime) and
calls `start()` immediately. (HotStuff spawn helpers are not wired into
the native/wasm adapters yet; construct `HotStuffNode` directly as
shown above.)

## `mistlib-consensus-wasm`

Build with `wasm-pack build --target web` and use from JS:

```js
import init, { WasmRaftNode, initPanicHook } from "./pkg/mistlib_consensus_wasm.js";

await init();
initPanicHook();

const node = new WasmRaftNode("a", JSON.stringify(["b", "c"]), null);

// The caller implements the actual network send
// (WebSocket, or a bridge to mistlib's L1Transport).
node.setSendCallback((toNodeId, bytes) => {
  mesh.sendTo(toNodeId, bytes);
});

// Feed received bytes back in here.
mesh.onMessage((from, bytes) => node.handleIncoming(from, bytes));

node.onCommitted((bytes) => applyToStateMachine(bytes));

node.start();

const index = await node.propose(new TextEncoder().encode("hello"));
console.log(node.role(), node.leaderHint());
```

The wasm side implements `AsyncRuntime` with
`wasm_bindgen_futures::spawn_local` + `gloo-timers` (`WasmRuntime`).
The transport deliberately avoids using
`mistlib-core::layers::l1::L1Transport` directly in favor of the
JS-callback-based `CallbackTransport`: in wasm environments the real
network implementation (WebSocket / WebRTC / mistlib's mesh) usually
lives on the JS side, and bridging raw bytes is looser coupling than
holding a concrete `L1Transport` in Rust. If `mistlib-wasm` implements
`L1Transport`, integration is a few lines of JS (or `mistlib-wasm`
code) calling `handleIncoming` from its receive path.

## Integrating with mistlib's L1Transport

On native, `MistTransport::new(Arc<dyn L1Transport>)` yields
`(RaftTransport, mpsc::UnboundedReceiver<(NodeId, RaftMessage)>)`. Wire
the receive path by calling
`mist_transport.handle_incoming(&from, &bytes)` from your
`L1Notifier`/message dispatcher, and hand the receiver to
`RaftNode::attach_inbound(rx)`.

```rust
let (transport, inbound_rx) = MistTransport::new(l1_transport);
let node = RaftNode::new(id, peers, config, runtime, Arc::new(transport), storage);
node.attach_inbound(inbound_rx);
node.start();
```

`HotStuffMistTransport` provides the identical shape for HotStuff.

## Tests

- `mistlib-consensus-core`: deterministic unit tests for both pure state
  machines (`state.rs`, `hotstuff/state.rs`), plus mock-transport
  cluster integration tests (`tests/cluster.rs`,
  `tests/cluster_advanced.rs`, `tests/hotstuff_cluster.rs` — leader
  failover/silence, partitions, an end-to-end Ed25519-signed HotStuff
  run, and the mode facade; all `#[cfg(not(target_arch = "wasm32"))]`).
- `mistlib-consensus-native`: a smoke test that a single-node cluster
  becomes leader and `propose` succeeds (`tests/smoke.rs`).

```sh
cargo build --workspace
cargo test --workspace
rustup target add wasm32-unknown-unknown
cargo build -p mistlib-consensus-wasm --target wasm32-unknown-unknown
cargo clippy --workspace
```

## Known deviations and caveats

- `RaftMessage::RequestVoteResponse`/`AppendEntriesResponse` carry
  `voter_id`/`follower_id` fields that are not in the spec sheet's field
  list. `RaftTransport::send` only addresses a destination (`to`) and
  `RaftState` has no way to tell who a response came from (messages are
  assumed to arrive over anonymous point-to-point links), so the sender
  is embedded in the message itself.
- Membership changes are plain `add_peer`/`remove_peer` without joint
  consensus, per the spec (configuration changes do not go through the
  log). A production deployment should replicate configuration changes
  as log entries; this was deliberately simplified for the current
  scope. The same operator-trusted simplification applies to HotStuff
  membership (BFT reconfiguration is out of scope for v1 — see
  `docs/hotstuff-design.md`).
- `MemoryStorage` / `MemoryBlockStore` are non-durable. Production use
  requires disk-backed implementations — for HotStuff this is
  safety-critical (a replica that forgets `last_voted_view` or
  `locked_qc` can be tricked into equivocating after a restart).
- `mistlib-consensus-wasm`'s `WasmRuntime::sleep` falls back to a no-op
  `std::future::ready(())` when built for non-wasm32 targets
  (`gloo-timers`' `Timeout` holds a `wasm_bindgen::Closure` and is not
  `Send`, so it cannot satisfy the `Send` bound required by
  `async-trait`'s non-wasm path). `WasmRuntime` is not intended for
  actual use outside wasm32.
