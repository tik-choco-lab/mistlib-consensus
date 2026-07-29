# Architecture

How the crates are put together. For the HotStuff protocol itself — the
voting rules, the 3-chain commit, the safety argument and the v1
limitations — see [`hotstuff-design.md`](hotstuff-design.md).

## Crate layout

```
mistlib-consensus/
├── mistlib-consensus-core    # Raft + HotStuff cores (pure, async-driven, platform-agnostic)
│   └── src/hotstuff/         # chained HotStuff BFT engine (state, node, crypto, storage, transport)
├── mistlib-consensus-native  # TokioRuntime + native startup helpers
└── mistlib-consensus-wasm    # WasmRuntime + wasm-bindgen FFI wrappers
```

Both engines follow the same sans-IO pattern. The Raft variants are
described below; the HotStuff counterparts mirror them one-for-one
(`hotstuff/state.rs`, `hotstuff/node.rs`, `hotstuff/transport.rs`,
`hotstuff/storage.rs`, plus `hotstuff/crypto.rs` for signatures).

## 1. Pure state machine (`state.rs`)

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

## 2. Transport (`transport.rs`)

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
  to a JavaScript callback.
- The HotStuff twin (`hotstuff/transport.rs`) additionally caps decoded
  message size (`MAX_WIRE_MESSAGE_BYTES`) because it faces explicitly
  untrusted peers.

## 3. Driver loop (`node.rs`)

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

## 4. Storage (`storage.rs`)

The `RaftStorage` trait owns persistence of the term, the vote, and the
log. `MemoryStorage` is a non-durable implementation for tests and
single-process use. The HotStuff counterpart is `BlockStore`
(`hotstuff/storage.rs`), which persists the block tree plus the
safety-critical `last_voted_view` / `locked_qc` / `high_qc` fields.

## Native adapter

```rust
let node = mistlib_consensus_native::spawn_native(
    NodeId("a".into()), peers, RaftConfig::default(), transport, MemoryStorage::new(),
);
```

A convenience function that builds a `RaftNode` on
`TokioRuntime::current()` (a handle to the ambient Tokio runtime) and
calls `start()` immediately. HotStuff spawn helpers are not wired into
the native/wasm adapters yet; construct `HotStuffNode` directly.

## WebAssembly adapter

The wasm side implements `AsyncRuntime` with
`wasm_bindgen_futures::spawn_local` + `gloo-timers` (`WasmRuntime`).

The transport deliberately avoids using `mistlib-core`'s `L1Transport`
directly in favor of the JS-callback-based `CallbackTransport`: in wasm
environments the real network implementation (WebSocket / WebRTC /
mistlib's mesh) usually lives on the JS side, and bridging raw bytes is
looser coupling than holding a concrete `L1Transport` in Rust. If
`mistlib-wasm` implements `L1Transport`, integration is a few lines of
JS (or `mistlib-wasm` code) calling `handleIncoming` from its receive
path.

## Integrating with mistlib's `L1Transport`

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

## Known deviations from the Raft paper

- `RaftMessage::RequestVoteResponse`/`AppendEntriesResponse` carry
  `voter_id`/`follower_id` fields that are not in the spec sheet's field
  list. `RaftTransport::send` only addresses a destination (`to`) and
  `RaftState` has no way to tell who a response came from (messages are
  assumed to arrive over anonymous point-to-point links), so the sender
  is embedded in the message itself.
- Membership changes are plain `add_peer`/`remove_peer` without joint
  consensus (configuration changes do not go through the log). A
  production deployment should replicate configuration changes as log
  entries; this was deliberately simplified for the current scope. The
  same operator-trusted simplification applies to HotStuff membership.
- `mistlib-consensus-wasm`'s `WasmRuntime::sleep` falls back to a no-op
  `std::future::ready(())` when built for non-wasm32 targets
  (`gloo-timers`' `Timeout` holds a `wasm_bindgen::Closure` and is not
  `Send`, so it cannot satisfy the `Send` bound required by
  `async-trait`'s non-wasm path). `WasmRuntime` is not intended for
  actual use outside wasm32.
