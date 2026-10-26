<div align="center">

# mistlib-consensus

**Replicated state machines for peer-to-peer meshes — Raft when you trust the peers, HotStuff when you don't.**

[![status](https://img.shields.io/badge/status-experimental-orange)](#status)
[![engine](https://img.shields.io/badge/mistlib-v0.7.1-2f6feb)](https://github.com/tik-choco-lab/mistlib)
[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

English · [日本語](README.ja.md) · [简体中文](README.zh.md)

</div>

---

A peer-to-peer mesh gives every node its own copy of the world, and copies drift. This library
puts an ordered log underneath them: propose a command anywhere, and every node applies the same
commands in the same order. Which engine does that depends on what your peers might do — crash,
or lie.

Built on [mistlib](https://github.com/tik-choco-lab/mistlib)'s runtime and transport
abstractions. Written in Rust, and the core compiles for `wasm32-unknown-unknown` as well as
native, so the same consensus runs in a browser tab and on a server.

> [!WARNING]
> Experimental and not security-audited. Read [Status](#status) before relying on it.

## Consensus modes

| Mode | Fault model | Engine | Use case |
| --- | --- | --- | --- |
| `ConsensusMode::Trusted` | crash faults (CFT), tolerates `f = (n-1)/2` | Raft | a mesh whose peers you run |
| `ConsensusMode::Untrusted` | Byzantine faults (BFT), tolerates `f = (n-1)/3` | chained HotStuff | a mesh where peers may lie |

The `ConsensusNode` facade hides the choice behind one `propose` / `subscribe_committed` API, so
the mode is a construction-time decision rather than something your application code sees.

> [!IMPORTANT]
> The two wire protocols are incompatible. Every node in a cluster must be built in the same mode.

## Install

Not on crates.io — `mistlib-core` is distributed as a git dependency, and crates.io does not
accept those. Depend on this repository directly:

```toml
[dependencies]
mistlib-consensus-core = { git = "https://github.com/tik-choco-lab/mistlib-consensus.git", rev = "<commit>" }
```

No releases are tagged yet, so pin a `rev` if you want a reproducible build. The engine comes
along as a pinned tag of [mistlib](https://github.com/tik-choco-lab/mistlib) — nothing else to
install.

## Usage

```rust
use mistlib_consensus_core::{ConsensusNode, NodeId, RaftConfig, RaftNode, MemoryStorage};

// Trusted mesh — Raft.
let raft = RaftNode::new(
    NodeId("a".into()),
    vec![NodeId("b".into()), NodeId("c".into())],
    RaftConfig::default(),
    runtime,      // Arc<dyn AsyncRuntime>
    transport,    // Arc<dyn RaftTransport>
    MemoryStorage::new(),
);
raft.start();
let node = ConsensusNode::trusted(raft);
```

```rust
use mistlib_consensus_core::hotstuff::{
    Ed25519Crypto, HotStuffNode, MemoryBlockStore, PacemakerConfig,
};

// Untrusted mesh — chained HotStuff with Ed25519 quorum certificates.
// You supply the signing key and the validator public-key registry;
// distributing those keys is out of scope for this library.
let hs = HotStuffNode::new(
    id, validators, PacemakerConfig::default(),
    runtime.clone(), transport,
    MemoryBlockStore::new(),
    Ed25519Crypto::new(signing_key, registry),
);
hs.start();
let node = ConsensusNode::untrusted(hs);
```

Either way, the rest is the same:

```rust
// Succeeds only on the current leader.
let seq = node.propose(b"command".to_vec()).await?;

// Apply committed commands in order, on every node.
let mut committed = node.subscribe_committed(&runtime);
while let Some(cmd) = committed.recv().await {
    apply(cmd.sequence, &cmd.command);
}
```

## API

| Call | Does |
| --- | --- |
| `propose(bytes)` | Submit a command. Only the leader accepts it; returns its sequence |
| `subscribe_committed(&runtime)` | Stream of `CommittedCommand { sequence, view, command }`, in order |
| `is_leader()` / `leader_hint()` | Whether this node leads, and who it thinks does |
| `add_peer(id)` / `remove_peer(id)` | Change the member set (operator-trusted — see Status) |
| `shutdown()` | Stop the driver tasks |

`sequence` is gapless and starts at 1, in both modes.

For tests or semi-trusted meshes, `NoopCrypto` disables signature checking while keeping
HotStuff's deterministic leader rotation. It makes the engine non-Byzantine-tolerant — do not
reach for it to silence a key-distribution problem.

## Configuration

Raft timing — `RaftConfig`:

| Key | Default | Meaning |
| --- | --- | --- |
| `election_timeout_min_ms` | `150` | Lower bound of the randomized election timeout |
| `election_timeout_max_ms` | `300` | Upper bound of the same |
| `heartbeat_interval_ms` | `50` | Leader heartbeat interval; keep it well below the minimum election timeout |

HotStuff pacemaker — `PacemakerConfig`:

| Key | Default | Meaning |
| --- | --- | --- |
| `base_timeout_ms` | `400` | View timeout, doubled per consecutive timeout until a QC advances the view |
| `max_timeout_ms` | `8000` | Ceiling for that backoff |

Both are latency budgets. Timeouts shorter than a round trip across your mesh cause spurious
leader changes; on a WAN mesh raise them.

## Running on mistlib

On native, `MistTransport` wraps mistlib's `L1Transport` and hands you the receive half to wire
into your dispatcher:

```rust
let (transport, inbound_rx) = MistTransport::new(l1_transport);
let node = RaftNode::new(id, peers, config, runtime, Arc::new(transport), storage);
node.attach_inbound(inbound_rx);
node.start();
```

Call `transport.handle_incoming(&from, &bytes)` from your `L1Notifier`. `HotStuffMistTransport`
has the identical shape.

In the browser the network usually lives on the JS side, so the wasm build takes a send callback
instead:

```js
import init, { WasmRaftNode, initPanicHook } from "./pkg/mistlib_consensus_wasm.js";

await init();
initPanicHook();

const node = new WasmRaftNode("a", JSON.stringify(["b", "c"]), null);
node.setSendCallback((toNodeId, bytes) => mesh.sendTo(toNodeId, bytes));
mesh.onMessage((from, bytes) => node.handleIncoming(from, bytes));
node.onCommitted((bytes) => applyToStateMachine(bytes));
node.start();

const index = await node.propose(new TextEncoder().encode("hello"));
```

## Building from source

```sh
cargo test --workspace

rustup target add wasm32-unknown-unknown
cd mistlib-consensus-wasm && wasm-pack build --target web --release
```

## Architecture

Both engines are sans-IO: a pure state machine with no clock, network or async, wrapped by a
driver that owns the timers and the event loop.

- [`docs/architecture.md`](docs/architecture.md) — crate layout, the four layers, adapters, and
  known deviations from the Raft paper
- [`docs/hotstuff-design.md`](docs/hotstuff-design.md) — HotStuff voting rules, the 3-chain
  commit, safety argument, and v1 limitations

## Status

Experimental. Usable for prototypes and research; not hardened for production. No external
security audit has been done. Specifically:

- **The bundled stores are in-memory.** `MemoryStorage` / `MemoryBlockStore` do not survive a
  restart. For HotStuff this is safety-critical, not merely inconvenient: a replica that forgets
  `last_voted_view` or `locked_qc` can be induced to equivocate after a restart. Production needs
  disk-backed implementations of `RaftStorage` / `BlockStore`.
- **Membership changes are operator-trusted.** `add_peer` / `remove_peer` apply directly rather
  than replicating through the log, and there is no BFT reconfiguration.
- **No block-sync protocol.** A HotStuff replica that misses a proposal whose parent it never
  stored ignores descendants until the pacemaker catches it up. Fine over reliable ordered links;
  a fetch-missing-ancestors protocol is future work.
- **Quorum certificates are O(n).** A vector of Ed25519 signatures; threshold/BLS aggregation is
  future work.
- **Inbound flow control is partial.** Wire messages are size-capped, but inbound channels are
  unbounded. Per-peer rate limiting is future work.

The API is not stable and the wire format may change between releases.

## License

[Mozilla Public License 2.0](LICENSE) — the same license as
[mistlib](https://github.com/tik-choco-lab/mistlib), the engine this builds on.
