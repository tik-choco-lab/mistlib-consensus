<div align="center">

# mistlib-consensus

**面向点对点网格的复制状态机 —— 信得过对等节点就用 Raft，信不过就用 HotStuff。**

[![status](https://img.shields.io/badge/status-experimental-orange)](#状态)
[![engine](https://img.shields.io/badge/mistlib-v0.7.1-2f6feb)](https://github.com/tik-choco-lab/mistlib)
[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

[English](README.md) · [日本語](README.ja.md) · 简体中文

</div>

---

在点对点网格中，每个节点都持有世界的一份副本，而副本终将产生分歧。本库在其下铺设一条有序日志：
在任意节点提交命令，所有节点都会以相同顺序应用相同的命令。由哪个引擎来完成这件事，取决于对等
节点可能做什么 —— 只是崩溃，还是会说谎。

构建在 [mistlib](https://github.com/tik-choco-lab/mistlib) 的运行时与传输抽象之上。使用 Rust
编写，核心除原生目标外还可编译到 `wasm32-unknown-unknown`，因此同一套共识既能跑在浏览器标签页
里，也能跑在服务器上。

> [!WARNING]
> 本项目属实验性质，未经过安全审计。在依赖它之前，请先阅读[状态](#状态)。

## 共识模式

| 模式 | 故障模型 | 引擎 | 适用场景 |
| --- | --- | --- | --- |
| `ConsensusMode::Trusted` | 崩溃故障 (CFT)，容忍 `f = (n-1)/2` | Raft | 由你自己运维的对等节点组成的网格 |
| `ConsensusMode::Untrusted` | 拜占庭故障 (BFT)，容忍 `f = (n-1)/3` | chained HotStuff | 对等节点可能说谎的网格 |

`ConsensusNode` 门面把引擎的选择隐藏在统一的 `propose` / `subscribe_committed` API 之后，因此
模式是构建时的决定，不会出现在你的应用代码里。

> [!IMPORTANT]
> 两种线路协议互不兼容。集群中所有节点必须以相同模式构建。

## 安装

未发布到 crates.io —— `mistlib-core` 以 git 依赖形式分发，而 crates.io 不接受 git 依赖。请直接
依赖本仓库：

```toml
[dependencies]
mistlib-consensus-core = { git = "https://github.com/tik-choco-lab/mistlib-consensus.git", rev = "<commit>" }
```

目前尚未打过发布标签，如需可重现的构建请固定 `rev`。引擎本身会作为
[mistlib](https://github.com/tik-choco-lab/mistlib) 的标签固定依赖一并引入，无需另行准备。

## 用法

```rust
use mistlib_consensus_core::{ConsensusNode, NodeId, RaftConfig, RaftNode, MemoryStorage};

// 可信网格 —— Raft。
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

// 不可信网格 —— 带 Ed25519 法定人数证书的 chained HotStuff。
// 签名密钥与验证者公钥注册表由调用方提供；
// 密钥分发不在本库范围内。
let hs = HotStuffNode::new(
    id, validators, PacemakerConfig::default(),
    runtime.clone(), transport,
    MemoryBlockStore::new(),
    Ed25519Crypto::new(signing_key, registry),
);
hs.start();
let node = ConsensusNode::untrusted(hs);
```

无论哪一种，之后的部分完全相同：

```rust
// 只有当前的领导者才会成功。
let seq = node.propose(b"command".to_vec()).await?;

// 在每个节点上按顺序应用已提交的命令。
let mut committed = node.subscribe_committed(&runtime);
while let Some(cmd) = committed.recv().await {
    apply(cmd.sequence, &cmd.command);
}
```

## API

| 调用 | 作用 |
| --- | --- |
| `propose(bytes)` | 提交一条命令。只有领导者会接受，返回其序号 |
| `subscribe_committed(&runtime)` | `CommittedCommand { sequence, view, command }` 的有序流 |
| `is_leader()` / `leader_hint()` | 本节点是否为领导者，以及它认为谁是领导者 |
| `add_peer(id)` / `remove_peer(id)` | 变更成员集合（运维者可信 —— 见状态一节） |
| `shutdown()` | 停止驱动任务 |

在两种模式下，`sequence` 都从 1 开始且没有空洞。

面向测试或半可信网格，`NoopCrypto` 在保留 HotStuff 确定性领导者轮换的同时关闭签名校验。它会让
引擎失去拜占庭容错能力 —— 不要用它来掩盖密钥分发上的问题。

## 配置

Raft 计时 —— `RaftConfig`：

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `election_timeout_min_ms` | `150` | 随机化选举超时的下界 |
| `election_timeout_max_ms` | `300` | 同一超时的上界 |
| `heartbeat_interval_ms` | `50` | 领导者心跳间隔；应远小于选举超时下界 |

HotStuff 起搏器 —— `PacemakerConfig`：

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `base_timeout_ms` | `400` | 视图超时；在 QC 推进视图之前，每次连续超时翻倍 |
| `max_timeout_ms` | `8000` | 该退避的上限 |

两者都是延迟预算。短于网格往返时间的超时会引发无谓的领导者更替；跨广域网的网格应调大。

## 在 mistlib 上运行

在原生环境中，`MistTransport` 包装 mistlib 的 `L1Transport`，并把接收端交给你接入自己的分发器：

```rust
let (transport, inbound_rx) = MistTransport::new(l1_transport);
let node = RaftNode::new(id, peers, config, runtime, Arc::new(transport), storage);
node.attach_inbound(inbound_rx);
node.start();
```

请从你的 `L1Notifier` 调用 `transport.handle_incoming(&from, &bytes)`。`HotStuffMistTransport`
的形态完全相同。

在浏览器中，网络实现通常位于 JS 一侧，因此 wasm 构建改为接收一个发送回调：

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

## 从源码构建

```sh
cargo test --workspace

rustup target add wasm32-unknown-unknown
cd mistlib-consensus-wasm && wasm-pack build --target web --release
```

## 架构

两个引擎都是 sans-IO 的：一个不接触时钟、网络与 async 的纯状态机，外面包一层持有定时器和事件
循环的驱动。

- [`docs/architecture.md`](docs/architecture.md) —— crate 布局、四个层次、适配器，以及相对
  Raft 论文的已知偏离
- [`docs/hotstuff-design.md`](docs/hotstuff-design.md) —— HotStuff 投票规则、3-chain 提交、
  安全性论证与 v1 限制

## 状态

实验性质。可用于原型与研究，但未针对生产环境做加固，也未经过外部安全审计。具体而言：

- **内置存储仅在内存中。** `MemoryStorage` / `MemoryBlockStore` 无法在重启后存活。对 HotStuff
  而言这不只是不便，而是**直接关乎安全性**：忘记 `last_voted_view` 或 `locked_qc` 的副本可能在
  重启后被诱导做出相互矛盾的投票。生产环境需要 `RaftStorage` / `BlockStore` 的磁盘持久化实现。
- **成员变更依赖运维者可信。** `add_peer` / `remove_peer` 直接生效，不经过日志复制，也没有 BFT
  重配置。
- **没有区块同步协议。** 若 HotStuff 副本错过了一个其父块从未存储过的提案，它会忽略其后代，
  直到起搏器把它追上。在可靠有序的链路上没有问题，但拉取缺失祖先的协议仍是后续工作。
- **法定人数证书的规模为 O(n)。** 它是一个 Ed25519 签名向量；门限签名 / BLS 聚合仍是后续工作。
- **入站流控不完整。** 线路消息有大小上限，但入站通道无界。按对等节点的限流仍是后续工作。

API 尚不稳定，线路格式也可能在版本之间变化。

## 许可

[Mozilla Public License 2.0](LICENSE) —— 与其所构建于之上的
[mistlib](https://github.com/tik-choco-lab/mistlib) 采用相同许可。
