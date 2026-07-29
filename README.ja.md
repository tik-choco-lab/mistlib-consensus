<div align="center">

# mistlib-consensus

**P2P メッシュのための複製ステートマシン — ピアを信頼できるなら Raft、できないなら HotStuff。**

[![status](https://img.shields.io/badge/status-experimental-orange)](#ステータス)
[![engine](https://img.shields.io/badge/mistlib-v0.6.0-2f6feb)](https://github.com/tik-choco-lab/mistlib)
[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

[English](README.md) · 日本語 · [简体中文](README.zh.md)

</div>

---

P2P メッシュでは各ノードが世界の複製を持ち、複製はやがてずれます。このライブラリはその下に順序
付きログを敷きます。どのノードからコマンドを提案しても、全ノードが同じコマンドを同じ順序で適用
します。どちらのエンジンがそれを担うかは、ピアが何をしうるか — 停止するだけか、嘘をつくか — で
決まります。

[mistlib](https://github.com/tik-choco-lab/mistlib) のランタイムとトランスポート抽象の上に構築
しています。Rust 製で、コアはネイティブに加えて `wasm32-unknown-unknown` 向けにもビルドできる
ため、同じ合意処理がブラウザのタブでもサーバでも動きます。

> [!WARNING]
> 実験的な実装であり、セキュリティ監査を受けていません。依存する前に
> [ステータス](#ステータス)を読んでください。

## 合意モード

| モード | 障害モデル | エンジン | 用途 |
| --- | --- | --- | --- |
| `ConsensusMode::Trusted` | クラッシュ障害 (CFT)、`f = (n-1)/2` まで許容 | Raft | 自分で運用するピアだけのメッシュ |
| `ConsensusMode::Untrusted` | ビザンチン障害 (BFT)、`f = (n-1)/3` まで許容 | chained HotStuff | ピアが嘘をつきうるメッシュ |

`ConsensusNode` ファサードが単一の `propose` / `subscribe_committed` API の裏にエンジンの選択を
隠すため、モードは構築時の判断であって、アプリケーションのコードには現れません。

> [!IMPORTANT]
> 2つのワイヤプロトコルには互換性がありません。クラスタ内の全ノードを同じモードでビルドする
> 必要があります。

## 導入

crates.io では配布していません。`mistlib-core` が git 依存として配布されており、crates.io は
git 依存を受け付けないためです。このリポジトリを直接参照してください。

```toml
[dependencies]
mistlib-consensus-core = { git = "https://github.com/tik-choco-lab/mistlib-consensus.git", rev = "<commit>" }
```

まだリリースタグを切っていないので、再現可能なビルドが必要なら `rev` を固定してください。
エンジン本体は [mistlib](https://github.com/tik-choco-lab/mistlib) のタグ固定依存として一緒に
入ります。ほかに用意するものはありません。

## 使い方

```rust
use mistlib_consensus_core::{ConsensusNode, NodeId, RaftConfig, RaftNode, MemoryStorage};

// 信頼できるメッシュ — Raft。
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

// 信頼できないメッシュ — Ed25519 quorum certificate 付き chained HotStuff。
// 署名鍵と validator 公開鍵レジストリは呼び出し側が用意します。
// 鍵の配布はこのライブラリの範囲外です。
let hs = HotStuffNode::new(
    id, validators, PacemakerConfig::default(),
    runtime.clone(), transport,
    MemoryBlockStore::new(),
    Ed25519Crypto::new(signing_key, registry),
);
hs.start();
let node = ConsensusNode::untrusted(hs);
```

どちらでも、その先は同じです。

```rust
// 成功するのは現在のリーダーだけ。
let seq = node.propose(b"command".to_vec()).await?;

// コミットされたコマンドを全ノードで同じ順序に適用する。
let mut committed = node.subscribe_committed(&runtime);
while let Some(cmd) = committed.recv().await {
    apply(cmd.sequence, &cmd.command);
}
```

## API

| 呼び出し | 動作 |
| --- | --- |
| `propose(bytes)` | コマンドを投入する。受け付けるのはリーダーのみで、シーケンス番号を返す |
| `subscribe_committed(&runtime)` | `CommittedCommand { sequence, view, command }` の順序付きストリーム |
| `is_leader()` / `leader_hint()` | 自ノードがリーダーか、誰がリーダーだと考えているか |
| `add_peer(id)` / `remove_peer(id)` | メンバー集合を変更する（運用者信頼 — ステータス参照） |
| `shutdown()` | ドライバタスクを停止する |

`sequence` はどちらのモードでも 1 から始まる連番で、欠番はありません。

テストや準信頼メッシュ向けに、`NoopCrypto` は HotStuff の決定的なリーダーローテーションを
保ったまま署名検証を無効化します。これはエンジンをビザンチン耐性のないものにするので、鍵配布の
問題を黙らせる目的で使わないでください。

## 設定

Raft のタイミング — `RaftConfig`:

| キー | 既定値 | 意味 |
| --- | --- | --- |
| `election_timeout_min_ms` | `150` | ランダム化された選挙タイムアウトの下限 |
| `election_timeout_max_ms` | `300` | 同じくその上限 |
| `heartbeat_interval_ms` | `50` | リーダーのハートビート間隔。選挙タイムアウト下限より十分小さくすること |

HotStuff のペースメーカ — `PacemakerConfig`:

| キー | 既定値 | 意味 |
| --- | --- | --- |
| `base_timeout_ms` | `400` | view タイムアウト。QC が view を進めるまで、連続タイムアウトごとに倍化する |
| `max_timeout_ms` | `8000` | そのバックオフの上限 |

いずれもレイテンシ予算です。メッシュの往復時間より短いタイムアウトは不要なリーダー交代を招くので、
WAN 越しのメッシュでは大きくしてください。

## mistlib 上で動かす

ネイティブでは `MistTransport` が mistlib の `L1Transport` を包み、ディスパッチャに繋ぐための
受信側を返します。

```rust
let (transport, inbound_rx) = MistTransport::new(l1_transport);
let node = RaftNode::new(id, peers, config, runtime, Arc::new(transport), storage);
node.attach_inbound(inbound_rx);
node.start();
```

`L1Notifier` から `transport.handle_incoming(&from, &bytes)` を呼んでください。
`HotStuffMistTransport` も同じ形です。

ブラウザではネットワーク実装が JS 側にあるのが普通なので、wasm ビルドは代わりに送信コールバックを
受け取ります。

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

## ソースからのビルド

```sh
cargo test --workspace

rustup target add wasm32-unknown-unknown
cd mistlib-consensus-wasm && wasm-pack build --target web --release
```

## アーキテクチャ

両エンジンとも sans-IO です。クロックもネットワークも async も持たない純粋なステートマシンを、
タイマーとイベントループを所有するドライバが包みます。

- [`docs/architecture.md`](docs/architecture.md) — クレート構成、4つの層、アダプタ、Raft 論文
  からの既知の逸脱
- [`docs/hotstuff-design.md`](docs/hotstuff-design.md) — HotStuff の投票規則、3-chain コミット、
  安全性の議論、v1 の制限

## ステータス

実験的な実装です。プロトタイプや研究には使えますが、本番向けの堅牢化はしていません。外部の
セキュリティ監査も受けていません。具体的には:

- **同梱のストアはメモリ上のみ。** `MemoryStorage` / `MemoryBlockStore` は再起動を越えて残り
  ません。HotStuff ではこれは不便というだけでなく**安全性に直結**します。`last_voted_view` や
  `locked_qc` を忘れたレプリカは、再起動後に二重投票させられうるからです。本番では
  `RaftStorage` / `BlockStore` のディスク永続実装が必要です。
- **メンバー変更は運用者信頼。** `add_peer` / `remove_peer` はログを経由せず直接適用され、BFT
  再構成もありません。
- **ブロック同期プロトコルがない。** 親ブロックを保存していない提案を取りこぼした HotStuff
  レプリカは、ペースメーカが追いつかせるまでその子孫を無視します。信頼できる順序保証付きリンク
  上なら問題ありませんが、欠落祖先の取得プロトコルは今後の課題です。
- **Quorum certificate のサイズが O(n)。** Ed25519 署名のベクタです。閾値署名 / BLS 集約は
  今後の課題です。
- **受信側のフロー制御が部分的。** ワイヤメッセージにはサイズ上限がありますが、受信チャネルは
  無制限です。ピア単位のレート制限は今後の課題です。

API は安定しておらず、ワイヤフォーマットもリリース間で変わりえます。

## ライセンス

[Mozilla Public License 2.0](LICENSE) — 土台である
[mistlib](https://github.com/tik-choco-lab/mistlib) と同じライセンスです。
