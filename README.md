# mistlib-consensus

`mistlib-dev` の上に構築した Raft コンセンサス実装。ネイティブと
`wasm32-unknown-unknown` の両方でビルド可能な、プラットフォーム非依存の
コア (`mistlib-consensus-core`) と、それぞれの実行環境向けアダプタ
(`mistlib-consensus-native` / `mistlib-consensus-wasm`) で構成される。

## `mistlib-core` への依存

`mistlib-core` は git 依存として `rev` 固定で参照している
（`https://github.com/tik-choco-lab/mistlib-dev.git`）。以前は
`../../mistlib-dev/mistlib-core` への相対パス依存だったため、この
リポジトリ単体でベンダリング/クローンすると `mistlib-dev` を特定の
兄弟ディレクトリに配置しない限りビルドできなかった（[#1](https://github.com/tik-choco-lab/mistlib-consensus/issues/1)）。
git 依存化によりこの制約はなくなっている。

`mistlib-core` 側の更新を取り込みたい場合は、各 `Cargo.toml` の
`mistlib-core = { git = "...", rev = "..." }` の `rev` を
`mistlib-dev` の最新コミットハッシュに書き換え、`cargo build` で
`Cargo.lock` を更新すること。

## クレート構成

```
mistlib-consensus/
├── mistlib-consensus-core    # Raft本体（純粋・非同期・プラットフォーム非依存）
├── mistlib-consensus-native  # TokioRuntime + ネイティブ向け起動ヘルパー
└── mistlib-consensus-wasm    # WasmRuntime + wasm-bindgen FFI ラッパー
```

## アーキテクチャ

### 1. 純粋なステートマシン (`state.rs`)

`RaftState<S: RaftStorage>` は Raft のコアロジックを **同期・非async** で実装する。
`step(event: Event) -> Vec<Output>` という純関数型のインターフェースを持ち、

- `Event`: `Message`（他ノードからの RPC）, `ElectionTimeout`,
  `HeartbeatTimeout`, `ClientPropose`, `AddPeer`/`RemovePeer`
- `Output`: `SendMessage`, `ApplyCommitted`, `BecameLeader`/`BecameFollower`,
  `ResetElectionTimer`/`ResetHeartbeatTimer`, `Proposed`/`ProposeRejected`

を入出力する。I/O や時間を一切扱わないため、`mistlib-consensus-core/src/state.rs`
の `#[cfg(test)]` にあるユニットテストのように、モックなしで決定論的に
テストできる（選挙の多数決、より高い term によるフォロワー降格、
ログ複製とコミット位置の前進、競合エントリの切り詰め、古いログを持つ
候補者への投票拒否）。

### 2. トランスポート (`transport.rs`)

`RaftTransport` トレイトは `mistlib-core` の `L1Transport` と同じ
`#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]` パターンを踏襲し、
`send(&self, to, msg) -> Result<()>` のみを要求する薄い抽象。

- `MistTransport`: `Arc<dyn L1Transport>` をラップし、`bincode` で
  シリアライズして `DeliveryMethod::ReliableOrdered` で送信する。
  受信側は `handle_incoming(from, bytes)` でバイト列を投入すると、
  内部の `mpsc` チャンネル経由で `RaftNode` に届く
  （`RaftNode::attach_inbound` で購読する）。
- `mistlib-consensus-wasm::CallbackTransport`: 実際の送信を
  JavaScript 側のコールバックに委譲する（後述）。

### 3. 駆動ループ (`node.rs`)

`RaftNode<S>` は `RaftState<S>` を async イベントループで駆動する:

- `AsyncRuntime::spawn` でイベントループ本体・選挙タイマー・
  ハートビートタイマーの3タスクを起動する。
- `tokio::select!` は使わない（`mistlib-core` の `tokio` 依存が
  `features = ["sync"]` のみで `macros`/`rt` を含まないため）。
  代わりに、選挙タイマーは `AtomicU64` の epoch カウンタで
  「スリープ中にリセットされたか」を判定する方式で再実装している。
- タイムアウトは `election_timeout_min_ms..=election_timeout_max_ms`
  からランダムに選ぶ（既定 150–300ms）。ハートビート間隔は既定 50ms。
- 状態機械の変更はすべて単一タスク内でのみ行われるため、
  排他制御は不要。`role()`/`leader_hint()` など外部公開の読み取り用に
  `std::sync::Mutex<Status>` を一つだけ使っている。

### 4. ストレージ (`storage.rs`)

`RaftStorage` トレイトは term・投票先・ログを永続化する責務を持つ。
`MemoryStorage` はテスト・単一プロセス用の非永続実装。

## 公開 API（`mistlib-consensus-core`）

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

// リーダーの場合のみ成功し、ログ index を返す
let index = node.propose(b"command".to_vec()).await?;

// コミットされたエントリを購読
let mut rx = node.subscribe_committed();
while let Ok(entry) = rx.recv().await { /* entry.command を適用 */ }

node.role();         // Option<Role>
node.leader_hint();  // Option<NodeId>
node.add_peer(id);
node.remove_peer(id);
node.shutdown();
```

## `mistlib-consensus-native`

```rust
let node = mistlib_consensus_native::spawn_native(
    NodeId("a".into()), peers, RaftConfig::default(), transport, MemoryStorage::new(),
);
```
`TokioRuntime::current()`（アンビエントな Tokio ランタイムのハンドル）を
使って `RaftNode` を構築し、即座に `start()` する便利関数。

## `mistlib-consensus-wasm`

`wasm-pack build --target web` でビルドし、JS から次のように使う:

```js
import init, { WasmRaftNode, initPanicHook } from "./pkg/mistlib_consensus_wasm.js";

await init();
initPanicHook();

const node = new WasmRaftNode("a", JSON.stringify(["b", "c"]), null);

// 実際のネットワーク送信は呼び出し側が実装する
// （WebSocket や mistlib の L1Transport への橋渡しなど）
node.setSendCallback((toNodeId, bytes) => {
  mesh.sendTo(toNodeId, bytes);
});

// 受信したバイト列をここに流し込む
mesh.onMessage((from, bytes) => node.handleIncoming(from, bytes));

node.onCommitted((bytes) => applyToStateMachine(bytes));

node.start();

const index = await node.propose(new TextEncoder().encode("hello"));
console.log(node.role(), node.leaderHint());
```

wasm 側は `AsyncRuntime` を `wasm_bindgen_futures::spawn_local` +
`gloo-timers` で実装した `WasmRuntime` を使う。トランスポートは
`mistlib-core::layers::l1::L1Transport` を直接使わず、JS コールバック
ベースの `CallbackTransport` を使う設計にしてある。これは、
wasm 環境では実際のネットワーク実装（WebSocket / WebRTC / mistlib の
メッシュ）が JS 側に存在することが多く、Rust 側で `L1Transport` の
具体実装を持たせるより、送受信のバイト列だけを橋渡しする方が
疎結合になるため。`mistlib-wasm` 側で `L1Transport` を実装している
場合は、その送信メソッドの中から `handleIncoming` を呼ぶ橋渡しコードを
JS（または `mistlib-wasm` 自体）に数行書くだけで統合できる。

## mistlib L1Transport との統合

ネイティブ側は `MistTransport::new(Arc<dyn L1Transport>)` で
`(RaftTransport, mpsc::UnboundedReceiver<(NodeId, RaftMessage)>)` を
得られる。受信経路は `L1Notifier`/メッセージディスパッチャ側から
`mist_transport.handle_incoming(&from, &bytes)` を呼び、返ってきた
receiver を `RaftNode::attach_inbound(rx)` に渡すだけでよい。

```rust
let (transport, inbound_rx) = MistTransport::new(l1_transport);
let node = RaftNode::new(id, peers, config, runtime, Arc::new(transport), storage);
node.attach_inbound(inbound_rx);
node.start();
```

## テスト

- `mistlib-consensus-core`: 純粋ステートマシンのユニットテスト5件
  （`state.rs`）+ モックトランスポートによる3ノードの結合テスト1件
  （`tests/cluster.rs`、`#[cfg(not(target_arch = "wasm32"))]`）。
- `mistlib-consensus-native`: 単一ノードクラスタがリーダーになり
  `propose` が成功することを確認するスモークテスト（`tests/smoke.rs`）。

```sh
cargo build --workspace
cargo test --workspace
rustup target add wasm32-unknown-unknown
cargo build -p mistlib-consensus-wasm --target wasm32-unknown-unknown
cargo clippy --workspace
```

## 既知の逸脱・注意点

- `RaftMessage::RequestVoteResponse`/`AppendEntriesResponse` に、仕様書の
  フィールド一覧にはなかった `voter_id`/`follower_id` を追加した。
  `RaftTransport::send` は宛先 (`to`) しか指定できず、応答メッセージの
  送信元が誰かを `RaftState` 側が判別する手段がないため
  （匿名の点対点リンクの向こう側から届く前提）、これを message 自体に
  含めている。
- メンバーシップ変更は仕様通り joint consensus なしの単純な
  `add_peer`/`remove_peer`（設定変更自体はログを経由しない）。
  本番運用では、より厳密には設定変更もログエントリとして複製すべき
  だが、今回のスコープでは意図的に単純化した。
- `RaftStorage::MemoryStorage` は非永続。実運用ではディスク永続化した
  実装に差し替える必要がある。
- `mistlib-consensus-wasm` の `WasmRuntime::sleep` は非 wasm32
  ターゲットでビルドする際、内部で `std::future::ready(())` を使う
  ノーオップにフォールバックする（`gloo-timers` の `Timeout` が
  `wasm_bindgen::Closure` を保持しており `Send` ではないため、
  `async-trait` の非wasm経路が要求する `Send` 境界を満たせない）。
  `WasmRuntime` は wasm32 以外で実際に使われることを意図していない。
