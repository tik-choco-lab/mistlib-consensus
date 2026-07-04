use crate::runtime::WasmRuntime;
use crate::transport::CallbackTransport;
use js_sys::{Function, Uint8Array};
use mistlib_consensus_core::{MemoryStorage, NodeId, RaftConfig, RaftNode, Role};
use serde::Deserialize;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WasmRaftConfig {
    #[serde(default = "default_min")]
    election_timeout_min_ms: u64,
    #[serde(default = "default_max")]
    election_timeout_max_ms: u64,
    #[serde(default = "default_heartbeat")]
    heartbeat_interval_ms: u64,
}

fn default_min() -> u64 {
    150
}
fn default_max() -> u64 {
    300
}
fn default_heartbeat() -> u64 {
    50
}

impl From<WasmRaftConfig> for RaftConfig {
    fn from(c: WasmRaftConfig) -> Self {
        RaftConfig {
            election_timeout_min_ms: c.election_timeout_min_ms,
            election_timeout_max_ms: c.election_timeout_max_ms,
            heartbeat_interval_ms: c.heartbeat_interval_ms,
        }
    }
}

/// wasm-bindgen entry point wrapping a [`RaftNode`] with an in-memory
/// store and a JS-callback-driven [`CallbackTransport`]. The actual
/// network I/O (WebSocket, WebRTC data channel, `mistlib-wasm`'s mesh,
/// ...) is entirely the embedder's responsibility: register a send
/// callback with `setSendCallback`, and forward received bytes in via
/// `handleIncoming`.
#[wasm_bindgen]
pub struct WasmRaftNode {
    inner: Arc<RaftNode<MemoryStorage>>,
    transport: Arc<CallbackTransport>,
}

#[wasm_bindgen]
impl WasmRaftNode {
    /// `peers_json` is a JSON array of peer id strings, e.g. `["b","c"]`.
    /// `config_json` is an optional JSON object with camelCase keys
    /// (`electionTimeoutMinMs`, `electionTimeoutMaxMs`,
    /// `heartbeatIntervalMs`); pass `null`/omit to use defaults.
    #[wasm_bindgen(constructor)]
    pub fn new(node_id: String, peers_json: String, config_json: Option<String>) -> Result<WasmRaftNode, JsError> {
        let peers: Vec<String> = serde_json::from_str(&peers_json)?;
        let peers: Vec<NodeId> = peers.into_iter().map(NodeId).collect();
        let config: RaftConfig = match config_json {
            Some(json) if !json.is_empty() => {
                serde_json::from_str::<WasmRaftConfig>(&json)?.into()
            }
            _ => RaftConfig::default(),
        };

        let transport = Arc::new(CallbackTransport::new());
        let runtime: Arc<dyn mistlib_core::runtime::AsyncRuntime> = Arc::new(WasmRuntime);
        let node = RaftNode::new(
            NodeId(node_id),
            peers,
            config,
            runtime,
            transport.clone() as Arc<dyn mistlib_consensus_core::RaftTransport>,
            MemoryStorage::new(),
        );

        Ok(WasmRaftNode { inner: node, transport })
    }

    /// Registers the JS function used to actually deliver outbound
    /// messages: `(toNodeId: string, bytes: Uint8Array) => void`.
    #[wasm_bindgen(js_name = setSendCallback)]
    pub fn set_send_callback(&self, cb: Function) {
        self.transport.set_send_callback(cb);
    }

    pub fn start(&self) {
        self.inner.start();
    }

    /// Registers a callback `(entryBytes: Uint8Array) => void` invoked
    /// once per committed log entry's command bytes, in commit order.
    #[wasm_bindgen(js_name = onCommitted)]
    pub fn on_committed(&self, cb: Function) {
        let mut rx = self.inner.subscribe_committed();
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                match rx.recv().await {
                    Ok(entry) => {
                        let array = Uint8Array::from(entry.command.as_slice());
                        let this = JsValue::NULL;
                        let _ = cb.call1(&this, &array);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    /// Feeds bytes received over whatever transport the embedder uses
    /// (WebSocket, WebRTC data channel, mistlib mesh, ...) into this
    /// node's Raft engine.
    #[wasm_bindgen(js_name = handleIncoming)]
    pub fn handle_incoming(&self, from: String, data: &[u8]) -> Result<(), JsError> {
        let msg = bincode::deserialize(data)?;
        self.inner.deliver(NodeId(from), msg);
        Ok(())
    }

    /// Proposes a command; resolves to the assigned log index, or
    /// rejects if this node isn't the leader.
    pub fn propose(&self, data: &[u8]) -> js_sys::Promise {
        let node = Arc::clone(&self.inner);
        let command = data.to_vec();
        future_to_promise(async move {
            node.propose(command)
                .await
                .map(|index| JsValue::from_f64(index as f64))
                .map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }

    pub fn role(&self) -> String {
        match self.inner.role() {
            Some(Role::Leader) => "leader".to_string(),
            Some(Role::Candidate) => "candidate".to_string(),
            Some(Role::Follower) | None => "follower".to_string(),
        }
    }

    #[wasm_bindgen(js_name = leaderHint)]
    pub fn leader_hint(&self) -> Option<String> {
        self.inner.leader_hint().map(|n| n.0)
    }

    pub fn shutdown(&self) {
        self.inner.shutdown();
    }
}

/// Initializes panic hook forwarding to the JS console; call once at
/// module load.
#[wasm_bindgen(js_name = initPanicHook)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}
