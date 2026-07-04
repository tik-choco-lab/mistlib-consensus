use crate::error::{RaftError, Result};
use crate::message::RaftMessage;
use async_trait::async_trait;
use mistlib_core::layers::l1::{L1Notifier, L1Transport};
use mistlib_core::types::{DeliveryMethod, HostSendSync, NodeId};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Abstraction the Raft driver sends messages through. Kept separate
/// from `mistlib_core::layers::l1::L1Transport` so the core crate can be
/// tested with trivial mock transports, and so the wasm crate can plug
/// in a JS-callback-based implementation instead of a real `L1Transport`.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RaftTransport: HostSendSync {
    async fn send(&self, to: &NodeId, msg: RaftMessage) -> Result<()>;
}

/// Adapts an `Arc<dyn L1Transport>` (mistlib's mesh transport) into a
/// [`RaftTransport`], serializing messages with bincode and delivering
/// them reliably/ordered, as Raft's correctness relies on FIFO delivery
/// per link (out-of-order AppendEntries is handled correctly but
/// wastefully; reliable-ordered keeps things efficient).
pub struct MistTransport {
    inner: Arc<dyn L1Transport>,
    inbound_tx: mpsc::UnboundedSender<(NodeId, RaftMessage)>,
}

impl MistTransport {
    /// Returns the adapter plus the receiving half of the inbound
    /// channel; the `RaftNode` driver owns the receiver and folds
    /// incoming messages into its event loop.
    pub fn new(
        inner: Arc<dyn L1Transport>,
    ) -> (Self, mpsc::UnboundedReceiver<(NodeId, RaftMessage)>) {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        (Self { inner, inbound_tx }, inbound_rx)
    }

    /// Feed raw bytes received from the mesh (e.g. from an
    /// `L1Notifier`-driven message dispatcher) into this transport so
    /// they get decoded and routed to the owning `RaftNode`.
    pub fn handle_incoming(&self, from: &NodeId, data: &[u8]) -> Result<()> {
        let msg: RaftMessage = bincode::deserialize(data)?;
        self.inbound_tx
            .send((from.clone(), msg))
            .map_err(|_| RaftError::ChannelClosed)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl RaftTransport for MistTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) -> Result<()> {
        let bytes = bincode::serialize(&msg)?;
        self.inner
            .send_message(to, bytes.into(), DeliveryMethod::ReliableOrdered)
            .await
            .map_err(|e| RaftError::Transport(e.to_string()))
    }
}

impl L1Notifier for MistTransport {
    fn notify_connected(&self, _node_id: &NodeId) {
        // No-op by default: membership is managed explicitly via
        // `RaftNode::add_peer`/`remove_peer`. Callers that want
        // automatic membership changes on mesh (dis)connect can wrap
        // this notifier and call those methods themselves.
    }

    fn notify_disconnected(&self, _node_id: &NodeId) {}

    fn notify_node_position_updated(&self, _node_id: &NodeId, _x: f32, _y: f32, _z: f32) {}
}
