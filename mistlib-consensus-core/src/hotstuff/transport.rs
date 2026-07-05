//! Transport abstraction for HotStuff, mirroring
//! [`crate::transport::RaftTransport`] / [`crate::transport::MistTransport`].

use super::message::HotStuffMessage;
use crate::error::{RaftError, Result};
use async_trait::async_trait;
use bincode::Options;
use mistlib_core::layers::l1::{L1Notifier, L1Transport};
use mistlib_core::types::{DeliveryMethod, HostSendSync, NodeId};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Upper bound on a single serialized consensus message accepted from
/// the network. Bounds what an untrusted peer can make us allocate;
/// generously above any legitimate proposal unless application
/// commands are huge (batch or chunk them below this at the app layer).
pub const MAX_WIRE_MESSAGE_BYTES: u64 = 4 * 1024 * 1024;

/// Abstraction the HotStuff driver sends messages through. Separate
/// from `L1Transport` so the core can be tested with mock transports
/// and the wasm crate can plug in a JS-callback implementation.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait HotStuffTransport: HostSendSync {
    async fn send(&self, to: &NodeId, msg: HotStuffMessage) -> Result<()>;
}

/// Adapts an `Arc<dyn L1Transport>` into a [`HotStuffTransport`],
/// serializing with bincode and delivering reliable/ordered — HotStuff
/// stays safe under reordering, but ordered delivery keeps vote
/// aggregation and the pacemaker efficient.
pub struct HotStuffMistTransport {
    inner: Arc<dyn L1Transport>,
    inbound_tx: mpsc::UnboundedSender<(NodeId, HotStuffMessage)>,
}

impl HotStuffMistTransport {
    /// Returns the adapter plus the receiving half of the inbound
    /// channel; the `HotStuffNode` driver owns the receiver and folds
    /// incoming messages into its event loop.
    pub fn new(
        inner: Arc<dyn L1Transport>,
    ) -> (Self, mpsc::UnboundedReceiver<(NodeId, HotStuffMessage)>) {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        (Self { inner, inbound_tx }, inbound_rx)
    }

    /// Feed raw bytes received from the mesh into this transport so
    /// they get decoded and routed to the owning `HotStuffNode`.
    ///
    /// Unlike the Raft transport this faces explicitly untrusted peers,
    /// so decoding is capped at [`MAX_WIRE_MESSAGE_BYTES`] to bound the
    /// allocation a crafted message can force.
    pub fn handle_incoming(&self, from: &NodeId, data: &[u8]) -> Result<()> {
        if data.len() as u64 > MAX_WIRE_MESSAGE_BYTES {
            return Err(RaftError::Transport(format!(
                "oversized consensus message from {from}: {} bytes (max {MAX_WIRE_MESSAGE_BYTES})",
                data.len()
            )));
        }
        // Same wire format as `bincode::serialize` on the send side
        // (fixint, little-endian, trailing bytes tolerated) plus an
        // allocation limit for untrusted input.
        let msg: HotStuffMessage = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .with_limit(MAX_WIRE_MESSAGE_BYTES)
            .deserialize(data)?;
        self.inbound_tx
            .send((from.clone(), msg))
            .map_err(|_| RaftError::ChannelClosed)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl HotStuffTransport for HotStuffMistTransport {
    async fn send(&self, to: &NodeId, msg: HotStuffMessage) -> Result<()> {
        let bytes = bincode::serialize(&msg)?;
        self.inner
            .send_message(to, bytes.into(), DeliveryMethod::ReliableOrdered)
            .await
            .map_err(|e| RaftError::Transport(e.to_string()))
    }
}

impl L1Notifier for HotStuffMistTransport {
    fn notify_connected(&self, _node_id: &NodeId) {
        // Membership is managed explicitly via the node's
        // `add_peer`/`remove_peer`; see `MistTransport` for rationale.
    }

    fn notify_disconnected(&self, _node_id: &NodeId) {}

    fn notify_node_position_updated(&self, _node_id: &NodeId, _x: f32, _y: f32, _z: f32) {}
}
