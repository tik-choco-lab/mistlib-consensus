use crate::runtime::TokioRuntime;
use mistlib_consensus_core::{RaftConfig, RaftNode, RaftStorage, RaftTransport};
use mistlib_core::types::NodeId;
use std::sync::Arc;

/// Convenience constructor for native binaries: builds a
/// [`RaftNode`] backed by [`TokioRuntime::current`] and immediately
/// starts its event loop / timers.
///
/// Must be called from within a Tokio runtime context (e.g. inside
/// `#[tokio::main]` or a `#[tokio::test]`).
pub fn spawn_native<S: RaftStorage + 'static>(
    id: NodeId,
    peers: Vec<NodeId>,
    config: RaftConfig,
    transport: Arc<dyn RaftTransport>,
    storage: S,
) -> Arc<RaftNode<S>> {
    let runtime: Arc<dyn mistlib_core::runtime::AsyncRuntime> = Arc::new(TokioRuntime::current());
    let node = RaftNode::new(id, peers, config, runtime, transport, storage);
    node.start();
    node
}
