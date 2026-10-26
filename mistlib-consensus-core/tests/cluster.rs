//! In-process, multi-node integration test using a mock in-memory
//! transport and a full Tokio runtime (this test is not compiled for
//! wasm32: it needs `tokio`'s multi-thread scheduler and time control,
//! neither of which the core crate depends on).
#![cfg(not(target_arch = "wasm32"))]

use async_trait::async_trait;
use mistlib_consensus_core::{
    LogEntry, MemoryStorage, NodeId, RaftConfig, RaftMessage, RaftNode, RaftTransport, Role,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::mpsc;
use web_time::Duration;

/// Trivial `AsyncRuntime` wrapping the ambient Tokio runtime.
struct TestRuntime(tokio::runtime::Handle);

#[async_trait]
impl mistlib_core::runtime::AsyncRuntime for TestRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.0.spawn(future);
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(StdDuration::from_millis(duration.as_millis() as u64)).await;
    }
}

/// Routes messages directly between in-process nodes via channels,
/// standing in for `MistTransport` + a real `L1Transport` mesh.
struct MockTransport {
    routes: HashMap<NodeId, mpsc::UnboundedSender<(NodeId, RaftMessage)>>,
    from: NodeId,
}

#[async_trait]
impl RaftTransport for MockTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) -> mistlib_consensus_core::Result<()> {
        if let Some(tx) = self.routes.get(to) {
            let _ = tx.send((self.from.clone(), msg));
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader_and_replicates() {
    let ids: Vec<NodeId> = ["n1", "n2", "n3"]
        .iter()
        .map(|s| NodeId(s.to_string()))
        .collect();

    let mut inbound_txs = HashMap::new();
    let mut inbound_rxs = HashMap::new();
    for id in &ids {
        let (tx, rx) = mpsc::unbounded_channel();
        inbound_txs.insert(id.clone(), tx);
        inbound_rxs.insert(id.clone(), rx);
    }

    let config = RaftConfig {
        election_timeout_min_ms: 60,
        election_timeout_max_ms: 120,
        heartbeat_interval_ms: 20,
    };

    let runtime: Arc<dyn mistlib_core::runtime::AsyncRuntime> =
        Arc::new(TestRuntime(tokio::runtime::Handle::current()));

    let mut nodes = HashMap::new();
    for id in &ids {
        let peers: Vec<NodeId> = ids.iter().filter(|p| *p != id).cloned().collect();
        let transport: Arc<dyn RaftTransport> = Arc::new(MockTransport {
            routes: inbound_txs.clone(),
            from: id.clone(),
        });
        let node = RaftNode::new(
            id.clone(),
            peers,
            config,
            Arc::clone(&runtime),
            transport,
            MemoryStorage::new(),
        );
        node.attach_inbound(inbound_rxs.remove(id).unwrap());
        node.start();
        nodes.insert(id.clone(), node);
    }

    // Wait for a leader to emerge.
    let leader_id = wait_for_leader(&nodes).await;

    // Subscribe every node to committed entries before proposing so we
    // don't race the first proposal's replication.
    let mut committed_rxs: HashMap<NodeId, tokio::sync::broadcast::Receiver<LogEntry>> = nodes
        .iter()
        .map(|(id, n)| (id.clone(), n.subscribe_committed()))
        .collect();

    let leader = &nodes[&leader_id];
    let mut proposed_indices = Vec::new();
    for i in 0..5u8 {
        let index = leader
            .propose(vec![i])
            .await
            .expect("leader propose should succeed");
        proposed_indices.push(index);
    }

    // Every node should observe the same committed sequence of commands.
    for (id, rx) in committed_rxs.iter_mut() {
        let mut applied = Vec::new();
        while applied.len() < 5 {
            let entry = tokio::time::timeout(StdDuration::from_secs(5), rx.recv())
                .await
                .unwrap_or_else(|_| panic!("node {id:?} timed out waiting for commits"))
                .expect("committed channel closed unexpectedly");
            applied.push(entry.command[0]);
        }
        assert_eq!(
            applied,
            vec![0, 1, 2, 3, 4],
            "node {id:?} applied out of order"
        );
    }

    for node in nodes.values() {
        node.shutdown();
    }
}

async fn wait_for_leader(nodes: &HashMap<NodeId, Arc<RaftNode<MemoryStorage>>>) -> NodeId {
    tokio::time::timeout(StdDuration::from_secs(5), async {
        loop {
            for (id, node) in nodes {
                if node.role() == Some(Role::Leader) {
                    return id.clone();
                }
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("cluster failed to elect a leader in time")
}
