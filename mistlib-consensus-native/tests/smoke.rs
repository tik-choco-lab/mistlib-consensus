use async_trait::async_trait;
use mistlib_consensus_native::{
    spawn_native, MemoryStorage, NodeId, RaftConfig, RaftMessage, RaftTransport, Role,
};
use std::sync::Arc;
use std::time::Duration;

struct NullTransport;

#[async_trait]
impl RaftTransport for NullTransport {
    async fn send(&self, _to: &NodeId, _msg: RaftMessage) -> mistlib_consensus_native::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn single_node_cluster_becomes_leader() {
    let node = spawn_native(
        NodeId("solo".into()),
        vec![],
        RaftConfig::default(),
        Arc::new(NullTransport) as Arc<dyn RaftTransport>,
        MemoryStorage::new(),
    );

    // A single-node cluster is its own quorum, so it should become
    // leader as soon as the first election timeout (or immediately, per
    // the single-node fast path in `become_candidate`) fires.
    let became_leader = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if node.role() == Some(Role::Leader) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(became_leader.is_ok(), "node never became leader");

    let index = node
        .propose(b"hello".to_vec())
        .await
        .expect("propose should succeed");
    assert_eq!(index, 1);

    node.shutdown();
}
