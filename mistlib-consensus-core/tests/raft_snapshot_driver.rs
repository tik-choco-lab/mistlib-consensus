#![cfg(not(target_arch = "wasm32"))]
mod common;

use common::{fast_config, test_runtime, wait_for, wait_for_leader_among, Network};
use mistlib_consensus_core::{MemoryStorage, NodeId, RaftApplied, RaftMembership, RaftNode, Role};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_lagging_replica_receives_snapshot_then_tail_in_application_order() {
    let ids: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|s| NodeId((*s).into()))
        .collect();
    let runtime = test_runtime();
    let (network, mut inbound) = Network::new(&ids);
    let mut nodes = HashMap::new();
    let mut streams = HashMap::new();
    for id in &ids {
        let node = RaftNode::new(
            id.clone(),
            ids.iter().filter(|p| *p != id).cloned().collect(),
            fast_config(),
            runtime.clone(),
            network.transport_for(id.clone()),
            MemoryStorage::new(),
        );
        node.attach_inbound(inbound.remove(id).unwrap());
        streams.insert(id.clone(), node.subscribe_applied());
        node.start();
        nodes.insert(id.clone(), node);
    }
    let leader_id = wait_for_leader_among(&nodes, &ids, Duration::from_secs(5)).await;
    let stopped = ids.iter().find(|id| **id != leader_id).unwrap().clone();
    nodes.get(&stopped).unwrap().shutdown();
    network.kill(&stopped);
    let leader = nodes.get(&leader_id).unwrap().clone();
    for command in 1..=8 {
        leader.propose(vec![command]).await.unwrap();
    }
    let rx = streams.get_mut(&leader_id).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut latest = 0;
        while latest < 8 {
            if let RaftApplied::Entries(entries) = rx.recv().await.unwrap() {
                latest = entries.last().unwrap().index;
            }
        }
    })
    .await
    .unwrap();
    leader.compact_snapshot(6, vec![1, 2, 3]);
    // The event queue processes compaction before this next proposal.
    assert_eq!(leader.propose(vec![9]).await.unwrap(), 9);
    let restored = RaftNode::new(
        stopped.clone(),
        ids.iter().filter(|p| **p != stopped).cloned().collect(),
        fast_config(),
        runtime,
        network.transport_for(stopped.clone()),
        MemoryStorage::new(),
    );
    restored.attach_inbound(network.revive(&stopped));
    let mut applied = restored.subscribe_applied();
    restored.start();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut boundary = 0;
        let mut tail = Vec::new();
        while tail.last().copied() != Some(9) {
            match applied.recv().await.unwrap() {
                RaftApplied::Snapshot(snapshot) => {
                    assert_eq!(boundary, 0, "snapshot must precede all tail entries");
                    assert_eq!(snapshot.last_included_index, 6);
                    assert_eq!(&*snapshot.data, &[1, 2, 3]);
                    boundary = 6;
                }
                RaftApplied::Entries(entries) => {
                    assert!(boundary > 0, "compacted prefix must arrive as a snapshot");
                    for entry in entries {
                        assert_eq!(entry.index, boundary + 1);
                        boundary = entry.index;
                        tail.push(entry.index);
                    }
                }
            }
        }
        assert_eq!(tail, vec![7, 8, 9]);
    })
    .await
    .unwrap();
    restored.shutdown();
    for node in nodes.values() {
        node.shutdown();
    }
    wait_for(Duration::from_secs(1), || Some(())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_learner_follows_then_promotes_through_joint_consensus() {
    let a = NodeId("a".into());
    let d = NodeId("d".into());
    let initial = RaftMembership {
        voters: vec![a.clone()],
        joint_voters: None,
        learners: vec![d.clone()],
    };
    let runtime = test_runtime();
    let (network, mut inbound) = Network::new(&[a.clone(), d.clone()]);
    let leader = RaftNode::with_membership(
        a.clone(),
        initial.clone(),
        fast_config(),
        runtime.clone(),
        network.transport_for(a.clone()),
        MemoryStorage::new(),
    )
    .unwrap();
    let learner = RaftNode::with_membership(
        d.clone(),
        initial,
        fast_config(),
        runtime,
        network.transport_for(d.clone()),
        MemoryStorage::new(),
    )
    .unwrap();
    leader.attach_inbound(inbound.remove(&a).unwrap());
    learner.attach_inbound(inbound.remove(&d).unwrap());
    let mut learner_applied = learner.subscribe_applied();
    leader.start();
    learner.start();
    wait_for(Duration::from_secs(5), || {
        (leader.role() == Some(Role::Leader)).then_some(())
    })
    .await;
    assert_eq!(learner.role(), Some(Role::Follower));
    assert_eq!(learner.membership().unwrap().voters, vec![a.clone()]);
    assert_eq!(leader.propose(vec![7]).await.unwrap(), 1);
    let applied = tokio::time::timeout(Duration::from_secs(5), learner_applied.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(applied,RaftApplied::Entries(entries) if entries.len()==1 && entries[0].index==1)
    );
    let target = vec![a.clone(), d.clone()];
    wait_for(Duration::from_secs(5), || {
        if leader
            .committed_membership()
            .as_ref()
            .is_some_and(|m| m.voters == target && m.joint_voters.is_none())
        {
            Some(())
        } else {
            leader.change_membership(target.clone(), vec![]);
            None
        }
    })
    .await;
    wait_for(Duration::from_secs(5), || {
        learner
            .committed_membership()
            .as_ref()
            .is_some_and(|m| m.voters == target && m.joint_voters.is_none())
            .then_some(())
    })
    .await;
    assert_eq!(leader.role(), Some(Role::Leader));
    assert_eq!(leader.propose(vec![8]).await.unwrap(), 4);
    leader.shutdown();
    learner.shutdown();
}
