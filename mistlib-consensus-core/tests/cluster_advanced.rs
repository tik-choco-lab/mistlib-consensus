//! Multi-node integration tests exercising failover, partitions,
//! restart-with-persistence, and randomized stress, all built on the
//! shared harness in `tests/common/mod.rs`. Not compiled for wasm32 for
//! the same reason as `tests/cluster.rs`: it needs a real Tokio runtime.
#![cfg(not(target_arch = "wasm32"))]

#[path = "common/mod.rs"]
mod common;

use common::*;
use mistlib_consensus_core::{MemoryStorage, NodeId, RaftNode, RaftStorage, Role};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

fn ids(names: &[&str]) -> Vec<NodeId> {
    names.iter().map(|s| NodeId(s.to_string())).collect()
}

/// B1: Leader failover. A 3-node cluster elects a leader and replicates;
/// the leader is then killed. The remaining 2 nodes must elect a new
/// leader, keep committing new proposals, and every live node's applied
/// log must stay identical (Log Matching / State Machine Safety).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_elects_new_leader_and_keeps_committing() {
    let all = ids(&["n1", "n2", "n3"]);
    let (net, mut rxs) = Network::new(&all);
    let config = fast_config();
    let runtime = test_runtime();

    let mut nodes: HashMap<NodeId, Arc<RaftNode<MemoryStorage>>> = HashMap::new();
    let mut applied: HashMap<NodeId, Arc<Mutex<Vec<_>>>> = HashMap::new();
    for id in &all {
        let peers: Vec<NodeId> = all.iter().filter(|p| *p != id).cloned().collect();
        let (node, log) = spawn_node(
            id.clone(),
            peers,
            config,
            &runtime,
            &net,
            rxs.remove(id).unwrap(),
            MemoryStorage::new(),
        );
        nodes.insert(id.clone(), node);
        applied.insert(id.clone(), log);
    }

    let timeout = StdDuration::from_secs(5);
    let leader_id = wait_for_leader_among(&nodes, &all, timeout).await;

    for i in 0..3u8 {
        nodes[&leader_id]
            .propose(vec![i])
            .await
            .expect("propose should succeed");
    }
    wait_for(timeout, || {
        (applied[&leader_id].lock().unwrap().len() >= 3).then_some(())
    })
    .await;
    assert_logs_consistent(&applied);

    // Kill the leader: remove it from the network and stop its event loop.
    net.kill(&leader_id);
    nodes[&leader_id].shutdown();

    let survivors: Vec<NodeId> = all.iter().filter(|id| **id != leader_id).cloned().collect();
    let new_leader_id = wait_for_leader_among(&nodes, &survivors, timeout).await;
    assert_ne!(new_leader_id, leader_id, "a survivor must take over");

    for i in 10..13u8 {
        nodes[&new_leader_id]
            .propose(vec![i])
            .await
            .expect("new leader must accept proposals");
    }
    for id in &survivors {
        wait_for(timeout, || {
            (applied[id].lock().unwrap().len() >= 6).then_some(())
        })
        .await;
    }

    let survivor_applied: HashMap<NodeId, _> = applied
        .iter()
        .filter(|(id, _)| survivors.contains(id))
        .map(|(id, v)| (id.clone(), Arc::clone(v)))
        .collect();
    assert_logs_consistent(&survivor_applied);

    let reference = survivor_applied[&survivors[0]].lock().unwrap().clone();
    for id in &survivors {
        assert_eq!(
            applied[id].lock().unwrap().clone(),
            reference,
            "surviving node {id:?} diverged from the rest"
        );
    }

    for id in &survivors {
        nodes[id].shutdown();
    }
}

/// B2: Network partition. 5 nodes; the leader is isolated into a
/// minority (itself + 1 other) that cannot reach quorum (3 of 5) and so
/// must NOT commit new proposals, while the majority side elects a new
/// leader and keeps committing. Healing the partition must make the old
/// leader step down and converge — no committed-entry divergence at any
/// point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_minority_cannot_commit_majority_progresses_and_heals() {
    let all = ids(&["n1", "n2", "n3", "n4", "n5"]);
    let (net, mut rxs) = Network::new(&all);
    let config = fast_config();
    let runtime = test_runtime();

    let mut nodes: HashMap<NodeId, Arc<RaftNode<MemoryStorage>>> = HashMap::new();
    let mut applied: HashMap<NodeId, Arc<Mutex<Vec<_>>>> = HashMap::new();
    for id in &all {
        let peers: Vec<NodeId> = all.iter().filter(|p| *p != id).cloned().collect();
        let (node, log) = spawn_node(
            id.clone(),
            peers,
            config,
            &runtime,
            &net,
            rxs.remove(id).unwrap(),
            MemoryStorage::new(),
        );
        nodes.insert(id.clone(), node);
        applied.insert(id.clone(), log);
    }

    let timeout = StdDuration::from_secs(5);
    let leader_id = wait_for_leader_among(&nodes, &all, timeout).await;
    nodes[&leader_id]
        .propose(vec![0])
        .await
        .expect("initial propose should commit");
    for id in &all {
        wait_for(timeout, || {
            (!applied[id].lock().unwrap().is_empty()).then_some(())
        })
        .await;
    }
    assert_logs_consistent(&applied);

    // Partition: leader + one other node form the minority group 1;
    // the remaining 3 stay in group 0 (the majority).
    let minority_peer = all.iter().find(|id| **id != leader_id).unwrap().clone();
    let minority: Vec<NodeId> = vec![leader_id.clone(), minority_peer];
    let majority: Vec<NodeId> = all
        .iter()
        .filter(|id| !minority.contains(id))
        .cloned()
        .collect();
    for id in &minority {
        net.set_group(id, 1);
    }

    // The isolated leader can still append locally, but must never reach
    // quorum (3 of 5) with only 2 nodes able to hear each other.
    let stuck_index = nodes[&leader_id]
        .propose(vec![99])
        .await
        .expect("leader still believes it's the leader and accepts the write locally");
    tokio::time::sleep(StdDuration::from_millis(300)).await;
    assert!(
        applied[&leader_id].lock().unwrap().len() < stuck_index as usize + 1
            || !applied[&leader_id]
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.command == vec![99]),
        "a minority leader must not be able to commit without quorum"
    );

    // Majority side must elect a new leader among just those 3 nodes and
    // keep committing.
    let new_leader_id = wait_for_leader_among(&nodes, &majority, timeout).await;
    assert!(majority.contains(&new_leader_id));
    for i in 1..4u8 {
        nodes[&new_leader_id]
            .propose(vec![i])
            .await
            .expect("majority leader must commit");
    }
    for id in &majority {
        wait_for(timeout, || {
            (applied[id].lock().unwrap().len() >= 4).then_some(())
        })
        .await;
    }
    let majority_applied: HashMap<NodeId, _> = applied
        .iter()
        .filter(|(id, _)| majority.contains(id))
        .map(|(id, v)| (id.clone(), Arc::clone(v)))
        .collect();
    assert_logs_consistent(&majority_applied);
    assert!(
        applied[&leader_id].lock().unwrap().len() < 4,
        "the old, isolated leader must not have applied the majority's new entries"
    );

    // Heal the partition: old leader must step down (it will hear a
    // higher term from the new leader) and converge onto the majority's
    // committed log, with no divergence anywhere in the overlap.
    net.heal(&minority);
    wait_for(timeout, || {
        (nodes[&leader_id].role() != Some(Role::Leader)).then_some(())
    })
    .await;
    for id in &all {
        wait_for(timeout, || {
            (applied[id].lock().unwrap().len() >= 4).then_some(())
        })
        .await;
    }
    assert_logs_consistent(&applied);

    for id in &all {
        nodes[id].shutdown();
    }
}

/// B4: Restart with persistence. A follower is stopped and a brand-new
/// `RaftNode` is created reusing the exact same storage instance
/// (`SharedMemoryStorage` shares its data via `Arc`), simulating a
/// process restart against the same disk. The revived node must rejoin,
/// catch up on missed entries with no double-apply, and its
/// term/voted_for must carry over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_reusing_same_storage_rejoins_without_double_apply() {
    let all = ids(&["n1", "n2", "n3"]);
    let (net, mut rxs) = Network::new(&all);
    let config = fast_config();
    let runtime = test_runtime();

    let mut nodes: HashMap<NodeId, Arc<RaftNode<SharedMemoryStorage>>> = HashMap::new();
    let mut applied: HashMap<NodeId, Arc<Mutex<Vec<_>>>> = HashMap::new();
    let mut storages: HashMap<NodeId, SharedMemoryStorage> = HashMap::new();
    for id in &all {
        let peers: Vec<NodeId> = all.iter().filter(|p| *p != id).cloned().collect();
        let storage = SharedMemoryStorage::new();
        storages.insert(id.clone(), storage.clone());
        let (node, log) = spawn_node(
            id.clone(),
            peers,
            config,
            &runtime,
            &net,
            rxs.remove(id).unwrap(),
            storage,
        );
        nodes.insert(id.clone(), node);
        applied.insert(id.clone(), log);
    }

    let timeout = StdDuration::from_secs(5);
    let leader_id = wait_for_leader_among(&nodes, &all, timeout).await;
    for i in 0..3u8 {
        nodes[&leader_id]
            .propose(vec![i])
            .await
            .expect("propose should succeed");
    }
    for id in &all {
        wait_for(timeout, || {
            (applied[id].lock().unwrap().len() >= 3).then_some(())
        })
        .await;
    }
    assert_logs_consistent(&applied);

    let follower_id = all.iter().find(|id| **id != leader_id).unwrap().clone();
    let term_before = nodes[&follower_id]
        .role()
        .map(|_| ())
        .and(Some(()))
        .and(Some(storages[&follower_id].current_term()));
    let term_before = term_before.unwrap();
    let voted_for_before = storages[&follower_id].voted_for();

    // Stop the follower (drop its inbound route too) and immediately
    // propose more entries while it's offline, so it has to catch up.
    net.kill(&follower_id);
    nodes[&follower_id].shutdown();
    for i in 10..13u8 {
        nodes[&leader_id]
            .propose(vec![i])
            .await
            .expect("propose should succeed while follower is down");
    }
    let remaining: Vec<NodeId> = all
        .iter()
        .filter(|id| **id != follower_id)
        .cloned()
        .collect();
    for id in &remaining {
        wait_for(timeout, || {
            (applied[id].lock().unwrap().len() >= 6).then_some(())
        })
        .await;
    }

    // "Restart": build a brand-new RaftNode for the same id, reusing the
    // very same SharedMemoryStorage instance (same Arc, same data).
    let same_storage = storages[&follower_id].clone();
    assert_eq!(
        same_storage.current_term(),
        term_before,
        "term must have persisted"
    );
    assert_eq!(
        same_storage.voted_for(),
        voted_for_before,
        "vote must have persisted"
    );

    let revived_rx = net.revive(&follower_id);
    let peers: Vec<NodeId> = all.iter().filter(|p| **p != follower_id).cloned().collect();
    let (revived_node, revived_applied) = spawn_node(
        follower_id.clone(),
        peers,
        config,
        &runtime,
        &net,
        revived_rx,
        same_storage,
    );
    nodes.insert(follower_id.clone(), revived_node);
    applied.insert(follower_id.clone(), revived_applied);

    wait_for(timeout, || {
        (applied[&follower_id].lock().unwrap().len() >= 6).then_some(())
    })
    .await;
    assert_logs_consistent(&applied);

    // No double-apply: exactly 6 entries applied, each exactly once.
    let follower_log = applied[&follower_id].lock().unwrap().clone();
    assert_eq!(
        follower_log.len(),
        6,
        "must not double-apply already-committed entries"
    );
    let mut indices: Vec<u64> = follower_log.iter().map(|e| e.index).collect();
    let dedup_len = {
        indices.sort_unstable();
        indices.dedup();
        indices.len()
    };
    assert_eq!(dedup_len, 6, "every index must be applied exactly once");

    for id in &all {
        nodes[id].shutdown();
    }
}

/// B5: Randomized stress. 3 nodes, 50 proposals fired at whoever is
/// currently believed to be the leader (retrying through `NotLeader`
/// with a leader-hint-aware follow-up), interleaved with small random
/// delays. Deterministic via a seeded RNG. At the end, every node's
/// applied log must be identical and contain every proposed command
/// exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn randomized_stress_all_nodes_converge_on_identical_log() {
    let all = ids(&["n1", "n2", "n3"]);
    let (net, mut rxs) = Network::new(&all);
    let config = fast_config();
    let runtime = test_runtime();

    let mut nodes: HashMap<NodeId, Arc<RaftNode<MemoryStorage>>> = HashMap::new();
    let mut applied: HashMap<NodeId, Arc<Mutex<Vec<_>>>> = HashMap::new();
    for id in &all {
        let peers: Vec<NodeId> = all.iter().filter(|p| *p != id).cloned().collect();
        let (node, log) = spawn_node(
            id.clone(),
            peers,
            config,
            &runtime,
            &net,
            rxs.remove(id).unwrap(),
            MemoryStorage::new(),
        );
        nodes.insert(id.clone(), node);
        applied.insert(id.clone(), log);
    }

    let timeout = StdDuration::from_secs(5);
    let mut current_leader = wait_for_leader_among(&nodes, &all, timeout).await;

    let mut rng = StdRng::seed_from_u64(42);
    const N: u32 = 50;
    let mut proposed_commands: Vec<Vec<u8>> = Vec::new();

    for n in 0..N {
        let command = n.to_le_bytes().to_vec();
        loop {
            match nodes[&current_leader].propose(command.clone()).await {
                Ok(_) => {
                    proposed_commands.push(command.clone());
                    break;
                }
                Err(_) => {
                    // Not the leader (anymore, or never was): find out
                    // who is and retry against them.
                    current_leader = wait_for_leader_among(&nodes, &all, timeout).await;
                }
            }
        }
        let delay_ms = rng.gen_range(0..5);
        tokio::time::sleep(StdDuration::from_millis(delay_ms)).await;
    }

    for id in &all {
        wait_for(StdDuration::from_secs(10), || {
            (applied[id].lock().unwrap().len() >= proposed_commands.len()).then_some(())
        })
        .await;
    }
    assert_logs_consistent(&applied);

    let reference = applied[&all[0]].lock().unwrap().clone();
    for id in &all {
        let log = applied[id].lock().unwrap();
        assert_eq!(
            log.len(),
            proposed_commands.len(),
            "node {id:?} has the wrong number of applied entries"
        );
        assert_eq!(*log, reference, "node {id:?} diverged");
    }
    let mut applied_commands: Vec<Vec<u8>> = reference.iter().map(|e| e.command.clone()).collect();
    applied_commands.sort();
    let mut expected = proposed_commands.clone();
    expected.sort();
    assert_eq!(
        applied_commands, expected,
        "every proposed command must appear exactly once"
    );

    for id in &all {
        nodes[id].shutdown();
    }
}
