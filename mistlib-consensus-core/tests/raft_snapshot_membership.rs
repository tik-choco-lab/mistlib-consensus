use mistlib_consensus_core::storage::{RaftMembership, RaftSnapshot};
use mistlib_consensus_core::{
    Event, LogEntry, MemoryStorage, NodeId, Output, RaftMessage, RaftState, RaftStorage, Role,
};
use std::sync::Arc;

fn id(s: &str) -> NodeId {
    NodeId(s.into())
}
fn nodes(v: &[&str]) -> Vec<NodeId> {
    v.iter().map(|s| id(s)).collect()
}
fn config(v: &[&str], l: &[&str]) -> RaftMembership {
    RaftMembership {
        voters: nodes(v),
        joint_voters: None,
        learners: nodes(l),
    }
}
fn node(s: &str, voters: &[&str], learners: &[&str]) -> RaftState<MemoryStorage> {
    RaftState::with_membership(id(s), config(voters, learners), MemoryStorage::new()).unwrap()
}
fn message(n: &mut RaftState<MemoryStorage>, from: &str, msg: RaftMessage) -> Vec<Output> {
    n.step(Event::Message {
        from: id(from),
        msg,
    })
}
fn leader(voters: &[&str], learners: &[&str]) -> RaftState<MemoryStorage> {
    let mut n = node("a", voters, learners);
    n.step(Event::ElectionTimeout);
    for peer in voters.iter().filter(|p| **p != "a") {
        let term = n.current_term();
        message(
            &mut n,
            peer,
            RaftMessage::RequestVoteResponse {
                term,
                vote_granted: true,
                voter_id: id(peer),
            },
        );
    }
    assert_eq!(n.role, Role::Leader);
    n
}
fn ack(n: &mut RaftState<MemoryStorage>, peer: &str, index: u64) -> Vec<Output> {
    let term = n.current_term();
    message(
        n,
        peer,
        RaftMessage::AppendEntriesResponse {
            term,
            success: true,
            match_index: index,
            follower_id: id(peer),
        },
    )
}
fn append(
    n: &mut RaftState<MemoryStorage>,
    prev: u64,
    entries: Vec<LogEntry>,
    commit: u64,
) -> Vec<Output> {
    message(
        n,
        "a",
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: id("a"),
            prev_log_index: prev,
            prev_log_term: if prev == 0 { 0 } else { 2 },
            entries,
            leader_commit: commit,
        },
    )
}
fn entries(from: u64, to: u64, term: u64) -> Vec<LogEntry> {
    (from..=to)
        .map(|index| LogEntry {
            term,
            index,
            command: vec![index as u8],
        })
        .collect()
}
fn snapshot(index: u64, term: u64) -> RaftSnapshot {
    RaftSnapshot {
        last_included_index: index,
        last_included_term: term,
        data: Arc::new(vec![42]),
        membership: Some(config(&["a", "b", "c"], &[])),
    }
}

#[test]
fn snapshot_compaction_restores_absolute_indices_and_term() {
    let mut n = leader(&["a"], &[]);
    for _ in 0..12 {
        n.step(Event::ClientPropose { command: vec![1] });
    }
    assert_eq!(n.commit_index, 12);
    assert!(n
        .step(Event::CompactSnapshot {
            index: 10,
            data: vec![9]
        })
        .contains(&Output::SnapshotCompacted { index: 10 }));
    assert_eq!(n.storage.log().len(), 2);
    assert_eq!(n.storage.term_at(9), None);
    assert_eq!(n.storage.term_at(10), Some(1));
    let mut restored = RaftState::with_membership(id("a"), config(&["a"], &[]), n.storage).unwrap();
    assert_eq!(restored.commit_index, 10);
    assert_eq!(restored.last_applied, 10);
    restored.step(Event::ElectionTimeout);
    restored.step(Event::ClientPropose { command: vec![2] });
    assert_eq!(restored.commit_index, 13);
    assert_eq!(restored.last_applied, 13);
}

#[test]
fn compaction_refuses_uncommitted_unapplied_or_repeated_index() {
    let mut n = leader(&["a", "b", "c"], &[]);
    n.step(Event::ClientPropose { command: vec![1] });
    assert!(n
        .step(Event::CompactSnapshot {
            index: 1,
            data: vec![]
        })
        .contains(&Output::SnapshotRejected));
    ack(&mut n, "b", 1);
    n.last_applied = 0;
    assert!(n
        .step(Event::CompactSnapshot {
            index: 1,
            data: vec![]
        })
        .contains(&Output::SnapshotRejected));
    n.last_applied = 1;
    n.step(Event::CompactSnapshot {
        index: 1,
        data: vec![],
    });
    assert!(n
        .step(Event::CompactSnapshot {
            index: 1,
            data: vec![]
        })
        .contains(&Output::SnapshotRejected));
}

#[test]
fn installing_matching_snapshot_preserves_tail_but_conflicting_snapshot_discards_it() {
    let mut storage = MemoryStorage::new();
    storage.append_entries(1, entries(1, 8, 2));
    assert!(storage.install_snapshot(snapshot(5, 2)));
    assert_eq!(
        storage.log().iter().map(|e| e.index).collect::<Vec<_>>(),
        vec![6, 7, 8]
    );
    assert!(storage.install_snapshot(snapshot(6, 3)));
    assert!(storage.log().is_empty());
    assert_eq!(storage.last_log_index(), 6);
    assert_eq!(storage.last_log_term(), 3);
}

#[test]
fn snapshot_install_application_effect_precedes_successful_ack() {
    let mut n = node("b", &["a", "b", "c"], &[]);
    let out = message(
        &mut n,
        "a",
        RaftMessage::InstallSnapshot {
            term: 2,
            leader_id: id("a"),
            snapshot: snapshot(20, 2),
        },
    );
    let apply = out
        .iter()
        .position(|o| matches!(o, Output::InstallSnapshot { .. }))
        .unwrap();
    let ack = out
        .iter()
        .position(|o| {
            matches!(
                o,
                Output::SendMessage {
                    msg: RaftMessage::InstallSnapshotResponse { success: true, .. },
                    ..
                }
            )
        })
        .unwrap();
    assert!(apply < ack);
    assert_eq!(n.commit_index, 20);
    assert_eq!(n.last_applied, 20);
    assert_eq!(n.storage.last_log_index(), 20);
    let out = append(&mut n, 20, entries(21, 22, 2), 22);
    assert!(out
        .iter()
        .any(|o| matches!(o,Output::ApplyCommitted{entries} if entries.len()==2)));
    assert_eq!(n.commit_index, 22);
}

#[test]
fn stale_snapshot_cannot_regress_committed_state() {
    let mut n = node("b", &["a", "b", "c"], &[]);
    append(&mut n, 0, entries(1, 8, 2), 8);
    let out = message(
        &mut n,
        "a",
        RaftMessage::InstallSnapshot {
            term: 2,
            leader_id: id("a"),
            snapshot: snapshot(5, 2),
        },
    );
    assert!(!out
        .iter()
        .any(|o| matches!(o, Output::InstallSnapshot { .. })));
    assert_eq!(n.commit_index, 8);
    assert_eq!(n.storage.last_log_index(), 8);
}

#[test]
fn lagging_follower_gets_snapshot_then_remaining_tail() {
    let mut n = leader(&["a", "b", "c"], &[]);
    for _ in 0..8 {
        n.step(Event::ClientPropose { command: vec![1] });
    }
    ack(&mut n, "b", 8);
    n.step(Event::CompactSnapshot {
        index: 6,
        data: vec![7],
    });
    message(
        &mut n,
        "c",
        RaftMessage::AppendEntriesConflict {
            term: 1,
            follower_id: id("c"),
            conflict_index: 1,
            conflict_term: None,
            rejected_prev_index: 8,
        },
    );
    let out = n.step(Event::HeartbeatTimeout);
    assert!(out.iter().any(|o|matches!(o,Output::SendMessage{to,msg:RaftMessage::InstallSnapshot{snapshot,..}} if to==&id("c") && snapshot.last_included_index==6)));
    let out = message(
        &mut n,
        "c",
        RaftMessage::InstallSnapshotResponse {
            term: 1,
            follower_id: id("c"),
            success: true,
            last_included_index: 6,
        },
    );
    assert!(out.iter().any(|o|matches!(o,Output::SendMessage{to,msg:RaftMessage::AppendEntries{prev_log_index:6,entries,..}} if to==&id("c") && entries.len()==2)));
}

#[test]
fn delayed_append_preserves_a_longer_matching_suffix_and_acks_only_its_prefix() {
    let mut n = node("b", &["a", "b", "c"], &[]);
    append(&mut n, 0, entries(1, 8, 2), 0);
    let out = append(&mut n, 0, entries(1, 2, 2), 8);
    assert_eq!(n.storage.last_log_index(), 8);
    assert_eq!(n.commit_index, 2);
    assert!(out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: RaftMessage::AppendEntriesResponse {
                success: true,
                match_index: 2,
                ..
            },
            ..
        }
    )));
}

#[test]
fn committed_conflicting_entry_and_nonsequential_append_are_rejected() {
    let mut n = node("b", &["a", "b", "c"], &[]);
    append(&mut n, 0, entries(1, 4, 2), 4);
    let out = message(
        &mut n,
        "a",
        RaftMessage::AppendEntries {
            term: 3,
            leader_id: id("a"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: entries(1, 4, 3),
            leader_commit: 4,
        },
    );
    assert!(!out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: RaftMessage::AppendEntriesResponse { success: true, .. },
            ..
        }
    )));
    assert_eq!(n.storage.term_at(1), Some(2));
    append(&mut n, 0, entries(2, 4, 2), 4);
    assert_eq!(n.storage.last_log_index(), 4);
}

#[test]
fn replication_backlog_is_batched_and_progress_drives_next_batch() {
    let mut n = leader(&["a", "b", "c"], &[]);
    for _ in 0..300 {
        n.step(Event::ClientPropose { command: vec![1] });
    }
    let out = n.step(Event::HeartbeatTimeout);
    assert!(out.iter().all(|o|!matches!(o,Output::SendMessage{msg:RaftMessage::AppendEntries{entries,..},..} if entries.len()>128)));
    let out = ack(&mut n, "b", 128);
    assert!(out.iter().any(|o|matches!(o,Output::SendMessage{msg:RaftMessage::AppendEntries{prev_log_index:128,entries,..},..} if entries.len()==128)));
}

#[test]
fn unknown_or_forged_response_cannot_advance_term_or_commit() {
    let mut n = leader(&["a", "b", "c"], &[]);
    n.step(Event::ClientPropose { command: vec![1] });
    message(
        &mut n,
        "intruder",
        RaftMessage::RequestVoteResponse {
            term: 90,
            voter_id: id("intruder"),
            vote_granted: true,
        },
    );
    assert_eq!(n.current_term(), 1);
    message(
        &mut n,
        "c",
        RaftMessage::AppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 1,
            follower_id: id("b"),
        },
    );
    ack(&mut n, "b", 100);
    assert_eq!(n.commit_index, 0);
}

#[test]
fn higher_term_without_a_leader_clears_stale_leader_hint() {
    let mut n = leader(&["a", "b", "c"], &[]);
    message(
        &mut n,
        "b",
        RaftMessage::RequestVoteResponse {
            term: 2,
            voter_id: id("b"),
            vote_granted: false,
        },
    );
    assert_eq!(n.role, Role::Follower);
    assert!(n.leader_id.is_none());
}

#[test]
fn learners_do_not_campaign_or_count_for_election() {
    let mut learner = node("d", &["a", "b", "c"], &["d"]);
    learner.step(Event::ElectionTimeout);
    assert_eq!(learner.role, Role::Follower);
    assert_eq!(learner.current_term(), 0);
    let mut candidate = node("a", &["a", "b", "c"], &["d"]);
    candidate.step(Event::ElectionTimeout);
    message(
        &mut candidate,
        "d",
        RaftMessage::RequestVoteResponse {
            term: 1,
            voter_id: id("d"),
            vote_granted: true,
        },
    );
    assert_eq!(candidate.role, Role::Candidate);
}

#[test]
fn adding_learner_is_replicated_without_changing_voter_quorum() {
    let mut n = leader(&["a", "b", "c"], &[]);
    let out = n.step(Event::AddLearner { peer: id("d") });
    assert!(!out.contains(&Output::MembershipRejected));
    assert_eq!(n.membership.voters, nodes(&["a", "b", "c"]));
    assert_eq!(n.membership.learners, nodes(&["d"]));
    assert_eq!(n.commit_index, 0);
    ack(&mut n, "b", 1);
    assert_eq!(n.commit_index, 1);
}

#[test]
fn promotion_requires_caught_up_learner_and_explicit_ack_even_at_zero() {
    let mut n = leader(&["a", "b", "c"], &["d"]);
    let e = Event::ChangeMembership {
        voters: nodes(&["a", "b", "c", "d"]),
        learners: vec![],
    };
    assert!(n.step(e.clone()).contains(&Output::MembershipRejected));
    n.step(Event::ClientPropose { command: vec![1] });
    ack(&mut n, "b", 1);
    ack(&mut n, "d", 0);
    assert!(n.step(e.clone()).contains(&Output::MembershipRejected));
    ack(&mut n, "d", 1);
    assert!(!n.step(e).contains(&Output::MembershipRejected));
}

#[test]
fn joint_change_requires_both_old_and_new_majorities_and_final_commit() {
    let mut n = leader(&["a", "b", "c"], &["d"]);
    n.step(Event::ClientPropose { command: vec![1] });
    ack(&mut n, "b", 1);
    ack(&mut n, "d", 1);
    n.step(Event::ChangeMembership {
        voters: nodes(&["a", "b", "c", "d"]),
        learners: vec![],
    });
    assert!(n.membership.joint_voters.is_some());
    ack(&mut n, "b", 2);
    assert_eq!(
        n.commit_index, 1,
        "old majority alone does not commit joint config"
    );
    ack(&mut n, "d", 2);
    assert_eq!(n.commit_index, 2);
    assert!(n.membership.joint_voters.is_none());
    assert_eq!(n.storage.last_log_index(), 3);
    ack(&mut n, "b", 3);
    assert_eq!(
        n.commit_index, 2,
        "new majority still required for final config"
    );
    ack(&mut n, "d", 3);
    assert_eq!(n.commit_index, 3);
}

#[test]
fn joint_membership_recovers_from_uncommitted_log_and_elections_need_both_quorums() {
    let mut n = leader(&["a", "b", "c"], &["d"]);
    ack(&mut n, "d", 0);
    n.step(Event::ChangeMembership {
        voters: nodes(&["a", "b", "c", "d"]),
        learners: vec![],
    });
    let mut n =
        RaftState::with_membership(id("a"), config(&["a", "b", "c"], &["d"]), n.storage).unwrap();
    assert!(n.membership.joint_voters.is_some());
    n.step(Event::ElectionTimeout);
    message(
        &mut n,
        "b",
        RaftMessage::RequestVoteResponse {
            term: 2,
            voter_id: id("b"),
            vote_granted: true,
        },
    );
    assert_eq!(n.role, Role::Candidate);
    message(
        &mut n,
        "d",
        RaftMessage::RequestVoteResponse {
            term: 2,
            voter_id: id("d"),
            vote_granted: true,
        },
    );
    assert_eq!(n.role, Role::Leader);
}

#[test]
fn compacted_membership_recovers_and_uncommitted_tail_can_be_replaced() {
    let mut n = leader(&["a", "b", "c"], &[]);
    n.step(Event::AddLearner { peer: id("d") });
    ack(&mut n, "b", 1);
    n.step(Event::CompactSnapshot {
        index: 1,
        data: vec![],
    });
    ack(&mut n, "d", 1);
    n.step(Event::ChangeMembership {
        voters: nodes(&["a", "b", "c", "d"]),
        learners: vec![],
    });
    let mut restored =
        RaftState::with_membership(id("b"), config(&["a", "b", "c"], &[]), n.storage).unwrap();
    assert!(restored.membership.joint_voters.is_some());
    message(
        &mut restored,
        "a",
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: id("a"),
            prev_log_index: 1,
            prev_log_term: 1,
            entries: entries(2, 2, 2),
            leader_commit: 2,
        },
    );
    assert!(restored.membership.joint_voters.is_none());
    assert_eq!(restored.membership.learners, nodes(&["d"]));
}

#[test]
fn removed_leader_steps_down_only_after_final_configuration_commits() {
    let mut n = leader(&["a", "b", "c"], &[]);
    n.step(Event::ChangeMembership {
        voters: nodes(&["b", "c"]),
        learners: vec![],
    });
    ack(&mut n, "b", 1);
    ack(&mut n, "c", 1);
    assert_eq!(n.commit_index, 1);
    assert_eq!(n.role, Role::Leader);
    ack(&mut n, "b", 2);
    assert_eq!(n.role, Role::Leader);
    let outputs = ack(&mut n, "c", 2);
    assert_eq!(n.commit_index, 2);
    assert_eq!(n.role, Role::Follower);
    for peer in ["b", "c"] {
        assert!(outputs.iter().any(|out| matches!(out, Output::SendMessage { to, msg: RaftMessage::AppendEntries { leader_commit: 2, .. } } if to.0 == peer)));
    }
}

#[test]
fn concurrent_change_and_empty_voter_set_are_rejected() {
    let mut n = leader(&["a", "b", "c"], &[]);
    assert!(n
        .step(Event::ChangeMembership {
            voters: vec![],
            learners: vec![]
        })
        .contains(&Output::MembershipRejected));
    n.step(Event::RemovePeer { peer: id("c") });
    assert!(n
        .step(Event::RemovePeer { peer: id("b") })
        .contains(&Output::MembershipRejected));
}

#[test]
fn snapshot_payload_is_shared_and_legacy_rpc_tags_are_unchanged() {
    let s = snapshot(1, 1);
    let clone = s.clone();
    assert!(Arc::ptr_eq(&s.data, &clone.data));
    let vote = RaftMessage::RequestVote {
        term: 1,
        candidate_id: id("a"),
        last_log_index: 0,
        last_log_term: 0,
    };
    assert_eq!(
        &bincode::serialize(&vote).unwrap()[..4],
        &0u32.to_le_bytes()
    );
    let ack = RaftMessage::AppendEntriesResponse {
        term: 1,
        success: true,
        match_index: 0,
        follower_id: id("b"),
    };
    assert_eq!(&bincode::serialize(&ack).unwrap()[..4], &3u32.to_le_bytes());
}

#[test]
fn snapshot_duplicate_is_idempotent_but_same_index_different_term_is_not_acked() {
    let mut n = node("b", &["a", "b", "c"], &[]);
    message(
        &mut n,
        "a",
        RaftMessage::InstallSnapshot {
            term: 3,
            leader_id: id("a"),
            snapshot: snapshot(5, 2),
        },
    );
    let out = message(
        &mut n,
        "a",
        RaftMessage::InstallSnapshot {
            term: 3,
            leader_id: id("a"),
            snapshot: snapshot(5, 2),
        },
    );
    assert!(!out
        .iter()
        .any(|o| matches!(o, Output::InstallSnapshot { .. })));
    assert!(out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: RaftMessage::InstallSnapshotResponse { success: true, .. },
            ..
        }
    )));
    let out = message(
        &mut n,
        "a",
        RaftMessage::InstallSnapshot {
            term: 3,
            leader_id: id("a"),
            snapshot: snapshot(5, 3),
        },
    );
    assert!(out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: RaftMessage::InstallSnapshotResponse { success: false, .. },
            ..
        }
    )));
    assert_eq!(n.storage.snapshot().unwrap().last_included_term, 2);
}

#[test]
fn effective_final_membership_is_not_reported_committed_until_final_log_entry_commits() {
    let mut n = leader(&["a", "b", "c"], &[]);
    n.step(Event::RemovePeer { peer: id("c") });
    ack(&mut n, "b", 1);
    assert!(n.membership.joint_voters.is_none());
    assert!(n.committed_membership().joint_voters.is_some());
    ack(&mut n, "b", 2);
    assert_eq!(n.committed_membership(), config(&["a", "b"], &[]));
}

#[test]
fn a_disjoint_new_majority_cannot_commit_an_uncommitted_joint_configuration() {
    let mut n = leader(&["a", "b", "c"], &["d", "e"]);
    ack(&mut n, "d", 0);
    ack(&mut n, "e", 0);
    n.step(Event::ChangeMembership {
        voters: nodes(&["a", "d", "e"]),
        learners: vec![],
    });
    ack(&mut n, "d", 1);
    ack(&mut n, "e", 1);
    assert_eq!(
        n.commit_index, 0,
        "new majority is insufficient without old majority"
    );
    assert!(n.membership.joint_voters.is_some());
    ack(&mut n, "b", 1);
    assert_eq!(n.commit_index, 1);
}

#[test]
fn invalid_snapshot_membership_is_rejected_during_restart() {
    let mut storage = MemoryStorage::new();
    let mut invalid = snapshot(1, 1);
    invalid.membership.as_mut().unwrap().learners = nodes(&["a"]);
    assert!(storage.install_snapshot(invalid));
    assert!(RaftState::with_membership(id("b"), config(&["a", "b", "c"], &[]), storage).is_none());
}

#[test]
fn removed_leader_remains_an_accepted_sender_until_final_commit_is_delivered() {
    let mut a = leader(&["a", "b", "c"], &[]);
    let mut b = node("b", &["a", "b", "c"], &[]);
    let outputs = a.step(Event::ChangeMembership {
        voters: nodes(&["b", "c"]),
        learners: vec![],
    });
    for output in outputs {
        if let Output::SendMessage { to, msg } = output {
            if to == id("b") {
                message(&mut b, "a", msg);
            }
        }
    }
    ack(&mut a, "b", 1);
    let outputs = ack(&mut a, "c", 1);
    for output in outputs {
        if let Output::SendMessage { to, msg } = output {
            if to == id("b") {
                message(&mut b, "a", msg);
            }
        }
    }
    assert_eq!(b.commit_index, 1);
    assert!(!b.peers.contains(&id("a")));
    assert!(b.accepts_peer(&id("a")));
    let out = message(
        &mut b,
        "a",
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: id("a"),
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 2,
        },
    );
    assert!(out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: RaftMessage::AppendEntriesResponse { success: true, .. },
            ..
        }
    )));
    assert_eq!(b.commit_index, 2);
    assert!(!b.accepts_peer(&id("a")));
}

#[test]
fn ephemeral_discovery_elects_one_leader_and_rejects_application_replication() {
    let mut a = RaftState::new_ephemeral(id("a"), vec![]);
    let mut b = RaftState::new_ephemeral(id("b"), vec![]);
    a.step(Event::AddPeer { peer: id("b") });
    b.step(Event::AddPeer { peer: id("a") });
    assert_eq!(a.membership, b.membership);
    let outputs = a.step(Event::ElectionTimeout);
    for output in outputs {
        if let Output::SendMessage { to, msg } = output {
            assert_eq!(to, id("b"));
            for reply in b.step(Event::Message { from: id("a"), msg }) {
                if let Output::SendMessage { msg, .. } = reply {
                    a.step(Event::Message { from: id("b"), msg });
                }
            }
        }
    }
    assert_eq!(a.role, Role::Leader);
    assert_eq!(b.role, Role::Follower);
    assert!(a
        .step(Event::ClientPropose { command: vec![1] })
        .iter()
        .any(|out| matches!(out, Output::ProposeRejected { .. })));
    assert!(a.storage.log().is_empty());
    assert!(a
        .step(Event::ChangeMembership {
            voters: nodes(&["a"]),
            learners: vec![]
        })
        .contains(&Output::MembershipRejected));
    a.step(Event::RemovePeer { peer: id("b") });
    assert_eq!(a.membership.voters, nodes(&["a"]));
    assert!(a.storage.log().is_empty());
}
