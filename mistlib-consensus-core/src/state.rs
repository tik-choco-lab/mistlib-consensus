//! Pure, synchronous Raft core. Nothing in this module touches async,
//! I/O, or wall-clock time — it only reacts to [`Event`]s and produces
//! [`Output`]s, which makes it straightforward to unit test (see the
//! `tests` module at the bottom of this file).

use crate::message::{LogEntry, RaftMessage};
use crate::storage::RaftStorage;
use mistlib_core::types::NodeId;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Inputs the driver feeds into the state machine.
#[derive(Debug, Clone)]
pub enum Event {
    Message { from: NodeId, msg: RaftMessage },
    ElectionTimeout,
    HeartbeatTimeout,
    ClientPropose { command: Vec<u8> },
    AddPeer { peer: NodeId },
    RemovePeer { peer: NodeId },
}

/// Effects the driver must carry out (send bytes over the network,
/// apply entries to the state machine, restart timers, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    SendMessage { to: NodeId, msg: RaftMessage },
    ApplyCommitted { entries: Vec<LogEntry> },
    BecameLeader,
    BecameFollower,
    ResetElectionTimer,
    ResetHeartbeatTimer,
    Proposed { index: u64 },
    ProposeRejected { leader_hint: Option<NodeId> },
}

pub struct RaftState<S: RaftStorage> {
    pub id: NodeId,
    pub peers: HashSet<NodeId>,
    pub role: Role,
    pub storage: S,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: Option<NodeId>,

    // Candidate-only state.
    votes_received: HashSet<NodeId>,

    // Leader-only state.
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
}

impl<S: RaftStorage> RaftState<S> {
    pub fn new(id: NodeId, peers: Vec<NodeId>, storage: S) -> Self {
        Self {
            id,
            peers: peers.into_iter().collect(),
            role: Role::Follower,
            storage,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes_received: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    pub fn current_term(&self) -> u64 {
        self.storage.current_term()
    }

    fn quorum_size(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    fn become_follower(&mut self, term: u64, out: &mut Vec<Output>) {
        let was_leader = self.role == Role::Leader;
        if term > self.storage.current_term() {
            self.storage.set_term_and_vote(term, None);
        }
        self.role = Role::Follower;
        self.votes_received.clear();
        self.next_index.clear();
        self.match_index.clear();
        if was_leader {
            out.push(Output::BecameFollower);
        }
    }

    fn become_candidate(&mut self, out: &mut Vec<Output>) {
        let new_term = self.storage.current_term() + 1;
        self.storage.set_term_and_vote(new_term, Some(self.id.clone()));
        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.id.clone());
        out.push(Output::ResetElectionTimer);

        let last_log_index = self.storage.last_log_index();
        let last_log_term = self.storage.last_log_term();
        for peer in self.peers.clone() {
            out.push(Output::SendMessage {
                to: peer,
                msg: RaftMessage::RequestVote {
                    term: new_term,
                    candidate_id: self.id.clone(),
                    last_log_index,
                    last_log_term,
                },
            });
        }

        // Single-node cluster: we are already a quorum of one.
        if self.cluster_size() == 1 {
            self.become_leader(out);
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Output>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        self.votes_received.clear();
        let next = self.storage.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for peer in self.peers.clone() {
            self.next_index.insert(peer.clone(), next);
            self.match_index.insert(peer, 0);
        }
        out.push(Output::BecameLeader);
        out.push(Output::ResetHeartbeatTimer);
        self.send_append_entries_to_all(out);
    }

    fn send_append_entries_to_all(&self, out: &mut Vec<Output>) {
        for peer in self.peers.clone() {
            out.push(self.append_entries_for(&peer));
        }
    }

    fn append_entries_for(&self, peer: &NodeId) -> Output {
        let next = *self.next_index.get(peer).unwrap_or(&1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.storage.term_at(prev_log_index).unwrap_or(0);
        let entries = self.storage.entries_from(next);
        Output::SendMessage {
            to: peer.clone(),
            msg: RaftMessage::AppendEntries {
                term: self.storage.current_term(),
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        }
    }

    fn maybe_apply_committed(&mut self, out: &mut Vec<Output>) {
        if self.commit_index > self.last_applied {
            let entries = self
                .storage
                .entries_from(self.last_applied + 1)
                .into_iter()
                .filter(|e| e.index <= self.commit_index)
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                self.last_applied = entries.last().unwrap().index;
                out.push(Output::ApplyCommitted { entries });
            }
        }
    }

    fn advance_leader_commit_index(&mut self, out: &mut Vec<Output>) {
        let mut indices: Vec<u64> = self.match_index.values().copied().collect();
        indices.push(self.storage.last_log_index()); // leader's own match index
        indices.sort_unstable_by(|a, b| b.cmp(a));
        let quorum_idx = indices[self.quorum_size() - 1];
        // Raft safety: only commit entries from the leader's current term
        // directly; earlier terms are committed transitively.
        if quorum_idx > self.commit_index {
            if let Some(term) = self.storage.term_at(quorum_idx) {
                if term == self.storage.current_term() {
                    self.commit_index = quorum_idx;
                    self.maybe_apply_committed(out);
                }
            }
        }
    }

    fn log_is_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        let my_last_term = self.storage.last_log_term();
        let my_last_index = self.storage.last_log_index();
        last_log_term > my_last_term
            || (last_log_term == my_last_term && last_log_index >= my_last_index)
    }

    pub fn step(&mut self, event: Event) -> Vec<Output> {
        let mut out = Vec::new();
        match event {
            Event::Message { from, msg } => self.handle_message(from, msg, &mut out),
            Event::ElectionTimeout => {
                if self.role != Role::Leader {
                    self.become_candidate(&mut out);
                }
            }
            Event::HeartbeatTimeout => {
                if self.role == Role::Leader {
                    out.push(Output::ResetHeartbeatTimer);
                    self.send_append_entries_to_all(&mut out);
                }
            }
            Event::ClientPropose { command } => {
                if self.role == Role::Leader {
                    let index = self.storage.last_log_index() + 1;
                    let term = self.storage.current_term();
                    self.storage
                        .append_entries(index, vec![LogEntry { term, index, command }]);
                    out.push(Output::Proposed { index });
                    if self.cluster_size() == 1 {
                        self.advance_leader_commit_index(&mut out);
                    } else {
                        self.send_append_entries_to_all(&mut out);
                    }
                } else {
                    out.push(Output::ProposeRejected {
                        leader_hint: self.leader_id.clone(),
                    });
                }
            }
            Event::AddPeer { peer } => {
                self.peers.insert(peer.clone());
                if self.role == Role::Leader {
                    let next = self.storage.last_log_index() + 1;
                    self.next_index.insert(peer.clone(), next);
                    self.match_index.insert(peer, 0);
                }
            }
            Event::RemovePeer { peer } => {
                self.peers.remove(&peer);
                self.next_index.remove(&peer);
                self.match_index.remove(&peer);
                self.votes_received.remove(&peer);
            }
        }
        out
    }

    fn handle_message(&mut self, _from: NodeId, msg: RaftMessage, out: &mut Vec<Output>) {
        let msg_term = msg.term();
        if msg_term > self.storage.current_term() {
            self.become_follower(msg_term, out);
        }

        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                let current_term = self.storage.current_term();
                if term < current_term {
                    out.push(Output::SendMessage {
                        to: candidate_id,
                        msg: RaftMessage::RequestVoteResponse {
                            term: current_term,
                            vote_granted: false,
                            voter_id: self.id.clone(),
                        },
                    });
                    return;
                }
                let voted_for = self.storage.voted_for();
                let can_vote = voted_for.is_none() || voted_for.as_ref() == Some(&candidate_id);
                let up_to_date = self.log_is_up_to_date(last_log_index, last_log_term);
                let grant = can_vote && up_to_date;
                if grant {
                    self.storage
                        .set_term_and_vote(term, Some(candidate_id.clone()));
                    out.push(Output::ResetElectionTimer);
                }
                out.push(Output::SendMessage {
                    to: candidate_id,
                    msg: RaftMessage::RequestVoteResponse {
                        term,
                        vote_granted: grant,
                        voter_id: self.id.clone(),
                    },
                });
            }
            RaftMessage::RequestVoteResponse {
                term,
                vote_granted,
                voter_id,
            } => {
                if self.role != Role::Candidate || term != self.storage.current_term() {
                    return;
                }
                if vote_granted {
                    self.votes_received.insert(voter_id);
                    if self.votes_received.len() >= self.quorum_size() {
                        self.become_leader(out);
                    }
                }
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                let current_term = self.storage.current_term();
                if term < current_term {
                    out.push(Output::SendMessage {
                        to: leader_id,
                        msg: RaftMessage::AppendEntriesResponse {
                            term: current_term,
                            success: false,
                            match_index: 0,
                            follower_id: self.id.clone(),
                        },
                    });
                    return;
                }
                // Valid leader for this (or a newer) term: reset our
                // election timer and step down from candidacy if needed.
                if self.role != Role::Follower {
                    self.become_follower(term, out);
                }
                self.leader_id = Some(leader_id.clone());
                out.push(Output::ResetElectionTimer);

                let consistent = self.storage.term_at(prev_log_index) == Some(prev_log_term);
                if !consistent {
                    out.push(Output::SendMessage {
                        to: leader_id,
                        msg: RaftMessage::AppendEntriesResponse {
                            term,
                            success: false,
                            match_index: 0,
                            follower_id: self.id.clone(),
                        },
                    });
                    return;
                }

                if !entries.is_empty() {
                    self.storage.append_entries(prev_log_index + 1, entries);
                }
                let new_last_index = self.storage.last_log_index();
                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(new_last_index);
                    self.maybe_apply_committed(out);
                }
                out.push(Output::SendMessage {
                    to: leader_id,
                    msg: RaftMessage::AppendEntriesResponse {
                        term,
                        success: true,
                        match_index: new_last_index,
                        follower_id: self.id.clone(),
                    },
                });
            }
            RaftMessage::AppendEntriesResponse {
                term,
                success,
                match_index,
                follower_id,
            } => {
                if self.role != Role::Leader || term != self.storage.current_term() {
                    return;
                }
                if success {
                    self.match_index.insert(follower_id.clone(), match_index);
                    self.next_index.insert(follower_id, match_index + 1);
                    self.advance_leader_commit_index(out);
                } else {
                    let next = self.next_index.entry(follower_id.clone()).or_insert(1);
                    *next = next.saturating_sub(1).max(1);
                    out.push(self.append_entries_for(&follower_id));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;

    fn node(id: &str, peers: &[&str]) -> RaftState<MemoryStorage> {
        RaftState::new(
            NodeId(id.to_string()),
            peers.iter().map(|p| NodeId(p.to_string())).collect(),
            MemoryStorage::new(),
        )
    }

    #[test]
    fn election_majority_votes_becomes_leader() {
        let mut n = node("a", &["b", "c"]);
        let out = n.step(Event::ElectionTimeout);
        assert_eq!(n.role, Role::Candidate);
        assert_eq!(n.current_term(), 1);
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::SendMessage { to, .. } if to.0 == "b")));
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::SendMessage { to, .. } if to.0 == "c")));

        // Self-vote + one peer vote is already a majority of 3.
        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::RequestVoteResponse {
                term: 1,
                vote_granted: true,
                voter_id: NodeId("b".into()),
            },
        });
        assert_eq!(n.role, Role::Leader);
        assert!(out.contains(&Output::BecameLeader));

        // A late vote from "c" arriving after leadership must not panic
        // or regress state.
        let out = n.step(Event::Message {
            from: NodeId("c".into()),
            msg: RaftMessage::RequestVoteResponse {
                term: 1,
                vote_granted: true,
                voter_id: NodeId("c".into()),
            },
        });
        assert_eq!(n.role, Role::Leader);
        assert!(!out.contains(&Output::BecameLeader));
    }

    #[test]
    fn higher_term_forces_follower() {
        let mut n = node("a", &["b", "c"]);
        n.step(Event::ElectionTimeout); // becomes candidate at term 1
        assert_eq!(n.role, Role::Candidate);

        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::AppendEntries {
                term: 5,
                leader_id: NodeId("b".into()),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        });
        assert_eq!(n.role, Role::Follower);
        assert_eq!(n.current_term(), 5);
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::SendMessage { msg: RaftMessage::AppendEntriesResponse { success: true, .. }, .. })));
    }

    #[test]
    fn vote_denied_on_stale_log() {
        let mut n = node("a", &["b"]);
        // Give the follower a log entry at term 1.
        n.storage.set_term_and_vote(1, None);
        n.storage
            .append_entries(1, vec![LogEntry { term: 1, index: 1, command: vec![] }]);

        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::RequestVote {
                term: 2,
                candidate_id: NodeId("b".into()),
                last_log_index: 0,
                last_log_term: 0,
            },
        });
        let granted = out.iter().any(|o| {
            matches!(
                o,
                Output::SendMessage {
                    msg: RaftMessage::RequestVoteResponse { vote_granted: true, .. },
                    ..
                }
            )
        });
        assert!(!granted, "candidate with stale log must not get the vote");
    }

    fn make_leader(id: &str, peers: &[&str]) -> RaftState<MemoryStorage> {
        let mut n = node(id, peers);
        n.step(Event::ElectionTimeout);
        for p in peers {
            n.step(Event::Message {
                from: NodeId(p.to_string()),
                msg: RaftMessage::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                    voter_id: NodeId(p.to_string()),
                },
            });
        }
        assert_eq!(n.role, Role::Leader);
        n
    }

    #[test]
    fn log_replication_and_commit_advance_on_quorum() {
        let mut leader = make_leader("a", &["b", "c"]);
        let out = leader.step(Event::ClientPropose { command: b"x".to_vec() });
        assert!(out.contains(&Output::Proposed { index: 1 }));
        assert_eq!(leader.commit_index, 0, "not yet committed, no acks");

        // "b" acknowledges -> still not a majority of the *other* votes
        // needed beyond the leader itself (2 of 3 total).
        let out = leader.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 1,
                follower_id: NodeId("b".into()),
            },
        });
        assert_eq!(leader.commit_index, 1, "leader + b = quorum of 2/3");
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::ApplyCommitted { entries } if entries.len() == 1)));

        // "c" acking later shouldn't re-apply already-applied entries.
        let out = leader.step(Event::Message {
            from: NodeId("c".into()),
            msg: RaftMessage::AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 1,
                follower_id: NodeId("c".into()),
            },
        });
        assert!(!out.iter().any(|o| matches!(o, Output::ApplyCommitted { .. })));
    }

    #[test]
    fn conflicting_entries_are_truncated() {
        let mut follower = node("a", &["leader"]);
        // Follower has a stale, uncommitted entry at index 1 term 1.
        follower.storage.set_term_and_vote(1, None);
        follower
            .storage
            .append_entries(1, vec![LogEntry { term: 1, index: 1, command: b"old".to_vec() }]);

        // Leader (term 2) sends a conflicting entry at index 1.
        let out = follower.step(Event::Message {
            from: NodeId("leader".into()),
            msg: RaftMessage::AppendEntries {
                term: 2,
                leader_id: NodeId("leader".into()),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry { term: 2, index: 1, command: b"new".to_vec() }],
                leader_commit: 1,
            },
        });
        assert!(out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::AppendEntriesResponse { success: true, .. },
                ..
            }
        )));
        assert_eq!(follower.storage.log().len(), 1);
        assert_eq!(follower.storage.log()[0].command, b"new".to_vec());
        assert_eq!(follower.storage.log()[0].term, 2);
    }

    /// A candidate that fails to reach quorum before its election timer
    /// fires again must start a fresh election at a strictly higher term,
    /// discarding the votes it had already collected.
    #[test]
    fn split_vote_starts_new_election_at_higher_term() {
        // 4-node cluster: quorum is 3 (self + 2), so a lone peer vote
        // isn't enough and a second timeout must occur.
        let mut n = node("a", &["b", "c", "d"]);
        n.step(Event::ElectionTimeout);
        assert_eq!(n.role, Role::Candidate);
        assert_eq!(n.current_term(), 1);

        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::RequestVoteResponse {
                term: 1,
                vote_granted: true,
                voter_id: NodeId("b".into()),
            },
        });
        assert_eq!(n.role, Role::Candidate, "2 of 4 votes is not a quorum of 3");
        assert!(!out.contains(&Output::BecameLeader));

        // Election timer fires again: split vote, start a new election.
        let out = n.step(Event::ElectionTimeout);
        assert_eq!(n.role, Role::Candidate);
        assert_eq!(n.current_term(), 2, "must bump to a strictly higher term");
        assert!(out.contains(&Output::ResetElectionTimer));

        // The stale vote from the previous term must not count now.
        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::RequestVoteResponse {
                term: 1,
                vote_granted: true,
                voter_id: NodeId("b".into()),
            },
        });
        assert_eq!(n.role, Role::Candidate);
        assert!(!out.contains(&Output::BecameLeader));

        // Two fresh votes at the new term reach quorum (self + b + c).
        n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::RequestVoteResponse {
                term: 2,
                vote_granted: true,
                voter_id: NodeId("b".into()),
            },
        });
        let out = n.step(Event::Message {
            from: NodeId("c".into()),
            msg: RaftMessage::RequestVoteResponse {
                term: 2,
                vote_granted: true,
                voter_id: NodeId("c".into()),
            },
        });
        assert_eq!(n.role, Role::Leader);
        assert!(out.contains(&Output::BecameLeader));
    }

    /// A candidate must step down the moment it hears from a leader whose
    /// term is at least as current as its own — even when the term is
    /// exactly equal (not just strictly higher).
    #[test]
    fn candidate_steps_down_on_append_entries_same_term() {
        let mut n = node("a", &["b", "c"]);
        n.step(Event::ElectionTimeout); // candidate at term 1
        assert_eq!(n.role, Role::Candidate);
        assert_eq!(n.current_term(), 1);

        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::AppendEntries {
                term: 1, // same term, not higher
                leader_id: NodeId("b".into()),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        });
        assert_eq!(n.role, Role::Follower);
        assert_eq!(n.current_term(), 1);
        assert_eq!(n.leader_id, Some(NodeId("b".into())));
        // Note: `BecameFollower` is only emitted when stepping down from
        // Leader (it signals leadership loss); a Candidate->Follower
        // transition doesn't emit it, but the accept response below
        // confirms the step-down actually took effect.
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::SendMessage { msg: RaftMessage::AppendEntriesResponse { success: true, .. }, .. })));
    }

    /// A leader that learns of a higher term (via any message, not just
    /// AppendEntries) must step down and drop its leader-only state.
    #[test]
    fn leader_steps_down_on_higher_term_response() {
        let mut leader = make_leader("a", &["b", "c"]);
        assert_eq!(leader.role, Role::Leader);

        let out = leader.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::AppendEntriesResponse {
                term: 9,
                success: false,
                match_index: 0,
                follower_id: NodeId("b".into()),
            },
        });
        assert_eq!(leader.role, Role::Follower);
        assert_eq!(leader.current_term(), 9);
        assert!(out.contains(&Output::BecameFollower));

        // Leader-only bookkeeping must be cleared so a subsequent
        // election doesn't inherit stale next_index/match_index state.
        assert!(leader.next_index.is_empty());
        assert!(leader.match_index.is_empty());

        // No longer the leader, so a client proposal must be rejected.
        let out = leader.step(Event::ClientPropose { command: b"x".to_vec() });
        assert!(out.iter().any(|o| matches!(o, Output::ProposeRejected { .. })));
    }

    /// A follower must reject an AppendEntries whose prev_log_index/term
    /// don't match its own log; the leader then backs off next_index and
    /// retries with an earlier prev_log_index until it succeeds.
    #[test]
    fn mismatched_prev_log_triggers_next_index_backoff_and_retry() {
        let mut leader = make_leader("a", &["b"]);
        for cmd in [b"1".to_vec(), b"2".to_vec(), b"3".to_vec()] {
            leader.step(Event::ClientPropose { command: cmd });
        }
        assert_eq!(leader.storage.last_log_index(), 3);

        // Simulate that "b" had previously acked up through index 2 (so
        // the leader believes next_index[b] == 3), advancing commit_index
        // to 2 via the 2-node quorum.
        let out = leader.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 2,
                follower_id: NodeId("b".into()),
            },
        });
        assert_eq!(leader.commit_index, 2);
        assert!(out.iter().any(|o| matches!(o, Output::ApplyCommitted { .. })));

        // But "b"'s actual log only has entry 1 (it lost entry 2 somehow),
        // so a prev_log_index=2 probe from the leader must be rejected.
        let mut follower = node("b", &["a"]);
        follower.storage.set_term_and_vote(1, None);
        follower
            .storage
            .append_entries(1, vec![LogEntry { term: 1, index: 1, command: b"1".to_vec() }]);

        let reject_out = follower.step(Event::Message {
            from: NodeId("a".into()),
            msg: RaftMessage::AppendEntries {
                term: 1,
                leader_id: NodeId("a".into()),
                prev_log_index: 2,
                prev_log_term: 1,
                entries: vec![LogEntry { term: 1, index: 3, command: b"3".to_vec() }],
                leader_commit: 2,
            },
        });
        assert!(reject_out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::AppendEntriesResponse { success: false, .. },
                ..
            }
        )));

        // Leader receives the rejection: decrements next_index[b] from 3
        // to 2 and immediately retries with prev_log_index=1.
        let retry_out = leader.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::AppendEntriesResponse {
                term: 1,
                success: false,
                match_index: 0,
                follower_id: NodeId("b".into()),
            },
        });
        let retry_msg = retry_out
            .into_iter()
            .find_map(|o| match o {
                Output::SendMessage { msg: msg @ RaftMessage::AppendEntries { .. }, .. } => {
                    Some(msg)
                }
                _ => None,
            })
            .expect("leader must retry with an AppendEntries after a rejection");
        let RaftMessage::AppendEntries { prev_log_index, prev_log_term, ref entries, .. } =
            retry_msg
        else {
            unreachable!()
        };
        assert_eq!(prev_log_index, 1, "must back off to the last index b actually has");
        assert_eq!(prev_log_term, 1);
        assert_eq!(entries.len(), 2, "must resend entries 2 and 3");

        // Feeding the retried AppendEntries to the follower now succeeds.
        let success_out = follower.step(Event::Message {
            from: NodeId("a".into()),
            msg: retry_msg,
        });
        assert!(success_out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::AppendEntriesResponse { success: true, match_index: 3, .. },
                ..
            }
        )));
        assert_eq!(follower.storage.log().len(), 3);
        assert_eq!(follower.storage.last_log_index(), leader.storage.last_log_index());
    }

    /// commit_index must be monotonically non-decreasing, and every
    /// entry must be handed to `ApplyCommitted` exactly once, in index
    /// order, across a sequence of steps.
    #[test]
    fn commit_index_monotonic_and_entries_applied_exactly_once_in_order() {
        let mut leader = make_leader("a", &["b", "c"]);
        let mut seen_commit_indices = vec![leader.commit_index];
        let mut applied_indices = Vec::new();

        for cmd in [b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()] {
            leader.step(Event::ClientPropose { command: cmd });
        }

        // Acks trickle in out of a strict order and with duplicates;
        // commit_index must never regress and applies must stay in order
        // with no repeats.
        let acks = [
            ("b", 1u64),
            ("c", 1),
            ("b", 3),
            ("c", 2), // stale relative to c's own later ack below is fine; matches must monotonically increase per-follower in real usage, but the state machine must tolerate this defensively
            ("c", 4),
            ("b", 4),
        ];
        for (voter, match_index) in acks {
            let out = leader.step(Event::Message {
                from: NodeId(voter.into()),
                msg: RaftMessage::AppendEntriesResponse {
                    term: 1,
                    success: true,
                    match_index,
                    follower_id: NodeId(voter.into()),
                },
            });
            assert!(
                leader.commit_index >= *seen_commit_indices.last().unwrap(),
                "commit_index must never go backwards"
            );
            seen_commit_indices.push(leader.commit_index);
            for o in out {
                if let Output::ApplyCommitted { entries } = o {
                    for e in entries {
                        applied_indices.push(e.index);
                    }
                }
            }
        }

        assert_eq!(applied_indices, vec![1, 2, 3, 4], "must apply exactly once, in order");
        assert_eq!(leader.commit_index, 4);
        assert_eq!(leader.last_applied, 4);
    }

    /// A node must vote at most once per term: a duplicate RequestVote
    /// from the same candidate re-grants (idempotent), but a competing
    /// candidate in the same term must be denied.
    #[test]
    fn votes_only_once_per_term() {
        let mut n = node("a", &["b", "c"]);

        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::RequestVote {
                term: 1,
                candidate_id: NodeId("b".into()),
                last_log_index: 0,
                last_log_term: 0,
            },
        });
        assert!(out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::RequestVoteResponse { vote_granted: true, .. },
                ..
            }
        )));
        assert_eq!(n.storage.voted_for(), Some(NodeId("b".into())));

        // A different candidate in the same term must be denied.
        let out = n.step(Event::Message {
            from: NodeId("c".into()),
            msg: RaftMessage::RequestVote {
                term: 1,
                candidate_id: NodeId("c".into()),
                last_log_index: 0,
                last_log_term: 0,
            },
        });
        assert!(out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::RequestVoteResponse { vote_granted: false, .. },
                ..
            }
        )));
        assert_eq!(n.storage.voted_for(), Some(NodeId("b".into())), "vote must not change");

        // A duplicate/retransmitted request from the same candidate we
        // already voted for is granted again (idempotent).
        let out = n.step(Event::Message {
            from: NodeId("b".into()),
            msg: RaftMessage::RequestVote {
                term: 1,
                candidate_id: NodeId("b".into()),
                last_log_index: 0,
                last_log_term: 0,
            },
        });
        assert!(out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::RequestVoteResponse { vote_granted: true, .. },
                ..
            }
        )));
        assert_eq!(n.storage.voted_for(), Some(NodeId("b".into())));
    }

    /// A single-node cluster (no peers) is already its own quorum: a
    /// client proposal must commit and apply in the very same step that
    /// produces it, with no network round-trip needed.
    #[test]
    fn single_node_cluster_commits_immediately_via_self_quorum() {
        let mut n = node("solo", &[]);
        let out = n.step(Event::ElectionTimeout);
        assert_eq!(n.role, Role::Leader, "sole node must self-elect immediately");
        assert!(out.contains(&Output::BecameLeader));

        let out = n.step(Event::ClientPropose { command: b"hello".to_vec() });
        assert!(out.contains(&Output::Proposed { index: 1 }));
        assert_eq!(n.commit_index, 1, "self-quorum must commit without any peer ack");
        assert!(out.iter().any(
            |o| matches!(o, Output::ApplyCommitted { entries } if entries == &vec![LogEntry { term: 1, index: 1, command: b"hello".to_vec() }])
        ));
        assert_eq!(n.last_applied, 1);
    }
}
