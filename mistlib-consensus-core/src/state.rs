//! Pure synchronous Raft core. Drivers persist storage and application
//! effects before transmitting the corresponding acknowledgements.

use crate::message::{LogEntry, RaftMessage};
use crate::storage::{RaftMembership, RaftSnapshot, RaftStorage};
use bincode::Options;
use mistlib_core::types::NodeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const MEMBERSHIP_PREFIX: &[u8] = b"\0mistlib-raft-membership-v1\0";
const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_APPEND_ENTRIES: usize = 128;
const MAX_APPEND_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub enum Event {
    Message {
        from: NodeId,
        msg: RaftMessage,
    },
    ElectionTimeout,
    HeartbeatTimeout,
    ClientPropose {
        command: Vec<u8>,
    },
    /// Compatibility alias: add a nonvoting learner through the log.
    AddPeer {
        peer: NodeId,
    },
    /// Compatibility alias: request a replicated membership change.
    RemovePeer {
        peer: NodeId,
    },
    CompactSnapshot {
        index: u64,
        data: Vec<u8>,
    },
    AddLearner {
        peer: NodeId,
    },
    ChangeMembership {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    },
}

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
    InstallSnapshot { snapshot: RaftSnapshot },
    SnapshotCompacted { index: u64 },
    SnapshotRejected,
    MembershipChanged { membership: RaftMembership },
    MembershipRejected,
}

pub struct RaftState<S: RaftStorage> {
    pub id: NodeId,
    /// All replication destinations, including nonvoting learners.
    pub peers: HashSet<NodeId>,
    pub membership: RaftMembership,
    pub role: Role,
    pub storage: S,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: Option<NodeId>,
    genesis: RaftMembership,
    votes_received: HashSet<NodeId>,
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    acknowledged: HashSet<NodeId>,
    ephemeral: bool,
}

pub fn valid_membership(m: &RaftMembership) -> bool {
    fn unique(v: &[NodeId]) -> bool {
        !v.is_empty()
            && v.len() <= 64
            && v.iter().all(|id| !id.0.is_empty() && id.0.len() <= 128)
            && v.iter().collect::<HashSet<_>>().len() == v.len()
    }
    unique(&m.voters)
        && m.joint_voters.as_ref().is_none_or(|v| unique(v))
        && m.learners.len() <= 64
        && m.learners.iter().collect::<HashSet<_>>().len() == m.learners.len()
        && m.learners.iter().all(|id| {
            !id.0.is_empty()
                && id.0.len() <= 128
                && !m.voters.contains(id)
                && !m.joint_voters.as_ref().is_some_and(|v| v.contains(id))
        })
}

fn membership_command(m: &RaftMembership) -> Vec<u8> {
    let mut bytes = MEMBERSHIP_PREFIX.to_vec();
    bytes.extend(bincode::serialize(m).expect("membership serialization"));
    bytes
}

pub fn decode_membership(command: &[u8]) -> Option<RaftMembership> {
    let payload = command.strip_prefix(MEMBERSHIP_PREFIX)?;
    if payload.len() > 1024 * 1024 {
        return None;
    }
    let m = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(1024 * 1024)
        .reject_trailing_bytes()
        .deserialize(payload)
        .ok()?;
    valid_membership(&m).then_some(m)
}

impl<S: RaftStorage> RaftState<S> {
    pub fn new(id: NodeId, peers: Vec<NodeId>, storage: S) -> Self {
        let mut voters: Vec<_> = peers
            .into_iter()
            .chain(std::iter::once(id.clone()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        voters.sort_by(|a, b| a.0.cmp(&b.0));
        let genesis = RaftMembership {
            voters,
            joint_voters: None,
            learners: Vec::new(),
        };
        Self::with_membership(id, genesis, storage).expect("valid initial membership")
    }

    /// Initial configuration is operator supplied, and must remain immutable
    /// across restarts. It may contain this node only as a learner.
    pub fn with_membership(id: NodeId, genesis: RaftMembership, storage: S) -> Option<Self> {
        if !valid_membership(&genesis) || genesis.joint_voters.is_some() {
            return None;
        }
        if storage.snapshot().is_some_and(|snapshot| {
            snapshot
                .membership
                .as_ref()
                .is_some_and(|m| !valid_membership(m))
        }) || storage.log().iter().any(|entry| {
            entry.command.starts_with(MEMBERSHIP_PREFIX)
                && decode_membership(&entry.command).is_none()
        }) {
            return None;
        }
        let boundary = storage.snapshot().map_or(0, |s| s.last_included_index);
        let mut state = Self {
            id,
            peers: HashSet::new(),
            membership: genesis.clone(),
            genesis,
            role: Role::Follower,
            storage,
            commit_index: boundary,
            last_applied: boundary,
            leader_id: None,
            votes_received: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            acknowledged: HashSet::new(),
            ephemeral: false,
        };
        state.refresh_membership();
        Some(state)
    }

    pub fn current_term(&self) -> u64 {
        self.storage.current_term()
    }

    pub fn committed_membership(&self) -> RaftMembership {
        self.config_at(self.commit_index)
    }

    pub fn accepts_peer(&self, peer: &NodeId) -> bool {
        if self.peers.contains(peer) {
            return true;
        }
        let committed = self.committed_membership();
        committed.voters.contains(peer)
            || committed
                .joint_voters
                .as_ref()
                .is_some_and(|v| v.contains(peer))
            || committed.learners.contains(peer)
    }

    pub fn matched_index(&self, peer: &NodeId) -> u64 {
        if peer == &self.id {
            self.storage.last_log_index()
        } else {
            self.match_index.get(peer).copied().unwrap_or(0)
        }
    }

    fn config_at(&self, index: u64) -> RaftMembership {
        let mut config = self
            .storage
            .snapshot()
            .and_then(|s| s.membership)
            .unwrap_or_else(|| self.genesis.clone());
        for entry in self.storage.log().iter().take_while(|e| e.index <= index) {
            if let Some(m) = decode_membership(&entry.command) {
                config = m;
            }
        }
        config
    }

    fn refresh_membership(&mut self) {
        self.membership = self.config_at(self.storage.last_log_index());
        let mut peers: HashSet<_> = self
            .membership
            .voters
            .iter()
            .chain(self.membership.joint_voters.iter().flatten())
            .chain(self.membership.learners.iter())
            .cloned()
            .collect();
        peers.remove(&self.id);
        self.next_index.retain(|p, _| peers.contains(p));
        self.match_index.retain(|p, _| peers.contains(p));
        self.acknowledged.retain(|p| peers.contains(p));
        for peer in &peers {
            self.next_index.entry(peer.clone()).or_insert(1);
            self.match_index.entry(peer.clone()).or_insert(0);
        }
        self.peers = peers;
    }

    fn voter(&self, id: &NodeId) -> bool {
        self.membership.voters.contains(id)
            || self
                .membership
                .joint_voters
                .as_ref()
                .is_some_and(|v| v.contains(id))
    }

    fn quorum(&self, acked: impl Fn(&NodeId) -> bool) -> bool {
        let majority = |v: &[NodeId]| v.iter().filter(|id| acked(id)).count() > v.len() / 2;
        majority(&self.membership.voters)
            && self
                .membership
                .joint_voters
                .as_ref()
                .is_none_or(|v| majority(v))
    }

    fn become_follower(&mut self, term: u64, out: &mut Vec<Output>) {
        let was_leader = self.role == Role::Leader;
        if term > self.storage.current_term() {
            self.storage.set_term_and_vote(term, None);
            self.leader_id = None;
        }
        self.role = Role::Follower;
        self.votes_received.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.acknowledged.clear();
        if was_leader {
            out.push(Output::BecameFollower);
        }
    }

    fn become_candidate(&mut self, out: &mut Vec<Output>) {
        if !self.voter(&self.id) {
            out.push(Output::ResetElectionTimer);
            return;
        }
        let new_term = self.current_term().saturating_add(1);
        self.storage
            .set_term_and_vote(new_term, Some(self.id.clone()));
        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.id.clone());
        out.push(Output::ResetElectionTimer);
        for peer in self.peers.iter().filter(|p| self.voter(p)) {
            out.push(Output::SendMessage {
                to: peer.clone(),
                msg: RaftMessage::RequestVote {
                    term: new_term,
                    candidate_id: self.id.clone(),
                    last_log_index: self.storage.last_log_index(),
                    last_log_term: self.storage.last_log_term(),
                },
            });
        }
        if self.quorum(|id| self.votes_received.contains(id)) {
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
        self.acknowledged.clear();
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), next);
            self.match_index.insert(peer.clone(), 0);
        }
        out.push(Output::BecameLeader);
        out.push(Output::ResetHeartbeatTimer);
        self.send_append_entries_to_all(out);
        self.finish_joint_if_committed(out);
    }

    fn send_append_entries_to_all(&self, out: &mut Vec<Output>) {
        for peer in &self.peers {
            out.push(self.append_entries_for(peer));
        }
    }

    fn append_entries_for(&self, peer: &NodeId) -> Output {
        let next = self.next_index.get(peer).copied().unwrap_or(1);
        if let Some(snapshot) = self.storage.snapshot() {
            if next <= snapshot.last_included_index {
                return Output::SendMessage {
                    to: peer.clone(),
                    msg: RaftMessage::InstallSnapshot {
                        term: self.current_term(),
                        leader_id: self.id.clone(),
                        snapshot,
                    },
                };
            }
        }
        let prev_log_index = next.saturating_sub(1);
        Output::SendMessage {
            to: peer.clone(),
            msg: RaftMessage::AppendEntries {
                term: self.current_term(),
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term: self.storage.term_at(prev_log_index).unwrap_or(0),
                entries: self
                    .storage
                    .entries_range(next, MAX_APPEND_ENTRIES, MAX_APPEND_BYTES),
                leader_commit: self.commit_index,
            },
        }
    }

    fn maybe_apply_committed(&mut self, out: &mut Vec<Output>) {
        if self.commit_index <= self.last_applied {
            return;
        }
        let entries: Vec<_> = self
            .storage
            .entries_from(self.last_applied + 1)
            .into_iter()
            .take_while(|e| e.index <= self.commit_index)
            .collect();
        if let Some(last) = entries.last() {
            self.last_applied = last.index;
        }
        for entry in &entries {
            if let Some(m) = decode_membership(&entry.command) {
                out.push(Output::MembershipChanged { membership: m });
            }
        }
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|e| decode_membership(&e.command).is_none())
            .collect();
        if !entries.is_empty() {
            out.push(Output::ApplyCommitted { entries });
        }
    }

    fn advance_leader_commit_index(&mut self, out: &mut Vec<Output>) {
        let mut indices: Vec<_> = self
            .match_index
            .values()
            .copied()
            .chain(std::iter::once(self.storage.last_log_index()))
            .collect();
        indices.sort_unstable_by(|a, b| b.cmp(a));
        indices.dedup();
        for index in indices {
            if index <= self.commit_index {
                break;
            }
            if self.storage.term_at(index) == Some(self.current_term())
                && self.quorum(|id| self.matched_index(id) >= index)
            {
                self.commit_index = index;
                self.maybe_apply_committed(out);
                self.finish_joint_if_committed(out);
                let committed_membership = self.config_at(self.commit_index);
                let still_voter = committed_membership.voters.contains(&self.id)
                    || committed_membership
                        .joint_voters
                        .as_ref()
                        .is_some_and(|v| v.contains(&self.id));
                if !still_voter {
                    self.send_append_entries_to_all(out);
                    self.become_follower(self.current_term(), out);
                    self.leader_id = None;
                }
                break;
            }
        }
    }

    fn finish_joint_if_committed(&mut self, out: &mut Vec<Output>) {
        let Some(voters) = self.membership.joint_voters.clone() else {
            return;
        };
        let latest_config = self
            .storage
            .log()
            .iter()
            .rev()
            .find(|e| decode_membership(&e.command).is_some())
            .map(|e| e.index)
            .unwrap_or_else(|| self.storage.snapshot().map_or(0, |s| s.last_included_index));
        if latest_config > self.commit_index {
            return;
        }
        let stable = RaftMembership {
            voters,
            joint_voters: None,
            learners: self.membership.learners.clone(),
        };
        self.append_config(stable, out);
    }

    fn append_config(&mut self, membership: RaftMembership, out: &mut Vec<Output>) {
        let index = self.storage.last_log_index() + 1;
        self.storage.append_entries(
            index,
            vec![LogEntry {
                term: self.current_term(),
                index,
                command: membership_command(&membership),
            }],
        );
        self.refresh_membership();
        out.push(Output::Proposed { index });
        self.send_append_entries_to_all(out);
        self.advance_leader_commit_index(out);
    }

    fn change_membership(
        &mut self,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        out: &mut Vec<Output>,
    ) {
        let target = RaftMembership {
            voters: voters.clone(),
            joint_voters: None,
            learners: learners.clone(),
        };
        let latest_config = self
            .storage
            .log()
            .iter()
            .rev()
            .find(|e| decode_membership(&e.command).is_some())
            .map_or(0, |e| e.index);
        if self.ephemeral
            || self.role != Role::Leader
            || !valid_membership(&target)
            || self.membership.joint_voters.is_some()
            || latest_config > self.commit_index
            || voters.iter().any(|id| {
                !self.membership.voters.contains(id)
                    && (!self.membership.learners.contains(id)
                        || !self.acknowledged.contains(id)
                        || self.matched_index(id) < self.commit_index)
            })
        {
            out.push(Output::MembershipRejected);
            return;
        }
        let joint = if voters == self.membership.voters {
            target
        } else {
            // A removed voter remains a voter until the joint transition completes.
            RaftMembership {
                voters: self.membership.voters.clone(),
                joint_voters: Some(voters),
                learners,
            }
        };
        if !valid_membership(&joint) {
            out.push(Output::MembershipRejected);
            return;
        }
        self.append_config(joint, out);
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
                if !self.ephemeral
                    && self.role == Role::Leader
                    && !command.starts_with(MEMBERSHIP_PREFIX)
                {
                    let index = self.storage.last_log_index() + 1;
                    self.storage.append_entries(
                        index,
                        vec![LogEntry {
                            term: self.current_term(),
                            index,
                            command,
                        }],
                    );
                    out.push(Output::Proposed { index });
                    self.advance_leader_commit_index(&mut out);
                    self.send_append_entries_to_all(&mut out);
                } else {
                    out.push(Output::ProposeRejected {
                        leader_hint: self.leader_id.clone(),
                    });
                }
            }
            Event::CompactSnapshot { index, data } => {
                let term = self.storage.term_at(index);
                let old = self.storage.snapshot().map_or(0, |s| s.last_included_index);
                if !self.ephemeral
                    && index > old
                    && index <= self.last_applied
                    && index <= self.commit_index
                    && data.len() <= MAX_SNAPSHOT_BYTES
                    && term.is_some_and(|t| t > 0)
                    && self.storage.install_snapshot(RaftSnapshot {
                        last_included_index: index,
                        last_included_term: term.unwrap(),
                        data: Arc::new(data),
                        membership: Some(self.config_at(index)),
                    })
                {
                    out.push(Output::SnapshotCompacted { index });
                } else {
                    out.push(Output::SnapshotRejected);
                }
            }
            Event::AddPeer { peer } if self.ephemeral => {
                if !peer.0.is_empty()
                    && peer.0.len() <= 128
                    && self.membership.voters.len() < 64
                    && !self.membership.voters.contains(&peer)
                {
                    self.membership.voters.push(peer);
                    self.membership.voters.sort_by(|a, b| a.0.cmp(&b.0));
                    self.genesis = self.membership.clone();
                    self.refresh_membership();
                }
            }
            Event::RemovePeer { peer } if self.ephemeral => {
                if peer != self.id {
                    self.membership.voters.retain(|id| id != &peer);
                    self.genesis = self.membership.clone();
                    self.refresh_membership();
                    self.votes_received.remove(&peer);
                }
            }
            Event::AddPeer { peer } | Event::AddLearner { peer } => {
                let mut learners = self.membership.learners.clone();
                if !self.voter(&peer) && !learners.contains(&peer) {
                    learners.push(peer);
                }
                self.change_membership(self.membership.voters.clone(), learners, &mut out);
            }
            Event::RemovePeer { peer } => {
                let voters = self
                    .membership
                    .voters
                    .iter()
                    .filter(|id| *id != &peer)
                    .cloned()
                    .collect();
                let learners = self
                    .membership
                    .learners
                    .iter()
                    .filter(|id| *id != &peer)
                    .cloned()
                    .collect();
                self.change_membership(voters, learners, &mut out);
            }
            Event::ChangeMembership { voters, learners } => {
                self.change_membership(voters, learners, &mut out)
            }
        }
        out
    }

    fn handle_message(&mut self, from: NodeId, msg: RaftMessage, out: &mut Vec<Output>) {
        if self.ephemeral {
            let election_only = match &msg {
                RaftMessage::RequestVote {
                    last_log_index,
                    last_log_term,
                    ..
                } => *last_log_index == 0 && *last_log_term == 0,
                RaftMessage::RequestVoteResponse { .. } => true,
                RaftMessage::AppendEntries {
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    ..
                } => {
                    *prev_log_index == 0
                        && *prev_log_term == 0
                        && entries.is_empty()
                        && *leader_commit == 0
                }
                RaftMessage::AppendEntriesResponse { match_index, .. } => *match_index == 0,
                _ => false,
            };
            if !election_only {
                return;
            }
        }
        let claimed = match &msg {
            RaftMessage::RequestVote { candidate_id, .. } => candidate_id,
            RaftMessage::RequestVoteResponse { voter_id, .. } => voter_id,
            RaftMessage::AppendEntries { leader_id, .. }
            | RaftMessage::InstallSnapshot { leader_id, .. } => leader_id,
            RaftMessage::AppendEntriesResponse { follower_id, .. }
            | RaftMessage::InstallSnapshotResponse { follower_id, .. }
            | RaftMessage::AppendEntriesConflict { follower_id, .. } => follower_id,
        };
        if &from != claimed || (!self.accepts_peer(&from) && from != self.id) {
            return;
        }
        let msg_term = msg.term();
        if msg_term > self.current_term() {
            self.become_follower(msg_term, out);
        }
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                let up_to_date = last_log_term > self.storage.last_log_term()
                    || (last_log_term == self.storage.last_log_term()
                        && last_log_index >= self.storage.last_log_index());
                let can_vote = self.storage.voted_for().is_none_or(|id| id == candidate_id);
                let grant = term == self.current_term()
                    && self.voter(&self.id)
                    && self.voter(&candidate_id)
                    && can_vote
                    && up_to_date;
                if grant {
                    self.storage
                        .set_term_and_vote(term, Some(candidate_id.clone()));
                    out.push(Output::ResetElectionTimer);
                }
                out.push(Output::SendMessage {
                    to: candidate_id,
                    msg: RaftMessage::RequestVoteResponse {
                        term: self.current_term(),
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
                if self.role != Role::Candidate
                    || term != self.current_term()
                    || !self.voter(&voter_id)
                {
                    return;
                }
                if vote_granted {
                    self.votes_received.insert(voter_id);
                    if self.quorum(|id| self.votes_received.contains(id)) {
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
                if term < self.current_term() {
                    out.push(Output::SendMessage {
                        to: leader_id,
                        msg: RaftMessage::AppendEntriesResponse {
                            term: self.current_term(),
                            success: false,
                            match_index: 0,
                            follower_id: self.id.clone(),
                        },
                    });
                    return;
                }
                if self.role != Role::Follower {
                    self.become_follower(term, out);
                }
                self.leader_id = Some(leader_id.clone());
                out.push(Output::ResetElectionTimer);
                let sequential = entries.iter().enumerate().all(|(i, e)| {
                    e.index == prev_log_index.saturating_add(i as u64 + 1)
                        && e.term > 0
                        && e.term <= term
                        && (!e.command.starts_with(MEMBERSHIP_PREFIX)
                            || decode_membership(&e.command).is_some())
                });
                if !sequential {
                    return;
                }
                if self.storage.term_at(prev_log_index) != Some(prev_log_term) {
                    let conflict_term = self.storage.term_at(prev_log_index);
                    let conflict_index = if let Some(t) = conflict_term {
                        self.storage
                            .log()
                            .iter()
                            .find(|e| e.term == t)
                            .map_or(prev_log_index, |e| e.index)
                    } else {
                        self.storage.last_log_index().saturating_add(1)
                    };
                    // Keep the legacy rejection for peers predating conflict hints.
                    out.push(Output::SendMessage {
                        to: leader_id.clone(),
                        msg: RaftMessage::AppendEntriesResponse {
                            term,
                            success: false,
                            match_index: 0,
                            follower_id: self.id.clone(),
                        },
                    });
                    out.push(Output::SendMessage {
                        to: leader_id,
                        msg: RaftMessage::AppendEntriesConflict {
                            term,
                            follower_id: self.id.clone(),
                            conflict_index,
                            conflict_term,
                            rejected_prev_index: prev_log_index,
                        },
                    });
                    return;
                }
                let matched = prev_log_index.saturating_add(entries.len() as u64);
                if let Some(position) = entries
                    .iter()
                    .position(|e| self.storage.term_at(e.index) != Some(e.term))
                {
                    let start = entries[position].index;
                    if start <= self.commit_index {
                        return;
                    }
                    self.storage
                        .append_entries(start, entries[position..].to_vec());
                    self.refresh_membership();
                }
                // A delayed partial append/heartbeat certifies only its prefix.
                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(matched);
                    self.maybe_apply_committed(out);
                }
                out.push(Output::SendMessage {
                    to: leader_id,
                    msg: RaftMessage::AppendEntriesResponse {
                        term,
                        success: true,
                        match_index: matched,
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
                if self.role != Role::Leader
                    || term != self.current_term()
                    || !self.peers.contains(&follower_id)
                {
                    return;
                }
                if success {
                    if match_index > self.storage.last_log_index() {
                        return;
                    }
                    self.acknowledged.insert(follower_id.clone());
                    let matched = self.match_index.entry(follower_id.clone()).or_insert(0);
                    *matched = (*matched).max(match_index);
                    self.next_index.insert(follower_id.clone(), *matched + 1);
                    self.advance_leader_commit_index(out);
                    if self.role == Role::Leader
                        && self
                            .next_index
                            .get(&follower_id)
                            .is_some_and(|n| *n <= self.storage.last_log_index())
                    {
                        out.push(self.append_entries_for(&follower_id));
                    }
                } else {
                    let next = self.next_index.entry(follower_id.clone()).or_insert(1);
                    *next = next.saturating_sub(1).max(1);
                    out.push(self.append_entries_for(&follower_id));
                }
            }
            RaftMessage::AppendEntriesConflict {
                term,
                follower_id,
                conflict_index,
                conflict_term,
                rejected_prev_index,
            } => {
                if self.role != Role::Leader || term != self.current_term() {
                    return;
                }
                let next = self.next_index.get(&follower_id).copied().unwrap_or(1);
                // Hint may follow its legacy rejection, which decremented next once.
                if next > rejected_prev_index.saturating_add(1)
                    || self.matched_index(&follower_id) >= rejected_prev_index
                {
                    return;
                }
                let jump = conflict_term
                    .and_then(|t| {
                        self.storage
                            .log()
                            .iter()
                            .rev()
                            .find(|e| e.term == t)
                            .map(|e| e.index + 1)
                    })
                    .unwrap_or(conflict_index)
                    .max(self.matched_index(&follower_id) + 1)
                    .min(self.storage.last_log_index() + 1);
                self.next_index.insert(follower_id.clone(), jump);
                out.push(self.append_entries_for(&follower_id));
            }
            RaftMessage::InstallSnapshot {
                term,
                leader_id,
                snapshot,
            } => {
                if term < self.current_term() {
                    out.push(Output::SendMessage {
                        to: leader_id,
                        msg: RaftMessage::InstallSnapshotResponse {
                            term: self.current_term(),
                            follower_id: self.id.clone(),
                            success: false,
                            last_included_index: snapshot.last_included_index,
                        },
                    });
                    return;
                }
                if self.role != Role::Follower {
                    self.become_follower(term, out);
                }
                self.leader_id = Some(leader_id.clone());
                out.push(Output::ResetElectionTimer);
                let local_snapshot = self.storage.snapshot();
                let boundary = local_snapshot.as_ref().map_or(0, |s| s.last_included_index);
                let valid = snapshot.last_included_index > 0
                    && snapshot.last_included_term > 0
                    && snapshot.last_included_term <= term
                    && snapshot.data.len() <= MAX_SNAPSHOT_BYTES
                    && snapshot.membership.as_ref().is_none_or(valid_membership);
                let already = valid
                    && (snapshot.last_included_index < boundary
                        || (snapshot.last_included_index == boundary
                            && local_snapshot.as_ref().is_some_and(|old| {
                                old.last_included_term == snapshot.last_included_term
                            })));
                let installed = valid
                    && snapshot.last_included_index > self.commit_index
                    && self.storage.install_snapshot(snapshot.clone());
                if installed {
                    self.commit_index = snapshot.last_included_index;
                    self.last_applied = snapshot.last_included_index;
                    self.refresh_membership();
                    out.push(Output::InstallSnapshot {
                        snapshot: snapshot.clone(),
                    });
                }
                out.push(Output::SendMessage {
                    to: leader_id,
                    msg: RaftMessage::InstallSnapshotResponse {
                        term: self.current_term(),
                        follower_id: self.id.clone(),
                        success: already || installed,
                        last_included_index: snapshot.last_included_index,
                    },
                });
            }
            RaftMessage::InstallSnapshotResponse {
                term,
                follower_id,
                success,
                last_included_index,
            } => {
                if self.role != Role::Leader || term != self.current_term() || !success {
                    return;
                }
                if !self
                    .storage
                    .snapshot()
                    .is_some_and(|s| s.last_included_index == last_included_index)
                {
                    return;
                }
                self.acknowledged.insert(follower_id.clone());
                let matched = self.match_index.entry(follower_id.clone()).or_insert(0);
                *matched = (*matched).max(last_included_index);
                self.next_index.insert(follower_id.clone(), *matched + 1);
                out.push(self.append_entries_for(&follower_id));
            }
        }
    }
}

impl RaftState<crate::storage::MemoryStorage> {
    /// Advisory election for a transient control plane. Discovery changes
    /// voters immediately, so this mode cannot replicate application data.
    pub fn new_ephemeral(id: NodeId, peers: Vec<NodeId>) -> Self {
        let mut state = Self::new(id, peers, crate::storage::MemoryStorage::new());
        state.ephemeral = true;
        state
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
        assert!(out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::AppendEntriesResponse { success: true, .. },
                ..
            }
        )));
    }

    #[test]
    fn vote_denied_on_stale_log() {
        let mut n = node("a", &["b"]);
        // Give the follower a log entry at term 1.
        n.storage.set_term_and_vote(1, None);
        n.storage.append_entries(
            1,
            vec![LogEntry {
                term: 1,
                index: 1,
                command: vec![],
            }],
        );

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
                    msg: RaftMessage::RequestVoteResponse {
                        vote_granted: true,
                        ..
                    },
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
        let out = leader.step(Event::ClientPropose {
            command: b"x".to_vec(),
        });
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
        assert!(!out
            .iter()
            .any(|o| matches!(o, Output::ApplyCommitted { .. })));
    }

    #[test]
    fn conflicting_entries_are_truncated() {
        let mut follower = node("a", &["leader"]);
        // Follower has a stale, uncommitted entry at index 1 term 1.
        follower.storage.set_term_and_vote(1, None);
        follower.storage.append_entries(
            1,
            vec![LogEntry {
                term: 1,
                index: 1,
                command: b"old".to_vec(),
            }],
        );

        // Leader (term 2) sends a conflicting entry at index 1.
        let out = follower.step(Event::Message {
            from: NodeId("leader".into()),
            msg: RaftMessage::AppendEntries {
                term: 2,
                leader_id: NodeId("leader".into()),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 2,
                    index: 1,
                    command: b"new".to_vec(),
                }],
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
        assert!(out.iter().any(|o| matches!(
            o,
            Output::SendMessage {
                msg: RaftMessage::AppendEntriesResponse { success: true, .. },
                ..
            }
        )));
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
        let out = leader.step(Event::ClientPropose {
            command: b"x".to_vec(),
        });
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::ProposeRejected { .. })));
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
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::ApplyCommitted { .. })));

        // But "b"'s actual log only has entry 1 (it lost entry 2 somehow),
        // so a prev_log_index=2 probe from the leader must be rejected.
        let mut follower = node("b", &["a"]);
        follower.storage.set_term_and_vote(1, None);
        follower.storage.append_entries(
            1,
            vec![LogEntry {
                term: 1,
                index: 1,
                command: b"1".to_vec(),
            }],
        );

        let reject_out = follower.step(Event::Message {
            from: NodeId("a".into()),
            msg: RaftMessage::AppendEntries {
                term: 1,
                leader_id: NodeId("a".into()),
                prev_log_index: 2,
                prev_log_term: 1,
                entries: vec![LogEntry {
                    term: 1,
                    index: 3,
                    command: b"3".to_vec(),
                }],
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
                Output::SendMessage {
                    msg: msg @ RaftMessage::AppendEntries { .. },
                    ..
                } => Some(msg),
                _ => None,
            })
            .expect("leader must retry with an AppendEntries after a rejection");
        let RaftMessage::AppendEntries {
            prev_log_index,
            prev_log_term,
            ref entries,
            ..
        } = retry_msg
        else {
            unreachable!()
        };
        assert_eq!(
            prev_log_index, 1,
            "must back off to the last index b actually has"
        );
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
                msg: RaftMessage::AppendEntriesResponse {
                    success: true,
                    match_index: 3,
                    ..
                },
                ..
            }
        )));
        assert_eq!(follower.storage.log().len(), 3);
        assert_eq!(
            follower.storage.last_log_index(),
            leader.storage.last_log_index()
        );
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

        assert_eq!(
            applied_indices,
            vec![1, 2, 3, 4],
            "must apply exactly once, in order"
        );
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
                msg: RaftMessage::RequestVoteResponse {
                    vote_granted: true,
                    ..
                },
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
                msg: RaftMessage::RequestVoteResponse {
                    vote_granted: false,
                    ..
                },
                ..
            }
        )));
        assert_eq!(
            n.storage.voted_for(),
            Some(NodeId("b".into())),
            "vote must not change"
        );

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
                msg: RaftMessage::RequestVoteResponse {
                    vote_granted: true,
                    ..
                },
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
        assert_eq!(
            n.role,
            Role::Leader,
            "sole node must self-elect immediately"
        );
        assert!(out.contains(&Output::BecameLeader));

        let out = n.step(Event::ClientPropose {
            command: b"hello".to_vec(),
        });
        assert!(out.contains(&Output::Proposed { index: 1 }));
        assert_eq!(
            n.commit_index, 1,
            "self-quorum must commit without any peer ack"
        );
        assert!(out.iter().any(
            |o| matches!(o, Output::ApplyCommitted { entries } if entries == &vec![LogEntry { term: 1, index: 1, command: b"hello".to_vec() }])
        ));
        assert_eq!(n.last_applied, 1);
    }
}
