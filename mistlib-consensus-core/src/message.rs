use crate::storage::RaftSnapshot;
use mistlib_core::types::NodeId;
use serde::{Deserialize, Serialize};

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

/// Wire messages exchanged between Raft peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
        /// Who is replying — needed since responses arrive over a
        /// point-to-point transport that only tells us the source `NodeId`.
        voter_id: NodeId,
    },
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
        /// Who is replying.
        follower_id: NodeId,
    },
    InstallSnapshot {
        term: u64,
        leader_id: NodeId,
        snapshot: RaftSnapshot,
    },
    InstallSnapshotResponse {
        term: u64,
        follower_id: NodeId,
        success: bool,
        last_included_index: u64,
    },
    AppendEntriesConflict {
        term: u64,
        follower_id: NodeId,
        conflict_index: u64,
        conflict_term: Option<u64>,
        rejected_prev_index: u64,
    },
}

impl RaftMessage {
    pub fn term(&self) -> u64 {
        match self {
            RaftMessage::RequestVote { term, .. } => *term,
            RaftMessage::RequestVoteResponse { term, .. } => *term,
            RaftMessage::AppendEntries { term, .. } => *term,
            RaftMessage::AppendEntriesResponse { term, .. } => *term,
            RaftMessage::InstallSnapshot { term, .. } => *term,
            RaftMessage::InstallSnapshotResponse { term, .. } => *term,
            RaftMessage::AppendEntriesConflict { term, .. } => *term,
        }
    }
}
