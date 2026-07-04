use crate::message::LogEntry;
use mistlib_core::types::{HostSendSync, NodeId};

/// Persistence for the durable subset of Raft state (current term, vote,
/// and the log). Implementations must make `append`/`set_term_and_vote`
/// durable before the corresponding RPC response is sent, per the Raft
/// paper — `MemoryStorage` here is for tests/single-process use and does
/// not actually persist anything to disk.
pub trait RaftStorage: HostSendSync {
    fn current_term(&self) -> u64;
    fn voted_for(&self) -> Option<NodeId>;
    fn set_term_and_vote(&mut self, term: u64, voted_for: Option<NodeId>);

    fn log(&self) -> &[LogEntry];
    /// Append entries, truncating any conflicting suffix first.
    /// `entries` are assumed already validated against `prev_log_index/term`.
    fn append_entries(&mut self, start_index: u64, entries: Vec<LogEntry>);
    fn last_log_index(&self) -> u64;
    fn last_log_term(&self) -> u64;
    fn term_at(&self, index: u64) -> Option<u64>;
    /// Entries starting at `from` (1-based), inclusive.
    fn entries_from(&self, from: u64) -> Vec<LogEntry>;
    fn truncate_from(&mut self, index: u64);
}

#[derive(Debug, Default)]
pub struct MemoryStorage {
    term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftStorage for MemoryStorage {
    fn current_term(&self) -> u64 {
        self.term
    }

    fn voted_for(&self) -> Option<NodeId> {
        self.voted_for.clone()
    }

    fn set_term_and_vote(&mut self, term: u64, voted_for: Option<NodeId>) {
        self.term = term;
        self.voted_for = voted_for;
    }

    fn log(&self) -> &[LogEntry] {
        &self.log
    }

    fn append_entries(&mut self, start_index: u64, entries: Vec<LogEntry>) {
        // start_index is 1-based; truncate any existing suffix from there.
        self.truncate_from(start_index);
        self.log.extend(entries);
    }

    fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log
            .iter()
            .find(|e| e.index == index)
            .map(|e| e.term)
    }

    fn entries_from(&self, from: u64) -> Vec<LogEntry> {
        self.log
            .iter()
            .filter(|e| e.index >= from)
            .cloned()
            .collect()
    }

    fn truncate_from(&mut self, index: u64) {
        self.log.retain(|e| e.index < index);
    }
}
