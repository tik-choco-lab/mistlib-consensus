use crate::message::LogEntry;
use mistlib_core::types::{HostSendSync, NodeId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMembership {
    pub voters: Vec<NodeId>,
    pub joint_voters: Option<Vec<NodeId>>,
    pub learners: Vec<NodeId>,
}

/// Application state at a committed absolute log index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    #[serde(with = "snapshot_bytes")]
    pub data: Arc<Vec<u8>>,
    pub membership: Option<RaftMembership>,
}

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
    fn snapshot(&self) -> Option<RaftSnapshot> {
        None
    }
    /// Atomically persist the snapshot and compact its log prefix. Retain the
    /// suffix only if its boundary term matches. False means unsupported.
    /// Drivers must durably install application state before sending ACKs.
    fn install_snapshot(&mut self, _snapshot: RaftSnapshot) -> bool {
        false
    }
    fn entries_range(&self, from: u64, max_entries: usize, max_bytes: usize) -> Vec<LogEntry> {
        let mut result = Vec::new();
        let mut bytes = 0usize;
        for entry in self
            .log()
            .iter()
            .filter(|e| e.index >= from)
            .take(max_entries)
        {
            let size = entry.command.len().saturating_add(32);
            if !result.is_empty() && bytes.saturating_add(size) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(size);
            result.push(entry.clone());
        }
        result
    }
}

#[derive(Debug, Default)]
pub struct MemoryStorage {
    term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    snapshot: Option<RaftSnapshot>,
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
        self.log
            .last()
            .map(|e| e.index)
            .unwrap_or_else(|| self.snapshot.as_ref().map_or(0, |s| s.last_included_index))
    }

    fn last_log_term(&self) -> u64 {
        self.log
            .last()
            .map(|e| e.term)
            .unwrap_or_else(|| self.snapshot.as_ref().map_or(0, |s| s.last_included_term))
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if let Some(snapshot) = &self.snapshot {
            if index == snapshot.last_included_index {
                return Some(snapshot.last_included_term);
            }
            if index < snapshot.last_included_index {
                return None;
            }
        }
        if index == 0 {
            return Some(0);
        }
        self.log.iter().find(|e| e.index == index).map(|e| e.term)
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

    fn snapshot(&self) -> Option<RaftSnapshot> {
        self.snapshot.clone()
    }

    fn install_snapshot(&mut self, snapshot: RaftSnapshot) -> bool {
        if snapshot.last_included_index == 0 || snapshot.last_included_term == 0 {
            return false;
        }
        if self
            .snapshot
            .as_ref()
            .is_some_and(|old| old.last_included_index >= snapshot.last_included_index)
        {
            return false;
        }
        if self.term_at(snapshot.last_included_index) == Some(snapshot.last_included_term) {
            self.log.retain(|e| e.index > snapshot.last_included_index);
        } else {
            self.log.clear();
        }
        self.snapshot = Some(snapshot);
        true
    }
}

mod snapshot_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;
    pub fn serialize<S: Serializer>(data: &Arc<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error> {
        data.as_ref().serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Arc<Vec<u8>>, D::Error> {
        Vec::<u8>::deserialize(deserializer).map(Arc::new)
    }
}
