/// Tunable timing parameters for a Raft node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftConfig {
    /// Minimum randomized election timeout, in milliseconds.
    pub election_timeout_min_ms: u64,
    /// Maximum randomized election timeout, in milliseconds.
    pub election_timeout_max_ms: u64,
    /// Interval between leader heartbeats (`AppendEntries` with no new
    /// entries), in milliseconds. Should be well below
    /// `election_timeout_min_ms`.
    pub heartbeat_interval_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
        }
    }
}
