//! Bounded recovery bookkeeping; all time here is supplied as driver ticks.
use super::*;
use crate::hotstuff::transport::MAX_WIRE_MESSAGE_BYTES;

pub(super) const TICK_MS: u64 = 100;
/// Protocol caps, independent of requester-provided counts.
pub const MAX_BLOCKS_PER_RESPONSE: usize = 32;
pub const MAX_BLOCK_RESPONSE_BYTES: u64 = MAX_WIRE_MESSAGE_BYTES;

pub(super) struct Fetch {
    root_view: ViewNumber,
    cursor: BlockHash,
    cursor_view: ViewNumber,
    cursor_height: Option<u64>,
    peers: Vec<NodeId>,
    peer_index: usize,
    attempts: u32,
    request_id: u64,
    due: u64,
}

impl<B: BlockStore, C: CryptoProvider> HotStuffState<B, C> {
    pub(super) fn in_view_window(&self, view: ViewNumber) -> bool {
        view >= self
            .current_view
            .saturating_sub(self.config.sync.max_future_views)
            && view
                <= self
                    .current_view
                    .saturating_add(self.config.sync.max_future_views)
            && view < u64::MAX
    }

    pub(super) fn trim_deferred(&mut self) {
        while self.pending_proposals.len() + self.future_proposals.len()
            > self.config.sync.max_deferred_blocks
            || self
                .pending_proposals
                .values()
                .chain(self.future_proposals.values())
                .map(|b| bincode::serialized_size(b).unwrap_or(u64::MAX))
                .sum::<u64>()
                > self.config.sync.max_deferred_bytes
        {
            let pending = self.pending_proposals.last_key_value().map(|(&v, _)| v);
            let future = self.future_proposals.last_key_value().map(|(&v, _)| v);
            if pending >= future {
                self.pending_proposals.pop_last();
            } else {
                self.future_proposals.pop_last();
            }
        }
    }

    pub(super) fn prune_pending(&mut self) {
        let floor = self.store.locked_qc().view.max(self.committed_view);
        let lower = self
            .current_view
            .saturating_sub(self.config.sync.max_future_views);
        let upper = self
            .current_view
            .saturating_add(self.config.sync.max_future_views);
        let committed_height = self.committed_height;
        let committed_hash = self.committed_hash;
        let relevant = |v: u64, b: &Block| {
            v > floor
                && v >= lower
                && v <= upper
                && b.height > committed_height
                && !(b.height == committed_height.saturating_add(1) && b.parent != committed_hash)
        };
        self.pending_proposals.retain(|&v, b| relevant(v, b));
        self.future_proposals
            .retain(|&v, b| v > self.current_view && relevant(v, b));
        self.proposal_views
            .retain(|&v, _| v > floor && v >= lower && v <= upper);
        self.serve_after
            .retain(|peer, _| self.validators.contains(peer));
        let high = self.store.high_qc().block_hash;
        self.fetches
            .retain(|root, f| *root == high || f.root_view > self.committed_view);
        self.trim_deferred();
    }

    pub(super) fn ensure_high_qc_fetch(&mut self, out: &mut Vec<Output>) {
        let qc = self.store.high_qc();
        if self.store.contains_block(&qc.block_hash)
            || self.fetches.contains_key(&qc.block_hash)
            || self.fetches.values().any(|f| f.cursor == qc.block_hash)
        {
            return;
        }
        if self.fetches.len() >= self.config.sync.max_inflight_requests {
            let victim = self
                .fetches
                .iter()
                .max_by_key(|(h, f)| (f.root_view, **h))
                .map(|(&h, _)| h);
            if let Some(hash) = victim {
                self.fetches.remove(&hash);
            }
        }
        let peer = qc
            .signatures
            .first()
            .map(|s| s.signer.clone())
            .unwrap_or(self.id.clone());
        self.request_block(qc.block_hash, qc.view, peer, out);
    }

    pub(super) fn restart_exhausted_fetches(&mut self, out: &mut Vec<Output>) {
        let roots: Vec<_> = self
            .fetches
            .iter()
            .filter_map(|(&h, f)| {
                (f.attempts >= self.config.sync.max_fetch_attempts && f.due <= self.sync_tick)
                    .then_some(h)
            })
            .collect();
        for root in roots {
            if let Some(f) = self.fetches.get_mut(&root) {
                f.attempts = 0;
            }
            self.send_fetch(root, out);
        }
    }

    pub(super) fn request_block(
        &mut self,
        hash: BlockHash,
        view: ViewNumber,
        preferred: NodeId,
        out: &mut Vec<Output>,
    ) {
        if self.store.contains_block(&hash)
            || self.fetches.contains_key(&hash)
            || self.fetches.values().any(|f| f.cursor == hash)
            || self.config.sync.max_inflight_requests == 0
            || self.config.sync.max_fetch_attempts == 0
        {
            return;
        }
        if self.fetches.len() >= self.config.sync.max_inflight_requests {
            if hash != self.store.high_qc().block_hash {
                return;
            }
            let victim = self
                .fetches
                .iter()
                .max_by_key(|(h, f)| (f.root_view, **h))
                .map(|(&h, _)| h);
            if let Some(victim) = victim {
                self.fetches.remove(&victim);
            }
        }
        let mut peers: Vec<_> = self
            .validators
            .iter()
            .filter(|p| **p != self.id)
            .cloned()
            .collect();
        if let Some(index) = peers.iter().position(|p| *p == preferred) {
            peers[..=index].rotate_right(1);
        }
        if peers.is_empty() {
            return;
        }
        self.fetches.insert(
            hash,
            Fetch {
                root_view: view,
                cursor: hash,
                cursor_view: view,
                cursor_height: None,
                peers,
                peer_index: 0,
                attempts: 0,
                request_id: 0,
                due: self.sync_tick,
            },
        );
        self.send_fetch(hash, out);
    }

    pub(super) fn send_fetch(&mut self, root: BlockHash, out: &mut Vec<Output>) {
        let Some(cursor) = self.fetches.get(&root).map(|f| f.cursor) else {
            return;
        };
        if self
            .fetches
            .iter()
            .any(|(other_root, f)| *other_root != root && f.cursor == cursor && f.attempts > 0)
        {
            return;
        }
        let fetch = self.fetches.get_mut(&root).expect("checked above");
        if fetch.attempts >= self.config.sync.max_fetch_attempts {
            return;
        }
        fetch.peer_index = fetch.attempts as usize % fetch.peers.len();
        fetch.attempts += 1;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        fetch.request_id = self.next_request_id;
        let delay = self
            .config
            .sync
            .retry_base_ms
            .max(TICK_MS)
            .saturating_mul(1u64 << (fetch.attempts - 1).min(5))
            .min(8_000);
        fetch.due = self.sync_tick.saturating_add(delay.div_ceil(TICK_MS));
        out.push(Output::SendMessage {
            to: fetch.peers[fetch.peer_index].clone(),
            msg: HotStuffMessage::BlockRequest {
                hash: fetch.cursor,
                request_id: fetch.request_id,
                max_blocks: MAX_BLOCKS_PER_RESPONSE as u32,
            },
        });
    }

    pub(super) fn sync_maintenance(&mut self, out: &mut Vec<Output>) {
        self.sync_tick = self.sync_tick.saturating_add(1);
        // Shared cursors are served by one request. Other roots resume from
        // their own head once that shared ancestor becomes available.
        self.fetches
            .retain(|root, _| !self.store.contains_block(root));
        for (&root, fetch) in &mut self.fetches {
            if self.store.contains_block(&fetch.cursor) {
                fetch.cursor = root;
                fetch.cursor_view = fetch.root_view;
                fetch.cursor_height = None;
                fetch.attempts = 0;
                fetch.due = self.sync_tick;
            }
        }
        let roots: Vec<_> = self
            .fetches
            .iter()
            .filter_map(|(&root, f)| {
                (f.due <= self.sync_tick && f.attempts < self.config.sync.max_fetch_attempts)
                    .then_some(root)
            })
            .collect();
        for root in roots {
            self.send_fetch(root, out);
        }
        out.push(Output::ScheduleSyncTick {
            duration_ms: TICK_MS,
        });
    }

    pub(super) fn serve_blocks(
        &mut self,
        from: NodeId,
        hash: BlockHash,
        request_id: u64,
        count: u32,
        out: &mut Vec<Output>,
    ) {
        if !self.is_validator(&from)
            || from == self.id
            || self
                .serve_after
                .get(&from)
                .is_some_and(|&due| due > self.sync_tick)
        {
            return;
        }
        self.serve_after.insert(
            from.clone(),
            self.sync_tick.saturating_add(
                self.config
                    .sync
                    .serve_interval_ms
                    .max(TICK_MS)
                    .div_ceil(TICK_MS),
            ),
        );
        let mut blocks = Vec::new();
        // Enum tag + hash + token + Vec length, using bincode fixint encoding.
        let mut bytes = 4 + 32 + 8 + 8;
        let mut cursor = hash;
        for _ in 0..(count as usize).min(MAX_BLOCKS_PER_RESPONSE) {
            let Some(block) = self.store.get_block(&cursor) else {
                break;
            };
            let Ok(size) = bincode::serialized_size(block) else {
                break;
            };
            if bytes + size > MAX_BLOCK_RESPONSE_BYTES {
                break;
            }
            bytes += size;
            blocks.push(block.clone());
            if block.height == 0 {
                break;
            }
            cursor = block.parent;
        }
        if !blocks.is_empty() {
            out.push(Output::SendMessage {
                to: from,
                msg: HotStuffMessage::BlockResponse {
                    hash,
                    request_id,
                    blocks,
                },
            });
        }
    }

    pub(super) fn receive_blocks(
        &mut self,
        from: NodeId,
        hash: BlockHash,
        request_id: u64,
        blocks: Vec<Block>,
        out: &mut Vec<Output>,
    ) {
        let Some((&root, fetch)) = self.fetches.iter().find(|(_, f)| {
            f.cursor == hash && f.request_id == request_id && f.peers[f.peer_index] == from
        }) else {
            return;
        };
        if blocks.is_empty() || blocks.len() > MAX_BLOCKS_PER_RESPONSE {
            return;
        }
        let bytes = blocks.iter().try_fold(52u64, |sum, b| {
            bincode::serialized_size(b).map(|n| sum.saturating_add(n))
        });
        if !bytes.is_ok_and(|n| n <= MAX_BLOCK_RESPONSE_BYTES) {
            return;
        }
        let mut expected_hash = hash;
        let mut expected_view = fetch.cursor_view;
        let mut expected_height = fetch.cursor_height;
        // Validate the entire response before storing anything or changing a cursor.
        for block in &blocks {
            if block.hash() != expected_hash
                || block.view != expected_view
                || expected_height.is_some_and(|h| h != block.height)
            {
                return;
            }
            if block.height == 0 {
                if *block != Block::genesis() {
                    return;
                }
            } else if block.parent != block.justify.block_hash
                || block.justify.view >= block.view
                || !self.verify_qc(&block.justify)
            {
                return;
            }
            if let Some(parent) = self.store.get_block(&block.parent) {
                if parent.height.checked_add(1) != Some(block.height)
                    || parent.view != block.justify.view
                {
                    return;
                }
            }
            expected_hash = block.parent;
            expected_view = block.justify.view;
            expected_height = block.height.checked_sub(1);
        }
        let last = blocks.last().expect("nonempty checked above");
        if !self.store.contains_block(&last.parent) && last.height != 0 {
            // Walk backwards without retaining unanchored batches. After an
            // anchored suffix is stored, restart from root. This trades some
            // bandwidth for memory independent of the missing chain's length.
            let fetch = self.fetches.get_mut(&root).expect("matched above");
            fetch.cursor = last.parent;
            fetch.cursor_view = last.justify.view;
            fetch.cursor_height = last.height.checked_sub(1);
            fetch.attempts = 0;
            self.send_fetch(root, out);
            return;
        }
        // Check the committed branch before any mutation, including when the
        // batch crosses a height we have already committed.
        if self.committed_height > 0 && !self.store.contains_block(&hash) {
            let compatible = blocks.iter().any(|b| b.hash() == self.committed_hash)
                || self.store.get_block(&last.parent).is_some_and(|p| {
                    p.hash() == self.committed_hash || self.extends(p, &self.committed_hash)
                });
            if !compatible {
                return;
            }
        }
        for block in blocks.into_iter().rev() {
            if block.height != 0 {
                self.accept_ready(block, false, out);
            }
        }
        if self.store.contains_block(&root) {
            self.fetches.remove(&root);
        } else if let Some(fetch) = self.fetches.get_mut(&root) {
            fetch.cursor = root;
            fetch.cursor_view = fetch.root_view;
            fetch.cursor_height = None;
            fetch.attempts = 0;
            self.send_fetch(root, out);
        }
    }
}
