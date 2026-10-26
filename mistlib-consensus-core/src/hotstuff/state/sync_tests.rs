use super::tests::{child, committed, deliver, ids, is_vote, nid, proposal_msg, qc};
use super::*;
use crate::hotstuff::{Ed25519Crypto, MemoryBlockStore, NoopCrypto};

fn node(name: &str, names: &[&str]) -> HotStuffState<MemoryBlockStore, NoopCrypto> {
    let mut n = super::tests::node(name, names);
    n.bootstrap();
    n
}
use ed25519_dalek::{Signer, SigningKey};

const NAMES: [&str; 5] = ["a", "b", "c", "d", "e"];

fn request(out: &[Output]) -> (NodeId, BlockHash, u64) {
    out.iter()
        .find_map(|o| match o {
            Output::SendMessage {
                to,
                msg:
                    HotStuffMessage::BlockRequest {
                        hash, request_id, ..
                    },
            } => Some((to.clone(), *hash, *request_id)),
            _ => None,
        })
        .expect("expected a block fetch")
}

fn response(hash: BlockHash, request_id: u64, blocks: Vec<Block>) -> HotStuffMessage {
    HotStuffMessage::BlockResponse {
        hash,
        request_id,
        blocks,
    }
}

#[test]
fn signed_equivocations_are_first_wins_and_globally_bounded() {
    let keys: Vec<_> = (1..=5).map(|i| SigningKey::from_bytes(&[i; 32])).collect();
    let registry = NAMES
        .iter()
        .zip(&keys)
        .map(|(n, k)| (nid(n), k.verifying_key()))
        .collect();
    let mut n = HotStuffState::new(
        nid("a"),
        ids(&NAMES),
        MemoryBlockStore::new(),
        Ed25519Crypto::new(keys[0].clone(), registry),
        PacemakerConfig {
            sync: SyncConfig {
                max_deferred_blocks: 4,
                max_future_views: 16,
                ..SyncConfig::default()
            },
            ..PacemakerConfig::default()
        },
    );
    n.bootstrap();
    let mut first = None;
    // b is the legitimate leader for views 1,6,11,16. All 400 proposals
    // have valid leader signatures and the same valid genesis QC.
    for view in [16, 11, 6, 1] {
        for variant in 0u32..100 {
            let b = child(
                &Block::genesis(),
                view,
                vec![variant.to_le_bytes().to_vec()],
                QuorumCert::genesis(),
            );
            if view == 6 && variant == 0 {
                first = Some(b.hash());
            }
            let sig = keys[1]
                .sign(&proposal_digest(view, &b.hash()))
                .to_bytes()
                .to_vec();
            n.step(Event::Message {
                from: nid("b"),
                msg: HotStuffMessage::Proposal {
                    block: b,
                    sig: PartialSig {
                        signer: nid("b"),
                        signature: sig,
                    },
                },
            });
            assert!(n.pending_proposals.len() + n.future_proposals.len() <= 4);
        }
    }
    assert_eq!(n.future_proposals[&6].hash(), first.unwrap());
    assert_eq!(n.store.last_voted_view(), 1);
    assert_eq!(n.committed_height(), 0);
    assert_eq!(n.store.blocks_len_for_test(), 2);
}

#[test]
fn deferred_cap_evicts_highest_view_and_bounds_fetches_and_aggregation() {
    let mut n = node("a", &NAMES);
    n.config.sync.max_deferred_blocks = 3;
    n.config.sync.max_future_views = 32;
    n.config.sync.max_inflight_requests = 2;
    let parent = child(&Block::genesis(), 1, vec![], QuorumCert::genesis());
    for view in (2..=30).rev() {
        let b = child(
            &parent,
            view,
            vec![vec![view as u8]],
            qc(&parent, &NAMES[1..]),
        );
        let leader = n.leader_of(view).unwrap();
        n.step(Event::Message {
            from: leader.clone(),
            msg: proposal_msg(&b, &leader.0),
        });
    }
    assert_eq!(
        n.pending_proposals.keys().copied().collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(n.fetches.len(), 1, "same parent is fetched once");
    for view in 1..1000 {
        for variant in 0..20 {
            let hash = BlockHash([variant; 32]);
            deliver(
                &mut n,
                "b",
                HotStuffMessage::Vote {
                    view,
                    block_hash: hash,
                    sig: PartialSig {
                        signer: nid("b"),
                        signature: vec![],
                    },
                },
            );
            deliver(
                &mut n,
                "b",
                HotStuffMessage::NewView {
                    view,
                    high_qc: QuorumCert::genesis(),
                },
            );
        }
    }
    assert!(n.votes.len() <= 33);
    assert!(n.new_views.len() <= 33);
    assert!(n.fetches.len() <= 2);
    let too_far = child(&Block::genesis(), 10_001, vec![], QuorumCert::genesis());
    deliver(&mut n, "b", proposal_msg(&too_far, "b"));
    assert_eq!(n.current_view, 1);
    n.config.sync.max_deferred_bytes = 1;
    n.step(Event::SyncTick);
    assert!(n.pending_proposals.is_empty());
    assert!(n.future_proposals.is_empty());
}

#[test]
fn missing_parent_fetch_replays_child_and_fetched_blocks_do_not_vote() {
    let mut n = node("a", &NAMES);
    n.bootstrap();
    let b1 = child(
        &Block::genesis(),
        1,
        vec![b"first".to_vec()],
        QuorumCert::genesis(),
    );
    let b2 = child(&b1, 2, vec![], qc(&b1, &NAMES[1..]));
    let b3 = child(&b2, 3, vec![], qc(&b2, &NAMES[1..]));
    let b4 = child(&b3, 4, vec![], qc(&b3, &NAMES[1..]));
    let out = deliver(&mut n, "e", proposal_msg(&b4, "e"));
    let (peer, hash, token) = request(&out);
    assert_eq!(peer, nid("e"));
    let out = deliver(
        &mut n,
        &peer.0,
        response(hash, token, vec![b3.clone(), b2.clone(), b1.clone()]),
    );
    assert_eq!(committed(&out)[0].command, b"first");
    assert!(n.store.contains_block(&b4.hash()));
    assert!(!out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: HotStuffMessage::Vote { view: 1..=3, .. },
            ..
        }
    )));
    assert!(n.fetches.is_empty());
}

#[test]
fn invalid_responses_are_atomic_and_leave_pending_state_unchanged() {
    let mut n = node("a", &NAMES);
    n.bootstrap();
    let b1 = child(&Block::genesis(), 1, vec![], QuorumCert::genesis());
    let b2 = child(&b1, 2, vec![], qc(&b1, &NAMES[1..]));
    let b3 = child(&b2, 3, vec![], qc(&b2, &NAMES[1..]));
    let out = deliver(&mut n, "d", proposal_msg(&b3, "d"));
    let (peer, hash, token) = request(&out);
    let mut wrong = b2.clone();
    wrong.commands.push(vec![42]);
    let mut bad_qc = b2.clone();
    bad_qc.justify.signatures[0].signer = nid("outsider");
    let mut duplicate_qc = b2.clone();
    duplicate_qc.justify.signatures[0] = duplicate_qc.justify.signatures[1].clone();
    let mut too_large = b2.clone();
    too_large.justify.signatures[0].signature = vec![0; MAX_BLOCK_RESPONSE_BYTES as usize];
    let mut bad_height = b1.clone();
    bad_height.height = 90;
    for msg in [
        response(hash, token, vec![wrong]),
        response(hash, token, vec![bad_qc, b1.clone()]),
        response(hash, token, vec![duplicate_qc, b1.clone()]),
        response(hash, token, vec![too_large]),
        response(hash, token, vec![b2.clone(); MAX_BLOCKS_PER_RESPONSE + 1]),
        response(hash, token, vec![b2.clone(), bad_height]),
        response(hash, token + 1, vec![b2.clone(), b1.clone()]),
        response(BlockHash::ZERO, token, vec![b2.clone(), b1.clone()]),
    ] {
        let before = (
            n.pending_proposals.len(),
            n.future_proposals.len(),
            n.fetches.len(),
            n.current_view,
        );
        let out = deliver(&mut n, &peer.0, msg);
        assert!(!is_vote(&out));
        assert_eq!(
            before,
            (
                n.pending_proposals.len(),
                n.future_proposals.len(),
                n.fetches.len(),
                n.current_view
            )
        );
        assert!(!n.store.contains_block(&b1.hash()));
        assert!(!n.store.contains_block(&b2.hash()));
    }
    deliver(
        &mut n,
        "outsider",
        response(hash, token, vec![b2.clone(), b1.clone()]),
    );
    assert!(!n.store.contains_block(&b2.hash()));
    deliver(&mut n, &peer.0, response(hash, token, vec![b2.clone(), b1]));
    assert!(n.store.contains_block(&b2.hash()));
    // Completed/unsolicited responses cannot grow storage.
    let arbitrary = child(&Block::genesis(), 10, vec![], QuorumCert::genesis());
    deliver(
        &mut n,
        &peer.0,
        response(arbitrary.hash(), token, vec![arbitrary.clone()]),
    );
    assert!(!n.store.contains_block(&arbitrary.hash()));
}

#[test]
fn fetch_retries_rotate_peers_back_off_and_stop_until_a_new_view() {
    let mut n = node("a", &NAMES);
    n.config.sync.max_fetch_attempts = 3;
    n.bootstrap();
    let b1 = child(&Block::genesis(), 1, vec![], QuorumCert::genesis());
    let b2 = child(&b1, 2, vec![], qc(&b1, &NAMES[1..]));
    let (peer, hash, old_token) = request(&deliver(&mut n, "c", proposal_msg(&b2, "c")));
    assert_eq!(peer, nid("c"));
    for _ in 0..2 {
        assert!(n
            .step(Event::SyncTick)
            .iter()
            .all(|o| !matches!(o, Output::SendMessage { .. })));
    }
    let (peer2, _, token2) = request(&n.step(Event::SyncTick));
    assert_eq!(peer2, nid("b"));
    assert_ne!(token2, old_token);
    deliver(&mut n, &peer.0, response(hash, old_token, vec![b1.clone()]));
    assert!(!n.store.contains_block(&hash), "stale response ignored");
    for _ in 0..5 {
        n.step(Event::SyncTick);
    }
    let (peer3, _, _) = request(&n.step(Event::SyncTick));
    assert_eq!(peer3, nid("d"));
    for _ in 0..100 {
        assert!(n
            .step(Event::SyncTick)
            .iter()
            .all(|o| !matches!(o, Output::SendMessage { .. })));
    }
    assert_eq!(request(&n.step(Event::ViewTimeout)).0, nid("c"));
}

#[test]
fn serving_caps_count_bytes_and_rate_limits_only_validators() {
    let mut n = node("a", &NAMES);
    n.bootstrap();
    let mut parent = Block::genesis();
    for view in 1..=40 {
        let block = child(
            &parent,
            view,
            vec![vec![0; 150_000]],
            qc(&parent, &NAMES[1..]),
        );
        n.store.put_block(block.clone());
        parent = block;
    }
    let msg = HotStuffMessage::BlockRequest {
        hash: parent.hash(),
        request_id: 7,
        max_blocks: u32::MAX,
    };
    assert!(deliver(&mut n, "outsider", msg.clone()).is_empty());
    assert!(n.serve_after.is_empty());
    let out = deliver(&mut n, "b", msg.clone());
    let response = out
        .iter()
        .find_map(|o| match o {
            Output::SendMessage {
                msg: HotStuffMessage::BlockResponse { blocks, .. },
                ..
            } => Some(blocks),
            _ => None,
        })
        .unwrap();
    assert!(response.len() < MAX_BLOCKS_PER_RESPONSE);
    assert!(
        bincode::serialized_size(
            &out.iter()
                .find_map(|o| match o {
                    Output::SendMessage { msg, .. } => Some(msg),
                    _ => None,
                })
                .unwrap()
        )
        .unwrap()
            <= MAX_BLOCK_RESPONSE_BYTES
    );
    assert!(deliver(&mut n, "b", msg.clone()).is_empty());
    n.step(Event::SyncTick);
    assert!(!deliver(&mut n, "b", msg).is_empty());
}

#[test]
fn ancestor_gap_larger_than_response_and_pending_caps_catches_up() {
    let mut n = node("a", &NAMES);
    n.config.sync.max_deferred_blocks = 2;
    n.config.sync.max_future_views = 8;
    n.bootstrap();
    let mut blocks = vec![Block::genesis()];
    for view in 1..=100 {
        blocks.push(child(
            blocks.last().unwrap(),
            view,
            vec![vec![view as u8]],
            qc(blocks.last().unwrap(), &NAMES[1..]),
        ));
    }
    // A freshly certified proposal can catch up beyond the lookahead. None
    // of the 100 permanently missed proposals is delivered to this node.
    let head = child(&blocks[100], 101, vec![], qc(&blocks[100], &NAMES[1..]));
    let mut out = deliver(&mut n, "b", proposal_msg(&head, "b"));
    for _ in 0..40 {
        if n.store.contains_block(&blocks[100].hash()) {
            break;
        }
        let (peer, hash, token) = request(&out);
        let index = blocks.iter().position(|b| b.hash() == hash).unwrap();
        let batch = blocks[index.saturating_sub(MAX_BLOCKS_PER_RESPONSE - 1)..=index]
            .iter()
            .rev()
            .cloned()
            .collect();
        out = deliver(&mut n, &peer.0, response(hash, token, batch));
        assert!(n.fetches.len() <= n.config.sync.max_inflight_requests);
        assert!(n.pending_proposals.len() + n.future_proposals.len() <= 2);
    }
    assert!(n.store.contains_block(&blocks[100].hash()));
    assert_eq!(n.committed_height(), 98);
    assert_eq!(n.exec_sequence, 98);
}

#[test]
fn fetched_justify_signatures_are_verified_with_real_crypto() {
    let keys: Vec<_> = (1..=5).map(|i| SigningKey::from_bytes(&[i; 32])).collect();
    let registry = NAMES
        .iter()
        .zip(&keys)
        .map(|(n, k)| (nid(n), k.verifying_key()))
        .collect();
    let mut n = HotStuffState::new(
        nid("a"),
        ids(&NAMES),
        MemoryBlockStore::new(),
        Ed25519Crypto::new(keys[0].clone(), registry),
        PacemakerConfig::default(),
    );
    n.bootstrap();
    let signed_qc = |block: &Block| QuorumCert {
        view: block.view,
        block_hash: block.hash(),
        signatures: NAMES
            .iter()
            .zip(&keys)
            .skip(1)
            .map(|(name, key)| PartialSig {
                signer: nid(name),
                signature: key
                    .sign(&vote_digest(block.view, &block.hash()))
                    .to_bytes()
                    .to_vec(),
            })
            .collect(),
    };
    let b1 = child(&Block::genesis(), 1, vec![], QuorumCert::genesis());
    let b2 = child(&b1, 2, vec![], signed_qc(&b1));
    let out = n.step(Event::Message {
        from: nid("b"),
        msg: HotStuffMessage::NewView {
            view: 3,
            high_qc: signed_qc(&b2),
        },
    });
    let (peer, hash, token) = request(&out);
    let mut forged = b2.clone();
    forged.justify.signatures[0].signature[0] ^= 1;
    assert_eq!(
        forged.hash(),
        hash,
        "QC witness bytes do not alter content hash"
    );
    n.step(Event::Message {
        from: peer.clone(),
        msg: response(hash, token, vec![forged, b1.clone()]),
    });
    assert_eq!(n.store.blocks_len_for_test(), 1);
    let out = n.step(Event::Message {
        from: peer,
        msg: response(hash, token, vec![b2, b1]),
    });
    assert_eq!(n.store.blocks_len_for_test(), 3);
    assert!(!is_vote(&out));
}

#[test]
fn locks_commits_and_known_conflicting_parents_prune_deferred_entries() {
    let mut n = node("a", &NAMES);
    let b1 = child(&Block::genesis(), 1, vec![], QuorumCert::genesis());
    let b2 = child(&b1, 2, vec![], qc(&b1, &NAMES[1..]));
    let b3 = child(&b2, 3, vec![], qc(&b2, &NAMES[1..]));
    let b4 = child(&b3, 4, vec![], qc(&b3, &NAMES[1..]));
    let unknown = child(
        &Block::genesis(),
        1,
        vec![b"fork".to_vec()],
        QuorumCert::genesis(),
    );
    let stale = child(&unknown, 2, vec![], qc(&unknown, &NAMES[1..]));
    deliver(&mut n, "c", proposal_msg(&stale, "c"));
    assert_eq!(n.pending_proposals.len(), 1);
    // A certified copy is fetched independently of a leader's first-wins
    // choice, so an equivocating proposal cannot veto the certified chain.
    let out = deliver(
        &mut n,
        "e",
        HotStuffMessage::NewView {
            view: 5,
            high_qc: qc(&b4, &NAMES[1..]),
        },
    );
    let (peer, hash, token) = request(&out);
    deliver(&mut n, &peer.0, response(hash, token, vec![b4, b3, b2, b1]));
    assert!(n.locked_qc().view >= 2);
    assert!(n.pending_proposals.is_empty());
    assert!(n.fetches.is_empty());
    assert_eq!(n.committed_height(), 2);
}

#[test]
fn receiving_a_current_proposal_after_fetch_can_vote_but_fetch_itself_cannot() {
    let mut n = node("a", &NAMES);
    let b1 = child(&Block::genesis(), 1, vec![], QuorumCert::genesis());
    let b2 = child(&b1, 2, vec![], qc(&b1, &NAMES[1..]));
    let b3 = child(&b2, 3, vec![], qc(&b2, &NAMES[1..]));
    let mut invalid_child = child(&b3, 4, vec![], qc(&b3, &NAMES[1..]));
    invalid_child.height = 99;
    let out = deliver(&mut n, "e", proposal_msg(&invalid_child, "e"));
    let (peer, hash, token) = request(&out);
    let out = deliver(
        &mut n,
        &peer.0,
        response(hash, token, vec![b3.clone(), b2, b1]),
    );
    assert!(!is_vote(&out));
    assert_eq!(n.current_view, 3);
    assert_eq!(n.store.last_voted_view(), 0);
    let out = deliver(&mut n, "d", proposal_msg(&b3, "d"));
    assert!(is_vote(&out));
    assert_eq!(n.store.last_voted_view(), 3);
}

#[test]
fn fresh_qc_with_a_view_gap_keeps_future_proposal_until_pacemaker_reaches_it() {
    let mut n = node("a", &NAMES);
    let parent = child(&Block::genesis(), 1, vec![], QuorumCert::genesis());
    n.store.put_block(parent.clone());
    let future = child(&parent, 3, vec![], qc(&parent, &NAMES[1..]));
    let out = deliver(&mut n, "d", proposal_msg(&future, "d"));
    assert!(!is_vote(&out));
    assert_eq!(n.current_view, 2, "only the QC advances the view");
    assert_eq!(n.future_proposals.len(), 1);
    assert!(!n.store.contains_block(&future.hash()));
    let out = n.step(Event::ViewTimeout);
    assert!(is_vote(&out));
    assert_eq!(n.store.last_voted_view(), 3);
    assert!(n.store.contains_block(&future.hash()));
}
