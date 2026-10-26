//! Deterministic schedules that reproduce slow-CPU message and timeout races.
use mistlib_consensus_core::hotstuff::{
    Block, BlockStore, Event, HotStuffMessage, HotStuffState, MemoryBlockStore, NoopCrypto, Output,
    PacemakerConfig, PartialSig, QuorumCert,
};
use mistlib_consensus_core::NodeId;

fn id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn node(name: &str, names: &[&str]) -> HotStuffState<MemoryBlockStore, NoopCrypto> {
    let mut n = HotStuffState::new(
        id(name),
        names.iter().map(|s| id(s)).collect(),
        MemoryBlockStore::new(),
        NoopCrypto,
        PacemakerConfig::default(),
    );
    n.bootstrap();
    n
}

fn qc(block: &Block, signers: &[&str]) -> QuorumCert {
    QuorumCert {
        view: block.view,
        block_hash: block.hash(),
        signatures: signers
            .iter()
            .map(|s| PartialSig {
                signer: id(s),
                signature: vec![],
            })
            .collect(),
    }
}

fn child(parent: &Block, view: u64, signers: &[&str]) -> Block {
    Block {
        parent: parent.hash(),
        view,
        height: parent.height + 1,
        commands: vec![vec![view as u8]],
        justify: if parent.view == 0 {
            QuorumCert::genesis()
        } else {
            qc(parent, signers)
        },
    }
}

fn proposal(n: &mut HotStuffState<MemoryBlockStore, NoopCrypto>, block: &Block) -> Vec<Output> {
    let leader = n.leader_of(block.view).unwrap();
    n.step(Event::Message {
        from: leader.clone(),
        msg: HotStuffMessage::Proposal {
            block: block.clone(),
            sig: PartialSig {
                signer: leader,
                signature: vec![],
            },
        },
    })
}

fn vote(
    n: &mut HotStuffState<MemoryBlockStore, NoopCrypto>,
    block: &Block,
    signer: &str,
) -> Vec<Output> {
    n.step(Event::Message {
        from: id(signer),
        msg: HotStuffMessage::Vote {
            view: block.view,
            block_hash: block.hash(),
            sig: PartialSig {
                signer: id(signer),
                signature: vec![],
            },
        },
    })
}

fn broadcast(out: &[Output]) -> Option<&Block> {
    out.iter().find_map(|o| match o {
        Output::Broadcast {
            msg: HotStuffMessage::Proposal { block, .. },
        } => Some(block),
        _ => None,
    })
}

fn assert_first_committed(out: &[Output]) {
    assert!(out
        .iter()
        .any(|o| matches!(o, Output::ApplyCommitted { commands }
        if commands.len() == 1 && commands[0].command == vec![1] && commands[0].sequence == 1)));
}

#[test]
fn timed_out_proposal_is_retained_without_a_late_vote() {
    let names = ["a", "b", "c", "d", "e"];
    let mut a = node("a", &names);
    let b1 = child(&Block::genesis(), 1, &names[1..]);
    a.step(Event::ViewTimeout);
    let out = proposal(&mut a, &b1);
    assert!(!out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: HotStuffMessage::Vote { .. },
            ..
        }
    )));
    let b2 = child(&b1, 2, &names[1..]);
    let b3 = child(&b2, 3, &names[1..]);
    let b4 = child(&b3, 4, &names[1..]);
    proposal(&mut a, &b2);
    proposal(&mut a, &b3);
    assert_first_committed(&proposal(&mut a, &b4));
}

#[test]
fn proposals_from_successive_leaders_can_arrive_in_reverse_order() {
    let names = ["a", "b", "c", "d", "e"];
    let mut a = node("a", &names);
    let b1 = child(&Block::genesis(), 1, &names[1..]);
    let b2 = child(&b1, 2, &names[1..]);
    let b3 = child(&b2, 3, &names[1..]);
    let b4 = child(&b3, 4, &names[1..]);
    for block in [&b4, &b3, &b2] {
        assert!(broadcast(&proposal(&mut a, block)).is_none());
    }
    assert_first_committed(&proposal(&mut a, &b1));
    assert_eq!(a.current_view, 5);
    assert_eq!(a.high_qc().view, 3);
}

#[test]
fn certified_future_proposal_catches_up_a_lagging_view() {
    let names = ["a", "b", "c", "d", "e"];
    let b1 = child(&Block::genesis(), 1, &names[1..]);
    let b2 = child(&b1, 2, &names[1..]);
    let b3 = child(&b2, 3, &names[1..]);
    // The blocks are known, but this replica has not learned their QCs yet.
    let mut store = MemoryBlockStore::new();
    store.put_block(b1);
    store.put_block(b2);
    let mut lagging = HotStuffState::new(
        id("a"),
        names.iter().map(|s| id(s)).collect(),
        store,
        NoopCrypto,
        PacemakerConfig::default(),
    );
    lagging.bootstrap();
    let out = proposal(&mut lagging, &b3);
    assert_eq!(lagging.current_view, 4);
    assert!(out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: HotStuffMessage::Vote { view: 3, .. },
            ..
        }
    )));
}

#[test]
fn future_leader_without_a_new_qc_cannot_skip_the_pacemaker() {
    let names = ["a", "b", "c", "d", "e"];
    let mut lagging = node("d", &names);
    let b4 = child(&Block::genesis(), 4, &names[1..]);
    assert!(proposal(&mut lagging, &b4).is_empty());
    assert_eq!(lagging.current_view, 1);
    lagging.step(Event::ViewTimeout);
    lagging.step(Event::ViewTimeout);
    let out = lagging.step(Event::ViewTimeout);
    assert_eq!(lagging.current_view, 5);
    assert!(out.iter().any(|o| matches!(
        o,
        Output::SendMessage {
            msg: HotStuffMessage::Vote { view: 4, .. },
            ..
        }
    )));
}

#[test]
fn qc_received_before_its_block_is_retried_when_the_block_arrives() {
    let names = ["a", "b", "c", "d"];
    let mut c = node("c", &names);
    let b1 = child(&Block::genesis(), 1, &names[..3]);
    for signer in ["a", "b", "d"] {
        assert!(broadcast(&vote(&mut c, &b1, signer)).is_none());
    }
    assert_eq!(c.high_qc().view, 1);
    let out = proposal(&mut c, &b1);
    let b2 = broadcast(&out).expect("certified parent is now available to extend");
    assert_eq!(b2.view, 2);
    assert_eq!(b2.parent, b1.hash());
}

#[test]
fn client_request_waits_for_the_previous_views_qc() {
    let names = ["a", "b", "c", "d"];
    let mut c = node("c", &names);
    let b1 = child(&Block::genesis(), 1, &names[..3]);
    proposal(&mut c, &b1);
    assert_eq!(c.current_view, 2);
    let out = c.step(Event::ClientPropose {
        command: b"queued".to_vec(),
    });
    assert!(
        broadcast(&out).is_none(),
        "leadership alone cannot authorize a proposal"
    );
    assert!(out.contains(&Output::Proposed { view: 2 }));
    vote(&mut c, &b1, "a");
    let out = vote(&mut c, &b1, "d");
    let b2 = broadcast(&out).expect("quorum authorizes the proposal");
    assert_eq!(b2.parent, b1.hash());
    assert_eq!(b2.commands, vec![b"queued".to_vec()]);
}

#[test]
fn backoff_survives_partial_progress_until_a_three_chain_commits() {
    let names = ["a", "b", "c", "d", "e", "f"];
    let mut a = node("a", &names);
    a.step(Event::ViewTimeout);
    let mut parent = Block::genesis();
    for view in 2..=5 {
        let block = child(&parent, view, &names[1..]);
        let out = proposal(&mut a, &block);
        let expected_ms = if view < 5 { 800 } else { 400 };
        assert!(
            out.contains(&Output::ResetViewTimer {
                duration_ms: expected_ms
            }),
            "view {view} must retain backoff until the first commit: {out:?}"
        );
        parent = block;
    }
    assert_eq!(a.committed_height(), 1);
}

#[test]
fn deferred_proposal_still_requires_valid_height_and_parent_view() {
    let names = ["a", "b", "c", "d", "e"];
    let mut a = node("a", &names);
    let b1 = child(&Block::genesis(), 1, &names[1..]);
    let mut invalid = child(&b1, 2, &names[1..]);
    invalid.height = 99;
    proposal(&mut a, &invalid);
    let mut wrong_view = child(&b1, 2, &names[1..]);
    wrong_view.justify.view = 0;
    proposal(&mut a, &wrong_view);
    proposal(&mut a, &b1);
    assert_eq!(a.current_view, 2);
    assert_eq!(a.high_qc().view, 0);
    // A valid copy can still be processed after the invalid child is rejected.
    proposal(&mut a, &child(&b1, 2, &names[1..]));
    assert_eq!(a.current_view, 3);
}
