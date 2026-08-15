//! Bootstrap and teardown lifecycle flows (INV-BOOT-1, -2, -4): pre-seed
//! reads, seed protection under the commit horizon, rollback to
//! just-after-bootstrap, and domain teardown/reuse via `reset`.
//!
//! Known-missing variants: mid-run timeline creation and rollback below its
//! seed (INV-BOOT-3) — blocked on the object-creation-as-event story.

use crate::common::stamp;
use mesocarp::state_management::{CopyTimeline, Cursor, Domain};

#[test]
fn test_e2e_bootstrap_seed_protection() {
    let mut d = Domain::new(64).unwrap();
    let mut tl: CopyTimeline<u64> = CopyTimeline::new(4, &d).unwrap();

    // Pre-seed: no state, no floor — and rollback-to-zero is already illegal,
    // because the horizon starts at GVT₀ = 0.
    assert_eq!(unsafe { tl.latest(&d) }.unwrap(), None);
    assert_eq!(tl.partial_chop(0), None);
    assert!(tl.partial_rollback(&d, 0).is_err());

    // Seed at GVT₀, then run events.
    tl.record(&mut d, 7, stamp(1, 0)).unwrap();
    tl.record(&mut d, 8, stamp(2, 0)).unwrap();
    tl.record(&mut d, 9, stamp(3, 0)).unwrap();
    tl.check_invariants();

    // INV-BOOT-1: the baseline is un-rollbackable...
    assert!(tl.partial_rollback(&d, 0).is_err());

    // ...while rolling back to just-after-bootstrap keeps exactly the seed.
    let mark = tl.partial_rollback(&d, 2).unwrap().expect("seed survives");
    unsafe { d.restore(mark).unwrap() };
    tl.check_invariants();
    tl.check_lockstep(&d);
    assert_eq!(unsafe { tl.latest(&d) }.unwrap(), Some(&7));
}

#[test]
fn test_e2e_bootstrap_teardown_and_reuse() {
    let mut d = Domain::new(64).unwrap();
    let c0 = d.cursor();
    assert_eq!(
        c0,
        Cursor {
            chunk: 0,
            offset: 0,
            d_id: d.id()
        }
    );

    let mut tl: CopyTimeline<u64> = CopyTimeline::new(4, &d).unwrap();
    tl.record(&mut d, 1, stamp(0, 0)).unwrap();
    tl.record(&mut d, 2, stamp(1, 0)).unwrap();

    // INV-BOOT-4: teardown drops the index first; `reset` then retires every
    // chunk id, so nothing allocated before it can ever validate again.
    drop(tl);
    unsafe { d.reset() };
    d.check_invariants();

    // The domain is reusable in place...
    let mut tl2: CopyTimeline<u64> = CopyTimeline::new(4, &d).unwrap();
    tl2.record(&mut d, 3, stamp(0, 0)).unwrap();
    tl2.check_lockstep(&d);
    assert_eq!(unsafe { tl2.latest(&d) }.unwrap(), Some(&3));

    // ...and pre-teardown cursors are dead, gated by BelowChopLine.
    assert!(unsafe { d.restore(c0) }.is_err());
}
