//! End-to-end Time Warp round over the public API only: seed → speculate →
//! straggler rollback (max of marks, INV-PROTO-2) → re-execute → GVT chop
//! (min of floors, INV-PROTO-3). Two timelines of different value types share
//! one domain, so allocations interleave in the arena.
//!
//! This is the scaffolding smoke flow; the full e2e matrix from the coverage
//! report (mixed-`None` rollback, oversize values under the protocol,
//! steady-state no-alloc cycling) lands here as it gets written.

use crate::common::{fold_marks, min_floor, stamp};
use mesocarp::transient::{Domain, Timeline};

const CHUNK_BYTES: usize = 64;
const INDEX_SLOTS: usize = 4;

#[test]
fn test_e2e_timewarp_straggler_rollback_round() {
    let mut d = Domain::new(CHUNK_BYTES).unwrap();
    let mut counts: Timeline<u64> = Timeline::new(INDEX_SLOTS, &d).unwrap();
    let mut states: Timeline<[u64; 4]> = Timeline::new(INDEX_SLOTS, &d).unwrap();

    // INV-BOOT-1: seed both timelines at GVT₀ = 0 before any event runs.
    counts.record(&mut d, 0, stamp(0, 0)).unwrap();
    states.record(&mut d, [0; 4], stamp(0, 1)).unwrap();

    // Speculative events at t = 1..=6, interleaving arena allocation.
    for t in 1..=6u64 {
        counts.record(&mut d, t * 10, stamp(t, 0)).unwrap();
        states.record(&mut d, [t; 4], stamp(t, 1)).unwrap();
    }
    counts.check_invariants();
    states.check_invariants();
    counts.check_lockstep(&d);
    states.check_lockstep(&d);

    // Straggler at t = 4: sweep every timeline, then rewind the domain once,
    // to the maximum mark — a single timeline's mark may sit below a sibling's
    // surviving value.
    let marks = [
        counts.partial_rollback(4).unwrap(),
        states.partial_rollback(4).unwrap(),
    ];
    let mark = fold_marks(marks).expect("seeded timelines always keep a survivor");
    unsafe { d.rewind(mark).unwrap() };
    counts.check_lockstep(&d);
    states.check_lockstep(&d);
    assert_eq!(unsafe { counts.live_state(&d) }.unwrap(), Some(&30));
    assert_eq!(unsafe { states.live_state(&d) }.unwrap(), Some(&[3; 4]));

    // Re-execution writes different values for the undone times.
    for t in 4..=6u64 {
        counts.record(&mut d, t * 100, stamp(t, 0)).unwrap();
        states.record(&mut d, [t + 7; 4], stamp(t, 1)).unwrap();
    }
    counts.check_invariants();
    states.check_invariants();
    assert_eq!(unsafe { counts.live_state(&d) }.unwrap(), Some(&600));

    // GVT reaches 5: sweep chops, release committed history at the min floor.
    let floors = [counts.partial_chop(5), states.partial_chop(5)];
    if let Some(floor) = min_floor(floors) {
        unsafe { d.release_front(floor) };
    }
    counts.check_invariants();
    states.check_invariants();
    counts.check_lockstep(&d);
    states.check_lockstep(&d);

    // Survivors read back unchanged through the chop.
    assert_eq!(unsafe { counts.live_state(&d) }.unwrap(), Some(&600));
    assert_eq!(unsafe { states.live_state(&d) }.unwrap(), Some(&[13; 4]));

    // INV-HORIZON-2: committed history is now un-rollbackable.
    assert!(counts.partial_rollback(5).is_err());
    assert!(states.partial_rollback(4).is_err());
    // Strictly above the horizon stays legal.
    assert!(counts.partial_rollback(6).is_ok());
}
