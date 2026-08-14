//! Unit tests for `Domain::release_front`: FIFO release of committed history —
//! exact-prefix frees, survivor stability, no-op edges, floor gates, and the
//! standard/oversize chunk split. Arena shape asserts use the `testing`
//! feature's `Domain::shape()` = `(base, live chunks, free chunks)`.

use crate::common::{filled, CS};
use mesocarp::transient::{Domain, Handle};

// Frees the exact prefix and recycles standard chunks.
#[test]
fn test_releaseFront_UpdatesBaseAndFreeList() {
    let (mut d, hs) = filled(24); // chunks 0, 1, 2
    assert_eq!(d.cursor().chunk, 2);
    assert_eq!((hs[0].chunk(), hs[8].chunk(), hs[16].chunk()), (0, 1, 2));

    unsafe { d.release_front(1) };
    assert_eq!(d.shape(), (1, 2, 1));

    unsafe { d.release_front(2) };
    assert_eq!(d.shape(), (2, 1, 2));

    // idempotent at the same floor
    unsafe { d.release_front(2) };
    assert_eq!(d.shape(), (2, 1, 2));
}

// Survivors stay intact through chop and recycled-chunk reuse.
#[test]
fn test_releaseFront_PreservesSurvivorValues() {
    let (mut d, hs) = filled(24);
    unsafe { d.release_front(2) };
    for (i, h) in hs.iter().enumerate().skip(16) {
        assert_eq!(unsafe { *h.get() }, i as u64);
    }

    // Refill: pops both recycled chunks back off the free list.
    let new: Vec<Handle<u64>> = (100..116u64).map(|i| d.alloc(i).unwrap()).collect();
    assert_eq!(d.shape().2, 0);
    // Ids keep counting past the chopped prefix.
    assert_eq!((new[0].chunk(), new[8].chunk()), (3, 4));

    for (i, h) in hs.iter().enumerate().skip(16) {
        assert_eq!(unsafe { *h.get() }, i as u64);
    }
    for (i, h) in new.iter().enumerate() {
        assert_eq!(unsafe { *h.get() }, 100 + i as u64);
    }
}

// No-op edges, including calls on an empty Domain.
#[test]
fn test_releaseFront_NoopAtBaseAndEmptyEdges() {
    let mut d = Domain::new(CS).unwrap();
    unsafe { d.release_front(0) }; // empty, floor == base
    assert_eq!(d.shape(), (0, 0, 0));

    let (mut d, _hs) = filled(24);
    unsafe { d.release_front(0) }; // floor == base: no-op
    assert_eq!(d.shape(), (0, 3, 0));

    let open = d.cursor().chunk;
    unsafe { d.release_front(open) }; // frees all but the open chunk
    assert_eq!(d.shape(), (2, 1, 2));
}

// A mis-derived floor fails loudly in debug builds.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "chop floor")]
fn test_releaseFront_PanicsWhenFloorAboveOpenChunk() {
    let mut d = Domain::new(CS).unwrap();
    d.alloc(1u64).unwrap();
    unsafe { d.release_front(5) };
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "chop floor")]
fn test_releaseFront_PanicsWhenFloorBelowBase() {
    let (mut d, _hs) = filled(24);
    unsafe { d.release_front(2) };
    unsafe { d.release_front(1) }; // derived from a fossil record
}

// A mixed standard/oversize run frees the right kinds.
#[test]
fn test_releaseFront_DeallocsOversizeRecyclesStd() {
    let mut d = Domain::new(CS).unwrap();
    let a = d.alloc(1u64).unwrap(); // chunk 0
    let big = d.alloc([7u8; 100]).unwrap(); // oversize: own chunk 1, reopens chunk 2
    let b = d.alloc(2u64).unwrap(); // chunk 2
    assert_eq!((a.chunk(), big.chunk(), b.chunk()), (0, 1, 2));

    unsafe { d.release_front(2) };
    // Chunk 0 recycled; exact-fit chunk 1 deallocated, not pooled.
    assert_eq!(d.shape(), (2, 1, 1));
    assert_eq!(unsafe { *b.get() }, 2);
}
