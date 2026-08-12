//! `Domain` unit tests: FIFO release (`release_front`), cursor capture, the
//! oversize/standard chunk split, and `Domain::new`'s gates.

use super::{filled, CS};
use crate::transient::*;

// T-1: chop frees the exact prefix and recycles standard chunks.
#[test]
fn chop_frees_prefix_and_recycles() {
    let (mut d, hs) = filled(24); // chunks 0, 1, 2
    assert_eq!(d.back_id(), 2);
    assert_eq!((hs[0].chunk(), hs[8].chunk(), hs[16].chunk()), (0, 1, 2));

    unsafe { d.release_front(1) };
    assert_eq!((d.base, d.chunks.len(), d.free.len()), (1, 2, 1));

    unsafe { d.release_front(2) };
    assert_eq!((d.base, d.chunks.len(), d.free.len()), (2, 1, 2));

    // idempotent at the same floor
    unsafe { d.release_front(2) };
    assert_eq!((d.base, d.chunks.len(), d.free.len()), (2, 1, 2));
}

// T-2: survivors stay intact through chop and recycled-chunk reuse.
#[test]
fn survivors_intact_after_chop_and_reuse() {
    let (mut d, hs) = filled(24);
    unsafe { d.release_front(2) };
    for (i, h) in hs.iter().enumerate().skip(16) {
        assert_eq!(unsafe { *h.get() }, i as u64);
    }

    // Refill: pops both recycled chunks back off the free list.
    let new: Vec<Handle<u64>> = (100..116u64).map(|i| d.alloc(i).unwrap()).collect();
    assert_eq!(d.free.len(), 0);
    // Ids keep counting past the chopped prefix.
    assert_eq!((new[0].chunk(), new[8].chunk()), (3, 4));

    for (i, h) in hs.iter().enumerate().skip(16) {
        assert_eq!(unsafe { *h.get() }, i as u64);
    }
    for (i, h) in new.iter().enumerate() {
        assert_eq!(unsafe { *h.get() }, 100 + i as u64);
    }
}

// T-3: no-op edges, including calls on an empty Domain.
#[test]
fn noop_edges() {
    let mut d = Domain::new(CS);
    unsafe { d.release_front(0) }; // empty, floor == base
    assert_eq!((d.base, d.chunks.len()), (0, 0));

    let (mut d, _hs) = filled(24);
    unsafe { d.release_front(0) }; // floor == base: no-op
    assert_eq!((d.base, d.chunks.len(), d.free.len()), (0, 3, 0));

    unsafe { d.release_front(d.back_id()) }; // frees all but the open chunk
    assert_eq!((d.base, d.chunks.len()), (2, 1));
}

// T-4: a mis-derived floor fails loudly in debug builds.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "chop floor")]
fn floor_above_open_chunk_asserts() {
    let mut d = Domain::new(CS);
    d.alloc(1u64).unwrap();
    unsafe { d.release_front(5) };
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "chop floor")]
fn floor_below_base_asserts() {
    let (mut d, _hs) = filled(24);
    unsafe { d.release_front(2) };
    unsafe { d.release_front(1) }; // derived from a fossil record
}

// T-5: cursor capture, including on a fresh Domain.
#[test]
fn cursor_capture() {
    let mut d = Domain::new(CS);
    assert_eq!(
        d.cursor(),
        Cursor {
            chunk: 0,
            offset: 0
        }
    );
    d.alloc(7u64).unwrap();
    assert_eq!(
        d.cursor(),
        Cursor {
            chunk: 0,
            offset: 8
        }
    );
    for i in 0..8u64 {
        d.alloc(i).unwrap();
    }
    assert_eq!(
        d.cursor(),
        Cursor {
            chunk: 1,
            offset: 8
        }
    );
}

// T-6: mixed standard/oversize run frees the right kinds.
#[test]
fn mixed_chop_dealloc_vs_recycle() {
    let mut d = Domain::new(CS);
    let a = d.alloc(1u64).unwrap(); // chunk 0
    let big = d.alloc([7u8; 100]).unwrap(); // oversize: own chunk 1, reopens chunk 2
    let b = d.alloc(2u64).unwrap(); // chunk 2
    assert_eq!((a.chunk(), big.chunk(), b.chunk()), (0, 1, 2));

    unsafe { d.release_front(2) };
    // Chunk 0 recycled; exact-fit chunk 1 deallocated, not pooled.
    assert_eq!((d.base, d.chunks.len(), d.free.len()), (2, 1, 1));
    assert_eq!(unsafe { *b.get() }, 2);
}

// C-4: offsets travel as u32, so oversized chunk_size is rejected up front.
#[test]
#[should_panic(expected = "fit u32")]
fn new_rejects_chunk_size_above_u32() {
    Domain::new(u32::MAX as usize + 1);
}
