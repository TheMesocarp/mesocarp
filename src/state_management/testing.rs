//! Structural invariant walkers, compiled only under `cfg(test)` or the
//! `testing` feature — never in production builds (the tokio `test-util`
//! pattern: the feature exists for this repo's own test suites, enabled via
//! the self dev-dependency; don't enable it downstream). Each walker panics on
//! the first violation, citing the invariant id from
//! `docs/transient/invariants.md`, so any test tier (unit, integration, future
//! state machine) can call it after a mutation and turn a silent structural
//! corruption into a loud failure.

use super::{CopyTimeline, Domain};

impl Domain {
    /// Walk the arena's structural invariants (INV-ARENA-3, -4, -8 and bump
    /// position sanity). Panics on the first violation.
    pub fn check_invariants(&self) {
        if let Some(back) = self.chunks.back() {
            assert!(back.std, "INV-ARENA-3: back chunk must be standard-size");
            assert!(
                self.cursor <= back.layout.size(),
                "bump position must lie inside the back chunk"
            );
        } else {
            assert_eq!(
                self.cursor, 0,
                "an empty domain must have a zeroed bump position"
            );
        }
        for c in &self.free {
            assert!(c.std, "INV-ARENA-4: free list holds only standard chunks");
            assert_eq!(
                c.layout.size(),
                self.chunk_size,
                "INV-ARENA-4: free chunks must be interchangeable"
            );
        }
        assert!(
            self.base as u64 + self.chunks.len() as u64 <= u32::MAX as u64 + 1,
            "INV-ARENA-8: chunk ids must fit u32"
        );
    }

    /// Gross arena shape for unit assertions: `(base, live chunks, free chunks)`.
    pub fn shape(&self) -> (u32, usize, usize) {
        (self.base, self.chunks.len(), self.free.len())
    }
}

impl<V> CopyTimeline<V> {
    /// Walk the index's structural invariants (INV-INDEX-1, -2, -3 and seal
    /// coherence). Panics on the first violation.
    pub fn check_invariants(&self) {
        let mut prev = None;
        for c in &self.chunks {
            assert!(c.len >= 1, "INV-INDEX-1: no empty chunk in the deque");
            assert!(c.len <= c.slots.len(), "chunk len must fit its capacity");
            if c.full() {
                assert!(c.seal, "a full chunk must be sealed");
            }
            let records = c.records();
            assert_eq!(
                c.lo, records[0].stamp,
                "INV-INDEX-2: lo mirrors the first record"
            );
            for r in records {
                if let Some(p) = prev {
                    assert!(p < r.stamp, "INV-INDEX-2: stamps strictly increase");
                }
                prev = Some(r.stamp);
            }
        }
        assert_eq!(
            self.latest, prev,
            "INV-INDEX-3: latest mirrors the newest record"
        );
        for c in &self.free {
            assert_eq!(c.len, 0, "recycled index chunks must be reset");
        }
    }

    /// Walk INV-PROTO-4 lockstep against `d`: every record's home chunk is
    /// still live in the domain and its value pointer lies inside that chunk,
    /// correctly aligned. Panics on the first violation.
    pub fn check_lockstep(&self, d: &Domain) {
        assert_eq!(self.domain_id, d.id, "INV-INDEX-6: foreign domain");
        for c in &self.chunks {
            for r in c.records() {
                let chunk = d
                    .fetch_chunk(r.value.chunk())
                    .expect("INV-PROTO-4: home chunk released under a live index entry");
                let lo = chunk.ptr.as_ptr() as usize;
                let p = r.value.ptr.as_ptr() as usize;
                assert!(
                    p >= lo && p + size_of::<V>() <= lo + chunk.layout.size(),
                    "INV-PROTO-4: value outside its home chunk"
                );
                assert!(
                    p.is_multiple_of(align_of::<V>()),
                    "value pointer misaligned"
                );
            }
        }
    }
}
