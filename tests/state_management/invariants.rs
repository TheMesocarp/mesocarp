//! Invariant registry tests — the `invariants.t.sol` analog for this crate.
//! One `mod` per category from `docs/transient/invariants.md`, tests named
//! `test_inv_<category>_<n>_<claim>` so the doc's `Tests:` lines stay greppable.
//! Sibling constellation files (`timewarp.rs`, `bootstrap.rs`, future
//! unit-family files in the create.t.sol / swap.t.sol style) hold flows;
//! this file holds direct pins of documented invariants.

mod arena {
    use mesocarp::state_management::{CopyTimeline, Cursor, Domain};
    use mesocarp::MesoError;
    use std::mem::MaybeUninit;

    use crate::common::{stamp, CS};

    // ────────────────────────────────────────────────────────────────────
    // INV-ARENA-2 — LIFO ids are reused
    //
    // Proof call graph:
    //   cursor() → restore(to) [pops back, base UNCHANGED, std → free list]
    //   → regrowth: alloc → bump → open() → free.pop(), back_id = base+len−1
    //   ⇒ popped ids re-issued over the same bytes.
    // Net inputs: a deterministic u64 schedule over 64-byte chunks, cursors
    //   captured mid-chunk-1 / into-chunk-2 / at bootstrap, one oversize
    //   value for the LIFO-dealloc variant.
    // Net observables: restore() Result variants across the popped→regrown
    //   transition, re-issued Handle::chunk() ids, pointer identity of the
    //   recycled bytes, cursor() round-trips.
    //
    // Not covered here: u32 id-space exhaustion (INV-ARENA-8 documented
    // limit, not exercisable); free-list pop *order* with several chunks
    // pooled (implementation detail — only single-candidate pops are
    // asserted); cross-domain forged cursors (INV-PROTO-6's gap); the
    // timeline-mediated stale *event* cursor story (INV-PROTO-5's future
    // test).
    // ────────────────────────────────────────────────────────────────────

    // The core pin: a stale cursor is gated while its chunk is popped, then
    // silently revalidates once regrowth re-issues the id over the same
    // bytes. This is the hazard INV-PROTO-5 (cursor discard) exists for.
    #[test]
    fn test_inv_arena_2_stale_cursor_revalidates_after_id_reuse() {
        let mut d = Domain::new(CS).unwrap();
        for i in 0..16u64 {
            d.alloc(i).unwrap();
        }
        let c_mid = d.cursor(); // {1, 64}: chunk 1 exactly full
        let h_old = d.alloc(16u64).unwrap(); // opens chunk 2
        assert_eq!(h_old.chunk(), 2);
        let c_spec = d.cursor(); // {2, 8}: a "speculative era" cursor

        // Rewind below the speculation: chunk 2 pops to the free list.
        unsafe { d.restore(c_mid).unwrap() };
        d.check_invariants();

        // While popped, the stale cursor IS gated.
        assert_eq!(
            unsafe { d.restore(c_spec) }.unwrap_err(),
            MesoError::PastTheHorizon
        );

        // Regrow one record: open() pops the recycled chunk — id 2 again,
        // physically the same bytes.
        let h_new = d.alloc(777u64).unwrap();
        assert_eq!(h_new.chunk(), 2);
        assert!(std::ptr::eq(unsafe { h_old.get() }, unsafe { h_new.get() }));

        // The hazard: the same stale cursor now passes every numeric gate.
        unsafe { d.restore(c_spec).unwrap() };
        assert_eq!(d.cursor(), c_spec);
        d.check_invariants();
    }

    // The ARENA-1 / ARENA-2 asymmetry side by side: the same "rewind
    // everything" intent retires ids under reset but reuses them under
    // restore-to-bootstrap.
    #[test]
    fn test_inv_arena_2_restore_reuses_where_reset_retires() {
        // LIFO flavor: restore(c0) keeps base, so regrowth re-issues id 1.
        let mut d = Domain::new(CS).unwrap();
        let c0 = d.cursor();
        for i in 0..24u64 {
            d.alloc(i).unwrap(); // chunks 0, 1, 2
        }
        unsafe { d.restore(c0).unwrap() }; // pops 2 and 1, keeps chunk 0
        for i in 0..8u64 {
            assert_eq!(d.alloc(i).unwrap().chunk(), 0); // refills chunk 0
        }
        assert_eq!(d.alloc(9u64).unwrap().chunk(), 1); // id 1 REUSED
        d.check_invariants();

        // FIFO flavor: reset advances base, so ids retire forever.
        let mut d = Domain::new(CS).unwrap();
        for i in 0..24u64 {
            d.alloc(i).unwrap(); // chunks 0, 1, 2
        }
        unsafe { d.reset() };
        assert_eq!(
            d.cursor(),
            Cursor {
                chunk: 3,
                offset: 0
            }
        );
        assert_eq!(d.alloc(0u64).unwrap().chunk(), 3); // id 3: RETIRED past 0..=2
        assert_eq!(
            unsafe {
                d.restore(Cursor {
                    chunk: 0,
                    offset: 0,
                })
            }
            .unwrap_err(),
            MesoError::BelowChopLine
        );
        d.check_invariants();
    }

    // LIFO release forks by chunk kind: popped std chunks pool for reuse,
    // popped exact-fit chunks deallocate — yet the *id* is re-issued either
    // way. Also covers INV-ARENA-4's LIFO leg; miri closes the
    // no-leak/no-double-free side.
    #[test]
    fn test_inv_arena_2_oversize_lifo_dealloc_still_reissues_id() {
        let mut d = Domain::new(CS).unwrap();
        let a = d.alloc(1u64).unwrap(); // chunk 0
        let c_after_a = d.cursor(); // {0, 8}
        let big = d.alloc([7u8; 100]).unwrap(); // exact-fit chunk 1, reopens 2
        let b = d.alloc(2u64).unwrap();
        assert_eq!((a.chunk(), big.chunk(), b.chunk()), (0, 1, 2));

        // Pops chunk 2 (std → free list) and chunk 1 (exact-fit → dealloc).
        unsafe { d.restore(c_after_a).unwrap() };
        d.check_invariants();

        let big2 = d.alloc([9u8; 100]).unwrap(); // fresh exact-fit, id 1 again
        let c = d.alloc(3u64).unwrap(); // recycled std bytes, id 2 again
        assert_eq!((big2.chunk(), c.chunk()), (1, 2));
        assert_eq!(unsafe { *a.get() }, 1); // below the ceiling: untouched
        assert_eq!(unsafe { *big2.get() }, [9u8; 100]);
        d.check_invariants();
    }

    // restore() does not validate the offset within a live chunk — benign
    // by construction: the next bump re-checks capacity and rolls over.
    // Pins the documented gate surface (chunk id gated, offset not).
    #[test]
    fn test_inv_arena_2_restore_offset_unvalidated_but_benign() {
        let mut d = Domain::new(CS).unwrap();
        d.alloc(1u64).unwrap();
        d.alloc(2u64).unwrap(); // chunk 0, bump at 16
        unsafe {
            d.restore(Cursor {
                chunk: 0,
                offset: 60,
            })
            .unwrap()
        };
        assert_eq!(
            d.cursor(),
            Cursor {
                chunk: 0,
                offset: 60
            }
        );
        // 60 aligns to 64; 64 + 8 exceeds the chunk, so allocation rolls
        // into a fresh chunk instead of touching out-of-bounds bytes.
        assert_eq!(d.alloc(3u64).unwrap().chunk(), 1);
        d.check_invariants();
    }

    // ────────────────────────────────────────────────────────────────────
    // INV-ARENA-5 — Reclamation is wholesale-only; no value is ever dropped
    //
    // Proof call graph:
    //   gate leg:    alloc<T> │ record→alloc  — needs_drop::<T>() ⇒
    //                Err(NeedsDrop) before bump; nothing mutated.
    //   no-drop leg: release_front │ restore │ reset → release →
    //                (std ? free-list : dealloc); Drop for Domain →
    //                dealloc(chunks ∪ free). Bytes move, destructors never
    //                run — enforceable only because the gate leg keeps every
    //                drop-glue type out, so the runtime proxy is miri
    //                leak/UB-freedom across all four paths.
    // Net inputs: droppy types (String, Vec<u8>, Box<u64>), an oversize
    //   droppy type ([String; 4] at 96 B > 64 B chunks), deceptive
    //   non-droppy types (MaybeUninit<String>, a ZST), and a schedule
    //   exercising every release path before Drop.
    // Net observables: Err variants, cursor() equality across rejections,
    //   Handle::chunk() contiguity (no phantom chunks), survivor reads,
    //   walkers, miri cleanliness.
    //
    // Not covered here: entry-point *closure* — that Domain::alloc and
    // CopyTimeline::record are the only allocation sites is structural and
    // re-verified at review, not runtime; any future alloc entry point must
    // cite the gate. And "destructor never runs" for a type that HAS one is
    // unreachable by construction — that unreachability is the invariant.
    // ────────────────────────────────────────────────────────────────────

    // Gate leg at the direct entry: every droppy shape rejected, domain
    // untouched, no phantom chunk allocated even on the oversize path.
    // (The rejected values themselves drop as ordinary locals inside
    // alloc's error return — they never entered the arena, and miri
    // confirms the rejection path leaks nothing.)
    #[test]
    fn test_inv_arena_5_needs_drop_rejected_at_alloc() {
        let mut d = Domain::new(CS).unwrap();
        let c0 = d.cursor();

        assert_eq!(
            d.alloc(String::from("heap")).unwrap_err(),
            MesoError::NeedsDrop
        );
        assert_eq!(d.alloc(vec![1u8, 2, 3]).unwrap_err(), MesoError::NeedsDrop);
        assert_eq!(d.alloc(Box::new(7u64)).unwrap_err(), MesoError::NeedsDrop);

        // Oversize AND droppy: the gate fires before the exact-fit path
        // could allocate a chunk of its own.
        let oversize: [String; 4] = std::array::from_fn(|_| String::new());
        assert_eq!(d.alloc(oversize).unwrap_err(), MesoError::NeedsDrop);

        assert_eq!(d.cursor(), c0);
        // First real allocation lands in chunk 0: no phantom chunks exist.
        assert_eq!(d.alloc(0xAB_u64).unwrap().chunk(), 0);
        d.check_invariants();
    }

    // Gate leg at the indirect entry: record propagates NeedsDrop after the
    // id/stamp gates but before any index write. Pins that the gate is
    // per-allocation — CopyTimeline::<String>::new itself succeeds today.
    #[test]
    fn test_inv_arena_5_needs_drop_rejected_at_record() {
        let mut d = Domain::new(CS).unwrap();
        let c0 = d.cursor();
        let mut tl: CopyTimeline<String> = CopyTimeline::new(4, &d).unwrap();

        assert_eq!(
            tl.record(&mut d, String::from("oops"), stamp(0, 0))
                .unwrap_err(),
            MesoError::NeedsDrop
        );

        // Neither side moved: no value entered the arena, no record entered
        // the index, and the failed stamp was not consumed.
        assert_eq!(d.cursor(), c0);
        assert_eq!(unsafe { tl.latest(&d) }.unwrap(), None);
        assert_eq!(tl.partial_chop(0), None);
        tl.check_invariants();
        d.check_invariants();
    }

    // The gate is exactly `needs_drop`, not "owns heap when initialized":
    // MaybeUninit<String> and ZSTs pass, and a drop-free oversize value
    // takes the exact-fit path normally.
    #[test]
    fn test_inv_arena_5_gate_is_exactly_needs_drop() {
        let mut d = Domain::new(CS).unwrap();

        // Never initialized, never dropped; freed wholesale at teardown.
        let _uninit = d.alloc(MaybeUninit::<String>::uninit()).unwrap();

        let c_before = d.cursor();
        let z = d.alloc(()).unwrap(); // ZST: allowed, occupies nothing
        assert_eq!(d.cursor(), c_before);
        assert_eq!(z.chunk(), 0);

        let big = d.alloc([0u8; 100]).unwrap(); // oversize, drop-free: fine
        assert_eq!(big.chunk(), 1);
        d.check_invariants();
    }

    // No-drop leg: walk every release path in one flow — FIFO recycle,
    // LIFO pop (std → free list, exact-fit → dealloc), reset, then Drop
    // with a populated free list. Survivors read back untouched at each
    // step; miri closes the proof (bytes all freed, no destructor ran,
    // nothing leaked, nothing double-freed).
    #[test]
    fn test_inv_arena_5_release_paths_free_bytes_only() {
        let mut d = Domain::new(CS).unwrap();
        let hs: Vec<_> = (0..24u64).map(|i| d.alloc(i).unwrap()).collect();
        let keeper = hs[16]; // chunk 2, offset 0
        let c2 = d.cursor(); // {2, 64}
        let _big = d.alloc([0xCD_u8; 100]).unwrap(); // exact-fit 3, reopens 4
        let tail = d.alloc(99u64).unwrap(); // chunk 4

        // FIFO: chunks 0 and 1 recycle to the free list.
        unsafe { d.release_front(2) };
        d.check_invariants();
        assert_eq!(unsafe { *keeper.get() }, 16);
        assert_eq!(unsafe { *tail.get() }, 99);

        // LIFO: pops chunk 4 (std → free) and chunk 3 (exact-fit →
        // dealloc). `keeper` sits below the ceiling and survives; big and
        // tail die with their chunks and are never read again.
        unsafe { d.restore(c2).unwrap() };
        d.check_invariants();
        assert_eq!(unsafe { *keeper.get() }, 16);

        // Wholesale teardown: base advances past the survivor chunk.
        unsafe { d.reset() };
        d.check_invariants();
        assert_eq!(
            d.cursor(),
            Cursor {
                chunk: 3,
                offset: 0
            }
        );
        assert_eq!(unsafe { *d.alloc(7u64).unwrap().get() }, 7);

        // Drop runs here with live chunks AND a populated free list;
        // miri's leak check verifies Drop frees both, bytes only.
    }

    // ────────────────────────────────────────────────────────────────────
    // INV-ARENA-6 — Domain ids are process-unique and never reused
    //
    // Proof call graph:
    //   uniqueness: Domain::new → NEXT_DOMAIN.fetch_add(1, Relaxed)
    //               ⇒ ids strictly increase; Drop returns nothing.
    //   binding:    CopyTimeline::new captures d.id once; no API mutates it.
    //   gate:       record(d) / latest(d) → d.id ≠ domain_id ⇒
    //               Err(ForeignDomain), before any mutation or deref.
    //   temporal closure: a dropped Domain's id is unpresentable forever —
    //               uniqueness bars any new domain from matching, borrowck
    //               bars presenting the dead domain itself.
    // Net inputs: a construction/DROP schedule (drop points are the temporal
    //   leg's real input), timeline bindings at chosen points — two sharing
    //   a domain, one outliving its domain with a recorded value so its
    //   dangling pointers make the ordering obligation real — plus a stamp
    //   for the not-consumed probe and a droppy value / stale stamp for the
    //   precedence rungs.
    // Net observables: ForeignDomain vs sibling error variants, Ok on every
    //   self-pair, foreign cursor() unchanged on rejection, the failed stamp
    //   still spendable at home, and miri: the id gate must fire BEFORE any
    //   deref of dangling record pointers.
    //
    // Not covered here: NEXT_DOMAIN usize overflow (2^64 constructions —
    // ARENA-8-style documented limit); concurrent-construction uniqueness
    // (guaranteed by fetch_add atomicity but publicly unobservable — Domain
    // and CopyTimeline are !Send/!Sync via their NonNull fields, so no
    // cross-thread presentation can even be written; structural, verified
    // at review); the monotonicity ≻ needs_drop rung (a droppy timeline can
    // never acquire `latest`, so only the code order shows it); and
    // cross-domain Cursor/HighMark application — the id gate covers the
    // Timeline↔Domain entry points only, the raw-mark gap is INV-PROTO-6's.
    // ────────────────────────────────────────────────────────────────────

    // The pairwise gate: cross-presentations fail on both deref-ish entry
    // points, self-pairs pass (including two timelines sharing one domain),
    // and a rejection mutates neither side — the foreign bump is unmoved
    // and the failed stamp is still spendable at home.
    #[test]
    fn test_inv_arena_6_foreign_domain_gated_on_every_deref_entry() {
        let mut d_a = Domain::new(CS).unwrap();
        let mut d_b = Domain::new(CS).unwrap();
        let mut tl_a: CopyTimeline<u64> = CopyTimeline::new(4, &d_a).unwrap();
        let mut tl_a2: CopyTimeline<u64> = CopyTimeline::new(4, &d_a).unwrap();
        let mut tl_b: CopyTimeline<u64> = CopyTimeline::new(4, &d_b).unwrap();

        // Self-pairs pass: the gate is per-binding, not exclusive ownership.
        tl_a.record(&mut d_a, 1, stamp(0, 0)).unwrap();
        tl_a2.record(&mut d_a, 2, stamp(0, 0)).unwrap();
        tl_b.record(&mut d_b, 3, stamp(0, 0)).unwrap();

        // Cross-pairs fail before anything moves.
        let c_b = d_b.cursor();
        assert_eq!(
            tl_a.record(&mut d_b, 9, stamp(1, 0)).unwrap_err(),
            MesoError::ForeignDomain
        );
        assert_eq!(
            unsafe { tl_a.latest(&d_b) }.unwrap_err(),
            MesoError::ForeignDomain
        );
        assert_eq!(
            unsafe { tl_b.latest(&d_a) }.unwrap_err(),
            MesoError::ForeignDomain
        );

        // Nothing mutated: the foreign arena's bump is unmoved, and the
        // stamp the foreign call failed with succeeds at home.
        assert_eq!(d_b.cursor(), c_b);
        tl_a.record(&mut d_a, 9, stamp(1, 0)).unwrap();
        assert_eq!(unsafe { tl_a.latest(&d_a) }.unwrap(), Some(&9));
        tl_a.check_lockstep(&d_a);
        tl_a2.check_lockstep(&d_a);
        tl_b.check_lockstep(&d_b);
    }

    // The temporal closure: after its domain drops, a timeline's records
    // dangle — and stay unreachable forever, because no later domain can
    // ever match the captured id. The very next construction is the
    // likeliest collision under any hypothetical id-reuse scheme, so the
    // probe starts there. Under miri this doubles as the ordering proof:
    // the id gate must reject BEFORE latest can deref a dangling
    // record pointer.
    #[test]
    fn test_inv_arena_6_dropped_domain_id_never_returns() {
        let mut d_old = Domain::new(CS).unwrap();
        let mut tl: CopyTimeline<u64> = CopyTimeline::new(4, &d_old).unwrap();
        tl.record(&mut d_old, 42, stamp(0, 0)).unwrap();
        drop(d_old); // tl now holds dangling pointers it must never follow

        for i in 0..16u64 {
            let mut d_new = Domain::new(CS).unwrap();
            assert_eq!(
                unsafe { tl.latest(&d_new) }.unwrap_err(),
                MesoError::ForeignDomain
            );
            assert_eq!(
                tl.record(&mut d_new, i, stamp(1, 0)).unwrap_err(),
                MesoError::ForeignDomain
            );
            // Churn poisons nothing: fresh bindings to fresh domains work.
            let mut tl_new: CopyTimeline<u64> = CopyTimeline::new(4, &d_new).unwrap();
            tl_new.record(&mut d_new, i, stamp(0, 0)).unwrap();
            assert_eq!(unsafe { tl_new.latest(&d_new) }.unwrap(), Some(&i));
            tl_new.check_lockstep(&d_new);
        }
        // The stale index is still structurally sound — it just can't deref.
        tl.check_invariants();
    }

    // Gate precedence: the id check is outermost. A foreign call reports
    // ForeignDomain even when the stamp is stale (would be
    // TimestampMonotonicityFailure at home) or the value droppy (would be
    // NeedsDrop at home) — a cross-LP wiring bug is never masked as a
    // stamp or type problem.
    #[test]
    fn test_inv_arena_6_id_gate_precedes_stamp_and_drop_gates() {
        let mut d_a = Domain::new(CS).unwrap();
        let mut d_b = Domain::new(CS).unwrap();

        let mut tl_u: CopyTimeline<u64> = CopyTimeline::new(4, &d_a).unwrap();
        tl_u.record(&mut d_a, 1, stamp(5, 0)).unwrap();
        assert_eq!(
            tl_u.record(&mut d_b, 2, stamp(1, 0)).unwrap_err(),
            MesoError::ForeignDomain
        );
        assert_eq!(
            tl_u.record(&mut d_a, 2, stamp(1, 0)).unwrap_err(),
            MesoError::TimestampMonotonicityFailure
        );

        let mut tl_s: CopyTimeline<String> = CopyTimeline::new(4, &d_a).unwrap();
        assert_eq!(
            tl_s.record(&mut d_b, String::from("x"), stamp(0, 0))
                .unwrap_err(),
            MesoError::ForeignDomain
        );
        assert_eq!(
            tl_s.record(&mut d_a, String::from("x"), stamp(0, 0))
                .unwrap_err(),
            MesoError::NeedsDrop
        );
    }
}
