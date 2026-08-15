# State Management Core Invariants

Properties `Domain` and `CopyTimeline<V>` (`src/state_management`) must uphold regardless of which
`Transient` object ties them together. The orchestration layer that will pair many
timelines with one arena is not built yet; properties only that layer can uphold are
stated here as protocol entries and marked **upheld by the caller** — they are the
safety contracts of the module's `unsafe` entry points. Invariant ids are the reference
points for contract review.

All `state_management` suites live in `tests/state_management/` (unit, invariant, and e2e files per
`skills/TEST.md`) and are meant to also run under miri:
`cargo +nightly miri test --test transient`.
"Tests: none yet" marks a gap, not a non-testable property.

---

## Arena (`Domain`)

### INV-ARENA-1 — Chunk ids are global, monotone, and FIFO-retired

A chunk's id is `base + index` into the deque. `base` only advances (`release_front`,
`reset`), so an id freed off the front can never validate again: any stale `Cursor` or
`HighMark` naming it fails the `id >= base` gate with `BelowChopLine`. Surviving ids
never move or get rewritten.

Tests: `tests/state_management/release_front.rs::test_releaseFront_UpdatesBaseAndFreeList`,
`…test_releaseFront_PreservesSurvivorValues` (fresh ids 3–4 after recycling),
`…test_releaseFront_NoopAtBaseAndEmptyEdges`,
`…test_releaseFront_PanicsWhenFloorAboveOpenChunk` / `…WhenFloorBelowBase` (debug
range asserts).

### INV-ARENA-2 — LIFO ids are reused

`restore` pops chunks off the back **without** advancing `base`; a later `open()`
re-issues the same ids. So unlike the FIFO edge, numeric gates cannot detect a stale
cursor whose chunk was popped and re-opened — a cursor captured for a rolled-back event
can become numerically valid again while pointing at semantically different memory.
This is why INV-PROTO-5 (cursor discard) exists.

Tests: `tests/state_management/invariants.rs::arena::test_inv_arena_2_stale_cursor_revalidates_after_id_reuse`
(the popped→regrown gate transition and physical byte reuse),
`…test_inv_arena_2_restore_reuses_where_reset_retires` (the ARENA-1 asymmetry),
`…test_inv_arena_2_oversize_lifo_dealloc_still_reissues_id`,
`…test_inv_arena_2_restore_offset_unvalidated_but_benign`. Not covered: u32 id-space
wrap (ARENA-8 limit), multi-chunk free-list pop order (implementation detail),
stale *event* cursors through the protocol (INV-PROTO-5's future test).

### INV-ARENA-3 — The back chunk is always standard-size

The deque is either empty or its back chunk is a standard (recyclable, `std`) chunk
that the bump pointer works into. `bump`'s oversize path immediately reopens a std
chunk behind the exact-fit allocation; `restore` reopens when a rewind lands on an
exact-fit chunk. Consequence: `Domain::cursor()` never names an oversize chunk, so
every captured `Cursor` is restorable.

Tests: `tests/state_management/release_front.rs::test_releaseFront_DeallocsOversizeRecyclesStd`
(bump-side reopen only — the value after the oversize alloc lands in the reopened
chunk). Restore-side reopen: none yet.

### INV-ARENA-4 — The free list holds only standard chunks

Every release path (`release_front`, `restore`, `reset`) recycles std chunks into
`free` and deallocates exact-fit chunks outright, so the free list stays uniform — no
size classes, no fit logic on reuse.

Tests: `tests/state_management/release_front.rs::test_releaseFront_DeallocsOversizeRecyclesStd`
(FIFO path);
`tests/state_management/invariants.rs::arena::test_inv_arena_2_oversize_lifo_dealloc_still_reissues_id`
(LIFO path); reset path walked by `check_invariants` in
`…test_inv_arena_5_release_paths_free_bytes_only`.

### INV-ARENA-5 — Reclamation is wholesale-only; no value is ever dropped

`alloc` rejects `needs_drop` types (`MesoError::NeedsDrop`), and no release path runs
value destructors — `release_front` / `restore` / `reset` / `Drop for Domain` free raw
bytes only. Values must not own heap resources.

Tests: `tests/state_management/invariants.rs::arena::test_inv_arena_5_needs_drop_rejected_at_alloc`
(direct gate, incl. the oversize-droppy ordering), `…rejected_at_record` (indirect
gate; also pins that `Timeline::<Droppy>::new` succeeds — the gate is per-alloc),
`…test_inv_arena_5_gate_is_exactly_needs_drop` (`MaybeUninit<String>`, ZST),
`…test_inv_arena_5_release_paths_free_bytes_only` (all four release paths under miri's
leak check). Not covered: entry-point closure (alloc/record being the only
allocation sites is structural, verified at review).

### INV-ARENA-6 — Domain ids are process-unique and never reused

Ids come from a global atomic counter, so a dropped `Domain`'s id can never be
presented again. This makes the `ForeignDomain` gate temporally sound: every deref-ish
entry point (`Timeline::record`, `Timeline::live_state`) requires presenting a live
`&Domain` whose id matches the one captured at `Timeline::new`.

Tests: `tests/state_management/invariants.rs::arena::test_inv_arena_6_foreign_domain_gated_on_every_deref_entry`
(pairwise gate, self-pairs pass, rejection mutates nothing),
`…test_inv_arena_6_dropped_domain_id_never_returns` (temporal closure; under miri also
proves the id check fires before any dangling deref),
`…test_inv_arena_6_id_gate_precedes_stamp_and_drop_gates` — note: `record`'s gate
order is now id → `TimeTravel` → stamp → drop (INV-HORIZON-4); the test predates
`TimeTravel` and pins three of the four — extension pending. Not covered: counter
overflow (2⁶⁴ constructions), concurrent-construction uniqueness (atomic by
construction; publicly unobservable — `Domain`/`Timeline` are `!Send`/`!Sync` via
their `NonNull` fields), and raw `Cursor`/`HighMark` cross-domain application
(INV-PROTO-6's gap, not closed by this gate).

### INV-ARENA-7 — Value addresses are stable

Chunk bytes never move once allocated: the deque stores chunk headers, not the bytes,
so deque growth, chop, rollback of *other* chunks, and free-list recycling never
relocate a live value. A `Handle`'s pointer is valid exactly as long as its home chunk
is unreleased.

Tests: `tests/state_management/release_front.rs::test_releaseFront_PreservesSurvivorValues`
(survivors re-read after chop and after recycled chunks are rewritten).

### INV-ARENA-8 — Offsets and ids fit `u32`

`chunk_size` is rounded up to `CHUNK_ALIGN` (16) and must land in `[16, u32::MAX]` —
`Domain::new` rejects zero (`InitializedWithNoSlots`) and aligned sizes past
`u32::MAX` (`ChunkSizeTooLarge`) — because offsets travel as `u32` in `bump` and
`Cursor`.
Documented limit: a single `Domain` can issue at most ~2³² chunk ids over its lifetime;
debug builds panic on overflow, release builds would wrap and break INV-ARENA-1.

Tests: `tests/state_management/init.rs::test_domainNew_RevertsWhenChunkSizeTooLarge` (upper
bound), `…test_domainNew_RevertsWhenInitializedWithNoSlots` (zero).

---

## Index (`Timeline<V>`)

Index chunks live on the regular heap (`Box<[MaybeUninit<Record<V>>]>`), not in the
arena — the timeline stores `(Stamp, Handle<V>)` records; only the values live in the
`Domain`. `std_chunk_size` counts record **slots**; `Domain::new`'s `chunk_size`
counts **bytes**.

### INV-INDEX-1 — Every index chunk holds at least one record

`record` only pushes chunks it immediately writes into; `partial_rollback` pops
wholly-dead chunks before truncating (its survivor `expect` relies on this), and
`partial_chop`'s compaction keeps ≥ 1 record (its `records()[0]` relies on this). No
code path leaves an empty chunk in the deque.

Tests: none yet.

### INV-INDEX-2 — Records are strictly stamp-ordered

`record` rejects `stamp <= latest` (`TimestampMonotonicityFailure`), so records are
strictly increasing within and across chunks, the deque is ordered by each chunk's
`lo` (first record's stamp; empty chunks reset `lo` to the max sentinel so they sort
last), and every `partition_point` / `lo`-skip loop in the partials is sound.
Granularity note: ordering is `(time, seq)`, but `partial_rollback` and `partial_chop`
cut on `time` only — `seq` breaks append ties, never reclamation boundaries.

Tests: none yet.

### INV-INDEX-3 — `latest` mirrors the newest record

`latest` equals the stamp of the newest record and is `None` iff the timeline is
empty. `record` sets it, `partial_rollback` rewinds it to the survivor (or clears it).

Tests: none yet.

### INV-INDEX-4 — Seal means closed-to-appends, and the partials may unseal

A chunk seals when it fills or when it is superseded as the back chunk
(`fetch_fresh_chunk`). Both partials recompute `seal = full()` on the boundary chunk,
deliberately re-opening a truncated back chunk (rollback) or a compacted single-chunk
front (chop) so subsequent `record` calls refill it instead of fetching a fresh chunk.
This refill path is what lets the allocator amortize across rollback cycles.

Tests: none yet.

### INV-INDEX-5 — Value-before-index write order

`record` arena-allocates the value before touching the index. An error between the
two (currently unreachable — the index path is infallible once `Timeline::new` has
rejected zero slots) would leave a ghost value owned by no record, reclaimed only
wholesale by a later rollback or chop.

Tests: N/A — documentation entry.

### INV-INDEX-6 — A timeline is bound to exactly one domain

`Timeline::new` captures the domain id; `record` and `live_state` gate on it
(`ForeignDomain`). Combined with INV-ARENA-6, a timeline can never deref through the
wrong (or a dead) arena via the safe-facing API.

Tests: none yet.

---

## Commit horizon & rollback legality

### INV-HORIZON-1 — The horizon is `None` until armed, then monotone

`commit_horizon` starts as `None` — nothing committed. GVT₀ is not encoded in the
initializer; it arrives like every later GVT, via the arming chop (INV-BOOT-1).
`partial_chop(until)` sets `Some(max(h, until))` where `h` is the prior value or
`until` itself — the `None → Some` transition happens at most once, `Some` values
only rise, and out-of-order chop calls cannot lower it. The update is hoisted above
every other statement and runs unconditionally: every timeline in a sweep learns the
GVT, empty or not. Sharp edge, by design: an unseeded timeline swept at GVT is
thereafter locked out of seeding at-or-below that GVT (`TimeTravel`) — its state at
committed times was never captured and cannot be retroactively committed.

Tests: none yet.

### INV-HORIZON-2 — Rollback strictly above the horizon, once armed

Once the horizon is `Some(h)`, `partial_rollback(to)` errors when `to <= h`, before
any mutation — a rejected rollback leaves the timeline byte-for-byte unchanged, so a
sweep that fails on one timeline has not half-applied it. Under `None` the gate is
dormant: every rollback is legal, including `to = 0`, which empties the timeline —
the pre-arming teardown path (INV-BOOT-4).

Naming note: three variants now sit on this line — `PastTheHorizon` (rollback gate),
`TimeTravel` (write gate, INV-HORIZON-4), and `BelowChopLine` (arena id gate, whose
message "below the chop line; the current commit horizon fixed by the GVT" is the
semantic match for the rollback case). The vocabulary should be settled across the
trio at once. Variant choice pending.

Tests: flow coverage in `tests/state_management/timewarp.rs::test_e2e_timewarp_straggler_rollback_round`
(post-chop rollback gates) and `tests/state_management/bootstrap.rs::test_e2e_bootstrap_seed_protection`
(the gate flips at the arming chop — legal rollback-to-0 before, rejected after);
direct pin: none yet.

### INV-HORIZON-3 — Chop keeps the at-or-before survivor

After `partial_chop(until)`, the oldest retained record is the *newest* record with
`time <= until` when one exists (the off-by-one that preserves restoration state), and
the returned floor is that record's home chunk. Together with INV-HORIZON-2, a legal
rollback (`to > h >= survivor.time`) can never empty a *committed* timeline — a
seeded-but-unarmed timeline legally empties under rollback-to-0. The survivor *is*
the committed seed: once any record sits at-or-below `Some(h)`, chop always preserves
the newest such record and no legal rollback can reach it, so seededness is
self-sustaining from first commitment — the explicit bootstrap seed is only the first
committed record, injected by fiat because no event produces it.

Tests: none yet.

### INV-HORIZON-4 — Records strictly above the horizon

Once the horizon is `Some(h)`, `record` rejects `stamp.time <= h`
(`MesoError::TimeTravel`), before allocation. The gate is dormant under `None`: a
bootstrap seed passes not because the timeline is empty but because nothing is yet
committed — a temporal precondition, not a structural exception; no emptiness
predicate exists anywhere in the gate. This closes the write-side gap: a timeline
emptied by legal rollback retains `Some(GVT)`, so committed history cannot be
rewritten through it even though `latest` is `None`. It also rejects writes into the
committed band between the survivor and the horizon (`latest < stamp.time <= h`),
which the `latest` gate alone permitted. Mirror of INV-HORIZON-2 on the write side.
Gate order in `record`: domain id → `TimeTravel` → stamp monotonicity → `NeedsDrop`.

Tests: none yet.

---

## Cross-object protocol (Timeline ⇄ Domain)

These are the safety contracts of the `unsafe` entry points, **upheld by the caller**
(the future orchestration layer), stated here so they have stable ids.

### INV-PROTO-1 — Single writer, stamp-ordered arena

One `Domain` per LP, single writer. Between rollbacks, arena allocation order across
*all* timelines sharing the domain is stamp-nondecreasing. This makes liveness a
prefix/suffix property of the chunk sequence — the premise of both wholesale
reclamation directions (`release_front` off the front, `restore` off the back).
Nothing in the module enforces cross-timeline ordering.

Tests: N/A — documentation entry (upheld by the caller).

### INV-PROTO-2 — Rollback sweep takes the max of marks

A rollback runs `partial_rollback` on **every** timeline sharing the domain, then
rewinds the domain once, to the *maximum* `HighMark`, ordered by chunk id then end
address — any single timeline's mark may sit below a sibling's surviving value, and
rewinding to it alone frees that survivor. `Ok(None)` from **all** timelines is the
only license for `Domain::reset`. The pre-arming teardown (INV-BOOT-4) is the one
legal flow that produces this license; once every timeline is armed and committed,
all-`None` is unreachable on compliant flows. `HighMark.end` is one-past-end of the
survivor's value, excluding alignment padding (sound: the next `bump` re-aligns).

Tests: flow coverage in `tests/state_management/timewarp.rs::test_e2e_timewarp_straggler_rollback_round`
(max-of-marks sweep via `common::fold_marks`); direct pin: none yet.

### INV-PROTO-3 — Chop sweep takes the min of floors

A chop runs `partial_chop` on every timeline and calls `release_front` at the
*minimum* returned floor; empty timelines (`None`) constrain nothing. `keep_from` must
be a liveness floor: no live `Handle` and no restorable `Cursor` may name a chunk
below it.

Tests: `release_front` mechanics under `tests/state_management/release_front.rs`; sweep
composition: flow coverage in
`tests/state_management/timewarp.rs::test_e2e_timewarp_straggler_rollback_round`
(min-of-floors via `common::min_floor`); direct pin: none yet.

### INV-PROTO-4 — Reads require lockstep

`live_state` is sound iff the domain has not been rewound below the timeline's newest
surviving record nor chopped above its floor — the id gate cannot see a protocol
violation that already happened. While the returned `&V` lives, the shared `&Domain`
borrow blocks every arena mutation (`alloc`, `restore`, `release_front`, `reset`);
timeline-side partials may still run but only edit the heap index, so the worst
in-borrow outcome is a stale-but-valid read, never a dangling one.

Tests: none yet (miri target).

### INV-PROTO-5 — Cursors of undone events are dead

After a rollback, every `Cursor` captured for a rolled-back event must be discarded
and re-captured on re-execution. Because of INV-ARENA-2, such a cursor can become
numerically valid again once re-execution regrows the arena — `restore`'s range gates
cannot detect it, and applying it silently corrupts the bump position.

Tests: none yet.

### INV-PROTO-6 — Cross-domain marks are only probabilistically gated (known gap)

`Cursor` carries no domain id: `restore` cannot distinguish a foreign domain's cursor
that happens to be numerically in range. `HighMark` fares better — `cursor_at`'s
containment check (`MarkOutsideHomeChunk`) almost certainly rejects a foreign pointer
— but neither is a soundness gate. Applying either across domains is a contract
violation. Candidate hardening: stamp the domain id into `Cursor` and gate like
`ForeignDomain`.

Tests: none yet.

---

## Bootstrap & lifecycle

### INV-BOOT-1 — Seed, then arm

Every timeline writes its baseline record at the initial GVT before any event
executes, and the orchestration then arms every timeline with a `partial_chop(GVT₀)`
sweep. Protection dates from the arming sweep, not from construction: post-arming,
under INV-HORIZON-2/3, the baseline — or a newer committed record — survives every
legal operation thereafter, `live_state` never reverts to `None`, and `reset` is
unreachable on the runtime path. Pre-arming, a rollback to 0 legally empties
everything (INV-BOOT-4's teardown window).

Seed-before-first-sweep is enforced, not conventional: a timeline that misses the
arming sweep can never seed at-or-below the swept GVT (`TimeTravel`, INV-HORIZON-1's
sharp edge). The protocol is uniform across start times — checkpoint restore at
GVT = g is the same seed-then-arm sequence at g; time 0 is not a special case.

Tests: flow coverage in `tests/state_management/timewarp.rs::test_e2e_timewarp_straggler_rollback_round`
(both timelines seeded at GVT₀) and `tests/state_management/bootstrap.rs::test_e2e_bootstrap_seed_protection`
(baseline protection from the arming sweep onward); direct pin: none yet.

### INV-BOOT-2 — GVT flows in via chop; rollback legality derives from it

The orchestration teaches each timeline the commit floor by calling
`partial_chop(gvt)` as GVT advances — including the *first* floor: GVT₀ enters via
the arming sweep, not the initializer, so all commitment flows through chop
uniformly. The orchestration only issues rollbacks strictly above GVT (the Time Warp
property: stragglers and anti-messages are above GVT by definition). INV-HORIZON-2 is
the local, per-timeline enforcement of this global invariant — a rollback at or below
GVT is a bug in the caller and fails loudly *once armed*; pre-arming, the timeline
knows no GVT and legality rests entirely on the caller, a window the mandatory arming
sweep closes.

Tests: flow coverage in `tests/state_management/timewarp.rs::test_e2e_timewarp_straggler_rollback_round`
(chop teaches the floor; rollback legality flips at GVT); direct pin: none yet.

### INV-BOOT-3 — Mid-run creation is rollbackable

A timeline created mid-run is seeded at its creation time `t`, not at GVT. A rollback
below `t` legitimately empties it (`Ok(None)` while siblings return marks — the mixed
case of INV-PROTO-2). This is only correct because object creation is itself an
event: undoing it must re-seed or drop the timeline. The mixed case is gate-backed:
the emptied timeline retains `Some(GVT)`, so committed history cannot be rewritten
through it (INV-HORIZON-4), and the legality of emptying is exactly that all its
records sat in its uncommitted region — its first commitment had not yet happened.

Tests: none yet.

### INV-BOOT-4 — The three full-reset paths are distinct

Restoring a cursor captured at bootstrap (`{base, 0}`) preserves `base`: earlier
cursors stay valid and the arena can be re-seeded in place. `Domain::reset` advances
`base` past every chunk: all prior cursors and handle ids retire permanently
(`BelowChopLine` thereafter). `reset` is the teardown/recycle path, not part of
runtime rollback; the runtime path is sweep → max mark → `rewind` (INV-PROTO-2).

Third path — abandon-bootstrap: *before* the arming sweep, `partial_rollback(0)` is
legal on every timeline and returns `Ok(None)` from all, which is precisely the
INV-PROTO-2 license for `Domain::reset`. Its window is pre-arming only; the first
commit closes it permanently.

Tests: flow coverage in `tests/state_management/bootstrap.rs::test_e2e_bootstrap_teardown_and_reuse`
(reset path); the restore-preserves-`base` half under
`tests/state_management/invariants.rs::arena::test_inv_arena_2_restore_reuses_where_reset_retires`;
direct pin: none yet.