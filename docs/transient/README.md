# `transient` — rollback-aware state storage for optimistic simulation

`src/transient` is the memory substrate for speculative state in a parallel
discrete event simulator (PDES). It stores every state write a logical process
makes, cheaply undoes writes that turn out to be premature, and reclaims
history wholesale once it is globally confirmed. The name is literal: until
the commit frontier passes it, every byte in here is provisional.

## Why this exists

In a discrete event simulation, state changes at discrete virtual times. PDES
splits the simulation across *logical processes* (LPs) that exchange
timestamped events. Under **optimistic synchronization** (Time Warp), an LP
does not wait to be certain no earlier event can still arrive — it executes
ahead speculatively. When a message lands in its past (a *straggler*), the LP
**rolls back** to just before it and re-executes. **Global Virtual Time**
(GVT) is the floor below which no rollback can ever reach; state older than
GVT is committed and reclaimable (*fossil collection*, called **chop** here).

The tax optimism pays is state saving: every mutation must stay undoable
until GVT passes it. This module is that tax collector, built so the three
operations an optimistic LP performs constantly are near-free:

- **save** — bump allocation plus an index append,
- **rollback** — truncate an index and rewind a bump pointer,
- **commit** — pop whole chunks off the front of a deque.

Nothing is ever freed per-value. Memory moves in chunks, from either end of
history, or not at all.

## Design at a glance

Two structs split the job. `Domain` owns bytes; `Timeline<V>` knows what they
mean.

```
        Timeline<V>  (index, ordinary heap)          Domain  (arena, raw bytes)
  ┌───────────────────────────────────┐      ┌───────────────────────────────────┐
  │ chunks: deque of record slabs     │      │ chunks: deque of byte slabs       │
  │   record = (Stamp, Handle<V>) ─────────────► values, bump-allocated          │
  │ latest: newest stamp              │      │ ids: base + index (u32, global)   │
  │ commit_horizon: rollback floor    │      │ cursor: next write position       │
  └───────────────────────────────────┘      └───────────────────────────────────┘
    partial_rollback(t) → HighMark  ────────►  rewind        (LIFO; ids reused)
    partial_chop(t)     → floor     ────────►  release_front (FIFO; ids retired)
```

**`Domain`** — one per LP, single writer. A bump arena over uniform chunks;
oversized or over-aligned values get exact-fit chunks of their own. Chunk ids
are global and monotone (`base + index`), so releasing committed history off
the front only advances `base` and never invalidates an id stored elsewhere.
Anything may allocate from a `Domain` (`alloc` returns a copyable
`Handle<T>`); the design intent is many rollback-aware objects sharing one
arena, of which `Timeline<V>` is the first.

**`Timeline<V>`** — a stamp-ordered index of one state type's history:
records pair a `Stamp { time, seq }` with the handle of the value written at
that stamp. Appends must be strictly stamp-increasing between rollbacks, which
makes allocation order equal stamp order — the property that turns liveness
into a prefix/suffix question and makes wholesale reclamation sound. Index
chunks live on the ordinary heap, not in the arena: only *values* occupy the
`Domain`.

**The two ends of history:**

| Operation | Direction | Trigger | Chunk fate | Id fate |
|---|---|---|---|---|
| `partial_chop` + `release_front` | front (oldest) | GVT advance | std recycled, exact-fit freed | retired forever |
| `partial_rollback` + `rewind`/`restore` | back (newest) | straggler | std recycled, exact-fit freed | **reused** |
| `reset` | everything | teardown | recycled/freed | retired forever |

Chop keeps the newest record at-or-before the GVT bound — the off-by-one that
preserves the state a rollback would restore to. Rollback discards every
record with `time >= to` and reports a `HighMark` naming the newest survivor;
the domain rewinds no further back than the maximum mark across all its
timelines. Each timeline also tracks a `commit_horizon` (raised by chop):
rollback targets at or below it are refused before anything mutates, so a
protocol bug upstream fails loudly instead of silently vaporizing committed
state.

## Lifecycle sketch

The orchestration layer that ties many timelines to one domain does not exist
yet (see *Status*), so the sweep logic below is spelled out by hand — it is
the same shape the future tying object will own. A complete runnable version
is `tests/transient/timewarp.rs`.

```rust
use mesocarp::transient::{Domain, Stamp, Timeline};

// One Domain per LP; one Timeline per state type, all sharing the arena.
let mut d = Domain::new(4096);                      // chunk size in BYTES
let mut hp:  Timeline<u64>      = Timeline::new(64, &d)?;   // slots per index chunk
let mut pos: Timeline<[f32; 2]> = Timeline::new(64, &d)?;

// Seed baselines at GVT₀ = 0 before any event runs (INV-BOOT-1).
hp.record(&mut d, 100, Stamp { time: 0, seq: 0 })?;
pos.record(&mut d, [0.0, 0.0], Stamp { time: 0, seq: 1 })?;

// Speculative execution: strictly increasing stamps between rollbacks.
hp.record(&mut d, 90, Stamp { time: 3, seq: 0 })?;
pos.record(&mut d, [1.5, 0.5], Stamp { time: 5, seq: 0 })?;

// Straggler at t = 4: sweep EVERY timeline, rewind once to the MAX mark —
// order marks by (chunk, end address); a single timeline's mark may sit
// below a sibling's surviving value (INV-PROTO-2).
let marks = [hp.partial_rollback(4)?, pos.partial_rollback(4)?];
let mark = marks.into_iter().flatten().reduce(max_by_chunk_then_end).unwrap();
unsafe { d.rewind(mark)? };

// GVT reaches 3: sweep chops, release at the MIN floor (INV-PROTO-3).
let floors = [hp.partial_chop(3), pos.partial_chop(3)];
if let Some(floor) = floors.into_iter().flatten().min() {
    unsafe { d.release_front(floor) };
}

// Newest surviving state; the borrow of `d` pins the arena while it lives.
let current = unsafe { hp.live_state(&d)? };
```

## Safety model

Three tiers, weakest last:

1. **Hard gates.** Every domain gets a process-unique, never-reused id from a
   global counter; `record` and `live_state` refuse a mismatched domain
   (`ForeignDomain`) before touching anything. Because ids never recur, a
   dropped domain's id is unpresentable forever — a timeline that outlives
   its arena holds dangling handles it can never be tricked into following.
   `alloc` rejects any type with drop glue (`NeedsDrop`): values are POD by
   fiat, which is what licenses byte-level wholesale reclamation.
2. **Borrow gates.** `live_state` ties the returned `&V` to the `&Domain`
   borrow, so no arena mutation can occur while a read is outstanding.
3. **Contract gates.** The `unsafe` entry points (`rewind`, `restore`,
   `release_front`, `reset`, `Handle::get`, `live_state`) each carry a
   liveness contract the caller must uphold — max-of-marks, min-of-floors,
   reset-only-when-empty, cursors-of-undone-events-are-dead. These are the
   obligations of the orchestration layer, catalogued as INV-PROTO-1..6.

The full invariant registry — structural properties, gate semantics, known
gaps, and which test pins what — is [`invariants.md`](./invariants.md) in
this directory. Structural invariants are also executable: `check_invariants`
/ `check_lockstep` walkers compile in under the `testing` feature and panic
with the violated invariant's id.

## Status and open edges

- **No tying object yet.** The `Transient` trait (`rollback`/`chop`) is the
  seam where per-LP orchestration will plug in; `ds/` is scaffolding for
  rollback-aware data structures built over `Domain`. Until then the sweep
  protocol lives in tests and in the caller's hands.
- **Single-threaded by construction.** `Domain` and `Timeline` are
  `!Send`/`!Sync` (raw-pointer fields). Moving domains to worker threads at
  simulator startup will require a deliberate `unsafe impl Send` decision.
- **Known open decisions**, tracked in the registry: chop on an empty
  timeline currently skips the horizon update (INV-HORIZON-1 deviation); a
  record-side horizon gate is proposed but not adopted (INV-HORIZON-4);
  `Cursor` carries no domain id, so cross-domain cursor application is gated
  only by contract (INV-PROTO-6); rollback granularity is `time`-only —
  `seq` breaks append ties, never reclamation boundaries.

## Map

| Path | What |
|---|---|
| `src/transient/mod.rs` | the module: `Domain`, `Timeline<V>`, `Stamp`, `Handle`, `Cursor`, `HighMark`, `Transient` |
| `src/transient/ds/` | future rollback-aware data structures (scaffolding) |
| `src/transient/testing.rs` | invariant walkers (`cfg(test)` / `testing` feature) |
| `src/transient/tests/` | white-box unit tier (private-state assertions; runs under miri) |
| `tests/transient/` | public-API tier: `invariants.rs` registry pins, `timewarp.rs` / `bootstrap.rs` flows |
| `docs/transient/invariants.md` | the invariant registry — ids, proofs, coverage, gaps |
| `benches/allocation.rs` | append throughput and the speculate/rollback recycling cycle |

Suggested reading order for newcomers: this file → the *Lifecycle sketch*
against `tests/transient/timewarp.rs` → [`invariants.md`](./invariants.md) →
`src/transient/mod.rs` top to bottom (it is a single, documented file).

Run the module's tests with:

```bash
cargo test --test transient && cargo test transient::
```

and the same under miri (`cargo +nightly miri test`) — the unsafe surface is
kept miri-clean as a standing requirement.
