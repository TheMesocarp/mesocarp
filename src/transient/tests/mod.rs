//! Unit and gate tests for the transient module. These compile into the crate
//! (`cfg(test)`), so they may reach private state — the tier for exercising
//! internals the public API deliberately hides. Public-API flows belong in the
//! top-level `tests/` integration suites instead.
//!
//! Test names reference invariant ids from `docs/invariants.md` where one is
//! being pinned. Run under miri with `cargo +nightly miri test transient::`.
//!
//! Planned suites (see the coverage report): `timeline` (record/rollback/chop
//! gates), `horizon` (INV-HORIZON-*), `machine` (proptest op-sequence state
//! machine against a reference model).

use super::*;

mod domain;

// 64-byte chunks: exactly eight u64 slots each.
const CS: usize = 64;

fn filled(n: u64) -> (Domain, Vec<Handle<u64>>) {
    let mut d = Domain::new(CS);
    let hs = (0..n).map(|i| d.alloc(i).unwrap()).collect();
    (d, hs)
}
