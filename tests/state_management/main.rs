//! Public-API integration suites for `src/transient`, one test crate for the
//! whole module. Schema: `tests/<module>/<constellation>.rs`, with `main.rs`
//! as the crate root — Cargo only auto-discovers `tests/*.rs` and
//! `tests/*/main.rs` as test targets, so constellation files hang off this
//! root as submodules (and share one linked binary). Future module suites
//! follow the same shape: `tests/comms/main.rs`, `tests/scheduling/main.rs`.
//!
//! Everything here links against mesocarp as an external consumer: public API
//! only, plus the invariant walkers via the `testing` feature.
//!
//! Three kinds of file (per `skills/TEST.md`): per-entrypoint unit suites
//! (`init.rs`, `cursor.rs`, `release_front.rs`); `invariants.rs`, the registry
//! file (the invariants.t.sol analog — direct pins of `docs/transient/invariants.md`
//! entries, grouped in category mods); and e2e usage-pattern flows
//! (`timewarp.rs`, `bootstrap.rs`). Planned next: a `record.rs` gate suite,
//! direct INV-HORIZON-* pins, and a proptest op-sequence machine.

#![allow(non_snake_case)] // unit-test names carry camelCase action segments (skills/TEST.md)

mod common;

mod bootstrap;
mod cursor;
mod init;
mod invariants;
mod release_front;
mod timewarp;
