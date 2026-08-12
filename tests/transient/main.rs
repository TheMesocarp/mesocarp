//! Public-API integration suites for `src/transient`, one test crate for the
//! whole module. Schema: `tests/<module>/<constellation>.rs`, with `main.rs`
//! as the crate root — Cargo only auto-discovers `tests/*.rs` and
//! `tests/*/main.rs` as test targets, so constellation files hang off this
//! root as submodules (and share one linked binary). Future module suites
//! follow the same shape: `tests/comms/main.rs`, `tests/scheduling/main.rs`.
//!
//! Everything here links against mesocarp as an external consumer: public API
//! only, plus the invariant walkers via the `testing` feature.

mod common;

mod bootstrap;
mod timewarp;
