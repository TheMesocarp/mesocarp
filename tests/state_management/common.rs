//! Shared harness for the transient integration constellations, pulled in once
//! by `main.rs` and reached as `crate::common` from each suite.
//!
//! The fold helpers encode the sweep halves of the cross-object protocol
//! (INV-PROTO-2 / INV-PROTO-3 in `docs/transient/invariants.md`) until the real
//! orchestration layer owns them.
#![allow(dead_code)]

use mesocarp::state_management::{Cursor, Domain, Handle, Stamp};

/// 64-byte chunks: exactly eight u64 slots each.
pub const CS: usize = 64;

/// Domain over `CS`-byte chunks with `n` sequential u64s allocated (8 per chunk).
pub fn filled(n: u64) -> (Domain, Vec<Handle<u64>>) {
    let mut d = Domain::new(CS).unwrap();
    let hs = (0..n).map(|i| d.alloc(i).unwrap()).collect();
    (d, hs)
}

pub fn stamp(time: u64, seq: u32) -> Stamp {
    Stamp { time, seq }
}

/// Fold per-timeline rollback marks into the single rewind target (INV-PROTO-2).
/// Empty timelines contribute `None`; all-`None` licenses `Domain::reset`.
pub fn fold_marks(marks: impl IntoIterator<Item = Option<Cursor>>) -> Option<Cursor> {
    marks.into_iter().flatten().max()
}

/// Fold per-timeline chop floors into the single release floor (INV-PROTO-3).
/// Empty timelines contribute `None` and constrain nothing.
pub fn min_floor(floors: impl IntoIterator<Item = Option<u32>>) -> Option<u32> {
    floors.into_iter().flatten().min()
}
