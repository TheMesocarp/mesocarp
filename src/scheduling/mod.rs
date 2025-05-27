//! A set of scheduling primitives with low algorithmic complexity, for high-performance time-based systems.
//!
//! Currently, this module contains timing wheel designs (both standard and hierarchical).
pub mod htw;

/// Trait for any time-series object for processing.
pub trait Scheduleable {
    fn time(&self) -> u64;
    fn commit_time(&self) -> u64;
}
