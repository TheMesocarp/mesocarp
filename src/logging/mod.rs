//! A module for high performance simulation data loggers.
//!
//! Each data logger in this module is optimized for a different purpose,
//! however, all of which aim to minimize runtime allocations and deallocations and unnecessary cloning.

pub mod journal;