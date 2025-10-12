//! This module provides high performance communication primitives for local multi-threaded and multi-process systems.
//! Currently only supports a local message bus using `crossbeam_queue::ArrayQueue` as lock-free channel.
pub mod mt;
