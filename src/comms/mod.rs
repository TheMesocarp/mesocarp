//! Single-producer, multi-consumer (SPMC) and single-producer, single-consumer (SPSC) channels.
//!
//! This module provides lock-free communication primitives for different producer-consumer
//! patterns. Currently, the `spsc` submodule contains a buffer wheel, while `spmc` provides a broadcast-subscribe
//! channel, as well as a work queue channel.
pub mod mailbox;
pub mod spmc;
pub mod spsc;
