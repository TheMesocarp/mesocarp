//! A "GVT" (Global Virtual Time) is a synchronization primitive, which are very
//! relevant to optimistic synchronization schemes in time-ordered systems. They
//! effectively compute the safe point for the system's optimism. Currently only one
//! GVT computation scheme is included, the one used in `aika`.

use std::fmt::Display;

pub mod aika;

/// Specifies the multi-threaded system arrangement for hybrid GVT schemes that can manage multiple topologies.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ComputeLayout {
    /// HubSpoke for systems with a GVT Master thread for handling all the computation and broadcasting updates.
    HubSpoke,
    /// Local GVT updates by thr producers themselves, producers send blocks to each other.
    Decentralized,
}

impl Display for ComputeLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComputeLayout::HubSpoke => write!(f, "Centralized/Master Channel"),
            ComputeLayout::Decentralized => write!(f, "Decentralized"),
        }
    }
}
