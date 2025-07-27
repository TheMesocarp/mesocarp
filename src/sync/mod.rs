use std::fmt::Display;

pub mod gvt;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ComputeLayout {
    HubSpoke,
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
