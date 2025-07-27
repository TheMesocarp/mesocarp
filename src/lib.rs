use std::{ffi::NulError, fmt::Display};

use thiserror::Error;

use crate::sync::ComputeLayout;

pub mod comms;
pub mod logging;
pub mod scheduling;
pub mod sync;

/// Wrapper type for `std::io::Error`
#[derive(Debug, Error)]
pub struct IoError(std::io::Error);

impl PartialEq for IoError {
    fn eq(&self, other: &Self) -> bool {
        self.0.kind() == other.0.kind()
    }
}

impl Eq for IoError {}
impl Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type for all primitives
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MesoError {
    #[error("Null pointer found, must have written as null")]
    ExpectedUpdate,
    #[error("Nothing to read, no pending updates")]
    NoPendingUpdates,
    #[error("No point-to-point communication was done")]
    NoDirectCommsToShare,
    #[error("Buffer full, cannot write until read")]
    BuffersFull,
    #[error("BroadcastWheel size N must be greater than 0")]
    InitializedWithZeroSlots,
    #[error("Initialized with zero clock slots")]
    NoClockSlots,
    #[error("Ordering error occured, time travel!")]
    TimeTravel,
    #[error("failed to add scheduleable item to slot")]
    ClockSubmissionFailed,
    #[error("No items found!")]
    NoItems,
    #[error("Type erased LogState pointer is null")]
    UninitializedState,
    #[error("Error reading/writing to shared memory")]
    SharedMemoryError,
    #[error("Shared memory operation failed: {0}")]
    GenericError(String),
    #[error("Invalid name for shared memory: {0}")]
    InvalidName(String),
    #[error("Shared memory not found: {name}")]
    NotFound { name: String },
    #[error("Shared memory already exists: {name}")]
    AlreadyExists { name: String },
    #[error("Permission denied for shared memory operation on: {name}")]
    PermissionDenied { name: String },
    #[error("I/O error during shared memory operation {:?}", err)]
    Io {
        #[from]
        err: IoError,
    },
    #[error("Shared memory header validation failed: {reason}")]
    HeaderMismatch { reason: String },
    #[error("Shared memory size mismatch: name='{name}', reason='{reason}'")]
    SizeMismatch { name: String, reason: String },
    #[error("Shared memory segment is not initialized: {name}")]
    NotInitialized { name: String },
    #[error("Invalid parameter for shared memory operation: {description}")]
    InvalidParameter { description: String },
    // need better error handling for this variant
    #[error("Failed to convert name to CString {source}")]
    NulError {
        #[from]
        source: NulError,
    },
    #[error("Improper handling of `Message` passing. Either fix a `to: usize` address or set None to broadcast.")]
    ImproperMessagePassing,
    #[error("Attempted to send a `Message` to a nonexistent user.")]
    InvalidUserId,
    #[error("Attempted to register a compute producer that expected one layout, but another was found: {0}")]
    ComputeLayoutExpectationMismatch(ComputeLayout),
    #[error("current processor is receiving messages from {0} blocks; too far in the past! Increase time bandwidth.")]
    DistantBlocks(usize),
    #[error(
        "Slotted block submissions show different `start`/`dur` values for the same `block_nmb`"
    )]
    MismatchBlockRanges,
}
