use std::{ffi::NulError, fmt::Display};

use thiserror::Error;

pub mod comms;
pub mod logging;
pub mod scheduling;

#[derive(Debug, Error)]
struct IoError(std::io::Error);

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
        err: IoError
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
        source: NulError
    },   
}

impl MesoError {
    #[cfg(unix)]
    fn from_errno(errno: i32, name: &str, context: &str) -> Self {
        match errno {
            libc::ENOENT => MesoError::NotFound { name: name.to_string() },
            libc::EACCES => MesoError::PermissionDenied { name: name.to_string() },
            libc::EEXIST => MesoError::AlreadyExists { name: name.to_string() },
            libc::EINVAL => MesoError::InvalidParameter { description: format!("Invalid argument for {} on '{}': {}", context, name, std::io::Error::from_raw_os_error(errno)) },
            _ => MesoError::Io { err: IoError(std::io::Error::from_raw_os_error(errno)) },
        }
    }

    #[cfg(windows)]
    fn from_last_error(name: &str, context: &str) -> Self {
        use winapi::shared::winerror;
        let error_code = unsafe { winapi::um::errhandlingapi::GetLastError() };
        match error_code {
            winerror::ERROR_FILE_NOT_FOUND => MesoError::NotFound { name: name.to_string() },
            winerror::ERROR_ACCESS_DENIED => MesoError::PermissionDenied { name: name.to_string() },
            winerror::ERROR_ALREADY_EXISTS => MesoError::AlreadyExists { name: name.to_string() }, // Should be handled by logic, but as error if unexpected
            winerror::ERROR_INVALID_PARAMETER => MesoError::InvalidParameter { description: format!("Invalid parameter for {} on '{}': code {}", context, name, error_code) },
            _ => MesoError::Io { source: std::io::Error::from_raw_os_error(error_code as i32) },
        }
    }
}