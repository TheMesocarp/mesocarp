use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("Null pointer found, must have written as null")]
    ExpectedUpdate,
    #[error("Nothing to read, no pending updates")]
    NoPendingUpdates,
    #[error("Buffer full, cannot write until read")]
    BuffersFull,
}
