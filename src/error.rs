use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
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
}
