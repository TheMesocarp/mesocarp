use thiserror::Error;

pub mod comms;
pub mod scheduling;
pub mod transient;

/// Error type for all primitives
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MesoError {
    #[error("No point-to-point communication was done")]
    NoDirectCommsToShare,
    #[error("Buffer full, cannot write until read")]
    BuffersFull,
    #[error("Initialized with zero clock slots")]
    NoClockSlots,
    #[error("Ordering error occured, time travel!")]
    TimeTravel,
    #[error("No items found!")]
    NoItems,
    #[error("Not found: {name}")]
    NotFound { name: String },
    #[error("Attempted to send a `Message` to a nonexistent user.")]
    InvalidUserId,
    #[error("Mark points outside the current housing chunk, an arithmatic error was made.")]
    MarkOutsideHomeChunk,
    #[error("handle or restore target below the chop line; the current commit horizon fixed by the GVT.")]
    BelowChopLine,
    #[error("Initialized data structure with no slots.")]
    InitializedWithNoSlots,
    #[error(
        "The timestamps of writes must always be monotonically increasing between rollback events"
    )]
    TimestampMonotonicityFailure,
    #[error("Attempted to restore a value from a non-existent memory arena")]
    PastTheHorizon,
    #[error("Attempted to allocate a type that needs a drop function to the memory arena.")]
    NeedsDrop,
    #[error("Attempting to read the state of a foreign `Domain` from the wrong `Timeline<V>`.")]
    ForeignDomain,
}
