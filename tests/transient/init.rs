//! Constructor gates, one file for every type's `new` (the `Create.t.sol`
//! pattern from `skills/TEST.md`): initialization is unit tested here, however
//! many types the module exposes.

use crate::common::CS;
use mesocarp::transient::{Domain, Timeline};
use mesocarp::MesoError;

#[test]
fn test_domainNew_RevertsWhenInitializedWithNoSlots() {
    assert!(matches!(
        Domain::new(0),
        Err(MesoError::InitializedWithNoSlots)
    ));
}

// Offsets travel as u32, so a chunk_size whose aligned value exceeds u32::MAX
// is rejected up front.
#[test]
fn test_domainNew_RevertsWhenChunkSizeTooLarge() {
    assert!(matches!(
        Domain::new(u32::MAX as usize + 1),
        Err(MesoError::ChunkSizeTooLarge)
    ));
}

#[test]
fn test_timelineNew_RevertsWhenInitializedWithNoSlots() {
    let d = Domain::new(CS).unwrap();
    assert!(matches!(
        Timeline::<u64>::new(0, &d),
        Err(MesoError::InitializedWithNoSlots)
    ));
}
