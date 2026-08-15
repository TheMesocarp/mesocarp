//! Unit tests for `Domain::cursor`: bump-position capture across allocation,
//! including on a fresh domain.

use crate::common::CS;
use mesocarp::state_management::{Cursor, Domain};

#[test]
fn test_cursor_UpdatesChunkAndOffset() {
    let mut d = Domain::new(CS).unwrap();
    assert_eq!(
        d.cursor(),
        Cursor {
            chunk: 0,
            offset: 0
        }
    );
    d.alloc(7u64).unwrap();
    assert_eq!(
        d.cursor(),
        Cursor {
            chunk: 0,
            offset: 8
        }
    );
    for i in 0..8u64 {
        d.alloc(i).unwrap();
    }
    assert_eq!(
        d.cursor(),
        Cursor {
            chunk: 1,
            offset: 8
        }
    );
}
