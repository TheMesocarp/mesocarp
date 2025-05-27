//! arena-based batch loggers for `bytemuck::Pod` data.
//!
//! This module provides `Mnemosyne`, a type-erased logger that uses memory arenas
//! to store timestamped data with minimal allocation overhead. Ideal
//! for performance-critical applications.
use bytemuck::{Pod, Zeroable};

use std::{
    alloc::{alloc, dealloc, Layout},
    ptr,
};

use crate::error::MesoError;

/// A tuple containing a pointer to logged data and its associated timestamp.
///
/// The pointer points to type-erased data in the arena, and the u64 represents
/// the time when the data was logged.
pub type LogState = (*mut u8, usize, usize, u64);

/// `Mnemosyne` is a type-erased arena-based batch logger designed for high-performance
/// logging of structured data with minimal allocation overhead.
///
/// This logger uses memory arenas to efficiently store logged data in contiguous memory,
/// reducing allocation overhead and improving cache locality. Data is stored in a type-erased
/// manner, allowing different types to be logged to the same arena while maintaining type
/// safety through the `Pod + Zeroable` trait bounds.
#[derive(Debug)]
pub struct Mnemosyne {
    /// Pointer to the current arena memory block
    arena: *mut u8,
    /// Size of each arena allocation in bytes
    size: usize,
    /// Current write position within the arena
    write: usize,
    /// Index of the current arena allocation in `self.allocations`
    alloc_idx: usize,
    /// Current state tuple (pointer to last written data, timestamp)
    state: LogState,
    /// Collection of all flushed log entries from previous arenas
    tape: Vec<(LogState, usize)>,
    /// Collection of all log entries in the current arena
    current_writes: Vec<(LogState, usize)>,
    /// Collection of all memory allocations for proper cleanup
    allocations: Vec<(*mut u8, Layout)>,
}

impl Mnemosyne {
    /// Creates a new `Mnemosyne` logger with the specified arena size.
    pub fn initialize(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 8).unwrap();
        let arena = unsafe { alloc(layout) };

        let state = (ptr::null_mut::<u8>(), 0, 8, 0u64);
        let tape = Vec::new();
        let current_writes = Vec::new();
        Self {
            arena,
            size,
            write: 0,
            alloc_idx: 0,
            state,
            tape,
            current_writes,
            allocations: vec![(arena, layout)],
        }
    }

    /// Writes a value with an associated timestamp to the logger.
    ///
    /// The value is stored in the current arena with proper alignment. If the arena
    /// doesn't have enough space, it will be flushed and a new arena allocated.
    /// If the value is too large for any arena, it will be allocated separately.
    pub fn write<T: Pod + Zeroable + 'static>(&mut self, state: T, time: u64) {
        if self.arena.is_null() {
            let layout = Layout::from_size_align(self.size, 8).unwrap();
            unsafe {
                let arena = alloc(layout);
                self.arena = arena;
                self.allocations.push((arena, layout));
            }
        }
        let bytes: &[u8] = bytemuck::bytes_of(&state);
        let size = bytes.len();
        let align = std::mem::align_of_val(&state);
        let offset = (self.write + align - 1) & !(align - 1);
        let mut end = offset + size;

        if end > self.size {
            self.flush(true);
            let offset = (align - 1) & !(align - 1);
            end = offset + size;

            if end > self.size {
                unsafe {
                    let len = self.allocations.len();
                    let layout = Layout::from_size_align(size, align).unwrap();
                    let ptr = alloc(layout);
                    self.allocations.push((ptr, layout));
                    let dst = std::slice::from_raw_parts_mut(ptr, size);
                    let src = std::slice::from_raw_parts(&state as *const T as *const u8, size);
                    dst.copy_from_slice(src);
                    let _ = state;
                    self.state = (dst.as_mut_ptr(), size, align, time);
                    self.current_writes.push((self.state, len));
                }
                return;
            }
        }

        unsafe {
            let dst = std::slice::from_raw_parts_mut(self.arena.add(offset), size);
            let src = std::slice::from_raw_parts(&state as *const T as *const u8, size);
            dst.copy_from_slice(src);
            let _ = state;
            self.state = (dst.as_mut_ptr(), size, align, time);
            self.write = end;
            self.current_writes.push((self.state, self.alloc_idx));
        }
    }

    /// Flushes the current arena, moving all current writes to the tape.
    ///
    /// This internal method is called automatically when the arena becomes full
    /// or manually during cleanup operations.
    fn flush(&mut self, reset: bool) {
        if self.write != 0 {
            let writes = std::mem::take(&mut self.current_writes);
            self.tape.extend(writes);

            if reset {
                let layout = Layout::from_size_align(self.size, 8).unwrap();
                unsafe {
                    let arena = alloc(layout);
                    self.arena = arena;
                    self.alloc_idx = self.allocations.len();
                    self.allocations.push((arena, layout));
                }
            } else {
                self.arena = ptr::null_mut();
            }
            self.write = 0;
        }
    }

    /// Reads the most recently written value from the logger.
    ///
    /// Returns a reference to the last value that was written to the logger,
    /// cast to the specified type.
    pub fn read_state<T: Pod + Zeroable + 'static>(&self) -> Result<&T, MesoError> {
        let (ptr, _, _, _) = self.state;
        if ptr.is_null() {
            return Err(MesoError::UninitializedState);
        }
        let out = unsafe { &*(ptr as *const T) };
        Ok(out)
    }

    /// Reads the most recently written value from the logger as a mutable reference.
    ///
    /// Similar to `read_state()` but the reference returned is mutable.
    pub fn read_state_mut<T: Pod + Zeroable + 'static>(&mut self) -> Result<&mut T, MesoError> {
        let (ptr, _, _, _) = self.state;
        if ptr.is_null() {
            return Err(MesoError::UninitializedState);
        }
        let out = unsafe { &mut *(ptr as *mut T) };
        Ok(out)
    }

    /// Reads all flushed entries from the tape as immutable references.
    ///
    /// Returns a vector of tuples containing references to the logged data and their
    /// associated timestamps. Only includes data that has been flushed to the tape,
    /// not data in the current arena.
    pub fn read_tape<T: Pod + Zeroable + 'static>(&self) -> Vec<(&T, u64)> {
        let mut out = Vec::new();
        for ((ptr, _, _, time), _) in &self.tape {
            unsafe {
                let data = &*(*ptr as *const T);
                out.push((data, *time))
            }
        }
        out
    }

    /// Reads all flushed entries from the tape as mutable references.
    ///
    /// Similar to `read_tape()`, but returns mutable references that allow
    /// modification of the logged data in place.
    pub fn read_tape_mut<T: Pod + Zeroable + 'static>(&mut self) -> Vec<(&mut T, u64)> {
        let mut out = Vec::new();
        for ((ptr, _, _, time), _) in &self.tape {
            unsafe {
                let data = &mut *(*ptr as *mut T);
                out.push((data, *time))
            }
        }
        out
    }

    /// Cleans up the logger and returns all logged data by value.
    ///
    /// This method performs a complete cleanup of the logger, collecting all logged
    /// data (both from the current arena and the tape) and returning it as owned values.
    /// After calling this method, the logger is reset to its initial state with all
    /// memory deallocated.
    pub fn cleanup<T: Pod + Zeroable + 'static>(&mut self) -> Vec<(T, u64)> {
        let mut out = Vec::new();
        self.flush(false);

        for ((ptr, _, _, time), _) in &self.tape {
            unsafe {
                let data = ptr::read(*ptr as *mut T);
                out.push((data, *time));
            }
        }

        for (i, layout) in &self.allocations {
            unsafe { dealloc(*i, *layout) };
        }

        self.write = 0;
        self.state = (ptr::null_mut(), 0, 8, 0);
        self.tape.clear();
        self.current_writes.clear();
        self.allocations.clear();
        out
    }

    pub fn rollback(&mut self, time: u64) {
        // Handle current_writes (data in the active arena)
        let mut new_current_writes_len = 0;
        let mut last_valid_in_current_arena: Option<LogState> = None;
        let mut new_write_pos_in_arena = 0;

        for (i, (logstate, _alloc_idx)) in self.current_writes.iter().enumerate() {
            if logstate.3 <= time {
                new_current_writes_len = i + 1;
                last_valid_in_current_arena = Some(*logstate);
                let aligned_offset = (new_write_pos_in_arena + logstate.2 - 1) & !(logstate.2 - 1);
                new_write_pos_in_arena = aligned_offset + logstate.1;
            } else {
                break;
            }
        }

        self.current_writes.truncate(new_current_writes_len);

        // Update arena state based on current_writes rollback
        if self.current_writes.is_empty() {
            self.arena = ptr::null_mut();
            self.write = 0;
            self.alloc_idx = 0;
        } else {
            self.write = new_write_pos_in_arena;
        }

        // Handle tape (flushed data)
        let mut new_tape_len = 0;
        let mut last_valid_in_tape: Option<LogState> = None;

        for (i, (logstate, _alloc_idx)) in self.tape.iter().enumerate() {
            if logstate.3 <= time {
                new_tape_len = i + 1;
                last_valid_in_tape = Some(*logstate);
            } else {
                // All subsequent entries on tape are rolled back
                break;
            }
        }

        self.tape.truncate(new_tape_len);

        // Update overall state (the last valid log entry across current_writes and tape)
        if let Some(state) = last_valid_in_current_arena {
            self.state = state;
        } else if let Some(state) = last_valid_in_tape {
            self.state = state;
        } else {
            self.state = (ptr::null_mut(), 0, 8, 0u64);
        }

        // IMPORTANT: Memory deallocation for rolled-back items is handled by `cleanup` or `drop`.
        // `rollback` itself does NOT deallocate to prevent double-frees and heap corruption.
    }
}

impl Drop for Mnemosyne {
    /// Automatic cleanup when the logger is dropped.
    fn drop(&mut self) {
        self.flush(false);
        for (i, layout) in &self.allocations {
            unsafe { dealloc(*i, *layout) };
        }

        self.write = 0;
        self.state = (ptr::null_mut(), 0, 8, 0);
        self.tape.clear();
        self.current_writes.clear();
        self.allocations.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Import items from the outer scope
    use bytemuck::{Pod, Zeroable};

    // Define a simple Pod struct for testing
    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(C)] // Required for Pod
    struct MyState {
        x: u32,
        y: f32,
        z: u64,
    }

    unsafe impl Pod for MyState {}
    unsafe impl Zeroable for MyState {}

    #[test]
    fn test_initialize() {
        let size = 1024;
        let mnemosyne = Mnemosyne::initialize(size);
        assert_eq!(mnemosyne.size, size);
        assert_eq!(mnemosyne.write, 0);
        assert!(!mnemosyne.arena.is_null());
        assert_eq!(mnemosyne.state, (ptr::null_mut(), 0, 8, 0));
        assert!(mnemosyne.tape.is_empty());
        assert!(mnemosyne.current_writes.is_empty());
        assert_eq!(mnemosyne.allocations.len(), 1); // Initial arena allocation
    }

    #[test]
    fn test_write_primitive_and_read_state() {
        let size = 64; // Small arena
        let mut mnemosyne = Mnemosyne::initialize(size);

        // Test u32
        let val_u32 = 12345u32;
        let time_u32 = 100u64;
        mnemosyne.write(val_u32, time_u32);
        assert_eq!(mnemosyne.read_state::<u32>().unwrap(), &val_u32);
        assert_eq!(mnemosyne.state.3, time_u32);
        assert_eq!(mnemosyne.current_writes.len(), 1);
        assert_eq!(mnemosyne.write, std::mem::size_of::<u32>()); // write pointer moved

        // Test f32
        let val_f32 = 3.10f32;
        let time_f32 = 200u64;
        mnemosyne.write(val_f32, time_f32);
        assert_eq!(mnemosyne.read_state::<f32>().unwrap(), &val_f32);
        assert_eq!(mnemosyne.state.3, time_f32);
        assert_eq!(mnemosyne.current_writes.len(), 2);
    }

    #[test]
    fn test_write_struct_and_read_state() {
        let size = 64;
        let mut mnemosyne = Mnemosyne::initialize(size);

        let state = MyState {
            x: 10,
            y: 20.5,
            z: 300,
        };
        let time = 500u64;
        mnemosyne.write(state, time);

        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &state);
        assert_eq!(mnemosyne.state.3, time);
        assert_eq!(mnemosyne.current_writes.len(), 1);
        assert_eq!(mnemosyne.write, std::mem::size_of::<MyState>());
    }

    #[test]
    fn test_read_state_uninitialized() {
        let size = 64;
        let mnemosyne = Mnemosyne::initialize(size);
        assert_eq!(
            mnemosyne.read_state::<u32>(),
            Err(MesoError::UninitializedState)
        );
    }

    #[test]
    fn test_arena_overflow_and_flush_true() {
        let size = std::mem::size_of::<u32>() * 2 + 1; // Arena can hold 2 u32s + 1 byte
        let mut mnemosyne = Mnemosyne::initialize(size);

        let val1 = 1u32;
        let time1 = 100u64;
        mnemosyne.write(val1, time1); // Fits

        let val2 = 2u32;
        let time2 = 200u64;
        mnemosyne.write(val2, time2); // Fits

        let val3 = 3u32;
        let time3 = 300u64;
        mnemosyne.write(val3, time3); // Should cause overflow and flush(true)

        assert_eq!(mnemosyne.tape.len(), 2); // val1 and val2
        assert_eq!(mnemosyne.read_tape::<u32>()[0], (&val1, time1));
        assert_eq!(mnemosyne.read_tape::<u32>()[1], (&val2, time2));

        // The last written value (val3) should be the current state
        assert_eq!(mnemosyne.read_state::<u32>().unwrap(), &val3);
        assert_eq!(mnemosyne.state.3, time3);

        // A new arena should have been allocated
        assert_eq!(mnemosyne.allocations.len(), 2);
        // The write pointer should be reset for the new arena
        assert_eq!(mnemosyne.write, std::mem::align_of::<u32>());
    }

    #[test]
    fn test_write_too_large_for_arena() {
        // Arena size is smaller than the struct size
        let size = std::mem::size_of::<MyState>() / 2;
        let mut mnemosyne = Mnemosyne::initialize(size);

        let state = MyState {
            x: 111,
            y: 22.2,
            z: 333,
        };
        let time = 700u64;
        mnemosyne.write(state, time); // Should allocate separately

        // Should not be in the arena, but in its own allocation
        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &state);
        assert_eq!(mnemosyne.state.3, time);
        assert_eq!(mnemosyne.current_writes.len(), 1);
        assert_eq!(mnemosyne.tape.len(), 0); // No flush occurred
        assert_eq!(mnemosyne.write, 0); // Arena write pointer should remain 0
        assert_eq!(mnemosyne.allocations.len(), 2); // Original arena + separate allocation
    }

    #[test]
    fn test_read_tape() {
        let size = 256;
        let mut mnemosyne = Mnemosyne::initialize(size);

        let s1 = MyState {
            x: 1,
            y: 1.0,
            z: 10,
        };
        let t1 = 100;
        mnemosyne.write(s1, t1);

        let s2 = MyState {
            x: 2,
            y: 2.0,
            z: 20,
        };
        let t2 = 200;
        mnemosyne.write(s2, t2);

        // Flush to move current_writes to tape
        mnemosyne.flush(false);

        let s3 = MyState {
            x: 3,
            y: 3.0,
            z: 30,
        };
        let t3 = 300;
        mnemosyne.write(s3, t3); // This will be in current_writes, not yet on tape

        let tape_data = mnemosyne.read_tape::<MyState>();
        assert_eq!(tape_data.len(), 2);
        assert_eq!(tape_data[0], (&s1, t1));
        assert_eq!(tape_data[1], (&s2, t2));

        // Ensure current state is still accessible
        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &s3);
    }

    #[test]
    fn test_read_tape_mut() {
        let size = 256;
        let mut mnemosyne = Mnemosyne::initialize(size);

        let s1 = MyState {
            x: 1,
            y: 1.0,
            z: 10,
        };
        let t1 = 100;
        mnemosyne.write(s1, t1);

        let s2 = MyState {
            x: 2,
            y: 2.0,
            z: 20,
        };
        let t2 = 200;
        mnemosyne.write(s2, t2);

        mnemosyne.flush(false); // Move s1, s2 to tape

        // Get mutable references and modify
        let mut tape_data_mut = mnemosyne.read_tape_mut::<MyState>();
        assert_eq!(tape_data_mut.len(), 2);

        tape_data_mut[0].0.x = 111;
        tape_data_mut[1].0.y = 222.0;

        // Read again to verify changes
        let tape_data = mnemosyne.read_tape::<MyState>();
        assert_eq!(tape_data[0].0.x, 111);
        assert_eq!(tape_data[1].0.y, 222.0);
    }

    #[test]
    fn test_cleanup() {
        let size = 64;
        let mut mnemosyne = Mnemosyne::initialize(size);

        let s1 = MyState {
            x: 1,
            y: 1.0,
            z: 10,
        };
        let t1 = 100;
        mnemosyne.write(s1, t1);

        let s2 = MyState {
            x: 2,
            y: 2.0,
            z: 20,
        };
        let t2 = 200;
        mnemosyne.write(s2, t2);

        let s3 = MyState {
            x: 3,
            y: 3.0,
            z: 30,
        }; // This will cause a flush and be in new arena
        let t3 = 300;
        mnemosyne.write(s3, t3);

        let collected_data = mnemosyne.cleanup::<MyState>();

        // Verify collected data
        assert_eq!(collected_data.len(), 3);
        assert_eq!(collected_data[0], (s1, t1));
        assert_eq!(collected_data[1], (s2, t2));
        assert_eq!(collected_data[2], (s3, t3)); // s3 should be collected too

        // Verify Mnemosyne is in a clean state
        assert_eq!(mnemosyne.write, 0);
        assert_eq!(mnemosyne.state, (ptr::null_mut(), 0, 8, 0));
        assert!(mnemosyne.tape.is_empty());
        assert!(mnemosyne.current_writes.is_empty());
        assert!(mnemosyne.allocations.is_empty()); // All allocations should be deallocated
        assert!(mnemosyne.arena.is_null()); // Arena pointer should be null
    }

    #[test]
    fn test_rollback() {
        let size = 64;
        let mut mnemosyne = Mnemosyne::initialize(size);

        let s1 = MyState {
            x: 1,
            y: 1.0,
            z: 10,
        };
        let t1 = 100;
        mnemosyne.write(s1, t1);
        let s2 = MyState {
            x: 2,
            y: 2.0,
            z: 20,
        };
        let t2 = 200;
        mnemosyne.write(s2, t2);
        let s3 = MyState {
            x: 3,
            y: 3.0,
            z: 30,
        };
        let t3 = 300;
        mnemosyne.write(s3, t3);
        // At this point, s1, s2, s3 are in current_writes. alloc_idx = 0. allocations.len() = 1.

        // Force a flush to move s1, s2, s3 to tape and start a new arena for s4, s5
        mnemosyne.flush(true); // s1, s2, s3 are now on tape. New arena is ready.
                               // allocations.len() = 2. alloc_idx = 1. tape.len() = 3. current_writes.len() = 0.

        let s4 = MyState {
            x: 4,
            y: 4.0,
            z: 40,
        };
        let t4 = 400;
        mnemosyne.write(s4, t4);
        let s5 = MyState {
            x: 5,
            y: 5.0,
            z: 50,
        };
        let t5 = 500;
        mnemosyne.write(s5, t5);
        // Current state: s5. Tape: [s1, s2, s3]. Current_writes: [s4, s5]. Allocations: 2. alloc_idx = 1.

        // Case 1: Rollback to a time before any writes (should clear everything)
        mnemosyne.rollback(50);
        assert_eq!(mnemosyne.state, (ptr::null_mut(), 0, 8, 0)); // State should be uninitialized
        assert!(mnemosyne.current_writes.is_empty());
        assert!(mnemosyne.tape.is_empty());
        assert_eq!(mnemosyne.write, 0);
        assert!(mnemosyne.arena.is_null()); // Arena should be null after full rollback
        assert_eq!(mnemosyne.alloc_idx, 0); // alloc_idx reset
        assert_eq!(mnemosyne.allocations.len(), 2); // Allocations are NOT deallocated by rollback

        // Re-add data for subsequent tests
        mnemosyne.write(s1, t1); // New arena will be allocated here (alloc_idx 0 or 2 depending on how allocations were managed)
        mnemosyne.write(s2, t2);
        mnemosyne.write(s3, t3);
        mnemosyne.flush(true); // Flush to tape
        mnemosyne.write(s4, t4);
        mnemosyne.write(s5, t5);
        // State: s5. Tape: [s1, s2, s3]. Current_writes: [s4, s5]. Allocations: 4 (original + new arena + 2 from re-add)

        // Case 2: Rollback within the current arena (discard s5, keep s4)
        mnemosyne.rollback(400); // Rollback to t4
        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &s4);
        assert_eq!(mnemosyne.state.3, t4);
        assert_eq!(mnemosyne.current_writes.len(), 1); // Only s4 should remain
        assert_eq!(mnemosyne.current_writes[0].0 .3, t4);
        assert_eq!(mnemosyne.tape.len(), 3); // Tape should be unchanged
        assert_eq!(mnemosyne.write, std::mem::size_of::<MyState>()); // Write pointer should be at the end of s4
        assert!(!mnemosyne.arena.is_null()); // Arena should still be valid
        assert_eq!(mnemosyne.allocations.len(), 4); // Allocations are NOT deallocated by rollback

        // Write new data after rollback to verify
        let s6 = MyState {
            x: 6,
            y: 6.0,
            z: 60,
        };
        let t6 = 600;
        mnemosyne.write(s6, t6);
        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &s6);
        assert_eq!(mnemosyne.state.3, t6);
        assert_eq!(mnemosyne.current_writes.len(), 2); // s4, s6
        assert_eq!(mnemosyne.current_writes[1].0 .3, t6);
        assert_eq!(mnemosyne.read_tape::<MyState>().len(), 3); // s1, s2, s3

        // Case 3: Rollback into the tape (discard s4, s6, and s3, keep s1, s2)
        mnemosyne.rollback(200); // Rollback to t2
        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &s2);
        assert_eq!(mnemosyne.state.3, t2);
        assert!(mnemosyne.current_writes.is_empty()); // Current writes should be cleared
        assert_eq!(mnemosyne.tape.len(), 2); // Only s1, s2 should remain on tape
        assert_eq!(mnemosyne.read_tape::<MyState>()[0], (&s1, t1));
        assert_eq!(mnemosyne.read_tape::<MyState>()[1], (&s2, t2));
        assert_eq!(mnemosyne.write, 0); // Write pointer should be reset
        assert!(mnemosyne.arena.is_null()); // Arena should be null as current_writes cleared
        assert_eq!(mnemosyne.allocations.len(), 4); // Allocations are NOT deallocated by rollback

        // Case 4: Rollback to an exact timestamp (rollback to t1, keeping s1)
        let mut mnemosyne_exact = Mnemosyne::initialize(size);
        mnemosyne_exact.write(s1, t1);
        mnemosyne_exact.write(s2, t2);
        mnemosyne_exact.flush(true); // s1, s2 on tape
        mnemosyne_exact.write(s3, t3); // s3 in current_writes

        mnemosyne_exact.rollback(t1);
        assert_eq!(mnemosyne_exact.read_state::<MyState>().unwrap(), &s1);
        assert_eq!(mnemosyne_exact.state.3, t1);
        assert!(mnemosyne_exact.current_writes.is_empty());
        assert_eq!(mnemosyne_exact.tape.len(), 1);
        assert_eq!(mnemosyne_exact.read_tape::<MyState>()[0], (&s1, t1));
        assert_eq!(mnemosyne_exact.write, 0);
        assert!(mnemosyne_exact.arena.is_null());
        assert_eq!(mnemosyne_exact.allocations.len(), 2); // Initial arena + new arena from flush

        // Case 5: Rollback causing current arena to be cleared and new arena allocated on next write
        let mut mnemosyne_reset_arena = Mnemosyne::initialize(size);
        mnemosyne_reset_arena.write(s1, t1); // In arena 0
        mnemosyne_reset_arena.flush(true); // s1 on tape, new arena 1
        mnemosyne_reset_arena.write(s2, t2); // In arena 1
        mnemosyne_reset_arena.write(s3, t3); // In arena 1
                                             // State: s3, Tape: [s1], Current_writes: [s2, s3]

        mnemosyne_reset_arena.rollback(250); // Rollback to a time between s2 and s3. s3 rolled back.
        assert_eq!(mnemosyne_reset_arena.read_state::<MyState>().unwrap(), &s2); // Last valid state is s2
        assert_eq!(mnemosyne_reset_arena.state.3, t2);
        assert_eq!(mnemosyne_reset_arena.current_writes.len(), 1); // Only s2 remains
        assert_eq!(mnemosyne_reset_arena.tape.len(), 1); // s1 should still be on tape
        assert_eq!(mnemosyne_reset_arena.read_tape::<MyState>()[0], (&s1, t1));
        assert_eq!(mnemosyne_reset_arena.write, std::mem::size_of::<MyState>()); // Write pointer at end of s2
        assert!(!mnemosyne_reset_arena.arena.is_null()); // Arena should still be valid (s2 is still there)
        assert_eq!(mnemosyne_reset_arena.allocations.len(), 2); // Allocations are NOT deallocated by rollback

        // Write new data to the existing arena
        let s7 = MyState {
            x: 7,
            y: 7.0,
            z: 70,
        };
        let t7 = 700;
        mnemosyne_reset_arena.write(s7, t7);
        assert_eq!(mnemosyne_reset_arena.read_state::<MyState>().unwrap(), &s7);
        assert_eq!(mnemosyne_reset_arena.state.3, t7);
        assert_eq!(mnemosyne_reset_arena.current_writes.len(), 2); // s2, s7
        assert_eq!(mnemosyne_reset_arena.tape.len(), 1); // Tape still has s1

        // Case 6: Rollback with separately allocated data
        let mut mnemosyne_separate_alloc =
            Mnemosyne::initialize(std::mem::size_of::<MyState>() / 2); // Small arena
        let large_state_1 = MyState {
            x: 100,
            y: 100.0,
            z: 1000,
        };
        let large_t1 = 1000;
        mnemosyne_separate_alloc.write(large_state_1, large_t1); // Separately allocated
        let large_state_2 = MyState {
            x: 200,
            y: 200.0,
            z: 2000,
        };
        let large_t2 = 2000;
        mnemosyne_separate_alloc.write(large_state_2, large_t2); // Separately allocated
        let large_state_3 = MyState {
            x: 300,
            y: 300.0,
            z: 3000,
        };
        let large_t3 = 3000;
        mnemosyne_separate_alloc.write(large_state_3, large_t3); // Separately allocated
                                                                 // State: large_state_3. current_writes: [large_state_1, large_state_2, large_state_3]. Tape: []. Allocations: 4 (initial + 3 separate).

        mnemosyne_separate_alloc.rollback(2500); // Rollback to t2 (large_state_2)
        assert_eq!(
            mnemosyne_separate_alloc.read_state::<MyState>().unwrap(),
            &large_state_2
        );
        assert_eq!(mnemosyne_separate_alloc.state.3, large_t2);
        assert_eq!(mnemosyne_separate_alloc.current_writes.len(), 2); // large_state_1, large_state_2
        assert_eq!(mnemosyne_separate_alloc.tape.len(), 0);
        assert_eq!(mnemosyne_separate_alloc.allocations.len(), 4); // Allocations are NOT deallocated by rollback

        mnemosyne_separate_alloc.rollback(500); // Rollback before large_state_1
        assert_eq!(mnemosyne_separate_alloc.state, (ptr::null_mut(), 0, 8, 0)); // State should be uninitialized
        assert!(mnemosyne_separate_alloc.current_writes.is_empty());
        assert!(mnemosyne_separate_alloc.tape.is_empty());
        assert_eq!(mnemosyne_separate_alloc.allocations.len(), 4); // Allocations are NOT deallocated by rollback
        assert!(mnemosyne_separate_alloc.arena.is_null()); // Arena should be null
    }

    #[test]
    fn test_allocation_on_flush() {
        let size = 16;
        let mut mnemosyne = Mnemosyne::initialize(size);
        // Track initial allocations
        let _initial_arena_ptr = mnemosyne.allocations[0].0;
        let initial_allocs_count = mnemosyne.allocations.len();

        // Write data to fill one arena and cause a flush, and write more to the new arena
        for i in 0..10 {
            mnemosyne.write(i as u32, i as u64 * 100);
        }
        assert!(mnemosyne.allocations.len() > initial_allocs_count); // Ensure more than one arena was allocated
    }

    #[test]
    fn test_cleanup_and_read_after_cleanup() {
        let size = 128;
        let mut mnemosyne = Mnemosyne::initialize(size);
        mnemosyne.write(100u32, 1000);
        mnemosyne.flush(true);
        mnemosyne.write(200u32, 2000);

        let collected = mnemosyne.cleanup::<u32>();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], (100u32, 1000));
        assert_eq!(collected[1], (200u32, 2000));

        // After cleanup, the logger should be in an uninitialized state
        assert_eq!(mnemosyne.write, 0);
        assert_eq!(mnemosyne.state, (ptr::null_mut(), 0, 8, 0));
        assert!(mnemosyne.tape.is_empty());
        assert!(mnemosyne.current_writes.is_empty());
        assert!(mnemosyne.allocations.is_empty());
        assert!(mnemosyne.arena.is_null());

        // Attempting to read after cleanup should fail
        assert_eq!(
            mnemosyne.read_state::<u32>(),
            Err(MesoError::UninitializedState)
        );
        assert!(mnemosyne.read_tape::<u32>().is_empty());
    }

    #[test]
    fn test_rollback_to_empty_state_correctly_nulls_arena() {
        let size = 64;
        let mut mnemosyne = Mnemosyne::initialize(size);

        mnemosyne.write(10u32, 100);
        mnemosyne.write(20u32, 200);
        mnemosyne.flush(true); // Now 10, 20 are on tape. New arena.
        mnemosyne.write(30u32, 300); // In current arena.

        assert!(!mnemosyne.arena.is_null()); // Arena should be active
        assert_eq!(mnemosyne.current_writes.len(), 1);
        assert_eq!(mnemosyne.tape.len(), 2);

        // Rollback past all entries
        mnemosyne.rollback(50);

        assert_eq!(mnemosyne.state, (ptr::null_mut(), 0, 8, 0));
        assert!(mnemosyne.current_writes.is_empty());
        assert!(mnemosyne.tape.is_empty());
        assert_eq!(mnemosyne.write, 0);
        assert!(mnemosyne.arena.is_null()); // Crucial: arena should be nullified
        assert_eq!(mnemosyne.alloc_idx, 0); // alloc_idx reset
                                            // Allocations remain, as rollback doesn't deallocate
        assert_eq!(mnemosyne.allocations.len(), 2);
    }

    #[test]
    fn test_rollback_does_not_deallocate() {
        let size = 64;
        let mut mnemosyne = Mnemosyne::initialize(size);

        mnemosyne.write(1u32, 100);
        mnemosyne.write(2u32, 200);
        mnemosyne.flush(true); // First arena flushed, new one allocated
        mnemosyne.write(3u32, 300); // In the second arena

        let initial_alloc_count = mnemosyne.allocations.len();
        assert!(initial_alloc_count >= 2); // At least initial + one flushed arena

        // Rollback, but do not expect deallocation
        mnemosyne.rollback(150); // Rollback to keep only 1u32 on tape

        assert_eq!(mnemosyne.allocations.len(), initial_alloc_count); // Allocation count should remain the same
        assert_eq!(mnemosyne.tape.len(), 1);
        assert!(mnemosyne.current_writes.is_empty());
        assert_eq!(mnemosyne.read_state::<u32>().unwrap(), &1u32); // Last state should be 1u32
    }

    #[test]
    fn test_rollback_write_after_rollback_to_tape_and_new_arena() {
        let size = std::mem::size_of::<MyState>() * 2;
        let mut mnemosyne = Mnemosyne::initialize(size);

        let s1 = MyState {
            x: 1,
            y: 1.0,
            z: 10,
        };
        let t1 = 100;
        mnemosyne.write(s1, t1);
        let s2 = MyState {
            x: 2,
            y: 2.0,
            z: 20,
        };
        let t2 = 200;
        mnemosyne.write(s2, t2);
        mnemosyne.flush(true); // s1, s2 on tape, new arena

        let s3 = MyState {
            x: 3,
            y: 3.0,
            z: 30,
        };
        let t3 = 300;
        mnemosyne.write(s3, t3);
        let s4 = MyState {
            x: 4,
            y: 4.0,
            z: 40,
        };
        let t4 = 400;
        mnemosyne.write(s4, t4);
        // Current: s4. Tape: [s1, s2]. Current_writes: [s3, s4]

        mnemosyne.rollback(250); // Rollback to just after s2 (on tape). s3, s4 are discarded.
        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &s2); // Last valid state is s2
        assert_eq!(mnemosyne.tape.len(), 2);
        assert!(mnemosyne.current_writes.is_empty());
        assert_eq!(mnemosyne.write, 0);
        assert!(mnemosyne.arena.is_null()); // Arena should be null after current_writes cleared

        // Now, write new data. This should allocate a new arena.
        let s5 = MyState {
            x: 5,
            y: 5.0,
            z: 50,
        };
        let t5 = 500;
        mnemosyne.write(s5, t5);
        assert_eq!(mnemosyne.read_state::<MyState>().unwrap(), &s5);
        assert_eq!(mnemosyne.state.3, t5);
        assert_eq!(mnemosyne.current_writes.len(), 1); // Only s5
        assert_eq!(mnemosyne.tape.len(), 2); // s1, s2 still there
        assert!(!mnemosyne.arena.is_null()); // New arena should be active
        assert_eq!(mnemosyne.write, std::mem::size_of::<MyState>());
        assert_eq!(mnemosyne.allocations.len(), 3); // Original, first flushed, new arena
    }

    #[test]
    fn test_flush_resets_alloc_idx_correctly_on_reset_true() {
        let size = 64;
        let mut mnemosyne = Mnemosyne::initialize(size);
        mnemosyne.write(1u32, 100);
        let _initial_alloc_idx = mnemosyne.alloc_idx;
        let initial_allocations_count = mnemosyne.allocations.len();

        mnemosyne.flush(true); // Should allocate a new arena and update alloc_idx
        assert_eq!(mnemosyne.alloc_idx, initial_allocations_count); // New alloc_idx should be the count of allocations before adding the new one
        assert_eq!(mnemosyne.allocations.len(), initial_allocations_count + 1);
        assert!(!mnemosyne.arena.is_null());
        assert_eq!(mnemosyne.write, 0);

        mnemosyne.write(2u32, 200); // Should use the new arena
        assert_eq!(mnemosyne.current_writes[0].1, mnemosyne.alloc_idx);
    }
}
