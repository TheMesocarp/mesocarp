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
pub type LogState = (*mut u8, u64);

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
    /// Current state tuple (pointer to last written data, timestamp)
    state: LogState,
    /// Collection of all flushed log entries from previous arenas
    tape: Vec<LogState>,
    /// Collection of all log entries in the current arena
    current_writes: Vec<LogState>,
    /// Collection of all memory allocations for proper cleanup
    allocations: Vec<(*mut u8, Layout)>,
}

impl Mnemosyne {
    /// Creates a new `Mnemosyne` logger with the specified arena size.
    pub fn initialize(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 8).unwrap();
        let arena = unsafe { alloc(layout) };

        let state = (ptr::null_mut::<u8>(), 0u64);
        let tape = Vec::new();
        let current_writes = Vec::new();
        Self {
            arena,
            size,
            write: 0,
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
                    let layout = Layout::from_size_align(size, align).unwrap();
                    let ptr = alloc(layout);
                    self.allocations.push((ptr, layout));
                    let dst = std::slice::from_raw_parts_mut(ptr, size);
                    let src = std::slice::from_raw_parts(&state as *const T as *const u8, size);
                    dst.copy_from_slice(src);
                    let _ = state;
                    self.state = (dst.as_mut_ptr(), time);
                    self.current_writes.push(self.state);
                }
                return;
            }
        }

        unsafe {
            let dst = std::slice::from_raw_parts_mut(self.arena.add(offset), size);
            let src = std::slice::from_raw_parts(&state as *const T as *const u8, size);
            dst.copy_from_slice(src);
            let _ = state;
            self.state = (dst.as_mut_ptr(), time);
            self.write = end;
            self.current_writes.push(self.state);
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
                let arena = unsafe { alloc(layout) };
                self.arena = arena;
                self.allocations.push((arena, layout));
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
        let (ptr, _) = self.state;
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
        let (ptr, _) = self.state;
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
        for (ptr, time) in &self.tape {
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
        for (ptr, time) in &self.tape {
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

        for (ptr, time) in &self.tape {
            unsafe {
                let data = ptr::read(*ptr as *mut T);
                out.push((data, *time));
            }
        }

        for (i, layout) in &self.allocations {
            unsafe { dealloc(*i, *layout) };
        }

        self.write = 0;
        self.state = (ptr::null_mut(), 0);
        self.tape.clear();
        self.current_writes.clear();
        self.allocations.clear();
        out
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
        self.state = (ptr::null_mut(), 0);
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
        assert_eq!(mnemosyne.state, (ptr::null_mut(), 0));
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
        assert_eq!(mnemosyne.state.1, time_u32);
        assert_eq!(mnemosyne.current_writes.len(), 1);
        assert_eq!(mnemosyne.write, std::mem::size_of::<u32>()); // write pointer moved

        // Test f32
        let val_f32 = 3.10f32;
        let time_f32 = 200u64;
        mnemosyne.write(val_f32, time_f32);
        assert_eq!(mnemosyne.read_state::<f32>().unwrap(), &val_f32);
        assert_eq!(mnemosyne.state.1, time_f32);
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
        assert_eq!(mnemosyne.state.1, time);
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
        assert_eq!(mnemosyne.state.1, time3);

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
        assert_eq!(mnemosyne.state.1, time);
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
        assert_eq!(mnemosyne.state, (ptr::null_mut(), 0));
        assert!(mnemosyne.tape.is_empty());
        assert!(mnemosyne.current_writes.is_empty());
        assert!(mnemosyne.allocations.is_empty()); // All allocations should be deallocated
        assert!(mnemosyne.arena.is_null()); // Arena pointer should be null
    }
}
