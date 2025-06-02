//! arena-based batch loggers for `bytemuck::Pod` data.
//!
//! This module provides `Journal`, a type-erased logger that uses memory arenas
//! to store timestamped data with minimal allocation overhead. Ideal
//! for performance-critical optimistic applications.
use std::{
    alloc::{alloc, dealloc, Layout},
    collections::BTreeSet,
    ptr::{self, null_mut},
};

use bytemuck::{Pod, Zeroable};

use crate::MesoError;

enum AllocKind {
    Arena { end: u64, counter: usize },
    Solo,
}

impl Default for AllocKind {
    fn default() -> Self {
        Self::Arena {
            end: u64::MAX,
            counter: 0,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct MetaLog {
    size: usize,
    offset: usize,
    align: usize,
    alloc_idx: usize,
}

impl MetaLog {
    fn new(size: usize, align: usize, offset: usize, alloc_idx: usize) -> Self {
        Self {
            size,
            align,
            offset,
            alloc_idx,
        }
    }
}

impl Default for MetaLog {
    fn default() -> Self {
        Self {
            size: 0,
            align: 8,
            offset: 0,
            alloc_idx: usize::MAX,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct LogState {
    state: *mut u8,
    meta: MetaLog,
    time: u64,
}

impl Ord for LogState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for LogState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Default for LogState {
    fn default() -> Self {
        Self {
            state: null_mut(),
            meta: MetaLog::default(),
            time: u64::MAX,
        }
    }
}

struct Allocation {
    kind: AllocKind,
    ptr: *mut u8,
    layout: Layout,
    active: bool,
    start: u64,
}

impl Allocation {
    unsafe fn new(size: usize, align: usize, kind: AllocKind, start: u64) -> Self {
        let layout = Layout::from_size_align(size, align).unwrap();
        let ptr = alloc(layout);
        Self {
            kind,
            ptr,
            layout,
            active: true,
            start,
        }
    }
}

/// `Journal` is a type-erased arena-based batch logger designed for high-performance logging of structured data with minimal allocation overhead.
///
/// This logger uses memory arenas to efficiently store logged data in contiguous memory, reducing allocation overhead and improving cache locality.
/// Data is stored in a type-erased manner, allowing different types to be logged to the same arena while maintaining type safety through the `Pod + Zeroable` trait bounds.
pub struct Journal {
    arena: *mut u8,
    active_id: usize,
    size: usize,
    offset: usize,
    state: LogState,
    current_writes: Vec<LogState>,
    tape: BTreeSet<LogState>,
    allocations: Vec<Allocation>,
}

impl Journal {
    /// Creates a new `Journal` logger with the specified arena size.
    pub fn init(size: usize) -> Self {
        let allocation = unsafe { Allocation::new(size, 8, AllocKind::default(), u64::MAX) };
        let arena = allocation.ptr;
        let allocations = vec![allocation];

        let state = LogState::default();
        let current_writes = Vec::new();
        let tape = BTreeSet::new();

        Self {
            arena,
            active_id: 0,
            size,
            offset: 0,
            state,
            current_writes,
            tape,
            allocations,
        }
    }

    /// Writes a value with an associated timestamp to the logger.
    ///
    /// The value is stored in the current arena with proper alignment. If the arena
    /// doesn't have enough space, it will be flushed and a new arena allocated.
    /// If the value is too large for any arena, it will be allocated separately.
    pub fn write<T: Pod + Zeroable + 'static>(
        &mut self,
        state: T,
        time: u64,
        horizon: Option<u64>,
    ) {
        if let Some(horizon) = horizon {
            self.check_chop_tail(horizon);
        }
        let bytes: &[u8] = bytemuck::bytes_of(&state);
        let size = bytes.len();
        let align = std::mem::align_of_val(&state);
        let mut offset = (self.offset + align - 1) & !(align - 1);
        let mut end = offset + size;

        if size > self.size || self.arena.is_null() {
            let allocation = unsafe { Allocation::new(size, align, AllocKind::Solo, time) };
            let meta = MetaLog::new(size, align, 0, self.allocations.len());
            let log_state = LogState {
                state: allocation.ptr,
                meta,
                time,
            };

            unsafe {
                let dst = std::slice::from_raw_parts_mut(allocation.ptr, size);
                dst.copy_from_slice(bytes);
            }
            let _ = state;

            self.allocations.push(allocation);
            self.state = log_state;
            self.current_writes.push(log_state);
            return;
        } else if end > self.size {
            self.flush(true);
            let current_addr = self.arena as usize;
            let desired_addr = (current_addr + self.offset + align - 1) & !(align - 1);
            offset = desired_addr - current_addr;
            end = offset + size;
        }
        unsafe {
            let ptr = self.arena.add(offset);
            let dst = std::slice::from_raw_parts_mut(ptr, size);
            dst.copy_from_slice(bytes);

            let _ = state;
            let meta = MetaLog::new(size, align, offset, self.active_id);
            self.state = LogState {
                state: ptr,
                meta,
                time,
            }
        }

        self.offset = end;
        self.current_writes.push(self.state);
        let mut update_start = false;
        if let AllocKind::Arena { end, counter } = &mut self.allocations[self.active_id].kind {
            if *counter == 0 {
                update_start = true;
            }
            *counter += 1;
            *end = time;
        }
        if update_start {
            self.allocations[self.active_id].start = time;
        }
    }

    /// Flushes the current arena, moving all current writes to the tape.
    ///
    /// This internal method is called automatically when the arena becomes full
    /// or manually during cleanup operations.
    fn flush(&mut self, allocate_new: bool) {
        self.tape.extend(self.current_writes.drain(..));
        self.offset = 0;

        if allocate_new {
            // Allocate a new arena
            let layout = Layout::from_size_align(self.size, 8).unwrap();
            unsafe {
                let ptr = alloc(layout);

                let allocation = Allocation {
                    ptr,
                    layout,
                    kind: AllocKind::default(),
                    start: u64::MAX,
                    active: true,
                };

                self.arena = ptr;
                self.active_id = self.allocations.len();
                self.allocations.push(allocation);
            }
        } else {
            self.arena = null_mut();
            // Note: active_id remains pointing to the last arena,
            // which is fine since we check arena.is_null() before using it
        }
    }

    /// Reads the most recently written value from the logger.
    ///
    /// Returns a reference to the last value that was written to the logger,
    /// cast to the specified type.
    pub fn read_state<T: Pod + Zeroable + 'static>(&self) -> Result<&T, MesoError> {
        let ptr = self.state.state;
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
        let ptr = self.state.state;
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
        self.tape
            .iter()
            .map(|log_state| unsafe {
                let data = &*(log_state.state as *const T);
                (data, log_state.time)
            })
            .collect()
    }

    /// Reads all flushed entries from the tape as mutable references.
    ///
    /// Similar to `read_tape()`, but returns mutable references that allow
    /// modification of the logged data in place.
    pub fn read_tape_mut<T: Pod + Zeroable + 'static>(&mut self) -> Vec<(&mut T, u64)> {
        self.tape
            .iter()
            .map(|log_state| unsafe {
                let data = &mut *(log_state.state as *mut T);
                (data, log_state.time)
            })
            .collect()
    }

    /// Reads all logged entries in timestamped order as immutable references
    pub fn read_all<T: Pod + Zeroable + 'static>(&self) -> Vec<(&T, u64)> {
        self.tape
            .iter()
            .chain(self.current_writes.iter())
            .map(|log_state| unsafe {
                let data = &*(log_state.state as *const T);
                (data, log_state.time)
            })
            .collect()
    }

    /// Deallocates arenas and removes logs with time stamp before a provided `horizon: usize` time
    fn check_chop_tail(&mut self, horizon: u64) {
        // Step 1: Find and deallocate allocations with end time < horizon
        for alloc in &mut self.allocations {
            if !alloc.active {
                continue;
            }

            let should_dealloc = match &alloc.kind {
                AllocKind::Arena { end, .. } => *end < horizon,
                AllocKind::Solo => alloc.start < horizon,
            };

            if should_dealloc {
                unsafe {
                    dealloc(alloc.ptr, alloc.layout);
                }
                alloc.active = false;
            }
        }

        let split_point = LogState {
            state: null_mut(),
            meta: MetaLog::default(),
            time: horizon,
        };
        self.tape = self.tape.split_off(&split_point);
        self.current_writes.retain(|log| log.time >= horizon);
        if self.state.time < horizon {
            self.state = LogState::default();
        }
    }

    /// Cleans up the logger and returns all logged data by value.
    ///
    /// After calling this method, the logger is reset to its initial state with all
    /// memory deallocated.
    pub fn cleanup<T: Pod + Zeroable + 'static>(&mut self) -> Vec<(T, u64)> {
        self.flush(false);

        let mut out = Vec::new();
        for log_state in &self.tape {
            unsafe {
                let data = ptr::read(log_state.state as *mut T);
                out.push((data, log_state.time));
            }
        }

        for alloc in &mut self.allocations {
            if alloc.active {
                unsafe {
                    dealloc(alloc.ptr, alloc.layout);
                }
                alloc.active = false;
            }
        }

        self.arena = null_mut();
        self.active_id = 0;
        self.offset = 0;
        self.state = LogState::default();
        self.tape.clear();
        self.current_writes.clear();
        self.allocations.clear();

        out
    }

    /// Rolls back the current state to what was active most recently at the provided `time: usize`.
    pub fn rollback(&mut self, time: u64) {
        if time >= self.state.time {
            return; // Nothing to rollback
        }

        // Deallocate all allocations that started after rollback time (this is right, dont fuck with this)
        for alloc in &mut self.allocations {
            if alloc.active && alloc.start > time {
                unsafe {
                    dealloc(alloc.ptr, alloc.layout);
                }
                alloc.active = false;
            }
        }

        // Clean up entries after rollback time
        let split_point = LogState {
            state: null_mut(),
            meta: MetaLog::default(),
            time: time + 1,
        };
        let to_remove = self.tape.split_off(&split_point);
        drop(to_remove);

        let mut writes_to_remove = Vec::new();
        for (i, log) in self.current_writes.iter().enumerate().rev() {
            if log.time > time {
                writes_to_remove.push(i);
                // Decrement counter for this entry's allocation if it's still active
                if self.allocations[log.meta.alloc_idx].active {
                    if let AllocKind::Arena { counter, .. } =
                        &mut self.allocations[log.meta.alloc_idx].kind
                    {
                        *counter = counter.saturating_sub(1);
                    }
                }
            }
        }

        for i in writes_to_remove {
            self.current_writes.remove(i);
        }

        // Find the new current state (last entry with time <= rollback time)
        let new_state = self
            .current_writes
            .iter()
            .rev()
            .find(|log| log.time <= time)
            .or_else(|| {
                self.tape
                    .range(
                        ..=LogState {
                            state: null_mut(),
                            meta: MetaLog::default(),
                            time,
                        },
                    )
                    .next_back()
            })
            .copied();

        match new_state {
            Some(state) => {
                self.state = state;

                // Step 4: Restore arena pointer if needed
                let alloc_idx = state.meta.alloc_idx;
                if self.allocations[alloc_idx].active {
                    // This allocation is still valid
                    self.arena = self.allocations[alloc_idx].ptr;
                    self.active_id = alloc_idx;
                    self.offset = state.meta.offset + state.meta.size;

                    // Update the arena's end time
                    if let AllocKind::Arena { end, .. } = &mut self.allocations[alloc_idx].kind {
                        *end = state.time;
                    }
                } else {
                    // The allocation was deallocated, need fresh arena
                    self.arena = null_mut();
                    self.offset = 0;
                }
            }
            None => {
                // No valid state remains
                self.state = LogState::default();
                self.arena = null_mut();
                self.offset = 0;
            }
        }
    }
}

impl Drop for Journal {
    /// Automatic cleanup when the logger is dropped.
    fn drop(&mut self) {
        for alloc in &mut self.allocations {
            if alloc.active {
                unsafe {
                    dealloc(alloc.ptr, alloc.layout);
                }
                alloc.active = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::{Pod, Zeroable};

    // Test structures with different sizes and alignments
    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(C)]
    struct SmallState {
        x: u32,
        y: f32,
    }

    unsafe impl Pod for SmallState {}
    unsafe impl Zeroable for SmallState {}

    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(C, align(16))] // Force 16-byte alignment
    struct AlignedState {
        data: [f64; 4],
    }

    unsafe impl Pod for AlignedState {}
    unsafe impl Zeroable for AlignedState {}

    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(C)]
    struct SimulationState {
        position: [f32; 3],
        velocity: [f32; 3],
        health: u32,
        mana: u32,
        flags: u64,
    }

    unsafe impl Pod for SimulationState {}
    unsafe impl Zeroable for SimulationState {}

    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(C)]
    struct MassiveState {
        data: [u8; 4096], // Force solo allocation
    }

    unsafe impl Pod for MassiveState {}
    unsafe impl Zeroable for MassiveState {}

    #[test]
    fn test_time_travel_branching() {
        let mut journal = Journal::init(256);

        // Timeline 1: Initial simulation run
        let s1 = SimulationState {
            position: [0.0, 0.0, 0.0],
            velocity: [1.0, 0.0, 0.0],
            health: 100,
            mana: 50,
            flags: 0b0001,
        };
        journal.write(s1, 100, None);

        let s2 = SimulationState {
            position: [1.0, 0.0, 0.0],
            velocity: [1.0, 0.0, 0.0],
            health: 95,
            mana: 45,
            flags: 0b0011,
        };
        journal.write(s2, 200, None);

        let s3 = SimulationState {
            position: [2.0, 0.0, 0.0],
            velocity: [1.0, 0.0, 0.0],
            health: 90,
            mana: 40,
            flags: 0b0111,
        };
        journal.write(s3, 300, None);

        // Player dies
        let s4 = SimulationState {
            position: [3.0, 0.0, 0.0],
            velocity: [0.0, 0.0, 0.0],
            health: 0,
            mana: 0,
            flags: 0b1111,
        };
        journal.write(s4, 400, None);

        // Rollback to before death
        journal.rollback(250);

        // Timeline 2: Different choices
        let s3_alt = SimulationState {
            position: [1.5, 1.0, 0.0],
            velocity: [0.5, 1.0, 0.0],
            health: 95, // Healed!
            mana: 30,
            flags: 0b0101,
        };
        journal.write(s3_alt, 300, None);

        // Verify correct state
        assert_eq!(journal.read_state::<SimulationState>().unwrap(), &s3_alt);

        // Verify only correct timeline exists
        let all_states = journal.read_all::<SimulationState>();
        assert_eq!(all_states.len(), 3); // s1, s2, s3_alt
        assert_eq!(all_states[2].0.health, 95); // Not dead!
    }

    #[test]
    fn test_mixed_types_and_alignments() {
        let mut journal = Journal::init(128); // Small arena to force flushes

        // Mix different types with different alignments
        journal.write(42u32, 100, None);

        let aligned = AlignedState {
            data: [1.0, 2.0, 3.0, 4.0],
        };
        journal.write(aligned, 200, None);

        journal.write(SmallState { x: 10, y: 20.0 }, 300, None);

        // This should trigger a flush
        let aligned2 = AlignedState {
            data: [5.0, 6.0, 7.0, 8.0],
        };
        journal.write(aligned2, 400, None);
        journal.flush(true);
        // Read back with correct types
        let tape = journal.read_tape::<u32>();
        assert_eq!(tape[0].0, &42);

        // Rollback to middle of first arena
        journal.rollback(250);

        // Write should continue in same arena
        journal.write(99u32, 350, None);
        assert_eq!(journal.read_state::<u32>().unwrap(), &99);
    }

    #[test]
    fn test_massive_allocations_and_rollback() {
        let mut journal = Journal::init(256);

        // Normal write
        journal.write(SmallState { x: 1, y: 1.0 }, 100, None);

        // Massive write - forces solo allocation
        let mut massive = MassiveState { data: [0; 4096] };
        massive.data[0] = 42;
        massive.data[4095] = 99;
        journal.write(massive, 200, None);

        // Another normal write - should get new arena
        journal.write(SmallState { x: 2, y: 2.0 }, 300, None);

        // Rollback past massive allocation
        journal.rollback(150);

        // Verify massive allocation was freed
        // Write another massive to same timestamp - should work
        let mut massive2 = MassiveState { data: [0; 4096] };
        massive2.data[0] = 77;
        journal.write(massive2, 200, None);

        assert_eq!(journal.read_state::<MassiveState>().unwrap().data[0], 77);
    }

    #[test]
    fn test_horizon_garbage_collection() {
        let mut journal = Journal::init(128);

        // Create multiple arenas worth of data
        for i in 0..20 {
            let state = SmallState { x: i, y: i as f32 };
            journal.write(state, i as u64 * 100, None);
        }

        // Add with horizon - should trigger cleanup of old data
        let state = SmallState { x: 99, y: 99.0 };
        journal.write(state, 2100, Some(1000));

        // Verify old data is gone
        let all = journal.read_all::<SmallState>();
        assert!(all.iter().all(|(_, t)| *t >= 1000));
        assert!(all.len() < 15); // Should have removed early entries
    }

    #[test]
    fn test_rapid_rollback_cycles() {
        let mut journal = Journal::init(256);

        // Simulate rapid save/load cycles in a game
        let mut states = Vec::new();

        // Create 10 save points
        for i in 0..10 {
            let state = SimulationState {
                position: [i as f32, 0.0, 0.0],
                velocity: [1.0, 0.0, 0.0],
                health: 100 - i * 5,
                mana: 50 + i * 5,
                flags: 1 << i,
            };
            journal.write(state, i as u64 * 100, None);
            states.push(state);
        }

        // Rapidly rollback and branch
        for i in (0..10).rev().step_by(2) {
            journal.rollback(i as u64 * 100);

            // Verify correct state restored
            assert_eq!(journal.read_state::<SimulationState>().unwrap(), &states[i]);

            // Branch off with new state
            let mut new_state = states[i];
            new_state.velocity = [0.0, 1.0, 0.0]; // Change direction
            journal.write(new_state, (i as u64 + 1) * 100 - 50, None);
        }
    }

    #[test]
    fn test_cleanup_returns_correct_data() {
        let mut journal = Journal::init(256);

        // Write mixed types
        journal.write(10u32, 100, None);
        journal.write(20u32, 200, None);
        journal.write(30u32, 300, None);

        // Flush to tape
        journal.flush(true);

        journal.write(40u32, 400, None);
        journal.write(50u32, 500, None);

        // Cleanup
        let data = journal.cleanup::<u32>();

        assert_eq!(data.len(), 5);
        assert_eq!(data[0], (10, 100));
        assert_eq!(data[4], (50, 500));

        // Journal should be reset
        assert!(journal.read_state::<u32>().is_err());
        assert!(journal.read_all::<u32>().is_empty());
    }

    #[test]
    fn test_empty_rollback() {
        let mut journal = Journal::init(256);

        journal.write(42u32, 1000, None);
        journal.write(43u32, 2000, None);

        // Rollback to before any writes
        journal.rollback(500);

        assert!(journal.read_state::<u32>().is_err());
        assert!(journal.read_all::<u32>().is_empty());

        // Should be able to write again
        journal.write(99u32, 3000, None);
        assert_eq!(journal.read_state::<u32>().unwrap(), &99);
    }

    #[test]
    fn test_alignment_edge_cases() {
        let mut journal = Journal::init(64); // Very small arena

        // Write u8 (align 1)
        journal.write(1u8, 100, None);

        // Write u32 (align 4) - should align properly
        journal.write(1000u32, 200, None);

        // Write AlignedState (align 16) - might trigger flush
        let aligned = AlignedState {
            data: [1.0, 2.0, 3.0, 4.0],
        };
        journal.write(aligned, 300, None);

        // Verify all can be read back
        assert_eq!(journal.read_all::<u8>()[0].0, &1);

        // Note: Reading with wrong type is UB in real code!
        // This is just for testing memory layout
    }

    #[test]
    fn test_btreeset_ordering_maintained() {
        let mut journal = Journal::init(256);

        // Write out of order (simulating time corrections)
        journal.write(1u32, 300, None);
        journal.write(2u32, 100, None);
        journal.write(3u32, 200, None);

        journal.flush(false);

        // Tape should be ordered
        let tape = journal.read_tape::<u32>();
        assert_eq!(tape[0], (&2, 100));
        assert_eq!(tape[1], (&3, 200));
        assert_eq!(tape[2], (&1, 300));
    }

    #[test]
    fn test_rollback_with_same_timestamp() {
        let mut journal = Journal::init(256);

        // Multiple writes at same timestamp (frame-based simulation)
        let s1 = SmallState { x: 1, y: 1.0 };
        let s2 = SmallState { x: 2, y: 2.0 };
        let s3 = SmallState { x: 3, y: 3.0 };

        journal.write(s1, 1000, None);
        journal.write(s2, 1000, None);
        journal.write(s3, 1000, None);

        journal.write(SmallState { x: 4, y: 4.0 }, 2000, None);

        // Rollback to timestamp with multiple entries
        journal.rollback(1000);

        // Should keep all entries at time 1000
        assert_eq!(journal.read_state::<SmallState>().unwrap(), &s3);
        assert_eq!(journal.read_all::<SmallState>().len(), 3);
    }

    #[test]
    fn test_arena_counter_accuracy() {
        let mut journal = Journal::init(128);

        // Fill an arena
        for i in 0..5 {
            journal.write(i as u32, i as u64 * 100, None);
        }

        // Check counter through rollbacks
        journal.rollback(250); // Should have 3 entries

        // Add more
        journal.write(99u32, 300, None);
        journal.write(98u32, 400, None);

        // The arena should still be active with correct count
        journal.rollback(150); // Back to 2 entries

        // Add one more and verify arena reuse
        journal.write(97u32, 250, None);
        assert_eq!(journal.read_state::<u32>().unwrap(), &97);
    }

    #[test]
    fn test_pathological_allocation_pattern() {
        let mut journal = Journal::init(100); // Awkward size

        // Alternate between sizes that barely fit and barely don't
        for i in 0..10 {
            if i % 2 == 0 {
                // Write 24 bytes (just fits with alignment)
                let state = [i as u64; 3];
                journal.write(state, i as u64 * 100, None);
            } else {
                // Write 32 bytes (triggers flush)
                let state = [i as u64; 4];
                journal.write(state, i as u64 * 100, None);
            }
        }

        // Rollback to middle
        journal.rollback(450);

        // Continue pattern
        for i in 5..8 {
            let state = [i as u64; 3];
            journal.write(state, i as u64 * 100, None);
        }

        // Verify no corruption
        let all = journal.read_all::<[u64; 3]>();
        assert!(all.len() >= 3);
    }

    #[test]
    fn test_deterministic_replay() {
        // Simulate deterministic replay system
        let mut journal = Journal::init(512);

        // Record initial game state
        let rng_seed = 0x12345678u64;
        journal.write(rng_seed, 0, None);

        // Simulate some frames
        let mut rng = rng_seed;
        for frame in 1..=100 {
            // Simple LCG for deterministic random
            rng = rng.wrapping_mul(1664525).wrapping_add(1013904223);

            let state = SimulationState {
                position: [
                    (rng & 0xFF) as f32,
                    ((rng >> 8) & 0xFF) as f32,
                    ((rng >> 16) & 0xFF) as f32,
                ],
                velocity: [1.0, 0.0, 0.0],
                health: 100,
                mana: 50,
                flags: rng >> 32,
            };

            journal.write(state, frame * 16, None); // 60fps = ~16ms per frame
        }

        // Rollback to frame 50
        journal.rollback(50 * 16);

        // Replay should be deterministic
        let checkpoint = *journal.read_state::<SimulationState>().unwrap();

        // Continue from checkpoint with same logic
        rng = 0x12345678u64;
        for _ in 1..=50 {
            rng = rng.wrapping_mul(1664525).wrapping_add(1013904223);
        }

        // State should match
        let expected_pos_x = (rng & 0xFF) as f32;
        assert_eq!(checkpoint.position[0], expected_pos_x);
    }

    #[test]
    fn test_network_rollback_scenario() {
        // Simulate network game with rollback
        let mut journal = Journal::init(256);

        // Local prediction
        for i in 0..10 {
            let state = SmallState { x: i, y: i as f32 };
            journal.write(state, i as u64 * 100, None);
        }

        // Server authoritative update arrives for time 500
        journal.rollback(500);
        let server_state = SmallState { x: 5, y: 5.5 }; // Slightly different!
        journal.write(server_state, 500, None);

        // Re-simulate forward with correction
        for i in 6..10 {
            let state = SmallState {
                x: i,
                y: i as f32 + 0.5,
            };
            journal.write(state, i as u64 * 100, None);
        }

        // Verify correction propagated
        assert_eq!(journal.read_state::<SmallState>().unwrap().y, 9.5);
    }

    #[test]
    fn test_arena_boundary_edge_case() {
        // Test writing exactly at arena boundaries
        let arena_size = 64;
        let mut journal = Journal::init(arena_size);

        // Write exactly arena_size - 8 bytes (accounting for any initial alignment)
        let data1 = [0u8; 64];
        journal.write(data1, 100, None);

        // This should exactly fill the arena
        let data2 = 0u64;
        journal.write(data2, 200, None);

        // This should trigger new arena
        let data3 = 0u8;
        journal.write(data3, 300, None);

        // Rollback to boundary
        journal.rollback(200);

        // Write should start new arena, not corrupt
        let data4 = 0u64;
        journal.write(data4, 250, None);

        assert_eq!(journal.read_all::<u64>().len(), 3);
    }

    #[test]
    fn test_multi_timeline_merge_scenario() {
        // Simulate parallel timeline exploration
        let mut journal = Journal::init(512);

        // Base timeline
        for i in 0..5 {
            journal.write(i as u32, i as u64 * 100, None);
        }

        // Branch point
        let branch_point = 200;

        // Timeline A
        journal.rollback(branch_point);
        journal.write(100u32, 300, None);
        journal.write(101u32, 400, None);

        // Timeline B
        journal.rollback(branch_point);
        journal.write(200u32, 300, None);
        journal.write(201u32, 400, None);

        // Timeline C
        journal.rollback(branch_point);
        journal.write(300u32, 300, None);
        journal.write(301u32, 400, None);

        // Each rollback should properly clean up the abandoned timeline
        let all = journal.read_all::<u32>();
        assert_eq!(all.len(), 5); // 0,1,2,300,301
        assert_eq!(all[3].0, &300);
        assert_eq!(all[4].0, &301);
    }

    #[test]
    fn stress_test_massive_simulation() {
        // Simulate a complex game with many objects
        let mut journal = Journal::init(4096);

        #[derive(Copy, Clone, Debug, PartialEq)]
        #[repr(C)]
        struct Entity {
            id: u64,
            transform: [f32; 16], // 4x4 matrix
            physics: [f32; 6],    // pos + vel
            state: u32,
        }

        unsafe impl Pod for Entity {}
        unsafe impl Zeroable for Entity {}

        // Simulate 1000 frames with 100 entities each
        for frame in 0..100 {
            for entity_id in 0..10 {
                let entity = Entity {
                    id: entity_id,
                    transform: [frame as f32; 16],
                    physics: [entity_id as f32; 6],
                    state: frame * 100 + entity_id as u32,
                };
                journal.write(entity, frame as u64 * 16, None);
            }

            // Every 10 frames, rollback 5 frames (lag compensation)
            if frame % 10 == 0 && frame > 5 {
                journal.rollback((frame - 5) as u64 * 16);

                // Rewrite with corrections
                for entity_id in 0..10 {
                    let entity = Entity {
                        id: entity_id,
                        transform: [(frame - 5) as f32 + 0.5; 16],
                        physics: [entity_id as f32 + 0.5; 6],
                        state: (frame - 5) * 100 + entity_id as u32,
                    };
                    journal.write(entity, (frame - 5) as u64 * 16, None);
                }
            }
        }

        // Should still be able to read everything
        let final_state = journal.read_state::<Entity>().unwrap();
        assert!(final_state.id < 10);

        // Cleanup should work
        let all_data = journal.cleanup::<Entity>();
        assert!(!all_data.is_empty());
    }

    #[test]
    fn test_extreme_time_values() {
        let mut journal = Journal::init(256);

        // Test with extreme timestamps
        journal.write(1u32, 0, None);
        journal.write(2u32, u64::MAX / 2, None);
        journal.write(3u32, u64::MAX - 1, None);

        // Rollback to middle of huge gap
        journal.rollback(1000000);

        assert_eq!(journal.read_state::<u32>().unwrap(), &1);

        // Write in the gap
        journal.write(4u32, 999999, None);

        let all = journal.read_all::<u32>();
        assert_eq!(all.len(), 2);
        assert_eq!(all[1].0, &4);
    }

    #[test]
    fn test_rollback_exactly_on_state() {
        let mut journal = Journal::init(256);

        journal.write(1u32, 100, None);
        journal.write(2u32, 200, None);
        journal.write(3u32, 300, None);

        // Rollback exactly to a state time
        journal.rollback(200);

        assert_eq!(journal.read_state::<u32>().unwrap(), &2);
        let all = journal.read_all::<u32>();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_type_punning_safety() {
        // This is evil but tests memory safety
        let mut journal = Journal::init(256);

        #[derive(Copy, Clone)]
        #[repr(C)]
        struct A {
            x: u32,
            y: u32,
        }

        unsafe impl Pod for A {}
        unsafe impl Zeroable for A {}

        #[derive(Copy, Clone)]
        #[repr(C)]
        struct B {
            val: u64,
        }

        unsafe impl Pod for B {}
        unsafe impl Zeroable for B {}

        // Both are 8 bytes, same alignment
        let a = A {
            x: 0xDEADBEEF,
            y: 0xCAFEBABE,
        };
        journal.write(a, 100, None);

        // This is UB in real code! But tests our memory handling
        unsafe {
            let b_ref = std::mem::transmute::<&A, &B>(journal.read_state::<A>().unwrap());
            assert_eq!(b_ref.val, 0xCAFEBABE_DEADBEEF_u64.to_le());
        }
    }

    #[test]
    fn test_repeated_rollback_to_same_time() {
        let mut journal = Journal::init(256);

        for i in 0..10 {
            journal.write(i as u32, i as u64 * 100, None);
        }

        // Rollback to same time multiple times
        for _ in 0..5 {
            journal.rollback(500);
            assert_eq!(journal.read_state::<u32>().unwrap(), &5);
        }

        // Should still work correctly
        journal.write(999u32, 600, None);
        assert_eq!(journal.read_all::<u32>().len(), 7);
    }

    #[test]
    fn test_allocation_flag_corruption_resistance() {
        let mut journal = Journal::init(128);

        // Create scenario where allocation flags might get confused
        journal.write(1u32, 100, None);
        journal.write(2u32, 200, None);

        // Force flush
        journal.write([0u8; 128], 300, None);

        // Write more in new arena
        journal.write(3u32, 400, None);

        // Rollback to first arena
        journal.rollback(150);

        // This write should reuse first arena, not corrupt second
        journal.write(4u32, 250, None);

        // Roll forward - second arena should still be valid
        journal.write(5u32, 500, None);

        let all = journal.read_all::<u32>();
        assert!(all.iter().any(|(x, _)| **x == 4));
        assert!(all.iter().any(|(x, _)| **x == 5));
    }

    #[test]
    fn test_btreeset_duplicate_handling() {
        let mut journal = Journal::init(256);

        // Write multiple states
        journal.write(1u32, 100, None);
        journal.write(2u32, 200, None);

        // Flush to move to tape
        journal.flush(true);

        // Rollback and rewrite same timestamps
        journal.rollback(50);
        journal.write(10u32, 100, None);
        journal.write(20u32, 200, None);
        journal.flush(false);
        // BTreeSet should handle this correctly
        let tape = journal.read_tape::<u32>();
        assert_eq!(tape.len(), 2);
        assert_eq!(tape[0].0, &10); // New values, not old
        assert_eq!(tape[1].0, &20);
    }
}
