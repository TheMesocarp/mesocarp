//! Hierarchical timing wheel for efficient event scheduling and time-based processing.
//!
//! This module provides a `Clock` data structure that implements a hierarchical timing wheel,
//! with `SLOTS` ticks, and `HEIGHT` hands. This enabling O(1) scheduling and processing
//! of time-ordered events, leveraging a bucket sort to minimize the necessary instructions.
//! This design supports multiple time granularities through a hierarchy of wheels, making it
//! suitable for high-performance time-based systems.
use std::{cmp::Reverse, collections::BinaryHeap};

use crate::MesoError;

use super::Scheduleable;
#[derive(Debug)]
/// Hierarchical Timing Wheel (HTW) for scheduling events and messages.
pub struct Clock<T: Scheduleable, const SLOTS: usize, const HEIGHT: usize> {
    pub wheels: [[Vec<T>; SLOTS]; HEIGHT], //timing wheels, with sizes {SLOTS, SLOTS^2, ..., SLOTS^HEIGHT}
    pub current_idxs: [usize; HEIGHT],     // wheel parsing offset index
    pub time: u64,
}

impl<T: Scheduleable, const SLOTS: usize, const HEIGHT: usize> Clock<T, SLOTS, HEIGHT> {
    /// New HTW from time-step size and terminal time.
    pub fn new() -> Result<Self, MesoError> {
        if HEIGHT < 1 {
            return Err(MesoError::NoClockSlots);
        }
        let wheels = std::array::from_fn(|_| std::array::from_fn(|_| Vec::new()));
        let current = [0_usize; HEIGHT];
        Ok(Clock {
            wheels,
            time: 0,
            current_idxs: current,
        })
    }
    /// Fix clock start to a specific timestamp
    pub fn set_time(&mut self, time: u64) {
        self.time = time
    }
    /// Find corresponding slot based on `Scheduleable::time()' output, and insert.
    pub fn insert(&mut self, event: T) -> Result<(), T> {
        let time = event.time();
        let deltaidx = (time - self.time) as usize;

        for k in 0..HEIGHT {
            let startidx = ((SLOTS).pow(1 + k as u32) - SLOTS) / (SLOTS - 1); // start index for each level
            let endidx = ((SLOTS).pow(2 + k as u32) - SLOTS) / (SLOTS - 1) - 1; // end index for each level
            if deltaidx >= startidx {
                if deltaidx >= (((SLOTS).pow(1 + HEIGHT as u32) - SLOTS) / (SLOTS - 1)) {
                    return Err(event);
                }
                if deltaidx > endidx {
                    continue;
                }
                let offset =
                    ((deltaidx - startidx) / (SLOTS.pow(k as u32)) + self.current_idxs[k]) % SLOTS; // slot based on the current offset index for level k.
                self.wheels[k][offset].push(event);
                return Ok(());
            }
        }
        Err(event)
    }
    /// consume the next step's pending events.
    pub fn tick(&mut self) -> Result<Vec<T>, MesoError> {
        let row: &mut [Vec<T>] = &mut self.wheels[0];
        let events = std::mem::take(&mut row[self.current_idxs[0]]);
        if !events.is_empty() && events[0].time() < self.time {
            println!("Time travel detected");
            return Err(MesoError::TimeTravel);
        }
        if events.is_empty() {
            return Err(MesoError::NoItems);
        }
        Ok(events)
    }
    /// roll clock forward one tick, and rotate higher wheels if necessary.
    pub fn increment(&mut self, overflow: &mut BinaryHeap<Reverse<T>>) {
        self.current_idxs[0] = (self.current_idxs[0] + 1) % SLOTS;
        self.time += 1;
        if self.current_idxs[0] as u64 == 0 {
            self.rotate(overflow);
        }
    }
    /// Rotate the timing wheel, moving events from the k-th wheel to fill the (k-1)-th wheel.
    pub fn rotate(&mut self, overflow: &mut BinaryHeap<Reverse<T>>) {
        for k in 1..HEIGHT {
            let wheel_period = SLOTS.pow(k as u32);
            if self.time % (wheel_period as u64) == 0 {
                if HEIGHT == k {
                    for _ in 0..SLOTS.pow(HEIGHT as u32 - 1) {
                        overflow.pop().map(|event| self.insert(event.0));
                    }
                    return;
                }
                let row = &mut self.wheels[k];
                let higher_events = std::mem::take(&mut row[self.current_idxs[k]]);
                self.current_idxs[k] = (self.current_idxs[k] + 1) % SLOTS;
                for event in higher_events {
                    let _ = self.insert(event).map_err(|event| {
                        overflow.push(Reverse(event));
                    });
                }
            }
        }
    }

    /// Rollback the clock. (this is done via a brute force dump and re-sort,
    /// which is certainly not the most efficient. however, the alternative via indexing,
    /// similar to the insertion and rotation methods, is much more complicated to do right.
    /// will certainly come back to it).
    pub fn rollback(&mut self, overflow: &mut BinaryHeap<Reverse<T>>, new_time: u64) {
        if new_time >= self.time {
            return;
        }

        let all_events = self
            .wheels
            .iter_mut()
            .flat_map(|wheel| wheel.iter_mut().flat_map(std::mem::take))
            .collect::<Vec<T>>();

        self.time = new_time;
        self.current_idxs = [0; HEIGHT];

        for event in all_events {
            if let Err(e) = self.insert(event) {
                overflow.push(Reverse(e));
            }
        }
    }
}

/// A simple one-level timing wheel
pub type TimingWheel<T, const SLOTS: usize> = Clock<T, SLOTS, 1>;

unsafe impl<T: Scheduleable, const SLOTS: usize, const HEIGHT: usize> Send
    for Clock<T, SLOTS, HEIGHT>
{
}
unsafe impl<T: Scheduleable, const SLOTS: usize, const HEIGHT: usize> Sync
    for Clock<T, SLOTS, HEIGHT>
{
}
