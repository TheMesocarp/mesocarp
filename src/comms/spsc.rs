//! Lock-free circular buffer for single-producer, single-consumer scenarios.
//!
//! This module provides `BufferWheel`, a fixed-size circular buffer that enables
//! lock-free communication between one producer and one consumer thread using
//! atomic operations. Ideal for high-performance message passing and streaming
//! data processing.

use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};

use crate::MesoError;

/// A lock-free circular buffer designed for single-producer, single-consumer (SPSC) scenarios.
#[derive(Debug)]
pub struct BufferWheel<const N: usize, T> {
    /// Array of atomic pointers to stored data. Each slot can hold one item.
    buffers: [AtomicPtr<T>; N],
    /// Current read position in the circular buffer
    read: AtomicUsize,
    /// Current write position in the circular buffer
    write: AtomicUsize,
    /// Flag indicating whether the buffer is completely full
    full: AtomicBool,
}

impl<const N: usize, T> Default for BufferWheel<N, T> {
    /// Creates a new empty `BufferWheel` with default initialization.
    fn default() -> Self {
        BufferWheel::new()
    }
}

impl<const N: usize, T> BufferWheel<N, T> {
    /// Creates a new empty `BufferWheel`.
    ///
    /// Initializes all buffer slots to null pointers and sets read/write positions to 0.
    /// The buffer starts in an empty state.
    pub fn new() -> Self {
        let buffers = array_init::array_init(|_| AtomicPtr::new(ptr::null_mut()));
        Self {
            buffers,
            read: AtomicUsize::new(0),
            write: AtomicUsize::new(0),
            full: AtomicBool::new(false),
        }
    }

    /// Writes data to the buffer.
    ///
    /// Attempts to write the provided data to the next available slot in the circular buffer.
    /// The data is moved into the buffer (not copied). If the buffer is full, returns an error
    /// without consuming the data.
    pub fn write(&self, data: T) -> Result<(), MesoError> {
        let write = self.write.load(Relaxed);
        if self.full.load(Acquire) {
            return Err(MesoError::BuffersFull);
        }
        let new_ptr = Box::into_raw(Box::new(data));
        let old_ptr = self.buffers[write].swap(new_ptr, AcqRel);
        if !old_ptr.is_null() {
            unsafe {
                drop(Box::from_raw(old_ptr));
            }
        }
        let next = (write + 1) % N;
        self.write.store(next, Release);

        if next == self.read.load(Acquire) {
            self.full.store(true, Release);
        }
        Ok(())
    }

    /// Reads data from the buffer.
    ///
    /// Attempts to read the next available data from the circular buffer. The data is moved
    /// out of the buffer (not copied) and returned to the caller. If the buffer is empty,
    /// returns an error.
    pub fn read(&self) -> Result<T, MesoError> {
        let read = self.read.load(Relaxed);
        if read == self.write.load(Acquire) && !self.full.load(Acquire) {
            return Err(MesoError::NoPendingUpdates);
        }
        let null = ptr::null_mut();
        let old_ptr = self.buffers[read].swap(null, AcqRel);
        if old_ptr.is_null() {
            return Err(MesoError::ExpectedUpdate);
        }
        let edge = unsafe { Box::from_raw(old_ptr) };
        let info = *edge;

        let next = (read + 1) % N;
        self.read.store(next, Release);

        self.full.store(false, Release);
        Ok(info)
    }
}

unsafe impl<const N: usize, T: Clone> Send for BufferWheel<N, T> {}
unsafe impl<const N: usize, T: Clone> Sync for BufferWheel<N, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn sequential_write_read() {
        let buf = BufferWheel::<3, i32>::default();

        // reading from an empty buffer should complain
        assert_eq!(buf.read().unwrap_err(), MesoError::NoPendingUpdates);

        // write two items
        buf.write(42).expect("first write okay");
        buf.write(1337).expect("second write okay");

        // read them back in order
        assert_eq!(buf.read().unwrap(), 42);
        assert_eq!(buf.read().unwrap(), 1337);

        // and now it's empty again
        assert_eq!(buf.read().unwrap_err(), MesoError::NoPendingUpdates);
    }

    #[test]
    fn capacity_full_and_recover() {
        // capacity 2 -> on the 2nd write 'full' flips on,
        // 3rd write errors until we read something.
        let buf = BufferWheel::<2, u8>::default();

        assert!(buf.write(10).is_ok());
        assert!(buf.write(20).is_ok());

        // buffer is full now
        let e = buf.write(30).unwrap_err();
        assert_eq!(e, MesoError::BuffersFull);

        // read one slot, which should clear `full`
        assert_eq!(buf.read().unwrap(), 10);

        // now we can write again
        buf.write(30).expect("recovered after read");

        // drain the rest
        assert_eq!(buf.read().unwrap(), 20);
        assert_eq!(buf.read().unwrap(), 30);

        // finally empty
        assert_eq!(buf.read().unwrap_err(), MesoError::NoPendingUpdates);
    }

    #[test]
    fn spsc_concurrent_spinning() {
        // tiny buffer of size 1; writer must spin until reader catches up
        let buf = Arc::new(BufferWheel::<1, usize>::default());
        let prod = Arc::clone(&buf);
        let cons = Arc::clone(&buf);

        let writer = thread::spawn(move || {
            for i in 0..100 {
                // spin on full
                loop {
                    match prod.write(i) {
                        Ok(_) => break,
                        Err(MesoError::BuffersFull) => continue,
                        Err(e) => panic!("unexpected write error: {e:?}"),
                    }
                }
            }
        });

        let reader = thread::spawn(move || {
            for expected in 0..100 {
                // spin on empty
                loop {
                    match cons.read() {
                        Ok(v) => {
                            assert_eq!(v, expected);
                            break;
                        }
                        Err(MesoError::NoPendingUpdates) => continue,
                        Err(e) => panic!("unexpected read error: {e:?}"),
                    }
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    // #[test]
    // fn spsc_concurrent_large_buffer() {
    //     const BUFFER_SIZE: usize = 1024;
    //     const NUM_MESSAGES: usize = 100_000;

    //     let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
    //     let prod = Arc::clone(&buf);
    //     let cons = Arc::clone(&buf);

    //     let writer = thread::spawn(move || {
    //         for i in 0..NUM_MESSAGES {
    //             loop {
    //                 match prod.write(i) {
    //                     Ok(_) => break,
    //                     Err(MesoError::BuffersFull) => thread::yield_now(), // Yield if full
    //                     Err(e) => panic!("unexpected write error: {e:?}"),
    //                 }
    //             }
    //         }
    //     });

    //     let reader = thread::spawn(move || {
    //         for expected in 0..NUM_MESSAGES {
    //             loop {
    //                 match cons.read() {
    //                     Ok(v) => {
    //                         assert_eq!(
    //                             v, expected,
    //                             "Data integrity check failed at message {expected}"
    //                         );
    //                         break;
    //                     }
    //                     Err(MesoError::NoPendingUpdates) => thread::yield_now(), // Yield if empty
    //                     Err(e) => panic!("unexpected read error: {e:?}"),
    //                 }
    //             }
    //         }
    //     });

    //     writer.join().unwrap();
    //     reader.join().unwrap();
    // }

    // #[test]
    // fn spsc_concurrent_alternating_write_read() {
    //     const BUFFER_SIZE: usize = 4;
    //     const ITERATIONS: usize = 1000;

    //     let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
    //     let prod = Arc::clone(&buf);
    //     let cons = Arc::clone(&buf);

    //     let writer = thread::spawn(move || {
    //         for i in 0..ITERATIONS {
    //             loop {
    //                 if prod.write(i).is_ok() {
    //                     break;
    //                 }
    //                 thread::yield_now();
    //             }
    //             // lil randomness
    //             if i % 50 == 0 {
    //                 thread::sleep(Duration::from_nanos(1));
    //             }
    //         }
    //     });

    //     let reader = thread::spawn(move || {
    //         for expected in 0..ITERATIONS {
    //             loop {
    //                 match cons.read() {
    //                     Ok(v) => {
    //                         assert_eq!(
    //                             v, expected,
    //                             "Alternating check: expected {expected}, got {v}"
    //                         );
    //                         break;
    //                     }
    //                     Err(MesoError::NoPendingUpdates) => thread::yield_now(),
    //                     Err(e) => panic!("unexpected read error: {e:?}"),
    //                 }
    //             }
    //             // lil randomness
    //             if expected % 75 == 0 {
    //                 thread::sleep(Duration::from_nanos(1));
    //             }
    //         }
    //     });

    //     writer.join().unwrap();
    //     reader.join().unwrap();
    // }

    // #[test]
    // fn spsc_concurrent_producer_faster() {
    //     const BUFFER_SIZE: usize = 4;
    //     const NUM_MESSAGES: usize = 1000;

    //     let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
    //     let prod = Arc::clone(&buf);
    //     let cons = Arc::clone(&buf);

    //     let writer = thread::spawn(move || {
    //         for i in 0..NUM_MESSAGES {
    //             loop {
    //                 match prod.write(i) {
    //                     Ok(_) => break,
    //                     Err(MesoError::BuffersFull) => thread::yield_now(),
    //                     Err(e) => panic!("unexpected write error: {e:?}"),
    //                 }
    //             }
    //         }
    //     });

    //     let reader = thread::spawn(move || {
    //         // Give producer a head start
    //         thread::sleep(Duration::from_millis(10));
    //         for expected in 0..NUM_MESSAGES {
    //             loop {
    //                 match cons.read() {
    //                     Ok(v) => {
    //                         assert_eq!(
    //                             v, expected,
    //                             "Producer faster: expected {expected}, got {v}"
    //                         );
    //                         break;
    //                     }
    //                     Err(MesoError::NoPendingUpdates) => thread::yield_now(),
    //                     Err(e) => panic!("unexpected read error: {e:?}"),
    //                 }
    //             }
    //         }
    //     });

    //     writer.join().unwrap();
    //     reader.join().unwrap();
    // }

    // #[test]
    // fn spsc_concurrent_consumer_faster() {
    //     const BUFFER_SIZE: usize = 4;
    //     const NUM_MESSAGES: usize = 1000;

    //     let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
    //     let prod = Arc::clone(&buf);
    //     let cons = Arc::clone(&buf);

    //     let writer = thread::spawn(move || {
    //         // Give consumer a head start
    //         thread::sleep(Duration::from_millis(10));
    //         for i in 0..NUM_MESSAGES {
    //             loop {
    //                 match prod.write(i) {
    //                     Ok(_) => break,
    //                     Err(MesoError::BuffersFull) => thread::yield_now(),
    //                     Err(e) => panic!("unexpected write error: {e:?}"),
    //                 }
    //             }
    //         }
    //     });

    //     let reader = thread::spawn(move || {
    //         for expected in 0..NUM_MESSAGES {
    //             loop {
    //                 match cons.read() {
    //                     Ok(v) => {
    //                         assert_eq!(
    //                             v, expected,
    //                             "Consumer faster: expected {expected}, got {v}"
    //                         );
    //                         break;
    //                     }
    //                     Err(MesoError::NoPendingUpdates) => thread::yield_now(),
    //                     Err(e) => panic!("unexpected read error: {e:?}"),
    //                 }
    //             }
    //         }
    //     });

    //     writer.join().unwrap();
    //     reader.join().unwrap();
    // }
}
