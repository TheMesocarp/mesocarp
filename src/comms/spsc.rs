//! Lock-free circular buffer for single-producer, single-consumer scenarios.
//!
//! This module provides `BufferWheel`, a fixed-size circular buffer that enables
//! lock-free communication between one producer and one consumer thread using
//! atomic operations. Ideal for high-performance message passing and streaming
//! data processing.

use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicUsize};

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
        }
    }

    /// Writes data to the buffer.
    ///
    /// Attempts to write the provided data to the next available slot in the circular buffer.
    /// The data is moved into the buffer (not copied). If the buffer is full, returns an error
    /// without consuming the data.
    pub fn write(&self, data: T) -> Result<(), MesoError> {
        let write = self.write.load(Relaxed);
        let next_write = (write + 1) % N;

        // The buffer is full if the next write position would be the same as the read position.
        if next_write == self.read.load(Acquire) {
            return Err(MesoError::BuffersFull);
        }

        // The old pointer should be null, as we never overwrite unread data.
        let new_ptr = Box::into_raw(Box::new(data));
        let old_ptr = self.buffers[write].swap(new_ptr, Release);
        assert!(
            old_ptr.is_null(),
            "BufferWheel write overwrote unread data, indicates a bug."
        );

        // Make the new data available by updating the write index.
        self.write.store(next_write, Release);

        Ok(())
    }

    /// Reads data from the buffer.
    ///
    /// This will fail if the buffer is empty.
    pub fn read(&self) -> Result<T, MesoError> {
        let read = self.read.load(Relaxed);

        // The buffer is empty if the read and write positions are the same.
        if read == self.write.load(Acquire) {
            return Err(MesoError::NoPendingUpdates);
        }

        // Take the pointer from the buffer, replacing it with null.
        let null = ptr::null_mut();
        let data_ptr = self.buffers[read].swap(null, Acquire);

        // This should never happen in a correct SPSC scenario.
        if data_ptr.is_null() {
            // This indicates the producer hasn't finished writing the data before updating the write index,
            // which points to a memory ordering bug. With correct Release/Acquire, this is a safeguard.
            return Err(MesoError::ExpectedUpdate);
        }

        let data = unsafe { *Box::from_raw(data_ptr) };

        // Make the slot available for writing by updating the read index.
        let next_read = (read + 1) % N;
        self.read.store(next_read, Release);

        Ok(data)
    }
}

unsafe impl<const N: usize, T: Clone> Send for BufferWheel<N, T> {}
unsafe impl<const N: usize, T: Clone> Sync for BufferWheel<N, T> {}

#[cfg(test)]
mod spsc_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    //use std::time::Duration;

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
        let buf = BufferWheel::<3, u8>::default();

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
        let buf = Arc::new(BufferWheel::<2, usize>::default());
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

    #[test]
    fn spsc_concurrent_large_buffer() {
        const BUFFER_SIZE: usize = 1024;
        const NUM_MESSAGES: usize = 100_000;

        let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
        let prod = Arc::clone(&buf);
        let cons = Arc::clone(&buf);

        let writer = thread::spawn(move || {
            for i in 0..NUM_MESSAGES {
                loop {
                    match prod.write(i) {
                        Ok(_) => break,
                        Err(MesoError::BuffersFull) => {
                            //println!("Buffers full");
                            thread::sleep(Duration::from_nanos(2))
                        } // Yield if full
                        Err(e) => panic!("unexpected write error: {e:?}"),
                    }
                }
            }
        });

        let reader = thread::spawn(move || {
            for expected in 0..NUM_MESSAGES {
                loop {
                    match cons.read() {
                        Ok(v) => {
                            //println!("managed a read");
                            assert_eq!(
                                v, expected,
                                "Data integrity check failed at message {expected}"
                            );
                            break;
                        }
                        Err(MesoError::NoPendingUpdates) => {
                            //println!("Buffers empty");
                            thread::sleep(Duration::from_nanos(2))
                        } // Yield if empty
                        Err(e) => panic!("unexpected read error: {e:?}"),
                    }
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn spsc_concurrent_alternating_write_read() {
        const BUFFER_SIZE: usize = 4;
        const ITERATIONS: usize = 1000;

        let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
        let prod = Arc::clone(&buf);
        let cons = Arc::clone(&buf);

        let writer = thread::spawn(move || {
            for i in 0..ITERATIONS {
                loop {
                    if prod.write(i).is_ok() {
                        break;
                    }
                    thread::yield_now();
                }
                // lil randomness
                if i % 50 == 0 {
                    thread::sleep(Duration::from_nanos(1));
                }
            }
        });

        let reader = thread::spawn(move || {
            for expected in 0..ITERATIONS {
                loop {
                    match cons.read() {
                        Ok(v) => {
                            assert_eq!(
                                v, expected,
                                "Alternating check: expected {expected}, got {v}"
                            );
                            break;
                        }
                        Err(MesoError::NoPendingUpdates) => thread::yield_now(),
                        Err(e) => panic!("unexpected read error: {e:?}"),
                    }
                }
                // lil randomness
                if expected % 75 == 0 {
                    thread::sleep(Duration::from_nanos(1));
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn spsc_concurrent_producer_faster() {
        const BUFFER_SIZE: usize = 4;
        const NUM_MESSAGES: usize = 1000;

        let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
        let prod = Arc::clone(&buf);
        let cons = Arc::clone(&buf);

        let writer = thread::spawn(move || {
            for i in 0..NUM_MESSAGES {
                loop {
                    match prod.write(i) {
                        Ok(_) => break,
                        Err(MesoError::BuffersFull) => thread::yield_now(),
                        Err(e) => panic!("unexpected write error: {e:?}"),
                    }
                }
            }
        });

        let reader = thread::spawn(move || {
            // Give producer a head start
            thread::sleep(Duration::from_millis(10));
            for expected in 0..NUM_MESSAGES {
                loop {
                    match cons.read() {
                        Ok(v) => {
                            assert_eq!(
                                v, expected,
                                "Producer faster: expected {expected}, got {v}"
                            );
                            break;
                        }
                        Err(MesoError::NoPendingUpdates) => thread::yield_now(),
                        Err(e) => panic!("unexpected read error: {e:?}"),
                    }
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn spsc_concurrent_consumer_faster() {
        const BUFFER_SIZE: usize = 4;
        const NUM_MESSAGES: usize = 1000;

        let buf = Arc::new(BufferWheel::<BUFFER_SIZE, usize>::default());
        let prod = Arc::clone(&buf);
        let cons = Arc::clone(&buf);

        let writer = thread::spawn(move || {
            // Give consumer a head start
            thread::sleep(Duration::from_millis(10));
            for i in 0..NUM_MESSAGES {
                loop {
                    match prod.write(i) {
                        Ok(_) => break,
                        Err(MesoError::BuffersFull) => thread::yield_now(),
                        Err(e) => panic!("unexpected write error: {e:?}"),
                    }
                }
            }
        });

        let reader = thread::spawn(move || {
            for expected in 0..NUM_MESSAGES {
                loop {
                    match cons.read() {
                        Ok(v) => {
                            assert_eq!(
                                v, expected,
                                "Consumer faster: expected {expected}, got {v}"
                            );
                            break;
                        }
                        Err(MesoError::NoPendingUpdates) => thread::yield_now(),
                        Err(e) => panic!("unexpected read error: {e:?}"),
                    }
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }
}

#[cfg(test)]
mod bufferwheel_brutal_stress_tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct StressPayload {
        id: u64,
        data: Vec<u8>,
        checksum: u64,
    }

    impl StressPayload {
        fn new(id: u64, size: usize) -> Self {
            let data: Vec<u8> = (0..size)
                .map(|i| (id as u8).wrapping_add(i as u8))
                .collect();
            let checksum = data.iter().map(|&b| b as u64).sum::<u64>().wrapping_add(id);
            Self { id, data, checksum }
        }

        fn verify(&self) -> bool {
            let expected_checksum = self
                .data
                .iter()
                .map(|&b| b as u64)
                .sum::<u64>()
                .wrapping_add(self.id);
            self.checksum == expected_checksum
        }
    }

    /// This test uses a tiny buffer to maximize contention. The producer aggressively writes,
    /// while the consumer validates every message. The final assertion ensures that the
    /// number of messages successfully written equals the number of messages received and validated.
    #[test]
    fn test_tiny_buffer_high_contention() {
        const BUFFER_SIZE: usize = 2;
        const NUM_MESSAGES: u64 = 10_000;

        let buffer = Arc::new(BufferWheel::<BUFFER_SIZE, StressPayload>::new());
        let barrier = Arc::new(Barrier::new(2));

        // --- Producer Thread ---
        let prod_buffer = Arc::clone(&buffer);
        let prod_barrier = Arc::clone(&barrier);
        let producer = thread::spawn(move || {
            let mut writes_succeeded = 0;
            prod_barrier.wait();

            for i in 0..NUM_MESSAGES {
                let payload = StressPayload::new(i, 8 + (i % 128) as usize);
                loop {
                    match prod_buffer.write(payload.clone()) {
                        Ok(()) => {
                            writes_succeeded += 1;
                            break;
                        }
                        Err(MesoError::BuffersFull) => {
                            thread::sleep(Duration::from_nanos(1)); // Sleep briefly when full
                            continue;
                        }
                        Err(e) => panic!("Unexpected producer error: {:?}", e),
                    }
                }
            }
            writes_succeeded
        });

        // --- Consumer Thread ---
        let cons_buffer = Arc::clone(&buffer);
        let cons_barrier = Arc::clone(&barrier);
        let consumer = thread::spawn(move || {
            let mut received_payloads = HashSet::new();
            cons_barrier.wait();

            while received_payloads.len() < NUM_MESSAGES as usize {
                match cons_buffer.read() {
                    Ok(payload) => {
                        assert!(payload.verify(), "Payload {} corrupted!", payload.id);
                        // Assert that we haven't seen this message before
                        assert!(
                            received_payloads.insert(payload),
                            "Duplicate payload received!"
                        );
                    }
                    Err(MesoError::NoPendingUpdates) => {
                        thread::sleep(Duration::from_nanos(1)); // Sleep briefly when empty
                        continue;
                    }
                    Err(e) => panic!("Unexpected consumer error: {:?}", e),
                }
            }
            received_payloads.len() as u64
        });

        let writes = producer.join().expect("Producer panicked");
        let reads = consumer.join().expect("Consumer panicked");

        println!("\n=== High Contention Test Results ===");
        println!("Messages to send: {}", NUM_MESSAGES);
        println!("Successful writes: {}", writes);
        println!("Verified reads:    {}", reads);

        assert_eq!(
            writes, NUM_MESSAGES,
            "Producer failed to write all messages!"
        );
        assert_eq!(
            reads, NUM_MESSAGES,
            "Consumer failed to receive all messages!"
        );
    }

    /// This test hammers multiple independent SPSC buffers concurrently. This helps
    /// ensure that there is no cross-talk or shared state issues between buffer instances.
    /// It verifies that the total messages sent across all buffers match the total received.
    #[test]
    fn test_multiple_isolated_buffers() {
        const NUM_BUFFERS: usize = 4;
        const MESSAGES_PER_BUFFER: u64 = 5_000;

        let mut handles = vec![];
        let total_writes = Arc::new(AtomicU64::new(0));
        let total_reads = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(NUM_BUFFERS * 2));

        for buffer_id in 0..NUM_BUFFERS {
            let buffer = Arc::new(BufferWheel::<3, StressPayload>::new());

            // --- Producer for this buffer ---
            let prod_buffer = Arc::clone(&buffer);
            let prod_barrier = Arc::clone(&barrier);
            let prod_writes = Arc::clone(&total_writes);
            handles.push(thread::spawn(move || {
                prod_barrier.wait();
                for i in 0..MESSAGES_PER_BUFFER {
                    let msg_id = (buffer_id as u64 * MESSAGES_PER_BUFFER) + i;
                    let payload = StressPayload::new(msg_id, 32);
                    loop {
                        if prod_buffer.write(payload.clone()).is_ok() {
                            prod_writes.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        thread::sleep(Duration::from_nanos(1));
                    }
                }
            }));

            // --- Consumer for this buffer ---
            let cons_buffer = Arc::clone(&buffer);
            let cons_barrier = Arc::clone(&barrier);
            let cons_reads = Arc::clone(&total_reads);
            handles.push(thread::spawn(move || {
                let mut received_count = 0;
                cons_barrier.wait();
                while received_count < MESSAGES_PER_BUFFER {
                    if let Ok(payload) = cons_buffer.read() {
                        assert!(
                            payload.verify(),
                            "Corrupted payload in buffer {}",
                            buffer_id
                        );
                        let expected_buffer_id = payload.id / MESSAGES_PER_BUFFER;
                        assert_eq!(
                            expected_buffer_id, buffer_id as u64,
                            "Message received in wrong buffer!"
                        );
                        cons_reads.fetch_add(1, Ordering::Relaxed);
                        received_count += 1;
                    } else {
                        thread::sleep(Duration::from_nanos(1));
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let final_writes = total_writes.load(Ordering::SeqCst);
        let final_reads = total_reads.load(Ordering::SeqCst);
        let expected_total = NUM_BUFFERS as u64 * MESSAGES_PER_BUFFER;

        println!("\n=== Multi-Buffer Test Results ===");
        println!("Total expected: {}", expected_total);
        println!("Total writes:   {}", final_writes);
        println!("Total reads:    {}", final_reads);

        assert_eq!(final_writes, expected_total, "Lost writes across buffers!");
        assert_eq!(final_reads, expected_total, "Lost reads across buffers!");
    }

    /// This test is designed to provoke memory ordering issues and false sharing.
    /// It uses multiple threads operating on independent buffers that might share CPU
    /// cache lines. The consumer verifies that it only receives data from its
    /// designated producer, checking for data corruption.
    #[test]
    fn test_cache_line_contention() {
        const BUFFER_SIZE: usize = 8;
        const NUM_ITERATIONS: u64 = 10_000;
        const NUM_PAIRS: usize = 4;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(NUM_PAIRS * 2));
        let mut handles = vec![];

        for i in 0..NUM_PAIRS {
            let buffer = Arc::new(BufferWheel::<BUFFER_SIZE, u64>::new());

            // --- Producer ---
            let prod_buffer = Arc::clone(&buffer);
            let prod_barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                prod_barrier.wait();
                for j in 0..NUM_ITERATIONS {
                    let value = (i as u64) << 32 | j;
                    loop {
                        if prod_buffer.write(value).is_ok() {
                            break;
                        }
                        std::hint::spin_loop(); // Aggressive spin to maximize cache pressure
                    }
                }
            }));

            // --- Consumer ---
            let cons_buffer = Arc::clone(&buffer);
            let cons_barrier = Arc::clone(&barrier);
            let cons_corruption_flag = Arc::clone(&corruption_detected);
            handles.push(thread::spawn(move || {
                cons_barrier.wait();
                for _ in 0..NUM_ITERATIONS {
                    loop {
                        if let Ok(value) = cons_buffer.read() {
                            let producer_id = (value >> 32) as usize;
                            if producer_id != i {
                                eprintln!(
                                    "CORRUPTION: Consumer {} got value from producer {}",
                                    i, producer_id
                                );
                                cons_corruption_flag.store(true, Ordering::Relaxed);
                            }
                            break;
                        }
                        std::hint::spin_loop(); // Aggressive spin
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        assert!(
            !corruption_detected.load(Ordering::Relaxed),
            "Data corruption detected, possibly due to cache line/memory ordering issues!"
        );
    }
}
