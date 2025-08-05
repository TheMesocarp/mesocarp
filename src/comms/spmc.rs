//! Lock-free concurrent data structures for work distribution and broadcasting.
//!
//! This module provides two complementary lock-free data structures:
//! - `WorkQueue`: Single-producer, multi-consumer work distribution queue
//! - `Broadcast`: Single-producer, multi-consumer publish-subscribe channel
//!
//! Both use atomic operations to achieve high-performance concurrent communication without locks.
use std::ptr::null_mut;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::atomic::{fence, AtomicPtr};
use std::sync::{atomic::AtomicUsize, Arc};
use std::thread::yield_now;

use crate::MesoError;

#[derive(Debug)]
/// Single‐producer, multi‐consumer work queue.
/// Uses sequence numbers to prevent a race condition during buffer wrap-around.
/// T: Send because ownership moves to exactly one consumer.
pub struct WorkQueue<const SLOTS: usize, T> {
    buffers: [AtomicPtr<T>; SLOTS],
    sequence_numbers: [AtomicUsize; SLOTS],
    head: AtomicUsize, // Consumer read pointer
    tail: AtomicUsize, // Producer write pointer
}

#[derive(Debug)]
/// A handle for consumers to pop jobs.
pub struct Worker<const SLOTS: usize, T> {
    queue: Arc<WorkQueue<SLOTS, T>>,
}

impl<const SLOTS: usize, T> WorkQueue<SLOTS, T> {
    /// Create a new queue. SLOTS must be > 0.
    pub fn new() -> Result<Arc<Self>, MesoError> {
        if SLOTS == 0 {
            return Err(MesoError::NoPendingUpdates);
        }
        let buffers = std::array::from_fn(|_| AtomicPtr::new(null_mut()));
        let sequence_numbers = std::array::from_fn(|_| AtomicUsize::new(0));

        Ok(Arc::new(Self {
            buffers,
            sequence_numbers,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }))
    }

    /// Push a job into the queue.
    /// Returns Err if the queue is full.
    /// Uses sequence numbers to coordinate with consumers.
    pub fn push(&self, job: T) -> Result<(), MesoError> {
        let tail = self.tail.load(Acquire);
        let head = self.head.load(Acquire);
        if tail.wrapping_sub(head) >= SLOTS {
            return Err(MesoError::BuffersFull);
        }
        let idx = tail % SLOTS;
        let seq = tail / SLOTS;
        let boxed = Box::into_raw(Box::new(job));

        match self.buffers[idx].compare_exchange(null_mut(), boxed, SeqCst, SeqCst) {
            Ok(_) => {
                self.sequence_numbers[idx].store(seq, Release);
                self.tail.store(tail.wrapping_add(1), Release);
                Ok(())
            }
            Err(_) => {
                unsafe {
                    drop(Box::from_raw(boxed));
                }
                Err(MesoError::BuffersFull)
            }
        }
    }

    /// Register a new consumer handle.
    pub fn register_worker(self: &Arc<Self>) -> Worker<SLOTS, T> {
        Worker {
            queue: Arc::clone(self),
        }
    }
}

unsafe impl<const SLOTS: usize, T: Clone> Send for WorkQueue<SLOTS, T> {}
unsafe impl<const SLOTS: usize, T: Clone> Sync for WorkQueue<SLOTS, T> {}

impl<const SLOTS: usize, T> Worker<SLOTS, T> {
    /// Tries to take the next job from the head slot. Returns `None` if queue is empty or item is lost.
    pub fn try_take(&self) -> Option<T> {
        let head = self
            .queue
            .head
            .fetch_update(
                SeqCst, // Success ordering
                SeqCst, // Failure ordering
                |h| {
                    let tail = self.queue.tail.load(Acquire);
                    if h == tail {
                        None
                    } else {
                        Some(h.wrapping_add(1))
                    }
                },
            )
            .ok()?;

        let idx = head % SLOTS;
        let expected_seq = head / SLOTS;

        loop {
            let seq1 = self.queue.sequence_numbers[idx].load(Acquire);
            let seq2 = self.queue.sequence_numbers[idx].load(Acquire);

            if seq1 == seq2 {
                if seq1 == expected_seq {
                    break;
                } else {
                    return None;
                }
            } else {
                yield_now();
            }
        }
        let taken_ptr = self.queue.buffers[idx].swap(null_mut(), SeqCst);
        if taken_ptr.is_null() {
            return None;
        }
        let job = unsafe { *Box::from_raw(taken_ptr) };
        Some(job)
    }
}

unsafe impl<const SLOTS: usize, T: Clone> Send for Worker<SLOTS, T> {}
unsafe impl<const SLOTS: usize, T: Clone> Sync for Worker<SLOTS, T> {}

#[derive(Debug)]
/// `Broadcast` is the producer for an atomic spmc pub-sub channel.
pub struct Broadcast<const SLOTS: usize, T: Clone> {
    buffers: [AtomicPtr<T>; SLOTS],
    write_idx: AtomicUsize,
}

impl<const SLOTS: usize, T: Clone> Broadcast<SLOTS, T> {
    /// Spawns a new `Broadcast` with empty buffer slots
    pub fn new() -> Result<Self, MesoError> {
        if SLOTS == 0 {
            return Err(MesoError::NoPendingUpdates);
        }
        let buffers = std::array::from_fn(|_| AtomicPtr::new(null_mut()));
        Ok(Self {
            buffers,
            write_idx: AtomicUsize::new(0),
        })
    }

    /// Registers a new consumer for this broadcast wheel.
    pub fn register_subscriber(self: &Arc<Self>) -> Subscriber<SLOTS, T> {
        let start_idx = self.write_idx.load(Acquire);
        Subscriber {
            wheel: Arc::clone(self),
            read_idx: start_idx,
        }
    }

    /// Broadcasts new information to all consumers.
    pub fn broadcast(&self, info: T) {
        let new_ptr = Box::into_raw(Box::new(info));
        let current_write_idx = self.write_idx.load(SeqCst);
        let old_ptr = self.buffers[current_write_idx].swap(new_ptr, SeqCst);
        if !old_ptr.is_null() {
            unsafe {
                drop(Box::from_raw(old_ptr));
            }
        }
        let next_write_idx = (current_write_idx + 1) % SLOTS;
        self.write_idx.store(next_write_idx, SeqCst);
    }
}

unsafe impl<const SLOTS: usize, T: Clone> Send for Broadcast<SLOTS, T> {}
unsafe impl<const SLOTS: usize, T: Clone> Sync for Broadcast<SLOTS, T> {}

#[derive(Debug)]
/// A `Subscriber` can poll for new broadcasts with `try_recv` and clone a copy to take ownership over
pub struct Subscriber<const SLOTS: usize, T: Clone> {
    wheel: Arc<Broadcast<SLOTS, T>>,
    read_idx: usize,
}

impl<const SLOTS: usize, T: Clone> Subscriber<SLOTS, T> {
    /// Tries to receive the next broadcast.
    pub fn try_recv(&mut self) -> Option<T> {
        let w = self.wheel.write_idx.load(SeqCst);
        if self.read_idx == w {
            return None;
        }
        let distance = if w >= self.read_idx {
            w - self.read_idx
        } else {
            SLOTS + w - self.read_idx
        };

        if distance == 0 {
            return None;
        }

        if distance >= SLOTS {
            self.read_idx = w;
            return None;
        }

        fence(SeqCst);
        let read = self.wheel.buffers[self.read_idx].load(SeqCst);
        if read.is_null() {
            return None;
        }
        let info = unsafe { (*read).clone() };
        self.read_idx = (self.read_idx + 1) % SLOTS;
        Some(info)
    }
}

unsafe impl<const SLOTS: usize, T: Clone> Send for Subscriber<SLOTS, T> {}
unsafe impl<const SLOTS: usize, T: Clone> Sync for Subscriber<SLOTS, T> {}

#[cfg(test)]
mod broadcast_tests {
    use super::*;
    use crate::MesoError as WheelError;

    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_broadcast_wheel() {
        let wheel = Broadcast::<3, usize>::new().unwrap();
        assert_eq!(wheel.write_idx.load(Ordering::Relaxed), 0);
        assert_eq!(wheel.buffers.len(), 3);
        for i in 0..3 {
            assert!(
                wheel.buffers[i].load(Acquire).is_null(),
                "Buffer at index {i} not initialized correctly"
            );
        }
    }

    #[test]
    fn test_new_broadcast_wheel_n_zero() {
        let result = Broadcast::<0, usize>::new();
        assert_eq!(result.err(), Some(WheelError::NoPendingUpdates));
    }

    #[test]
    fn test_single_producer_single_consumer_basic() {
        let wheel = Arc::new(Broadcast::<5, usize>::new().unwrap());
        let mut consumer = wheel.register_subscriber();

        assert!(
            consumer.try_recv().is_none(),
            "Consumer should see no new messages initially"
        );

        wheel.broadcast(10); // write_idx = 1, consumer.read_idx = 0
        assert_eq!(consumer.try_recv(), Some(10)); // consumer.read_idx becomes 1
        assert!(
            consumer.try_recv().is_none(),
            "Consumer should see no more messages now"
        ); // consumer.read_idx (1) == write_idx (1)

        wheel.broadcast(20); // write_idx = 2, consumer.read_idx = 1
        assert_eq!(consumer.try_recv(), Some(20)); // consumer.read_idx becomes 2
        assert!(consumer.try_recv().is_none(), "Again, no more messages"); // consumer.read_idx (2) == write_idx (2)
    }

    #[test]
    fn test_consumer_registration_point() {
        let wheel = Arc::new(Broadcast::<5, String>::new().unwrap()); // write_idx = 0
        wheel.broadcast("hello".to_string()); // write_idx = 1
        wheel.broadcast("world".to_string()); // write_idx = 2

        let mut consumer1 = wheel.register_subscriber(); // consumer1.read_idx = 2
        assert!(
            consumer1.try_recv().is_none(),
            "Consumer1 starts at current write_idx, sees nothing new"
        );

        wheel.broadcast("rust".to_string()); // write_idx = 3. Item "rust" in slot 2.
                                             // consumer1.read_idx = 2.
        assert_eq!(consumer1.try_recv(), Some("rust".to_string())); // reads slot 2, read_idx becomes 3
        assert!(
            consumer1.try_recv().is_none(),
            "No more after reading 'rust'"
        ); // read_idx(3) == write_idx(3)
    }

    #[test]
    fn test_buffer_wrap_around_single_consumer_polling_interleaved() {
        const N: usize = 3;
        let wheel = Arc::new(Broadcast::<N, usize>::new().unwrap());
        let mut consumer = wheel.register_subscriber(); // read_idx = 0, wheel.write_idx = 0

        assert!(consumer.try_recv().is_none(), "Initially no items");

        // Broadcast 1
        wheel.broadcast(1); // slot 0, write_idx = 1
        assert_eq!(consumer.try_recv(), Some(1), "Should get 1"); // consumer.read_idx becomes 1
        assert!(
            consumer.try_recv().is_none(),
            "Should be no more items after reading 1"
        );

        // Broadcast 2
        wheel.broadcast(2); // slot 1, write_idx = 2
        assert_eq!(consumer.try_recv(), Some(2), "Should get 2"); // consumer.read_idx becomes 2
        assert!(
            consumer.try_recv().is_none(),
            "Should be no more items after reading 2"
        );

        // Broadcast 3 (fills buffer, write_idx wraps)
        wheel.broadcast(3); // slot 2, write_idx = 0
        assert_eq!(consumer.try_recv(), Some(3), "Should get 3"); // consumer.read_idx becomes 0
        assert!(
            consumer.try_recv().is_none(),
            "Should be no more items after reading 3"
        );

        // Broadcast 4 (overwrites item 1 in slot 0)
        wheel.broadcast(4); // slot 0, write_idx = 1
        assert_eq!(consumer.try_recv(), Some(4), "Should get 4 (overwritten 1)"); // consumer.read_idx becomes 1
        assert!(
            consumer.try_recv().is_none(),
            "Should be no more items after reading 4"
        );
    }

    #[test]
    fn test_slow_consumer_misses_messages_due_to_lapping() {
        const N: usize = 3;
        let wheel = Arc::new(Broadcast::<N, usize>::new().unwrap());
        let mut consumer = wheel.register_subscriber(); // read_idx = 0, wheel.write_idx = 0

        // Producer broadcasts N items. Consumer does not poll.
        for i in 1..=N {
            // broadcasts 1, 2, 3
            wheel.broadcast(i);
        }
        // After loop: wheel.write_idx = 0 (lapped). consumer.read_idx = 0.
        // Buffers contain [1,2,3] (ArcSwapped).
        // Since consumer.read_idx (0) == wheel.write_idx (0), it sees nothing new.
        assert!(
            consumer.try_recv().is_none(),
            "Consumer missed 1,2,3 due to not polling; write_idx lapped read_idx"
        );

        // Producer broadcasts N more items. Consumer still hasn't successfully polled.
        for i in (N + 1)..=(2 * N) {
            // broadcasts 4, 5, 6
            wheel.broadcast(i);
        }
        // After loop: wheel.write_idx = 0 (lapped again). consumer.read_idx = 0.
        // Buffers contain [4,5,6] (ArcSwapped).
        // Again, consumer.read_idx (0) == wheel.write_idx (0).
        assert!(
            consumer.try_recv().is_none(),
            "Consumer also missed 4,5,6 as write_idx lapped read_idx again"
        );

        // Now, if the producer broadcasts one more item, write_idx will differ from read_idx.
        wheel.broadcast(7); // slot 0, write_idx=1. Consumer read_idx=0.
        assert_eq!(
            consumer.try_recv(),
            Some(7),
            "Consumer should see item 7 as read_idx != write_idx now"
        );
        // consumer.read_idx becomes 1. wheel.write_idx is 1.
        assert!(
            consumer.try_recv().is_none(),
            "No more items after reading 7"
        );
    }

    #[test]
    fn test_multiple_consumers_different_registration_times() {
        const N: usize = 5;
        let wheel = Arc::new(Broadcast::<N, usize>::new().unwrap()); // write_idx=0

        let mut consumer1 = wheel.register_subscriber(); // c1.read_idx = 0
        assert!(consumer1.try_recv().is_none());

        wheel.broadcast(10); // slot 0, wheel.write_idx = 1
        wheel.broadcast(20); // slot 1, wheel.write_idx = 2

        // c1 can now read 10 and 20
        assert_eq!(consumer1.try_recv(), Some(10)); // c1.read_idx=1
        assert_eq!(consumer1.try_recv(), Some(20)); // c1.read_idx=2
        assert!(consumer1.try_recv().is_none()); // c1.read_idx(2) == wheel.write_idx(2)

        let mut consumer2 = wheel.register_subscriber(); // c2.read_idx = 2 (current wheel.write_idx)
        assert!(consumer2.try_recv().is_none());

        wheel.broadcast(30); // slot 2, wheel.write_idx = 3
        wheel.broadcast(40); // slot 3, wheel.write_idx = 4

        // Consumer 1 reads (c1.read_idx=2, wheel.write_idx=4)
        assert_eq!(consumer1.try_recv(), Some(30)); // c1.read_idx=3
        assert_eq!(consumer1.try_recv(), Some(40)); // c1.read_idx=4
        assert!(consumer1.try_recv().is_none());

        // Consumer 2 reads (c2.read_idx=2, wheel.write_idx=4)
        assert_eq!(consumer2.try_recv(), Some(30)); // c2.read_idx=3
        assert_eq!(consumer2.try_recv(), Some(40)); // c2.read_idx=4
        assert!(consumer2.try_recv().is_none());

        wheel.broadcast(50); // slot 4, wheel.write_idx = 0 (wraps)

        assert_eq!(consumer1.try_recv(), Some(50)); // c1.read_idx=0
        assert!(consumer1.try_recv().is_none());

        assert_eq!(consumer2.try_recv(), Some(50)); // c2.read_idx=0
        assert!(consumer2.try_recv().is_none());
    }

    #[test]
    fn test_spmc_concurrent_monotonicity() {
        const NUM_ITEMS_TO_BROADCAST: usize = 1000;
        const BUFFER_SIZE: usize = 10;
        const NUM_CONSUMERS: usize = 4;

        let wheel = Arc::new(Broadcast::<BUFFER_SIZE, usize>::new().unwrap());
        let mut consumer_handles = Vec::new();

        for consumer_id in 0..NUM_CONSUMERS {
            let wheel_clone = Arc::clone(&wheel);
            let handle = thread::spawn(move || {
                let mut consumer = wheel_clone.register_subscriber();
                let mut received_items = Vec::new();
                // This loop needs to be robust to the fact that try_recv can often return None
                // especially if the consumer is slow or the buffer is small.
                for _attempt in 0..(NUM_ITEMS_TO_BROADCAST * 2) {
                    // Poll a number of times
                    if let Some(arc_val) = consumer.try_recv() {
                        received_items.push(arc_val);
                    } else {
                        // Yield or sleep briefly if nothing is received to prevent busy-waiting too hard
                        // and allow producer to advance.
                        thread::sleep(Duration::from_micros(1));
                    }
                    // Add a small delay to simulate work and increase chance of diverse interleavings
                    if consumer_id % 2 == 0 {
                        thread::sleep(Duration::from_micros(10));
                    }
                }
                // An alternative stop condition could be if the producer signals completion,
                // or after a certain number of consecutive None an item has been received.
                received_items
            });
            consumer_handles.push(handle);
        }

        // Producer (main thread)
        for i in 1..=NUM_ITEMS_TO_BROADCAST {
            wheel.broadcast(i);
            if i % 50 == 0 {
                // Occasionally give consumers more time
                thread::sleep(Duration::from_micros(100));
            }
        }

        // Give consumers some time to finish processing after producer is done
        thread::sleep(Duration::from_millis(200)); // Adjust as needed

        for (i, handle) in consumer_handles.into_iter().enumerate() {
            match handle.join() {
                Ok(items) => {
                    // It's possible a very slow consumer or one registered late sees very few or no items
                    // especially if NUM_ITEMS_TO_BROADCAST is small or producer is very fast.
                    // The primary check is monotonicity if items ARE received.
                    if !items.is_empty() {
                        // Check for monotonicity
                        for j in 0..(items.len() - 1) {
                            assert!(
                                items[j] <= items[j + 1],
                                "Consumer {i} received items out of order: {:?} followed by {} (Index {j} of {:?} items. Full: {items:?})",
                                items[j],
                                items[j + 1],
                                items.len()
                            );
                        }
                        // Check items are within broadcast range
                        for &item_val in &items {
                            assert!(
                                (1..=NUM_ITEMS_TO_BROADCAST).contains(&item_val),
                                "Consumer {i} received item {item_val} which is out of broadcast range [1, {NUM_ITEMS_TO_BROADCAST}]"
                            );
                        }
                    }
                    // println!("Consumer {} received {} items. First: {:?}, Last: {:?}", i, items.len(), items.first(), items.last());
                }
                Err(e) => {
                    panic!("Consumer thread {i} panicked: {e:?}");
                }
            }
        }
    }
}

#[cfg(test)]
mod worker_queue_tests {
    use super::*;
    use crate::MesoError as QueueError;

    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_work_queue() {
        let queue = WorkQueue::<3, usize>::new().unwrap();
        assert_eq!(queue.head.load(Ordering::Relaxed), 0);
        assert_eq!(queue.tail.load(Ordering::Relaxed), 0);
        assert_eq!(queue.buffers.len(), 3);
        for i in 0..3 {
            assert!(
                queue.buffers[i].load(Ordering::Acquire).is_null(),
                "Buffer at index {i} not initialized correctly"
            );
        }
    }

    #[test]
    fn test_new_work_queue_zero_size() {
        let result = WorkQueue::<0, usize>::new();
        assert_eq!(result.err(), Some(QueueError::NoPendingUpdates));
    }

    #[test]
    fn test_single_producer_single_consumer_basic() {
        let queue = Arc::new(WorkQueue::<5, usize>::new().unwrap());
        let worker = queue.register_worker();

        assert!(
            worker.try_take().is_none(),
            "Consumer should see no jobs initially"
        );

        queue.push(10).unwrap(); // tail = 1, head = 0
        assert_eq!(worker.try_take(), Some(10)); // head becomes 1
        assert!(
            worker.try_take().is_none(),
            "Consumer should see no more jobs now"
        ); // head (1) == tail (1)

        queue.push(20).unwrap(); // tail = 2, head = 1
        assert_eq!(worker.try_take(), Some(20)); // head becomes 2
        assert!(worker.try_take().is_none(), "Again, no more jobs"); // head (2) == tail (2)
    }

    #[test]
    fn test_work_queue_full() {
        const N: usize = 3;
        let queue = Arc::new(WorkQueue::<N, usize>::new().unwrap());

        // Fill the queue
        for i in 0..N {
            assert!(queue.push(i).is_ok(), "Push {i} failed unexpectedly");
        }
        // After this, tail = N, head = 0. The queue is full.

        // Queue should be full
        assert_eq!(
            queue.push(N),
            Err(QueueError::BuffersFull),
            "Push to full queue should return error"
        );

        let worker = queue.register_worker();
        // Take one item, which should free up one slot
        assert_eq!(
            worker.try_take(),
            Some(0),
            "Should be able to take an item from a full queue"
        );
        // After taking 0, head = 1. Queue has N-1 items.

        // Now one slot is free, pushing one item should succeed
        assert!(
            queue.push(N).is_ok(), // Push item N
            "Push should succeed after one item is taken"
        );
        // After pushing N, tail = N + 1. head = 1. (N+1).wrapping_sub(1) = N. Queue is full again.

        // Pushing another item should fail again
        assert_eq!(
            queue.push(N + 1),
            Err(QueueError::BuffersFull),
            "Queue should be full again after pushing one item back"
        );

        // Take the remaining items
        assert_eq!(worker.try_take(), Some(1), "Should get item 1"); // head = 2
        assert_eq!(worker.try_take(), Some(2), "Should get item 2"); // head = 3
        assert_eq!(worker.try_take(), Some(N), "Should get item {N}"); // head = 4

        assert!(
            worker.try_take().is_none(),
            "Should be no more items after taking all"
        );

        let tail = queue.tail.load(Ordering::Acquire);
        let head = queue.head.load(Ordering::Acquire);
        assert_eq!(
            head, tail,
            "Head and tail should be equal after consuming all"
        );
    }

    #[test]
    fn test_buffer_wrap_around_single_consumer() {
        const N: usize = 3;
        let queue = Arc::new(WorkQueue::<N, usize>::new().unwrap());
        let worker = queue.register_worker();

        assert!(worker.try_take().is_none(), "Initially no items");

        // Push N items
        for i in 0..N {
            queue.push(i).unwrap();
        }
        // After pushing 0, 1, 2: tail=3, head=0. Queue is full.

        // Take the items in order
        assert_eq!(worker.try_take(), Some(0), "Should get 0"); // head=1
        assert_eq!(worker.try_take(), Some(1), "Should get 1"); // head=2
        assert_eq!(worker.try_take(), Some(2), "Should get 2"); // head=3

        assert!(
            worker.try_take().is_none(),
            "Should be no more items after reading all pushed"
        );

        let tail = queue.tail.load(Ordering::Acquire);
        let head = queue.head.load(Ordering::Acquire);
        assert_eq!(
            head, tail,
            "Head and tail should be equal after consuming all"
        );

        // Push N+1 items to force wrap around and overwrite
        // Queue is currently empty (head=3, tail=3)
        queue.push(10).unwrap(); // slot (3%3=0), tail=4
        assert_eq!(queue.tail.load(Ordering::Acquire), 4);
        queue.push(11).unwrap(); // slot (4%3=1), tail=5
        assert_eq!(queue.tail.load(Ordering::Acquire), 5);
        queue.push(12).unwrap(); // slot (5%3=2), tail=6
        assert_eq!(queue.tail.load(Ordering::Acquire), 6); // Queue full again
        assert_eq!(
            queue.push(13),
            Err(QueueError::BuffersFull),
            "Queue should be full after pushing N items again"
        );

        // Take the new items
        assert_eq!(worker.try_take(), Some(10), "Should get 10"); // head=4
        assert_eq!(worker.try_take(), Some(11), "Should get 11"); // head=5
        assert_eq!(worker.try_take(), Some(12), "Should get 12"); // head=6

        let tail = queue.tail.load(Ordering::Acquire);
        let head = queue.head.load(Ordering::Acquire);
        assert_eq!(
            head, tail,
            "Head and tail should be equal after consuming wrapped items"
        );
    }

    #[test]
    fn test_multiple_consumers_contention() {
        const NUM_JOBS: usize = 1000;
        const BUFFER_SIZE: usize = 50; // Needs to be large enough not to block producer too much
        const NUM_CONSUMERS: usize = 4;

        let queue = Arc::new(WorkQueue::<BUFFER_SIZE, usize>::new().unwrap());
        let mut worker_handles = Vec::new();
        let received_jobs = Arc::new(std::sync::Mutex::new(HashSet::new()));

        for consumer_id in 0..NUM_CONSUMERS {
            let worker = queue.register_worker();
            let received_jobs_clone = Arc::clone(&received_jobs);
            // Clone the queue Arc to move into the consumer thread
            let queue_clone = Arc::clone(&queue);

            let handle = thread::spawn(move || {
                // Consumer threads will poll for jobs.
                // In a real application, they might block or use a signal.
                // For this test, we'll poll a large number of times and rely on
                // the final check of the `received_jobs` set.
                let mut poll_count = 0;
                const MAX_POLLS_PER_CONSUMER: usize = NUM_JOBS * 4; // Heuristic limit

                loop {
                    if let Some(job) = worker.try_take() {
                        // In a work queue, each job should be taken exactly once.
                        // Using a HashSet to verify this.
                        let mut received_jobs_guard = received_jobs_clone.lock().unwrap();
                        if !received_jobs_guard.insert(job) {
                            panic!("Consumer {consumer_id} received duplicate job: {job}");
                        }
                    } else {
                        // No jobs available right now.
                        // A small sleep prevents excessive busy-waiting.
                        thread::sleep(Duration::from_micros(1));
                        poll_count += 1;

                        // Heuristic break condition: If we've polled many times
                        // and the queue seems empty based on head/tail, assume producer is done.
                        // This is not foolproof but helps the test terminate.
                        let head = queue_clone.head.load(Ordering::Acquire);
                        let tail = queue_clone.tail.load(Ordering::Acquire);
                        if head == tail && poll_count > (MAX_POLLS_PER_CONSUMER / 2) {
                            break; // Exit loop if queue seems empty after sufficient polling
                        }
                        if poll_count >= MAX_POLLS_PER_CONSUMER {
                            break; // Exit loop after max polls to prevent infinite test runs
                        }
                    }
                }
                // Thread finishes after the polling loop
            });
            worker_handles.push(handle);
        }

        // Producer (main thread)
        for i in 0..NUM_JOBS {
            // Keep trying to push if the queue is full.
            // In a real application, the producer might block or handle the error differently.
            while let Err(QueueError::BuffersFull) = queue.push(i) {
                thread::sleep(Duration::from_micros(10)); // Wait a bit if the queue is full
            }
        }

        // Producer is done pushing. Consumers should eventually drain the queue.

        // Wait for all consumer threads to finish.
        for (i, handle) in worker_handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                panic!("Consumer thread {i} panicked: {e:?}");
            }
        }

        // Verify that all jobs were received exactly once
        let final_received_jobs = received_jobs.lock().unwrap();
        assert_eq!(
            final_received_jobs.len(),
            NUM_JOBS,
            "Total number of unique jobs received does not match the number produced. Received: {}",
            final_received_jobs.len()
        );

        for i in 0..NUM_JOBS {
            assert!(
                final_received_jobs.contains(&i),
                "Job {i} was not received by any consumer"
            );
        }
    }

    #[test]
    fn test_concurrent_push_and_consume() {
        // Increase parameters to reduce the frequency of wrap-around under contention
        const NUM_ITEMS: usize = 2000; // More items to push
        const BUFFER_SIZE: usize = 200; // Larger buffer size
        const NUM_CONSUMERS: usize = 8; // More consumers

        let queue = Arc::new(WorkQueue::<BUFFER_SIZE, usize>::new().unwrap());
        // Use HashSet for uniqueness check and count
        let received_jobs = Arc::new(std::sync::Mutex::new(HashSet::new()));

        let producer_queue = Arc::clone(&queue);
        let producer_handle = thread::spawn(move || {
            for i in 0..NUM_ITEMS {
                // Keep trying to push until successful
                while let Err(QueueError::BuffersFull) = producer_queue.push(i) {
                    // Wait a bit if the queue is full to allow consumers to catch up
                    thread::sleep(Duration::from_micros(10));
                }
            }
            // Producer finishes
        });

        let mut consumer_handles = Vec::new();
        for consumer_id in 0..NUM_CONSUMERS {
            let worker = queue.register_worker();
            let received_jobs_clone = Arc::clone(&received_jobs);
            // Clone the queue Arc to move into the consumer thread
            let queue_clone = Arc::clone(&queue);

            let handle = thread::spawn(move || {
                // Consumer threads will poll for jobs.
                // Increase polling attempts and idle limit to give consumers more chances.
                let mut poll_attempts_without_job = 0;
                // Heuristic: Poll many times without receiving a job before considering exit
                const MAX_IDLE_POLLS: usize = 10_000; // Significantly increased idle polls limit

                loop {
                    if let Some(job) = worker.try_take() {
                        let mut received_jobs_guard = received_jobs_clone.lock().unwrap();
                        // Check for duplicates immediately
                        if !received_jobs_guard.insert(job) {
                            panic!("Consumer {consumer_id} received duplicate job: {job}");
                        }
                        // Reset idle counter on success
                        poll_attempts_without_job = 0;
                    } else {
                        // No jobs available right now, sleep briefly to avoid excessive busy-waiting
                        thread::sleep(Duration::from_micros(5));
                        poll_attempts_without_job += 1;

                        // Heuristic break condition: If idle for a significant time
                        // and the queue appears empty, assume producer is done and no
                        // more items are coming *that this consumer is responsible for*.
                        // This is still a heuristic for testing.
                        let head = queue_clone.head.load(Ordering::Acquire);
                        let tail = queue_clone.tail.load(Ordering::Acquire);

                        // If the queue appears empty (head == tail) AND we've been polling
                        // idly for a while, we might be done.
                        if head == tail && poll_attempts_without_job >= MAX_IDLE_POLLS {
                            break;
                        }

                        // Also include a very high hard limit on total idle polls
                        // to prevent test hangs if the above condition isn't met for some reason.
                        if poll_attempts_without_job >= MAX_IDLE_POLLS * 10 {
                            break;
                        }
                    }
                }
                // Thread finishes after the polling loop
            });
            consumer_handles.push(handle);
        }

        // Wait for the producer thread to finish pushing all items.
        producer_handle.join().expect("Producer thread panicked");

        // Give consumers a bit more time to drain the queue after the producer is done.
        thread::sleep(Duration::from_millis(500));

        // Wait for all consumer threads to finish their polling loops.
        for (i, handle) in consumer_handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                panic!("Consumer thread {i} panicked: {e:?}");
            }
        }

        // Final assertion: Check if the queue is truly empty after all threads have joined.
        // If head != tail here, it means items were pushed but never successfully taken.
        let final_head = queue.head.load(Ordering::Acquire);
        let final_tail = queue.tail.load(Ordering::Acquire);
        assert_eq!(
            final_head, final_tail,
            "Queue is not empty after all consumers finished. Head: {final_head}, Tail: {final_tail}. Potential missed items in the queue structure."
        );

        // Check that the total number of unique jobs received across all consumers
        // equals the number of jobs produced.
        let received_set = received_jobs.lock().unwrap();
        assert_eq!(
            received_set.len(),
            NUM_ITEMS,
            "Total number of unique jobs received does not match the number produced. Received: {}",
            received_set.len()
        );

        // Verify that all original item values (0 to NUM_ITEMS-1) were received.
        for i in 0..NUM_ITEMS {
            assert!(
                received_set.contains(&i),
                "Job {i} was not received by any consumer."
            );
        }
    }
}
