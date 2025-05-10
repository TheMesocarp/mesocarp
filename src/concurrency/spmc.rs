use std::ptr::null_mut;
use std::sync::atomic::Ordering::{Acquire, SeqCst};
use std::sync::atomic::{AtomicPtr, fence};
use std::sync::{Arc, atomic::AtomicUsize};

use crate::error::Error;

pub struct BroadcastWheel<const N: usize, T: Clone> {
    buffers: [AtomicPtr<T>; N],
    write_idx: AtomicUsize,
}

pub struct Consumer<const N: usize, T: Clone> {
    wheel: Arc<BroadcastWheel<N, T>>,
    read_idx: usize,
}

impl<const N: usize, T: Clone> BroadcastWheel<N, T> {
    pub fn new() -> Result<Self, Error> {
        if N == 0 {
            return Err(Error::NoPendingUpdates);
        }
        let buffers = std::array::from_fn(|_| AtomicPtr::new(null_mut()));
        Ok(Self {
            buffers,
            write_idx: AtomicUsize::new(0),
        })
    }

    /// Registers a new consumer for this broadcast wheel.
    pub fn register_consumer(self: &Arc<Self>) -> Consumer<N, T> {
        let start_idx = self.write_idx.load(Acquire);
        Consumer {
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
        let next_write_idx = (current_write_idx + 1) % N;
        self.write_idx.store(next_write_idx, SeqCst);
    }
}

impl<const N: usize, T: Clone> Consumer<N, T> {
    /// Tries to receive the next piece of information.
    pub fn try_recv(&mut self) -> Option<T> {
        let w = self.wheel.write_idx.load(SeqCst);
        if self.read_idx == w {
            return None;
        }
        let distance = if w >= self.read_idx {
            w - self.read_idx
        } else {
            N + w - self.read_idx
        };

        if distance == 0 {
            return None;
        }

        if distance >= N {
            self.read_idx = w;
            return None;
        }

        fence(SeqCst);
        let read = self.wheel.buffers[self.read_idx].load(SeqCst);
        if read.is_null() {
            return None;
        }
        let info = unsafe { (*read).clone() };
        self.read_idx = (self.read_idx + 1) % N;
        Some(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error as WheelError; // Assuming your error enum setup

    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_broadcast_wheel() {
        let wheel = BroadcastWheel::<3, usize>::new().unwrap();
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
        let result = BroadcastWheel::<0, usize>::new();
        assert_eq!(result.err(), Some(WheelError::NoPendingUpdates));
    }

    #[test]
    fn test_single_producer_single_consumer_basic() {
        let wheel = Arc::new(BroadcastWheel::<5, usize>::new().unwrap());
        let mut consumer = wheel.register_consumer();

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
        let wheel = Arc::new(BroadcastWheel::<5, String>::new().unwrap()); // write_idx = 0
        wheel.broadcast("hello".to_string()); // write_idx = 1
        wheel.broadcast("world".to_string()); // write_idx = 2

        let mut consumer1 = wheel.register_consumer(); // consumer1.read_idx = 2
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
        let wheel = Arc::new(BroadcastWheel::<N, usize>::new().unwrap());
        let mut consumer = wheel.register_consumer(); // read_idx = 0, wheel.write_idx = 0

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
        let wheel = Arc::new(BroadcastWheel::<N, usize>::new().unwrap());
        let mut consumer = wheel.register_consumer(); // read_idx = 0, wheel.write_idx = 0

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
        let wheel = Arc::new(BroadcastWheel::<N, usize>::new().unwrap()); // write_idx=0

        let mut consumer1 = wheel.register_consumer(); // c1.read_idx = 0
        assert!(consumer1.try_recv().is_none());

        wheel.broadcast(10); // slot 0, wheel.write_idx = 1
        wheel.broadcast(20); // slot 1, wheel.write_idx = 2

        // c1 can now read 10 and 20
        assert_eq!(consumer1.try_recv(), Some(10)); // c1.read_idx=1
        assert_eq!(consumer1.try_recv(), Some(20)); // c1.read_idx=2
        assert!(consumer1.try_recv().is_none()); // c1.read_idx(2) == wheel.write_idx(2)

        let mut consumer2 = wheel.register_consumer(); // c2.read_idx = 2 (current wheel.write_idx)
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

        let wheel = Arc::new(BroadcastWheel::<BUFFER_SIZE, usize>::new().unwrap());
        let mut consumer_handles = Vec::new();

        for consumer_id in 0..NUM_CONSUMERS {
            let wheel_clone = Arc::clone(&wheel);
            let handle = thread::spawn(move || {
                let mut consumer = wheel_clone.register_consumer();
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
