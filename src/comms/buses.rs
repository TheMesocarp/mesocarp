//! This module contains a thread-safe atomic message bus.
use std::sync::Arc;

use crate::{
    comms::{
        spmc::{Broadcast, Subscriber},
        spsc::BufferWheel,
    },
    MesoError,
};

// Basic trait for a direct message between two entities
pub trait Message: Clone {
    fn to(&self) -> Option<usize>;
    fn from(&self) -> usize;
}

#[derive(Debug)]
/// Manages message passing between multiple threads
pub struct ThreadedMessenger<const SLOTS: usize, T: Message> {
    dirin: Vec<Arc<BufferWheel<SLOTS, T>>>,
    dirout: Vec<Arc<BufferWheel<SLOTS, T>>>,
    broadcaster: Arc<Broadcast<SLOTS, T>>,
    pub capacity: usize,
    pub registered: usize,
}

impl<const SLOTS: usize, T: Message> ThreadedMessenger<SLOTS, T> {
    /// Creates a new messenger for the given agent IDs
    pub fn new(user_count: usize) -> Result<Self, MesoError> {
        let len = user_count;
        let mut dirin = Vec::with_capacity(len);
        let mut dirout = Vec::with_capacity(len);

        for _ in 0..len {
            dirin.push(Arc::new(BufferWheel::new()));
            dirout.push(Arc::new(BufferWheel::new()));
        }

        let broadcaster = Arc::new(Broadcast::new()?);

        Ok(Self {
            dirin,
            dirout,
            broadcaster,
            capacity: len,
            registered: 0,
        })
    }

    /// Gets a user interface for the specified thread
    pub fn get_user(&mut self) -> Result<ThreadedMessengerUser<SLOTS, T>, MesoError> {
        if self.registered >= self.capacity {
            return Err(MesoError::InvalidUserId);
        }
        let subscriber = self.broadcaster.register_subscriber();
        let i = self.registered;
        self.registered += 1;
        Ok(ThreadedMessengerUser {
            thread_id: i,
            comms: [
                Arc::clone(&self.dirin[i]),  // incoming
                Arc::clone(&self.dirout[i]), // outgoing
            ],
            subscriber,
            user_count: self.capacity,
        })
    }

    /// Polls all outboxes and returns messages ready for delivery
    pub fn poll(&mut self) -> Result<Vec<(usize, T)>, MesoError> {
        let mut to_write = Vec::new();

        for outbox in self.dirout.iter() {
            // Keep polling this outbox until it's empty
            loop {
                match outbox.read() {
                    Ok(msg) => {
                        if let Some(to) = msg.to() {
                            // Fix: Validate target exists
                            if to >= self.capacity {
                                return Err(MesoError::NotFound {
                                    name: format!("Target agent {to} not found"),
                                });
                            }
                            to_write.push((to, msg));
                        } else {
                            // It's a broadcast message
                            self.broadcaster.broadcast(msg);
                        }
                    }
                    Err(MesoError::NoPendingUpdates) => {
                        // This outbox is empty, move to the next one
                        break;
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
        }

        if to_write.is_empty() {
            return Err(MesoError::NoDirectCommsToShare);
        }
        Ok(to_write)
    }

    /// Delivers messages to their target inboxes
    pub fn deliver(&mut self, msgs: Vec<(usize, T)>) -> Result<(), MesoError> {
        for (target_idx, msg) in msgs {
            self.dirin[target_idx].write(msg)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
/// Thread user interface for sending and receiving messages from a specific thread
pub struct ThreadedMessengerUser<const SLOTS: usize, T: Message> {
    thread_id: usize,
    comms: [Arc<BufferWheel<SLOTS, T>>; 2], // [inbox, outbox]
    subscriber: Subscriber<SLOTS, T>,
    user_count: usize,
}

impl<const SLOTS: usize, T: Message> ThreadedMessengerUser<SLOTS, T> {
    /// Send a message through the world's routing system
    pub fn send(&self, message: T) -> Result<(), MesoError> {
        // Write to our outbox - world will route it during poll()
        if let Some(id) = message.to() {
            if id >= self.user_count {
                return Err(MesoError::InvalidUserId);
            }
        }
        self.comms[1].write(message)
    }

    /// Poll for incoming messages (direct + broadcast)
    pub fn poll(&mut self) -> Option<Vec<T>> {
        let mut output = Vec::new();
        let mut counter = 0;

        while counter < SLOTS {
            counter += 1;
            let mut clean = false;

            match self.comms[0].read() {
                Ok(msg) => output.push(msg),
                Err(_) => {
                    clean = true;
                }
            }

            // Check broadcast messages
            if let Some(msg) = self.subscriber.try_recv() {
                if msg.from() != self.thread_id {
                    // Filter out own broadcasts
                    output.push(msg);
                }
            } else if clean {
                break;
            }
        }

        if output.is_empty() {
            return None;
        }
        Some(output)
    }

    /// Returns this thread's ID
    pub fn thread_id(&self) -> usize {
        self.thread_id
    }
}

unsafe impl<const SLOTS: usize, T: Message> Send for ThreadedMessenger<SLOTS, T> {}
unsafe impl<const SLOTS: usize, T: Message> Sync for ThreadedMessenger<SLOTS, T> {}

unsafe impl<const SLOTS: usize, T: Message> Send for ThreadedMessengerUser<SLOTS, T> {}
unsafe impl<const SLOTS: usize, T: Message> Sync for ThreadedMessengerUser<SLOTS, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestMessage {
        timestamp: u64,
        commit_time: u64,
        from_id: usize,
        to_id: Option<usize>,
        is_broadcast: bool,
        data: String,
    }

    impl Message for TestMessage {
        fn to(&self) -> Option<usize> {
            self.to_id
        }

        fn from(&self) -> usize {
            self.from_id
        }
    }

    #[test]
    fn test_world_creation_and_mapping() {
        let mut world = ThreadedMessenger::<16, TestMessage>::new(3).unwrap();

        // Check users can be created
        let user0 = world.get_user().unwrap();
        let user2 = world.get_user().unwrap();
        let user5 = world.get_user().unwrap();

        assert_eq!(user0.thread_id(), 0);
        assert_eq!(user2.thread_id(), 1);
        assert_eq!(user5.thread_id(), 2);

        // Check invalid user fails
        assert!(world.get_user().is_err());
        assert!(world.get_user().is_err());
    }

    #[test]
    fn test_message_routing() {
        let mut world = ThreadedMessenger::<16, TestMessage>::new(2).unwrap();
        let user0 = world.get_user().unwrap();
        let mut user1 = world.get_user().unwrap();

        // Send message from 0 to 1
        let msg = TestMessage {
            timestamp: 100,
            commit_time: 90,
            from_id: 0,
            to_id: Some(1),
            is_broadcast: false,
            data: "hello".to_string(),
        };

        user0.send(msg.clone()).unwrap();

        // Before polling world, user1 shouldn't see it
        assert!(user1.poll().is_none());

        // Poll world to route messages
        let out = world.poll().unwrap();
        world.deliver(out).unwrap();
        // Now user1 should see it
        let received = user1.poll().unwrap();
        assert!(received.contains(&msg));
    }

    #[test]
    fn test_broadcast_routing() {
        let mut world = ThreadedMessenger::<16, TestMessage>::new(3).unwrap();
        let user0 = world.get_user().unwrap();
        let mut user1 = world.get_user().unwrap();
        let mut user2 = world.get_user().unwrap();

        // Send broadcast
        let broadcast_msg = TestMessage {
            timestamp: 200,
            commit_time: 190,
            from_id: 0,
            to_id: None,
            is_broadcast: true,
            data: "broadcast".to_string(),
        };

        user0.send(broadcast_msg.clone()).unwrap();
        assert_eq!(world.poll().err().unwrap(), MesoError::NoDirectCommsToShare);

        // Both users should receive broadcast
        let received1 = user1.poll().unwrap();
        let received2 = user2.poll().unwrap();

        assert!(received1.contains(&broadcast_msg));
        assert!(received2.contains(&broadcast_msg));
    }
}

#[cfg(test)]
mod threaded_messenger_stress_tests {
    use super::*;
    //use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    // A unique, identifiable message for stress testing.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct StressMessage {
        // A unique ID for every message sent.
        sequence: u64,
        from_id: usize,
        to_id: Option<usize>,
    }

    impl Message for StressMessage {
        fn to(&self) -> Option<usize> {
            self.to_id
        }
        fn from(&self) -> usize {
            self.from_id
        }
    }

    /// Test Goal: Ensure no lost messages or delivery mismatches with only direct messages.
    ///
    /// How it works:
    /// 1. Every user (thread) sends one message to every other user.
    /// 2. The main thread continuously polls and delivers messages.
    /// 3. Each user thread polls its inbox until it has received all expected messages.
    /// 4. After all threads finish, we assert that every user received the exact set
    ///    of messages intended for them and no more.
    #[test]
    fn stress_test_all_to_all_direct_messages() {
        const NUM_USERS: usize = 4;
        const SLOTS: usize = 17; // Small buffer to increase contention.

        let mut world = ThreadedMessenger::<SLOTS, StressMessage>::new(NUM_USERS).unwrap();
        let mut users = Vec::new();
        for _ in 0..NUM_USERS {
            users.push(world.get_user().unwrap());
        }
        let world = Arc::new(std::sync::Mutex::new(world));

        let barrier = Arc::new(Barrier::new(NUM_USERS + 1));
        let mut handles = vec![];

        // Spawn user threads
        for mut user in users {
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let my_id = user.thread_id();
                let mut sent_messages = Vec::new();
                let mut received_messages = Vec::new();

                barrier.wait(); // Sync all threads to start at once

                // 1. Send one message to every *other* user
                for i in 0..NUM_USERS {
                    if i == my_id {
                        continue;
                    }
                    let msg = StressMessage {
                        sequence: (my_id * NUM_USERS + i) as u64,
                        from_id: my_id,
                        to_id: Some(i),
                    };
                    user.send(msg.clone()).unwrap();
                    sent_messages.push(msg);
                }

                // 2. Poll until we receive all messages intended for us
                let expected_msg_count = NUM_USERS - 1;
                while received_messages.len() < expected_msg_count {
                    if let Some(msgs) = user.poll() {
                        received_messages.extend(msgs);
                    }
                    thread::sleep(Duration::from_micros(10)); // Prevent overly aggressive spinning
                }

                // Final check: Did we get messages meant for someone else?
                for msg in &received_messages {
                    assert_eq!(
                        msg.to_id,
                        Some(my_id),
                        "FATAL: Received a message not intended for me!"
                    );
                }

                (sent_messages, received_messages)
            }));
        }

        // Spawn the main world poller thread
        let world_clone = Arc::clone(&world);
        let barrier_clone = Arc::clone(&barrier);
        let poller = thread::spawn(move || {
            barrier_clone.wait();
            // Poll for a fixed duration, assuming it's long enough to clear all messages
            for _ in 0..5000 {
                let mut w = world_clone.lock().unwrap();
                if let Ok(msgs) = w.poll() {
                    w.deliver(msgs).unwrap();
                }
                thread::sleep(Duration::from_micros(10));
            }
        });

        poller.join().unwrap();
        let mut total_received_count = 0;
        for handle in handles {
            let (_, received) = handle.join().unwrap();
            total_received_count += received.len();
        }

        let expected_total_messages = NUM_USERS * (NUM_USERS - 1);
        assert_eq!(
            total_received_count, expected_total_messages,
            "Mismatch between sent and received message counts!"
        );
    }

    // /// Test Goal: Create chaos with a mix of direct and broadcast messages to find
    // /// subtle race conditions, lost messages, or duplicate deliveries.
    // ///
    // /// How it works:
    // /// 1. Each user thread sends a mix of direct and broadcast messages.
    // /// 2. We use atomic counters to track sends and receives globally.
    // /// 3. Each user uses a HashSet to ensure it never receives a duplicate message.
    // /// 4. After all threads complete, we assert that the global counts match exactly.
    // #[test]
    // fn stress_test_mixed_direct_and_broadcast_chaos() {
    //     const NUM_USERS: usize = 4;
    //     const MSGS_PER_USER: usize = 250;
    //     const SLOTS: usize = 8; // Even smaller buffer for more chaos

    //     let mut world = ThreadedMessenger::<SLOTS, StressMessage>::new(NUM_USERS).unwrap();
    //     let sent_direct = Arc::new(AtomicUsize::new(0));
    //     let cloned_dir = Arc::clone(&sent_direct);
    //     let sent_broadcast = Arc::new(AtomicUsize::new(0));
    //     let cloned_b = Arc::clone(&sent_broadcast);
    //     let received_direct = Arc::new(AtomicUsize::new(0));
    //     let received_broadcast = Arc::new(AtomicUsize::new(0));

    //     let users: Vec<_> = (0..NUM_USERS).map(|_| world.get_user().unwrap()).collect();
    //     let world = Arc::new(std::sync::Mutex::new(world));

    //     thread::scope(|s| {
    //         // Spawn the main world poller thread
    //         let world_clone = Arc::clone(&world);
    //         s.spawn(move || {
    //             // Poll until all messages have been sent and processed
    //             loop {
    //                 let mut w = world_clone.lock().unwrap();
    //                 if let Ok(msgs) = w.poll() {
    //                     w.deliver(msgs).unwrap();
    //                 }

    //                 // A simple condition to eventually stop the poller.
    //                 // In a real app, this would be a more robust shutdown signal.
    //                 let total_sent = cloned_dir.load(Ordering::Relaxed) + cloned_b.load(Ordering::Relaxed);
    //                 if total_sent >= NUM_USERS * MSGS_PER_USER {
    //                     // Poll a few more times to drain any in-flight messages
    //                     for _ in 0..100 {
    //                        if let Ok(msgs) = w.poll() { w.deliver(msgs).unwrap(); }
    //                        thread::sleep(Duration::from_micros(1));
    //                     }
    //                     break;
    //                 }
    //                 thread::sleep(Duration::from_micros(10));
    //             }
    //         });

    //         // Spawn user threads
    //         for mut user in users {
    //             let sd = Arc::clone(&sent_direct);
    //             let sb = Arc::clone(&sent_broadcast);
    //             let rd = Arc::clone(&received_direct);
    //             let rb = Arc::clone(&received_broadcast);

    //             s.spawn(move || {
    //                 let my_id = user.thread_id();
    //                 let mut received_log = std::collections::HashSet::new();

    //                 for i in 0..MSGS_PER_USER {
    //                     let seq = (my_id * MSGS_PER_USER + i) as u64;
    //                     // Send a mix of direct and broadcast messages
    //                     if i % 3 == 0 {
    //                         // Broadcast
    //                         let msg = StressMessage { sequence: seq, from_id: my_id, to_id: None };
    //                         user.send(msg).unwrap();
    //                         sb.fetch_add(1, Ordering::Relaxed);
    //                     } else {
    //                         // Direct message to a different user
    //                         let target_id = (my_id + 1 + (i % (NUM_USERS - 1))) % NUM_USERS;
    //                         let msg = StressMessage { sequence: seq, from_id: my_id, to_id: Some(target_id) };
    //                         user.send(msg).unwrap();
    //                         sd.fetch_add(1, Ordering::Relaxed);
    //                     }

    //                     // Poll for messages intermittently
    //                     if i % 5 == 0 {
    //                         if let Some(msgs) = user.poll() {
    //                             for msg in msgs {
    //                                 assert!(received_log.insert(msg.clone()), "FATAL: Duplicate message received: {:?}", msg);
    //                                 if msg.to_id.is_some() {
    //                                     assert_eq!(msg.to_id, Some(my_id), "FATAL: Received direct message for wrong user!");
    //                                     rd.fetch_add(1, Ordering::Relaxed);
    //                                 } else {
    //                                     rb.fetch_add(1, Ordering::Relaxed);
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }

    //                 // Final poll to drain any remaining messages
    //                 loop {
    //                     if let Some(msgs) = user.poll() {
    //                        for msg in msgs {
    //                             assert!(received_log.insert(msg.clone()), "FATAL: Duplicate message received: {:?}", msg);
    //                             if msg.to_id.is_some() {
    //                                 assert_eq!(msg.to_id, Some(my_id), "FATAL: Received direct message for wrong user!");
    //                                 rd.fetch_add(1, Ordering::Relaxed);
    //                             } else {
    //                                 rb.fetch_add(1, Ordering::Relaxed);
    //                             }
    //                         }
    //                     } else {
    //                         // Break when no more messages are coming in for a bit.
    //                         // This is heuristic but fine for a test.
    //                         thread::sleep(Duration::from_millis(50));
    //                         if user.poll().is_none() { break; }
    //                     }
    //                 }
    //             });
    //         }
    //     });

    //     // Final Assertions
    //     let total_direct_sent = sent_direct.load(Ordering::Relaxed);
    //     let total_broadcast_sent = sent_broadcast.load(Ordering::Relaxed);
    //     let total_direct_received = received_direct.load(Ordering::Relaxed);
    //     let total_broadcast_received = received_broadcast.load(Ordering::Relaxed);

    //     let expected_broadcast_received = total_broadcast_sent * (NUM_USERS - 1);

    //     println!("\n--- Chaos Test Results ---");
    //     println!("Direct Sent:      {}", total_direct_sent);
    //     println!("Direct Received:  {}", total_direct_received);
    //     println!("Broadcast Sent:     {}", total_broadcast_sent);
    //     println!("Broadcast Received: {}", total_broadcast_received);
    //     println!("(Expected Broadcast Received: {})", expected_broadcast_received);

    //     assert_eq!(total_direct_sent, total_direct_received, "Mismatch in direct message counts!");
    //     assert_eq!(expected_broadcast_received, total_broadcast_received, "Mismatch in broadcast message counts!");
    // }
}
