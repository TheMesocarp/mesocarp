//! This module contains a thread-safe atomic message bus.
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

use crate::MesoError;
use super::{Broadcaster, Subscriber};

// Basic trait for a direct message between two entities
pub trait Message: Clone {
    fn to(&self) -> usize;
    fn from(&self) -> usize;
}

#[derive(Debug)]
/// Manages message passing between multiple threads
pub struct ThreadedMessenger<T: Message> {
    dirin: Box<[Arc<ArrayQueue<T>>]>,
    dirout: Box<[Arc<ArrayQueue<T>>]>,
    broadcaster: Broadcaster<T>,
    slots: usize,
    pub capacity: usize,
    pub registered: usize,
}

impl<T: Message> ThreadedMessenger<T> {
    /// Creates a new messenger for the given agent IDs
    pub fn new(user_count: usize, slots: usize) -> Result<Self, MesoError> {
        let len = user_count;

        let mut dirin = Vec::with_capacity(len);
        let mut dirout = Vec::with_capacity(len);

        for _ in 0..len {
            dirin.push(Arc::new(ArrayQueue::new(slots)));
            dirout.push(Arc::new(ArrayQueue::new(slots)));
        }

        let dirin = dirin.into_boxed_slice();
        let dirout = dirout.into_boxed_slice();

        let broadcaster = Broadcaster::new(user_count, slots);

        Ok(Self {
            dirin,
            dirout,
            broadcaster,
            slots,
            capacity: len,
            registered: 0,
        })
    }

    /// Gets a user interface for the specified thread
    pub fn get_user(&mut self) -> Result<ThreadedMessengerUser<T>, MesoError> {
        if self.registered >= self.capacity {
            return Err(MesoError::InvalidUserId);
        }
        let subscriber = self.broadcaster.subscribe(self.registered);
        let i = self.registered;
        self.registered += 1;
        Ok(ThreadedMessengerUser {
            thread_id: i,
            comms: [
                Arc::clone(&self.dirin[i]),
                Arc::clone(&self.dirout[i]),
            ],
            subscriber,
            user_count: self.capacity,
            slots: self.slots,
        })
    }

    /// Polls all outboxes and returns messages ready for delivery
    pub fn poll(&mut self) -> Result<Vec<(usize, T)>, MesoError> {
        let mut to_write = Vec::new();

        for outbox in self.dirout.iter() {
            // Keep polling this outbox until it's empty
            while let Some(msg) = outbox.pop() {
                let to = msg.to();
                if to != usize::MAX {
                    if to >= self.capacity {
                        return Err(MesoError::NotFound {
                            name: format!("Target agent {to} not found"),
                        });
                    }
                    to_write.push((to, msg));
                } else {
                    self.broadcaster
                        .push(msg)
                        .map_err(|_| MesoError::BuffersFull)?;
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
            self.dirin[target_idx]
                .push(msg)
                .map_err(|_| MesoError::BuffersFull)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
/// Thread user interface for sending and receiving messages from a specific thread
pub struct ThreadedMessengerUser<T: Message> {
    thread_id: usize,
    comms: [Arc<ArrayQueue<T>>; 2], // [inbox, outbox]
    subscriber: Subscriber<T>,
    user_count: usize,
    slots: usize,
}

impl<T: Message> ThreadedMessengerUser<T> {
    /// Send a message through the world's routing system
    pub fn send(&self, message: T) -> Result<(), MesoError> {
        let id = message.to();
        // Write to our outbox - world will route it during poll()
        if message.to() != usize::MAX && id >= self.user_count {
            return Err(MesoError::InvalidUserId);
        }
        self.comms[1]
            .push(message)
            .map_err(|_| MesoError::BuffersFull)
    }

    /// Poll for incoming messages (direct + broadcast)
    pub fn poll(&mut self) -> Option<Vec<T>> {
        let mut output = Vec::new();
        let mut counter = 0;

        while counter < self.slots {
            counter += 1;
            let mut clean = false;

            match self.comms[0].pop() {
                Some(msg) => output.push(msg),
                None => {
                    clean = true;
                }
            }

            // Check broadcast messages
            if let Some(msg) = self.subscriber.pop() {
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

unsafe impl<T: Message> Send for ThreadedMessenger<T> {}
unsafe impl<T: Message> Sync for ThreadedMessenger<T> {}

unsafe impl<T: Message> Send for ThreadedMessengerUser<T> {}
unsafe impl<T: Message> Sync for ThreadedMessengerUser<T> {}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestMessage {
        timestamp: u64,
        commit_time: u64,
        from_id: usize,
        to_id: usize,
        is_broadcast: bool,
        data: String,
    }

    impl Message for TestMessage {
        fn to(&self) -> usize {
            self.to_id
        }

        fn from(&self) -> usize {
            self.from_id
        }
    }

    #[test]
    fn test_world_creation_and_mapping() {
        let mut world = ThreadedMessenger::<TestMessage>::new(3, 16).unwrap();

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
        let mut world = ThreadedMessenger::<TestMessage>::new(2, 16).unwrap();
        let user0 = world.get_user().unwrap();
        let mut user1 = world.get_user().unwrap();

        // Send message from 0 to 1
        let msg = TestMessage {
            timestamp: 100,
            commit_time: 90,
            from_id: 0,
            to_id: 1,
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
        let mut world = ThreadedMessenger::<TestMessage>::new(3, 16).unwrap();
        let user0 = world.get_user().unwrap();
        let mut user1 = world.get_user().unwrap();
        let mut user2 = world.get_user().unwrap();

        // Send broadcast
        let broadcast_msg = TestMessage {
            timestamp: 200,
            commit_time: 190,
            from_id: 0,
            to_id: usize::MAX,
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