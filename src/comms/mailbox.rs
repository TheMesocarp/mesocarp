use std::{collections::BTreeSet, sync::Arc};

use crate::{
    comms::{
        spmc::{Broadcast, Subscriber},
        spsc::BufferWheel,
    },
    scheduling::Scheduleable,
    MesoError,
};

pub trait Message: Scheduleable + Ord + Clone {
    fn to(&self) -> Option<usize>;
    fn from(&self) -> usize;
    fn broadcast(&self) -> bool;
}

pub struct ThreadWorld<const SLOTS: usize, T: Message> {
    agents: Vec<usize>,
    id_to_idx: Vec<usize>,
    dirin: Vec<Arc<BufferWheel<SLOTS, T>>>,
    dirout: Vec<Arc<BufferWheel<SLOTS, T>>>,
    broadcaster: Arc<Broadcast<SLOTS, T>>,
}

impl<const SLOTS: usize, T: Message> ThreadWorld<SLOTS, T> {
    pub fn new(agent_ids: Vec<usize>) -> Result<Self, MesoError> {
        let max_id = agent_ids.iter().max().copied().unwrap_or(0);

        let mut id_to_idx = vec![usize::MAX; max_id + 1];
        for (idx, &id) in agent_ids.iter().enumerate() {
            id_to_idx[id] = idx;
        }

        let len = agent_ids.len();
        let mut dirin = Vec::with_capacity(len);
        let mut dirout = Vec::with_capacity(len);

        for _ in 0..len {
            dirin.push(Arc::new(BufferWheel::new()));
            dirout.push(Arc::new(BufferWheel::new()));
        }

        let broadcaster = Arc::new(Broadcast::new()?);

        Ok(Self {
            agents: agent_ids,
            id_to_idx,
            dirin,
            dirout,
            broadcaster,
        })
    }

    pub fn get_user(&self, thread_id: usize) -> Result<ThreadWorldUser<SLOTS, T>, MesoError> {
        if thread_id >= self.id_to_idx.len() || self.id_to_idx[thread_id] == usize::MAX {
            return Err(MesoError::NotFound {
                name: format!("Agent ID {thread_id} not found within this thread world"),
            });
        }

        let subscriber = self.broadcaster.register_subscriber();
        let i = self.id_to_idx[thread_id];

        Ok(ThreadWorldUser {
            thread_id,
            comms: [
                Arc::clone(&self.dirin[i]),  // incoming
                Arc::clone(&self.dirout[i]), // outgoing
            ],
            subscriber,
        })
    }

    pub fn poll(&mut self) -> Result<Vec<(usize, T)>, MesoError> {
        let mut to_write = Vec::new();

        for outbox in self.dirout.iter() {
            match outbox.read() {
                Ok(msg) => {
                    if let Some(to) = msg.to() {
                        // Fix: Validate target exists
                        if to >= self.id_to_idx.len() || self.id_to_idx[to] == usize::MAX {
                            return Err(MesoError::NotFound {
                                name: format!("Target agent {to} not found"),
                            });
                        }
                        let target_idx = self.id_to_idx[to];
                        to_write.push((target_idx, msg));
                    } else if msg.broadcast() {
                        self.broadcaster.broadcast(msg);
                    } else {
                        return Err(MesoError::ImproperMessagePassing);
                    }
                }
                Err(MesoError::NoPendingUpdates) => {
                    continue;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        Ok(to_write)
    }

    pub fn deliver(&mut self, msgs: Vec<(usize, T)>) -> Result<(), MesoError> {
        for (target_idx, msg) in msgs {
            self.dirin[target_idx].write(msg)?;
        }
        Ok(())
    }

    pub fn agents(&self) -> &[usize] {
        &self.agents
    }
}

pub struct ThreadWorldUser<const SLOTS: usize, T: Message> {
    thread_id: usize,
    comms: [Arc<BufferWheel<SLOTS, T>>; 2], // [inbox, outbox]
    subscriber: Subscriber<SLOTS, T>,
}

impl<const SLOTS: usize, T: Message> ThreadWorldUser<SLOTS, T> {
    /// Send a message through the world's routing system
    pub fn send(&self, message: T) -> Result<(), MesoError> {
        // Write to our outbox - world will route it during poll()
        self.comms[1].write(message)
    }

    /// Poll for incoming messages (direct + broadcast)
    pub fn poll(&mut self) -> Option<BTreeSet<T>> {
        let mut output: Option<BTreeSet<T>> = None;
        let mut counter = 0;

        while counter < SLOTS {
            counter += 1;
            let mut clean = false;

            match self.comms[0].read() {
                Ok(msg) => match &mut output {
                    Some(set) => {
                        set.insert(msg);
                    }
                    None => {
                        let mut set = BTreeSet::new();
                        set.insert(msg);
                        output = Some(set);
                    }
                },
                Err(_) => {
                    clean = true;
                }
            }

            // Check broadcast messages
            if let Some(msg) = self.subscriber.try_recv() {
                match &mut output {
                    Some(set) => {
                        set.insert(msg);
                    }
                    None => {
                        let mut set = BTreeSet::new();
                        set.insert(msg);
                        output = Some(set);
                    }
                }
            } else if clean {
                break;
            }
        }

        output
    }

    pub fn thread_id(&self) -> usize {
        self.thread_id
    }
}

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

    impl Scheduleable for TestMessage {
        fn time(&self) -> u64 {
            self.timestamp
        }
        fn commit_time(&self) -> u64 {
            self.commit_time
        }
    }

    impl Message for TestMessage {
        fn to(&self) -> Option<usize> {
            self.to_id
        }

        fn from(&self) -> usize {
            self.from_id
        }

        fn broadcast(&self) -> bool {
            self.is_broadcast
        }
    }

    #[test]
    fn test_world_creation_and_mapping() {
        let world = ThreadWorld::<16, TestMessage>::new(vec![0, 2, 5]).unwrap();

        // Check agent list
        assert_eq!(world.agents(), &[0, 2, 5]);

        // Check users can be created
        let user0 = world.get_user(0).unwrap();
        let user2 = world.get_user(2).unwrap();
        let user5 = world.get_user(5).unwrap();

        assert_eq!(user0.thread_id(), 0);
        assert_eq!(user2.thread_id(), 2);
        assert_eq!(user5.thread_id(), 5);

        // Check invalid user fails
        assert!(world.get_user(1).is_err());
        assert!(world.get_user(3).is_err());
    }

    #[test]
    fn test_message_routing() {
        let mut world = ThreadWorld::<16, TestMessage>::new(vec![0, 1]).unwrap();
        let user0 = world.get_user(0).unwrap();
        let mut user1 = world.get_user(1).unwrap();

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
        world.poll().unwrap();

        // Now user1 should see it
        let received = user1.poll().unwrap();
        assert!(received.contains(&msg));
    }

    #[test]
    fn test_broadcast_routing() {
        let mut world = ThreadWorld::<16, TestMessage>::new(vec![0, 1, 2]).unwrap();
        let user0 = world.get_user(0).unwrap();
        let mut user1 = world.get_user(1).unwrap();
        let mut user2 = world.get_user(2).unwrap();

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
        world.poll().unwrap();

        // Both users should receive broadcast
        let received1 = user1.poll().unwrap();
        let received2 = user2.poll().unwrap();

        assert!(received1.contains(&broadcast_msg));
        assert!(received2.contains(&broadcast_msg));
    }
}
