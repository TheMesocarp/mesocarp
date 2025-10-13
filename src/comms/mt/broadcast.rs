use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

use crate::MesoError;

/// SPMC broadcast channel, wrapping `crossbeam_queue::ArrayQueue<T>` for `T: Clone` types.
///
/// Requires up-front specification of the number of feeders listening, and the number of slots each has available
#[derive(Debug)]
pub struct Broadcaster<T: Clone>(Box<[Arc<ArrayQueue<T>>]>);

impl<T: Clone> Broadcaster<T> {
    pub fn new(feeders: usize, slots: usize) -> Self {
        let mut bc = Vec::new();
        for _ in 0..feeders {
            bc.push(Arc::new(ArrayQueue::new(slots)));
        }
        let bc = bc.into_boxed_slice();
        Self(bc)
    }

    pub fn subscribe(&self, subscriber_id: usize) -> Subscriber<T> {
        Subscriber(Arc::clone(&self.0[subscriber_id]))
    }

    pub fn push(&self, value: T) -> Result<(), MesoError> {
        self.0
            .iter()
            .try_for_each(|x| x.push(value.clone()).map_err(|_| MesoError::BuffersFull))?;
        Ok(())
    }
}

/// Subscriber or feeder to the broadcast.
#[derive(Debug)]
pub struct Subscriber<T: Clone>(Arc<ArrayQueue<T>>);

impl<T: Clone> Subscriber<T> {
    pub fn pop(&mut self) -> Option<T> {
        self.0.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom::sync::Arc as LoomArc;
    use loom::thread;

    #[test]
    fn loom_test_broadcaster_all_subscribers_receive() {
        loom::model(|| {
            const NUM_SUBSCRIBERS: usize = 3;
            const NUM_MESSAGES: usize = 5;

            let broadcaster = LoomArc::new(Broadcaster::new(NUM_SUBSCRIBERS, 16));
            let mut subscribers = Vec::new();

            for i in 0..NUM_SUBSCRIBERS {
                subscribers.push(broadcaster.subscribe(i));
            }

            let producer = thread::spawn({
                let broadcaster = broadcaster.clone();
                move || {
                    for i in 0..NUM_MESSAGES {
                        broadcaster.push(i).unwrap();
                    }
                }
            });

            let mut consumer_handles = Vec::new();
            for mut subscriber in subscribers {
                let handle = thread::spawn(move || {
                    let mut received = Vec::new();
                    while received.len() < NUM_MESSAGES {
                        if let Some(msg) = subscriber.pop() {
                            received.push(msg);
                        }
                    }

                    // Check for order and no duplicates
                    assert_eq!(received.len(), NUM_MESSAGES);
                    for (i, msg) in received.iter().enumerate().take(NUM_MESSAGES) {
                        assert_eq!(*msg, i);
                    }
                });
                consumer_handles.push(handle);
            }

            producer.join().unwrap();
            for handle in consumer_handles {
                handle.join().unwrap();
            }
        });
    }

    #[test]
    fn loom_test_concurrent_pushes_and_pops() {
        loom::model(|| {
            const NUM_THREADS: usize = 1;
            const MESSAGES_PER_THREAD: usize = 3;

            let broadcaster = LoomArc::new(Broadcaster::<usize>::new(NUM_THREADS, 32));
            let mut subscribers = Vec::new();
            for i in 0..NUM_THREADS {
                subscribers.push(broadcaster.subscribe(i));
            }

            let mut producer_handles = Vec::new();
            for i in 0..NUM_THREADS {
                let broadcaster = broadcaster.clone();
                let handle = thread::spawn(move || {
                    for j in 0..MESSAGES_PER_THREAD {
                        // Unique message from each thread
                        let msg = i * MESSAGES_PER_THREAD + j;
                        broadcaster.push(msg).unwrap();
                    }
                });
                producer_handles.push(handle);
            }

            let mut consumer_handles = Vec::new();
            for mut subscriber in subscribers {
                let handle = thread::spawn(move || {
                    let mut received_count = 0;
                    let mut received_sum = 0;
                    let total_messages = NUM_THREADS * MESSAGES_PER_THREAD;

                    while received_count < total_messages {
                        if let Some(msg) = subscriber.pop() {
                            received_sum += msg;
                            received_count += 1;
                        }
                    }

                    assert_eq!(received_count, total_messages);

                    let expected_sum: usize = (0..total_messages).sum();
                    assert_eq!(received_sum, expected_sum);
                });
                consumer_handles.push(handle);
            }

            for handle in producer_handles {
                handle.join().unwrap();
            }

            for handle in consumer_handles {
                handle.join().unwrap();
            }
        });
    }
}
