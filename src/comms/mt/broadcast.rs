use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

use crate::MesoError;

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

    pub fn push(&mut self, value: T) -> Result<(), MesoError> {
        self.0.iter_mut().try_for_each(|x| {
            x.push(value.clone()).map_err(|_| MesoError::BuffersFull)
        })?;
        Ok(())
    }
} 


#[derive(Debug)]
pub struct Subscriber<T: Clone>(Arc<ArrayQueue<T>>);

impl<T: Clone> Subscriber<T> {
    pub fn pop(&mut self) -> Option<T> {
        self.0.pop()
    }
}