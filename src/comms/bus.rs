use std::sync::Arc;

use bytemuck::{Pod, Zeroable};

use crate::{logging::journal::Journal, scheduling::{htw::Clock, Scheduleable}};

use super::spsc::BufferWheel;


#[derive(Debug, Copy, Clone)]
pub struct Message<T: Pod + Zeroable + 'static> {
    data: T,
    from: usize,
    to: usize,
    time_sent: u64,
    time_recv: u64
}

unsafe impl<T: Pod + Zeroable + 'static> Pod for Message<T> {}
unsafe impl<T: Pod + Zeroable + 'static> Zeroable for Message<T> {}  

pub struct LocalBus<T: Scheduleable + Pod + Zeroable + Ord + 'static, const MSG_SLOTS: usize, const CLOCK_SLOTS: usize, const CLOCK_HEIGHT: usize> {
    producers: [Vec<Arc<BufferWheel<MSG_SLOTS, Message<T>>>>; 2],
    tape: Journal,
    schedule: Clock<T, CLOCK_SLOTS, CLOCK_HEIGHT>,
}