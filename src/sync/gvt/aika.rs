use std::sync::Arc;

use bytemuck::{Pod, Zeroable};

use crate::{
    comms::{
        spmc::{Broadcast, Subscriber},
        spsc::BufferWheel,
    },
    logging::journal::Journal,
    sync::ComputeLayout,
    MesoError,
};

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Block<const BANDWIDTH: usize> {
    pub start: u64,
    pub dur: u64,
    pub max_dur: u64,
    pub sends: usize,
    pub recvs_current_block: usize,
    pub delayed_recvs: [usize; BANDWIDTH],
    pub local_corrections: isize,
    pub delayed_corrections: [isize; BANDWIDTH],
    pub block_nmb: usize,
    pub producer_id: usize,
}

impl<const BANDWIDTH: usize> Block<BANDWIDTH> {
    pub fn new(start: u64, dur: u64, block_nmb: usize, producer_id: usize) -> Self {
        Self {
            start,
            dur,
            max_dur: dur,
            sends: 0,
            recvs_current_block: 0,
            delayed_recvs: [0; BANDWIDTH],
            local_corrections: 0,
            delayed_corrections: [0; BANDWIDTH],
            block_nmb,
            producer_id,
        }
    }

    pub fn send(&mut self) {
        self.sends += 1
    }

    pub fn recv(&mut self, commit_time: u64, rollback_correction: bool) -> Result<(), MesoError> {
        if commit_time < self.start {
            let real_diff = self.start - commit_time - 1;
            let blocks = (real_diff / self.dur) as usize;
            if blocks >= BANDWIDTH {
                return Err(MesoError::DistantBlocks(blocks));
            }
            if !rollback_correction {
                self.delayed_recvs[blocks] += 1;
            } else {
                self.delayed_corrections[blocks] += 1;
            }
        } else {
            self.recvs_current_block += 1;
        }
        Ok(())
    }

    pub fn send_anti(&mut self, commit_time: u64) -> Result<(), MesoError> {
        let real_diff = self.start - commit_time - 1;
        let blocks = (real_diff / self.max_dur) as usize;
        if blocks >= BANDWIDTH {
            return Err(MesoError::DistantBlocks(blocks));
        }
        self.delayed_corrections[blocks] -= 1;
        Ok(())
    }

    pub fn block_id(&self) -> (usize, usize) {
        (self.producer_id, self.block_nmb)
    }
}

unsafe impl<const BANDWIDTH: usize> Send for Block<BANDWIDTH> {}
unsafe impl<const BANDWIDTH: usize> Sync for Block<BANDWIDTH> {}

unsafe impl<const BANDWIDTH: usize> Pod for Block<BANDWIDTH> {}
unsafe impl<const BANDWIDTH: usize> Zeroable for Block<BANDWIDTH> {}

pub struct BlockProcessor<const BANDWIDTH: usize> {
    mode: ComputeLayout,
    block_receiver_centralized: Option<Vec<Arc<BufferWheel<BANDWIDTH, Block<BANDWIDTH>>>>>,
    safe_point_centralized: Option<Arc<Broadcast<BANDWIDTH, u64>>>,
    centralized_registrations: usize,
    block_receiver_decentralized: Option<Vec<Subscriber<BANDWIDTH, Block<BANDWIDTH>>>>,
}

impl<const BANDWIDTH: usize> BlockProcessor<BANDWIDTH> {
    pub fn new(mode: ComputeLayout) -> Result<Self, MesoError> {
        let block_receiver_centralized = match mode {
            ComputeLayout::HubSpoke => Some(Vec::new()),
            ComputeLayout::Decentralized => None,
        };
        let safe_point_centralized = match mode {
            ComputeLayout::HubSpoke => Some(Arc::new(Broadcast::new()?)),
            ComputeLayout::Decentralized => None,
        };
        let block_receiver_decentralized = match mode {
            ComputeLayout::HubSpoke => None,
            ComputeLayout::Decentralized => Some(Vec::new()),
        };
        Ok(Self {
            mode,
            block_receiver_centralized,
            safe_point_centralized,
            centralized_registrations: 0,
            block_receiver_decentralized,
        })
    }

    pub fn register_centralized_producer(&mut self) -> Result<BlockSpoke<BANDWIDTH>, MesoError> {
        if self.mode != ComputeLayout::HubSpoke {
            return Err(MesoError::ComputeLayoutExpectationMismatch(self.mode));
        }
        let wheel = Arc::new(BufferWheel::new());
        let cloned = Arc::clone(&wheel);
        self.block_receiver_centralized
            .as_mut()
            .unwrap()
            .push(wheel);
        let sub = self
            .safe_point_centralized
            .as_mut()
            .unwrap()
            .register_subscriber();
        self.centralized_registrations += 1;
        Ok(BlockSpoke {
            submitter: cloned,
            subscriber: sub,
            block: Block::new(0, 0, 0, self.centralized_registrations - 1),
        })
    }

    pub fn register_decentralized_producer(
        &mut self,
        sub: Subscriber<BANDWIDTH, Block<BANDWIDTH>>,
    ) -> Result<(), MesoError> {
        if self.mode != ComputeLayout::Decentralized {
            return Err(MesoError::ComputeLayoutExpectationMismatch(self.mode));
        }
        self.block_receiver_decentralized
            .as_mut()
            .unwrap()
            .push(sub);
        Ok(())
    }

    pub fn register_producer(
        &mut self,
        sub: Option<Subscriber<BANDWIDTH, Block<BANDWIDTH>>>,
    ) -> Result<Option<BlockSpoke<BANDWIDTH>>, MesoError> {
        match self.mode {
            ComputeLayout::HubSpoke => Ok(Some(self.register_centralized_producer()?)),
            ComputeLayout::Decentralized => {
                let sub = sub.ok_or(MesoError::ComputeLayoutExpectationMismatch(self.mode))?;
                self.register_decentralized_producer(sub)?;
                Ok(None)
            }
        }
    }

    pub fn poll(&mut self) -> Result<Vec<Option<Vec<Block<BANDWIDTH>>>>, MesoError> {
        let mut output = Vec::new();
        match self.mode {
            ComputeLayout::HubSpoke => {
                let comms = self.block_receiver_centralized.as_mut().unwrap();
                for i in comms {
                    let mut planet_blocks = Vec::new();
                    for _ in 0..BANDWIDTH {
                        match i.read() {
                            Ok(block) => planet_blocks.push(block),
                            Err(err) => {
                                if let MesoError::NoPendingUpdates = err {
                                    break;
                                }
                                return Err(err);
                            }
                        }
                    }
                    if !planet_blocks.is_empty() {
                        output.push(Some(planet_blocks));
                        continue;
                    }
                    output.push(None);
                }
            }
            ComputeLayout::Decentralized => {
                // poll subscribers
            }
        }
        Ok(output)
    }

    pub fn broadcast_new_safe_point(&mut self, gvt: u64) -> Result<(), MesoError> {
        if self.mode != ComputeLayout::HubSpoke {
            return Err(MesoError::ComputeLayoutExpectationMismatch(self.mode));
        }
        self.safe_point_centralized.as_mut().unwrap().broadcast(gvt);
        Ok(())
    }
}

pub struct Consensus<const BANDWIDTH: usize> {
    pub processor: BlockProcessor<BANDWIDTH>,
    queue: Vec<[Option<Block<BANDWIDTH>>; BANDWIDTH]>,
    next: Vec<Option<Block<BANDWIDTH>>>,
    pub blocks: Journal,
    pub safe_point: u64,
    pub block_nmb: usize,
}

impl<const BANDWIDTH: usize> Consensus<BANDWIDTH> {
    pub fn new(mode: ComputeLayout, batch_size: usize) -> Result<Self, MesoError> {
        let blocksize = BANDWIDTH * 16 + 48;
        Ok(Self {
            processor: BlockProcessor::new(mode)?,
            queue: Vec::new(),
            next: Vec::new(),
            blocks: Journal::init(batch_size * blocksize),
            safe_point: 0,
            block_nmb: 0,
        })
    }

    pub fn register_producer(
        &mut self,
        sub: Option<Subscriber<BANDWIDTH, Block<BANDWIDTH>>>,
    ) -> Result<Option<BlockSpoke<BANDWIDTH>>, MesoError> {
        self.processor.register_producer(sub)
    }

    pub fn poll_n_slot(&mut self) -> Result<(), MesoError> {
        let new_blocks = self.processor.poll()?;
        for (i, planet) in new_blocks.into_iter().enumerate() {
            if let Some(blocks) = planet {
                for block in blocks {
                    let diff = block.block_nmb - self.block_nmb - 1;
                    if diff == 0 {
                        self.next[i] = Some(block);
                        continue;
                    }
                    if diff > BANDWIDTH {
                        return Err(MesoError::DistantBlocks(diff));
                    }
                    self.queue[i][diff - 1] = Some(block);
                }
            }
        }
        Ok(())
    }

    pub fn fetch_latest_uncommited_blocks(
        &mut self,
    ) -> Result<Vec<Option<Block<BANDWIDTH>>>, MesoError> {
        let mut latests = Vec::new();
        for i in &self.next {
            latests.push(*i);
        }

        for (producer, row) in self.queue.iter().enumerate() {
            if let Some(block) = row.iter().rev().find_map(|&x| x) {
                let cloned = Some(block);
                latests[producer] = cloned;
            }
        }
        Ok(latests)
    }

    pub fn check_update_safe_point(&mut self) -> Result<Option<u64>, MesoError> {
        if !self.next.iter().all(|x| x.is_some()) {
            return Ok(None);
        }
        let mut start = 0;
        let mut dur = 0;

        let mut sends = 0;
        let mut recvs = 0;
        let mut delayed_recvs = [0usize; BANDWIDTH];
        let mut correction_factor = 0isize;
        for block in &mut self.next.iter_mut().flatten() {
            if start == dur && dur == 0 {
                start = block.start;
                dur = block.dur;
            }
            if dur != block.dur || start != block.start {
                return Err(MesoError::MismatchBlockRanges);
            }
            sends += block.sends;
            recvs += block.recvs_current_block;
            delayed_recvs
                .iter_mut()
                .zip(block.delayed_recvs.iter())
                .for_each(|(x, y)| *x += *y);
            correction_factor += block.local_corrections;
        }
        let mut lates = 0;
        for producer_queue in self.queue.iter() {
            for (slot, maybe) in producer_queue.iter().enumerate() {
                match maybe {
                    Some(block) => {
                        lates += block.delayed_recvs[slot];
                        correction_factor += block.delayed_corrections[slot];
                    }
                    None => break,
                }
            }
        }

        let normalized_sends = sends.checked_add_signed(correction_factor).unwrap();
        let normalized_recvs = recvs + lates;

        if normalized_sends - normalized_recvs == 0 {
            self.commit_block(start, dur, sends, recvs, delayed_recvs, correction_factor);
            return Ok(Some(self.safe_point));
        }
        Ok(None)
    }

    pub fn check_status(&self) -> bool {
        if !self.next.iter().all(|x| x.is_none()) {
            return false;
        }
        for row in self.queue.iter() {
            if !row.iter().all(|x| x.is_none()) {
                return false;
            }
        }
        true
    }

    fn commit_block(
        &mut self,
        start: u64,
        dur: u64,
        sends: usize,
        recvs: usize,
        delayed_recvs: [usize; BANDWIDTH],
        net_corrections: isize,
    ) {
        self.block_nmb += 1;
        self.safe_point = start + dur;

        let mut block = Block::<BANDWIDTH>::new(start, dur, self.block_nmb, usize::MAX);
        block.recvs_current_block = recvs;
        block.sends = sends;
        block.delayed_recvs = delayed_recvs;
        block.local_corrections = net_corrections;

        self.blocks.write(block, self.safe_point, None);

        self.next.fill(None);
        for (producer, queue) in self.queue.iter_mut().enumerate() {
            if let Some(block) = queue[0].take() {
                self.next[producer] = Some(block);
            }
            for i in 0..(BANDWIDTH - 1) {
                queue[i] = queue[i + 1].take();
            }
            queue[BANDWIDTH - 1] = None;
        }
    }
}

pub struct BlockSpoke<const BANDWIDTH: usize> {
    pub submitter: Arc<BufferWheel<BANDWIDTH, Block<BANDWIDTH>>>,
    pub subscriber: Subscriber<BANDWIDTH, u64>,
    pub block: Block<BANDWIDTH>,
}
