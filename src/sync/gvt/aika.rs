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
    pub delayed_recvs: [isize; BANDWIDTH],
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
            println!("added recv to slot {:?} in delayed queue", blocks);
            if !rollback_correction {
                self.delayed_recvs[blocks - 1] += 1;
            } else {
                self.delayed_corrections[blocks - 1] += 1;
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
        println!("subbed anti recv to slot {:?} in delayed queue", blocks);
        self.delayed_corrections[blocks] -= 1;
        Ok(())
    }

    pub fn recv_anti(&mut self, commit_time: u64) -> Result<(), MesoError> {
        if commit_time < self.start {
            let real_diff = self.start - commit_time - 1;
            let blocks = (real_diff / self.dur) as usize;
            if blocks >= BANDWIDTH {
                return Err(MesoError::DistantBlocks(blocks));
            }
            println!("added recv to slot {:?} in delayed queue", blocks);
            self.delayed_recvs[blocks] -= 1;
        } else {
            self.recvs_current_block -= 1;
        }
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

impl<const BANDWIDTH: usize> Default for Block<BANDWIDTH> {
    fn default() -> Self {
        Self {
            start: 0,
            dur: u64::MAX,
            max_dur: u64::MAX,
            sends: 0,
            recvs_current_block: 0,
            delayed_recvs: [0; BANDWIDTH],
            local_corrections: 0,
            delayed_corrections: [0; BANDWIDTH],
            block_nmb: 0,
            producer_id: usize::MAX,
        }
    }
}

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
                    let diff = block.block_nmb - self.block_nmb;
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
        let mut delayed_recvs = [0isize; BANDWIDTH];
        let mut correction_factor = 0isize;
        for block in &mut self.next.iter_mut().flatten() {
            if start == dur && dur == 0 {
                start = block.start;
                dur = block.dur;
            }
            println!(
                "dur: {dur} {:?}, start: {start} {:?}",
                block.dur, block.start
            );
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

        let normalized_sends = (sends.checked_add_signed(correction_factor).unwrap()) as isize;
        let normalized_recvs = recvs as isize + lates;
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
        delayed_recvs: [isize; BANDWIDTH],
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

#[cfg(test)]
mod unit_tests {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread,
        time::Duration,
    };

    use super::*;
    use crate::sync::ComputeLayout;

    const BANDWIDTH: usize = 16;
    const NUM_PRODUCERS: usize = 2;
    const BLOCK_DURATION: u64 = 100;

    fn setup_consensus(num_producers: usize) -> (Consensus<BANDWIDTH>, Vec<BlockSpoke<BANDWIDTH>>) {
        let mut consensus =
            Consensus::<BANDWIDTH>::new(ComputeLayout::HubSpoke, num_producers).unwrap();
        let mut spokes = Vec::new();

        for _ in 0..num_producers {
            let spoke = consensus.register_producer(None).unwrap().unwrap();
            spokes.push(spoke);
        }

        consensus.queue = vec![[None; BANDWIDTH]; num_producers];
        consensus.next = vec![None; num_producers];

        (consensus, spokes)
    }

    fn submit_block(spoke: &mut BlockSpoke<BANDWIDTH>, block: Block<BANDWIDTH>) {
        spoke.submitter.write(block).unwrap();
    }

    #[test]
    fn test_initialization_and_registration() {
        let (consensus, spokes) = setup_consensus(NUM_PRODUCERS);
        assert_eq!(consensus.processor.centralized_registrations, NUM_PRODUCERS);
        assert_eq!(spokes.len(), NUM_PRODUCERS);
        assert_eq!(consensus.safe_point, 0);
        assert!(consensus.check_status());
    }

    #[test]
    fn test_single_producer_gvt_advance() {
        let (mut consensus, mut spokes) = setup_consensus(1);
        let spoke = &mut spokes[0];

        let mut block1 = Block::new(0, BLOCK_DURATION, 0, 0);
        block1.send();
        block1.send();
        submit_block(spoke, block1);

        consensus.poll_n_slot().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert!(gvt_update.is_none());

        let mut block2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 0);
        block2.delayed_recvs[0] += 2;
        block2.send();
        block2.send();
        block2.recvs_current_block = 2;
        submit_block(spoke, block2);

        consensus.poll_n_slot().unwrap();

        let _ = consensus.check_update_safe_point().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert!(gvt_update.is_some());
        assert_eq!(gvt_update.unwrap(), 2 * BLOCK_DURATION);
        assert_eq!(consensus.safe_point, 2 * BLOCK_DURATION);
    }

    #[test]
    fn test_multi_producer_gvt_advance() {
        let (mut consensus, mut spokes) = setup_consensus(NUM_PRODUCERS);

        for (i, spoke) in spokes.iter_mut().enumerate().take(NUM_PRODUCERS) {
            let mut block1 = Block::new(0, BLOCK_DURATION, 0, i);
            block1.send();
            submit_block(spoke, block1);
        }

        consensus.poll_n_slot().unwrap();
        assert!(consensus.check_update_safe_point().unwrap().is_none());

        for (i, spoke) in spokes.iter_mut().enumerate().take(NUM_PRODUCERS - 1) {
            let mut block2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, i);
            block2.delayed_recvs[0] += 1;
            submit_block(spoke, block2);
        }

        let block2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, NUM_PRODUCERS - 1);
        submit_block(&mut spokes[NUM_PRODUCERS - 1], block2);

        consensus.poll_n_slot().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();
        println!("{:?}", gvt_update);
        assert!(gvt_update.is_none());

        let mut block3 = Block::new(2 * BLOCK_DURATION, BLOCK_DURATION, 2, NUM_PRODUCERS - 1);
        block3.delayed_recvs[1] += 1;
        submit_block(&mut spokes[NUM_PRODUCERS - 1], block3);

        for (i, spoke) in spokes.iter_mut().enumerate().take(NUM_PRODUCERS - 1) {
            let block3 = Block::new(2 * BLOCK_DURATION, BLOCK_DURATION, 2, i);
            submit_block(spoke, block3);
        }

        consensus.poll_n_slot().unwrap();
        let _ = consensus.check_update_safe_point().unwrap();
        consensus.poll_n_slot().unwrap();
        let _ = consensus.check_update_safe_point().unwrap();
        consensus.poll_n_slot().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();

        assert_eq!(gvt_update, Some(3 * BLOCK_DURATION));
        assert_eq!(consensus.safe_point, 3 * BLOCK_DURATION);
    }

    #[test]
    fn test_recv_greater_than_sends_blocks_gvt() {
        let (mut consensus, mut spokes) = setup_consensus(NUM_PRODUCERS);

        let block1_p1 = Block::new(0, BLOCK_DURATION, 0, 0);
        submit_block(&mut spokes[0], block1_p1);

        let mut block1_p2 = Block::new(0, BLOCK_DURATION, 0, 1);
        block1_p2.sends = 1;
        submit_block(&mut spokes[1], block1_p2);

        consensus.poll_n_slot().unwrap();
        assert!(consensus.check_update_safe_point().unwrap().is_none());

        let mut block2_p1 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 0);
        block2_p1.recvs_current_block = 2;
        submit_block(&mut spokes[0], block2_p1);

        let block2_p2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 1);
        submit_block(&mut spokes[1], block2_p2);

        consensus.poll_n_slot().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert!(gvt_update.is_none());
    }

    #[test]
    fn test_delayed_recvs_are_accounted() {
        let (mut consensus, mut spokes) = setup_consensus(1);
        let spoke = &mut spokes[0];

        let mut block1 = Block::new(0, BLOCK_DURATION, 0, 0);
        block1.sends = 1;
        submit_block(spoke, block1);
        consensus.poll_n_slot().unwrap();
        assert!(consensus.check_update_safe_point().unwrap().is_none());

        let block2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 0);
        submit_block(spoke, block2);
        consensus.poll_n_slot().unwrap();
        assert!(consensus.check_update_safe_point().unwrap().is_none());

        let mut block3 = Block::new(2 * BLOCK_DURATION, BLOCK_DURATION, 2, 0);
        block3.recv(BLOCK_DURATION / 2, false).unwrap();
        submit_block(spoke, block3);
        consensus.poll_n_slot().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert_eq!(gvt_update, Some(BLOCK_DURATION));
    }

    #[test]
    fn test_corrections_are_accounted() {
        let (mut consensus, mut spokes) = setup_consensus(1);
        let spoke = &mut spokes[0];

        let mut block1 = Block::new(0, BLOCK_DURATION, 0, 0);
        block1.sends = 5;
        block1.local_corrections = -2;
        submit_block(spoke, block1);
        consensus.poll_n_slot().unwrap();
        assert!(consensus.check_update_safe_point().unwrap().is_none());

        let mut block2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 0);
        block2.delayed_recvs[0] = 3;
        submit_block(spoke, block2);

        consensus.poll_n_slot().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert_eq!(gvt_update, Some(BLOCK_DURATION));
    }

    #[test]
    fn test_delayed_corrections_are_accounted() {
        let (mut consensus, mut spokes) = setup_consensus(1);
        let spoke = &mut spokes[0];

        let mut block1 = Block::new(0, BLOCK_DURATION, 0, 0);
        block1.sends = 2;
        submit_block(spoke, block1);
        consensus.poll_n_slot().unwrap();
        assert!(consensus.check_update_safe_point().unwrap().is_none());

        let mut block2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 0);
        block2.recvs_current_block = 3;
        block2.delayed_recvs[0] = 2;
        block2.send_anti(BLOCK_DURATION / 2).unwrap();
        submit_block(spoke, block2);
        consensus.poll_n_slot().unwrap();

        let mut block3 = Block::new(2 * BLOCK_DURATION, BLOCK_DURATION, 2, 0);
        block3.recv_anti(BLOCK_DURATION / 2).unwrap();
        submit_block(spoke, block3);
        consensus.poll_n_slot().unwrap();

        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert_eq!(gvt_update, Some(BLOCK_DURATION));
    }

    #[test]
    fn test_out_of_order_submission() {
        let (mut consensus, mut spokes) = setup_consensus(1);
        let spoke = &mut spokes[0];

        let mut block2 = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 0);
        block2.recvs_current_block = 1;
        submit_block(spoke, block2);

        consensus.poll_n_slot().unwrap();
        assert!(consensus.check_update_safe_point().unwrap().is_none());
        assert!(consensus.next[0].is_none());
        assert!(consensus.queue[0][0].is_some());

        let mut block1 = Block::new(0, BLOCK_DURATION, 0, 0);
        block1.sends = 1;
        block1.recvs_current_block = 1;
        submit_block(spoke, block1);

        consensus.poll_n_slot().unwrap();
        assert!(consensus.next[0].is_some());
        assert!(consensus.queue[0][0].is_some());

        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert_eq!(gvt_update, Some(BLOCK_DURATION));
        assert_eq!(consensus.safe_point, BLOCK_DURATION);

        assert!(consensus.next[0].is_some());
        assert!(consensus.queue[0][0].is_none());
    }

    #[test]
    fn test_non_monotonic_gvt_is_rejected() {
        let (mut consensus, mut spokes) = setup_consensus(2);

        // First, advance GVT to 100 correctly
        let mut b0p0 = Block::new(0, BLOCK_DURATION, 0, 0);
        b0p0.sends = 1;
        b0p0.recvs_current_block = 1;
        submit_block(&mut spokes[0], b0p0);
        let b0p1 = Block::new(0, BLOCK_DURATION, 0, 1);
        submit_block(&mut spokes[1], b0p1);

        consensus.poll_n_slot().unwrap();
        let gvt_update = consensus.check_update_safe_point().unwrap();
        assert_eq!(gvt_update, Some(BLOCK_DURATION));
        assert_eq!(consensus.safe_point, BLOCK_DURATION);

        // Now, producers should submit blocks for the next window (starting at 100).
        // Producer 0 submits a valid block.
        let b1p0_valid = Block::new(BLOCK_DURATION, BLOCK_DURATION, 1, 0);
        submit_block(&mut spokes[0], b1p0_valid);

        // Producer 1 submits a block with the correct number, but an old, invalid timestamp.
        let b1p1_invalid = Block::new(0, BLOCK_DURATION, 1, 1);
        submit_block(&mut spokes[1], b1p1_invalid);

        // Poll to load the new blocks into the `next` slots.
        consensus.poll_n_slot().unwrap();

        // Check for GVT update. This should fail because the blocks in `next` have mismatched start times.
        let result = consensus.check_update_safe_point();
        assert!(
            matches!(result, Err(MesoError::MismatchBlockRanges)),
            "Consensus should reject blocks with mismatched time ranges"
        );
    }

    #[test]
    fn test_parallel_producers_contention() {
        const PARALLEL_PRODUCERS: usize = 4;
        const BLOCKS_PER_PRODUCER: usize = 50;

        // Setup consensus that can be shared via Mutex
        let mut consensus =
            Consensus::<BANDWIDTH>::new(ComputeLayout::HubSpoke, PARALLEL_PRODUCERS).unwrap();
        let mut spokes = Vec::new();
        for _ in 0..PARALLEL_PRODUCERS {
            spokes.push(consensus.register_producer(None).unwrap().unwrap());
        }
        consensus.queue = vec![[None; BANDWIDTH]; PARALLEL_PRODUCERS];
        consensus.next = vec![None; PARALLEL_PRODUCERS];
        let shared_consensus = Arc::new(std::sync::Mutex::new(consensus));

        let running = Arc::new(AtomicBool::new(true));
        let mut handles = Vec::new();

        for i in 0..PARALLEL_PRODUCERS {
            // Each thread gets its own spoke. No sharing of spokes needed.
            let spoke = spokes.remove(0);
            let running_clone = Arc::clone(&running);

            let handle = thread::spawn(move || {
                for block_nmb in 0..BLOCKS_PER_PRODUCER {
                    if !running_clone.load(Ordering::Relaxed) {
                        break;
                    }
                    let start_time = (block_nmb as u64) * BLOCK_DURATION;

                    // Create a balanced block to ensure GVT can advance.
                    // A message is "sent" in block `k` and "received" in block `k+1`.
                    let mut block = Block::new(start_time, BLOCK_DURATION, block_nmb, i);
                    if block_nmb < BLOCKS_PER_PRODUCER - 1 {
                        block.sends = 1;
                    }
                    if block_nmb > 0 {
                        block.delayed_recvs[0] += 1;
                    }

                    // Submit block via the lock-free SPSC channel.
                    spoke.submitter.write(block).unwrap();
                    thread::sleep(Duration::from_micros(5));
                }
            });
            handles.push(handle);
        }

        // Main thread: poll and advance GVT until the target is reached.
        let final_gvt = (BLOCKS_PER_PRODUCER as u64) * BLOCK_DURATION;
        loop {
            let mut consensus_guard = shared_consensus.lock().unwrap();
            for _ in 0..10 {
                consensus_guard.poll_n_slot().unwrap();
                let _ = consensus_guard.check_update_safe_point();
            }

            if consensus_guard.safe_point >= final_gvt {
                break;
            }

            // Drop lock to let producers work.
            drop(consensus_guard);
            thread::sleep(Duration::from_nanos(10));
        }

        running.store(false, Ordering::Relaxed);
        for handle in handles {
            handle.join().unwrap();
        }

        let consensus_guard = shared_consensus.lock().unwrap();
        assert_eq!(consensus_guard.safe_point, final_gvt);
        assert_eq!(consensus_guard.block_nmb, BLOCKS_PER_PRODUCER);
        // Clean up remaining queued items from the final GVT round if any.
        let mut final_consensus = consensus_guard;
        final_consensus.poll_n_slot().unwrap();
        let _ = final_consensus.check_update_safe_point();
        assert!(
            final_consensus.check_status(),
            "Consensus queues not empty at end of test"
        );
    }
}
