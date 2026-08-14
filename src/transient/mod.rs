//! Arena-backed storage for stamp-ordered records in speculative simulation.
//!
//! Values are bump-allocated into uniform chunks owned by a [`Domain`] and
//! addressed by copyable [`Handle`]s; a record pairs a handle with the
//! [`Stamp`] giving its place in the canonical order. Memory is reclaimed
//! wholesale from either end of history, never per value:
//!
//! - Chop: once time is confirmed up to some bound (e.g. GVT), chunks lying
//!   entirely below it are released off the front.
//! - Rollback: a [`Cursor`] captured before an event runs restores the bump
//!   position, discarding everything allocated after it.
//!
//! Chunk ids are global and monotone (`base + index` into the deque), so
//! releasing from the front only bumps `base` and never invalidates ids
//! already stored in live handles or cursors.

pub mod ds;
#[cfg(any(test, feature = "testing"))]
mod testing;

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::cmp::max;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::MesoError;

const CHUNK_ALIGN: usize = 16;

/// Canonical ordering: virtual time first, `seq` breaks ties.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Stamp {
    pub time: u64,
    // tie breaker sequencing number
    pub seq: u32,
}

impl Stamp {
    // maximum time and sequence number to skip partitions.
    fn max() -> Self {
        Self {
            time: u64::MAX,
            seq: u32::MAX,
        }
    }
}

#[derive(Debug)]
/// Handle for a value of type `T` allocated from a [`Domain`].
///
/// Copyable; holds the raw pointer and the id of its home chunk.
pub struct Handle<T> {
    ptr: NonNull<T>,
    home_chunk: u32,
    _t: PhantomData<*mut T>,
}

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Handle<T> {}

impl<T> Handle<T> {
    /// Id of the chunk the value lives in.
    pub fn chunk(self) -> u32 {
        self.home_chunk
    }
    /// Live only while the owning index entry is live.
    pub unsafe fn get(self) -> *mut T {
        self.ptr.as_ptr()
    }
}

#[derive(Debug)]
/// A record is the canonical order and handle for a value of type `V`.
struct Record<V> {
    stamp: Stamp,
    value: Handle<V>,
}

impl<V> Clone for Record<V> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<V> Copy for Record<V> {}

/// Fixed-capacity slab. Chop and rollback release these wholesale.
struct Chunk<V> {
    slots: Box<[MaybeUninit<Record<V>>]>,
    /// Initialized prefix of `slots`.
    len: usize,
    /// Stamp bounds of the records held, so searches can skip whole
    /// partitions. `lo` starts maxed so an empty chunk sorts after everything.
    lo: Stamp,
    seal: bool,
}

impl<V> Chunk<V> {
    fn new(slot_size: usize) -> Result<Self, MesoError> {
        if slot_size == 0 {
            return Err(MesoError::InitializedWithNoSlots);
        }
        Ok(Self {
            slots: Box::new_uninit_slice(slot_size),
            len: 0,
            lo: Stamp::max(),
            seal: false,
        })
    }
    fn reset(&mut self) {
        self.seal = false;
        self.len = 0;
        self.lo = Stamp::max();
    }
    fn full(&self) -> bool {
        self.len == self.slots.len()
    }
    /// The initialized prefix, as records.
    fn records(&self) -> &[Record<V>] {
        let s = &self.slots[..self.len];
        unsafe { &*(s as *const [MaybeUninit<Record<V>>] as *const [Record<V>]) }
    }

    fn write(&mut self, record: Record<V>) -> Result<(), Record<V>> {
        if self.full() || self.seal {
            return Err(record);
        }
        let len = self.len;
        if self.len == 0 {
            self.lo = record.stamp;
        }
        self.slots[len].write(record);
        self.len += 1;
        if self.full() {
            self.seal();
        }
        Ok(())
    }

    fn seal(&mut self) {
        self.seal = true
    }
}

/// End of the newest allocation a timeline still references after rollback.
/// The associated `Domain`'s bump may be rewound no further back than the
/// max of these across every timeline allocating from it.
#[derive(Clone, Copy, Debug)]
pub struct HighMark {
    /// Home chunk of the newest surviving record.
    pub chunk: u32,
    /// One past the end of that record's value in the raw chunk.
    pub end: NonNull<u8>,
}

pub struct Timeline<V> {
    chunks: VecDeque<Chunk<V>>,
    free: Vec<Chunk<V>>,
    latest: Option<Stamp>,
    std_chunk_size: usize,
    domain_id: usize,
    commit_horizon: Option<u64>
}

impl<V> Timeline<V> {
    pub fn new(std_chunk_size: usize, d: &Domain) -> Result<Self, MesoError> {
        if std_chunk_size == 0 {
            return Err(MesoError::InitializedWithNoSlots);
        }
        Ok(Self {
            domain_id: d.id,
            chunks: VecDeque::new(),
            free: Vec::new(),
            latest: None,
            std_chunk_size,
            commit_horizon: None,
        })
    }

    fn fetch_fresh_chunk(&mut self) -> Result<Chunk<V>, MesoError> {
        if self.chunks.back().is_some() {
            let back = self.chunks.back_mut().unwrap();
            if !back.seal {
                back.seal();
            }
        }
        let mut chunk = match self.free.pop() {
            Some(c) => c,
            None => Chunk::new(self.std_chunk_size)?,
        };
        chunk.reset();
        Ok(chunk)
    }

    pub unsafe fn live_state<'d>(&self, d: &'d Domain) -> Result<Option<&'d V>, MesoError> {
        if d.id != self.domain_id {
            return Err(MesoError::ForeignDomain);
        }
        match self.live_record() {
            Some(record) => Ok(Some(record.value.ptr.as_ref())),
            None => Ok(None),
        }
    }

    fn live_record(&self) -> Option<&Record<V>> {
        if self.chunks.len() == 0 {
            return None;
        };
        let record = self.chunks.back()?;
        let live = record.records().last()?;

        Some(live)
    }

    pub fn record(&mut self, d: &mut Domain, value: V, stamp: Stamp) -> Result<(), MesoError> {
        if d.id != self.domain_id {
            return Err(MesoError::ForeignDomain);
        }
        if let Some(t) = self.commit_horizon {
            if stamp.time <= t {
                return Err(MesoError::TimeTravel);
            }
        }
        if let Some(l) = self.latest {
            if stamp <= l {
                return Err(MesoError::TimestampMonotonicityFailure);
            }
        }

        let value = d.alloc(value)?;
        let record = Record { stamp, value };

        if self.chunks.len() != 0 {
            let tail = self.chunks.back_mut().unwrap();
            let Err(_) = tail.write(record) else {
                self.latest = Some(stamp);
                return Ok(());
            };
        }

        let mut chunk = self.fetch_fresh_chunk()?;
        chunk.write(record).ok().expect("fresh chunk has capacity");
        self.chunks.push_back(chunk);
        self.latest = Some(stamp);

        Ok(())
    }

    /// Rollback hook for this timeline: discard every record of type V with `stamp.time >= to`.
    pub fn partial_rollback(&mut self, to: u64) -> Result<Option<HighMark>, MesoError> {
        if let Some(t) = self.commit_horizon { 
            if to <= t {
                return Err(MesoError::PastTheHorizon) 
            }
        }
        // Recycle wholly-invalid chunks off the back
        while self.chunks.back().is_some_and(|c| c.lo.time >= to) {
            let mut dead = self.chunks.pop_back().unwrap();
            dead.reset();
            self.free.push(dead);
        }

        let Some(back) = self.chunks.back_mut() else {
            self.latest = None;
            return Ok(None);
        };
        let keep = back.records().partition_point(|r| r.stamp.time < to);
        back.len = keep;
        back.seal = back.full();

        let last = *back.records().last().expect("lo.time < to ⇒ keep ≥ 1");
        self.latest = Some(last.stamp);
        let end = unsafe {
            NonNull::new_unchecked(last.value.ptr.as_ptr().cast::<u8>().wrapping_add(size_of::<V>()))
        };
        Ok(Some(HighMark {
            chunk: last.value.home_chunk,
            end,
        }))
    }

    /// Chop hook for this timeline: retire history superseded at or before `until`
    pub fn partial_chop(&mut self, until: u64) -> Option<u32> {
        let mut update_horizon = until;
        if let Some(t) = self.commit_horizon {
            update_horizon = max(t, until);
        }
        self.commit_horizon = Some(update_horizon);
        // A front chunk is wholly superseded iff the next chunk already opens
        // at or before `until`
        while self.chunks.get(1).is_some_and(|next| next.lo.time <= until) {
            let mut dead = self.chunks.pop_front().unwrap();
            dead.reset();
            self.free.push(dead);
        }

        let front = self.chunks.front_mut()?;

        // Compact the boundary chunk
        let idx = front.records().partition_point(|r| r.stamp.time <= until);
        if idx > 1 {
            let start = idx - 1;
            front.slots.copy_within(start..front.len, 0);
            front.len -= start;
            front.lo = front.records()[0].stamp;
            front.seal = front.full();
        }

        let floor = front.records().first().map(|r| r.value.home_chunk);
        floor
    }
}

/// Storage chunk for raw bytes.
struct RawChunk {
    ptr: NonNull<u8>,
    layout: Layout,
    std: bool,
}

/// Position of the bump pointer. Captured before an event runs; restored if that
/// event is rolled back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Cursor {
    pub chunk: u32,
    pub offset: u32,
}

static NEXT_DOMAIN: AtomicUsize = AtomicUsize::new(0);
/// Bump arena over a deque of uniform chunks. The back of the deque is always
/// the live chunk being bumped into; oversize requests get exact-fit chunks of
/// their own. Reclamation is wholesale from either end, never per allocation.
pub struct Domain {
    chunks: VecDeque<RawChunk>,
    /// Global id of `chunks[0]`. `release_front` bumps it, so ids already stored
    /// in live `Handle`s and `Cursor`s stay valid and never need rewriting.
    base: u32,
    /// Recycled standard-size chunks only. Exact-fit oversize chunks are freed,
    /// so every chunk in here is interchangeable — no size classes, no fit logic.
    free: Vec<RawChunk>,
    cursor: usize,
    chunk_size: usize,
    id: usize,
}

impl Domain {
    /// Arena over `chunk_size`-byte chunks (rounded up to `CHUNK_ALIGN`).
    /// Rejects zero (`InitializedWithNoSlots`) and sizes whose aligned value
    /// exceeds `u32::MAX` (`ChunkSizeTooLarge`) — offsets travel as `u32`.
    pub fn new(chunk_size: usize) -> Result<Self, MesoError> {
        if chunk_size == 0 {
            return Err(MesoError::InitializedWithNoSlots);
        }
        let chunk_size = (chunk_size + CHUNK_ALIGN - 1) & !(CHUNK_ALIGN - 1);
        // Offsets travel as u32 in `bump` returns and `Cursor`.
        if chunk_size > u32::MAX as usize {
            return Err(MesoError::ChunkSizeTooLarge);
        }
        let id = NEXT_DOMAIN.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            chunks: VecDeque::new(),
            base: 0,
            free: Vec::new(),
            cursor: 0,
            chunk_size,
            id,
        })
    }

    /// Current bump position. Capture before an event runs.
    pub fn cursor(&self) -> Cursor {
        if self.chunks.is_empty() {
            // Nothing allocated yet; restoring here means "rewind to the start".
            return Cursor {
                chunk: self.base,
                offset: 0,
            };
        }
        Cursor {
            chunk: self.back_id(),
            offset: self.cursor as u32,
        }
    }

    /// Translate a rollback mark into a bump position in this domain.
    fn cursor_at(&self, mark: HighMark) -> Result<Cursor, MesoError> {
        let chunk = self.fetch_chunk(mark.chunk)?;
        let off = (mark.end.as_ptr() as usize)
            .checked_sub(chunk.ptr.as_ptr() as usize)
            .ok_or(MesoError::MarkOutsideHomeChunk)?;
        if off > chunk.layout.size() {
            return Err(MesoError::MarkOutsideHomeChunk);
        };
        Ok(Cursor {
            chunk: mark.chunk,
            offset: off as u32,
        })
    }

    /// Reserve room for `layout`; returns (chunk id, byte offset).
    fn bump(&mut self, layout: Layout) -> (u32, u32) {
        let (size, align) = (layout.size(), layout.align());

        if size > self.chunk_size || align > CHUNK_ALIGN {
            // Own chunk, exact fit, never bumped into again. Then reopen, so the
            // back of the deque is always the live chunk. Costs the tail of the
            // chunk we were in; buys one fewer piece of state to keep honest.
            let c = self.alloc_raw_chunk(layout, false);
            self.chunks.push_back(c);
            let id = self.back_id();
            self.open();
            return (id, 0);
        }

        self.ensure_open();
        let cap = self.chunks.back().unwrap().layout.size();
        let off = (self.cursor + align - 1) & !(align - 1);

        if off + size > cap {
            self.open();
            self.cursor = size;
            return (self.back_id(), 0);
        }
        self.cursor = off + size;
        (self.back_id(), off as u32)
    }

    fn ensure_open(&mut self) {
        if self.chunks.is_empty() {
            self.open();
        }
    }

    fn open(&mut self) {
        let layout = Layout::from_size_align(self.chunk_size, CHUNK_ALIGN).unwrap();
        let c = self
            .free
            .pop()
            .unwrap_or_else(|| self.alloc_raw_chunk(layout, true));
        self.chunks.push_back(c);
        self.cursor = 0;
    }

    fn back_id(&self) -> u32 {
        self.base + self.chunks.len() as u32 - 1
    }

    fn fetch_chunk(&self, id: u32) -> Result<&RawChunk, MesoError> {
        if !(id >= self.base) {
            return Err(MesoError::BelowChopLine);
        };
        self.chunks
            .get((id - self.base) as usize)
            .ok_or(MesoError::PastTheHorizon)
    }

    /// FIFO release of committed history: frees every chunk with id < `keep_from`.
    /// Advances `base`, so surviving ids never move. No-op when keep_from == base.
    ///
    /// # Safety
    /// `keep_from` must be a liveness floor: no live `Handle` and no restorable
    /// `Cursor` may reference a chunk with id < `keep_from`. Under the write
    /// contract (single writer, stamps non-decreasing between rollbacks), the home
    /// chunk of the newest record at-or-before GVT satisfies this.
    pub unsafe fn release_front(&mut self, keep_from: u32) {
        debug_assert!(
            keep_from >= self.base && (self.chunks.is_empty() || keep_from <= self.back_id()),
            "chop floor out of range"
        );
        while self.base < keep_from && self.chunks.len() > 1 {
            let c = self.chunks.pop_front().unwrap();
            self.base += 1;
            unsafe { self.release(c) };
        }
    }

    unsafe fn release(&mut self, c: RawChunk) {
        if c.std {
            self.free.push(c);
        } else {
            unsafe { dealloc(c.ptr.as_ptr(), c.layout) };
        }
    }

    /// LIFO release of speculated allocation: frees every chunk with id >
    /// `to.chunk` and rewinds the bump to `to.offset`. Mirror of `release_front`.
    ///
    /// # Safety
    /// `to` must be a liveness ceiling: no live `Handle` may point at or past
    /// it. A `Cursor` captured before the rolled-back event satisfies this
    /// (stamps non-decreasing between rollbacks)
    pub unsafe fn restore(&mut self, to: Cursor) -> Result<(), MesoError> {
        if to.chunk < self.base {
            return Err(MesoError::BelowChopLine);
        };
        if !self.chunks.is_empty() {
            if to.chunk > self.back_id() {
                return Err(MesoError::PastTheHorizon);
            };
            while self.back_id() > to.chunk {
                let c = self.chunks.pop_back().unwrap();
                unsafe { self.release(c) };
            }

            self.cursor = to.offset as usize;
            if self.chunks.back().is_some_and(|c| !c.std) {
                self.open();
            }
            return Ok(());
        }
        if to.chunk > self.base {
            return Err(MesoError::PastTheHorizon);
        };
        Ok(())
    }

    pub unsafe fn rewind(&mut self, mark: HighMark) -> Result<(), MesoError> {
        let c = self.cursor_at(mark)?;
        unsafe { self.restore(c) }?;
        Ok(())
    }

    /// Only called when all timelines referencing to domain return None on partial_rollback
    /// # Safety
    /// Every timeline allocating from this domain must have returned `None` from
    /// `partial_rollback` — i.e. no live `Handle` and no restorable `Cursor` may
    /// reference any chunk in this domain.
    pub unsafe fn reset(&mut self) {
        self.base += self.chunks.len() as u32;
        while let Some(c) = self.chunks.pop_back() {
            unsafe { self.release(c) };
        }
        self.cursor = 0;
    }

    /// Write `val` into the arena and return its handle.
    ///
    /// Values are never dropped individually — storage is released wholesale
    /// by chop or rollback — so `T` should not own heap resources.
    pub fn alloc<T>(&mut self, val: T) -> Result<Handle<T>, MesoError> {
        if std::mem::needs_drop::<T>() {
            return Err(MesoError::NeedsDrop);
        }
        let (home_chunk, offset) = self.bump(Layout::new::<T>());
        let base = self.fetch_chunk(home_chunk)?.ptr.as_ptr();
        let p = unsafe { base.add(offset as usize) } as *mut T;
        unsafe { ptr::write(p, val) };
        Ok(Handle {
            ptr: unsafe { NonNull::new_unchecked(p) },
            home_chunk,
            _t: PhantomData,
        })
    }

    fn alloc_raw_chunk(&self, layout: Layout, std: bool) -> RawChunk {
        let layout = layout.align_to(CHUNK_ALIGN).unwrap().pad_to_align();
        let ptr = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout)
        };
        RawChunk { ptr, layout, std }
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        // Frees bytes, drops nothing. Timelines must be dropped first
        for c in self.chunks.drain(..).chain(self.free.drain(..)) {
            unsafe { dealloc(c.ptr.as_ptr(), c.layout) }
        }
    }
}

pub trait Transient {
    fn rollback(&mut self, to: u64);
    fn chop(&mut self, until: u64);
}
