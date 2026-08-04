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

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

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
        Self { time: u64::MAX, seq: u32::MAX }
    }
}
 
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

/// A record is the canonical order and handle for a value of type `V`.
struct Record<V> {
    stamp: Stamp,
    value: Handle<V>,
}

impl<V> Clone for Record<V> {
    fn clone(&self) -> Self { *self }
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
}

impl<V> Chunk<V> {
    fn new<const SLOTS: usize>() -> Self {
        let mut slots: Vec<MaybeUninit<Record<V>>> = Vec::with_capacity(SLOTS);
        unsafe { slots.set_len(SLOTS) };
      Self { 
        slots: slots.into_boxed_slice(), len: 0, lo: Stamp::max() }
    }
    fn reset(&mut self) {
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
}

/// Storage chunk for raw bytes.
struct RawChunk {
    ptr: NonNull<u8>,
    layout: Layout,
}

/// Position of the bump pointer. Captured before an event runs; restored if that
/// event is rolled back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Cursor {
    pub chunk: u32,
    pub offset: u32,
}

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
}

impl Domain {
    pub fn new(chunk_size: usize) -> Self {
        let chunk_size = (chunk_size + CHUNK_ALIGN - 1) & !(CHUNK_ALIGN - 1);
        assert!(chunk_size >= CHUNK_ALIGN);
        // Offsets travel as u32 in `bump` returns and `Cursor`.
        assert!(chunk_size <= u32::MAX as usize, "chunk_size must fit u32 offsets");
        Self {
            chunks: VecDeque::new(),
            base: 0,
            free: Vec::new(),
            cursor: 0,
            chunk_size,
        }
    }

    /// Current bump position. Capture before an event runs.
    pub fn cursor(&self) -> Cursor {
        if self.chunks.is_empty() {
            // Nothing allocated yet; restoring here means "rewind to the start".
            return Cursor { chunk: self.base, offset: 0 };
        }
        Cursor { chunk: self.back_id(), offset: self.cursor as u32 }
    }

    /// Reserve room for `layout`; returns (chunk id, byte offset).
    fn bump(&mut self, layout: Layout) -> (u32, u32) {
        let (size, align) = (layout.size(), layout.align());
 
        if size > self.chunk_size || align > CHUNK_ALIGN {
            // Own chunk, exact fit, never bumped into again. Then reopen, so the
            // back of the deque is always the live chunk. Costs the tail of the
            // chunk we were in; buys one fewer piece of state to keep honest.
            let c = self.alloc_raw_chunk(layout);
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
        let c = self.free.pop().unwrap_or_else(|| self.alloc_raw_chunk(layout));
        self.chunks.push_back(c);
        self.cursor = 0;
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
        while self.base < keep_from && self.chunks.len() > 1 {
            let c = self.chunks.pop_front().unwrap();
            self.base += 1;
            self.release(c);
        }
    }

    fn release(&mut self, c: RawChunk) {
        if c.layout.size() == self.chunk_size {
            self.free.push(c);
        } else {
            unsafe { dealloc(c.ptr.as_ptr(), c.layout) }
        }
    }
 
    fn back_id(&self) -> u32 {
        self.base + self.chunks.len() as u32 - 1
    }

    fn fetch_chunk(&self, id: u32) -> &RawChunk {
        debug_assert!(id >= self.base, "handle below the chop line");
        &self.chunks[(id - self.base) as usize]
    }

    /// Write `val` into the arena and return its handle.
    ///
    /// Values are never dropped individually — storage is released wholesale
    /// by chop or rollback — so `T` should not own heap resources.
    pub fn alloc<T>(&mut self, val: T) -> Handle<T> {
        let (home_chunk, offset) = self.bump(Layout::new::<T>());
        let base = self.fetch_chunk(home_chunk).ptr.as_ptr();
        let p = unsafe { base.add(offset as usize) } as *mut T;
        unsafe { ptr::write(p, val) };
        Handle { ptr: unsafe { NonNull::new_unchecked(p) }, home_chunk, _t: PhantomData }
    }

    fn alloc_raw_chunk(&self, layout: Layout) -> RawChunk {
        let layout = layout.align_to(CHUNK_ALIGN).unwrap().pad_to_align();
        let ptr = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(ptr) else { handle_alloc_error(layout) };
        RawChunk { ptr, layout }
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

#[cfg(test)]
mod tests {
    use super::*;

    // 64-byte chunks: exactly eight u64 slots each.
    const CS: usize = 64;

    fn filled(n: u64) -> (Domain, Vec<Handle<u64>>) {
        let mut d = Domain::new(CS);
        let hs = (0..n).map(|i| d.alloc(i)).collect();
        (d, hs)
    }

    // T-1: chop frees the exact prefix and recycles standard chunks.
    #[test]
    fn chop_frees_prefix_and_recycles() {
        let (mut d, hs) = filled(24); // chunks 0, 1, 2
        assert_eq!(d.back_id(), 2);
        assert_eq!((hs[0].chunk(), hs[8].chunk(), hs[16].chunk()), (0, 1, 2));

        unsafe { d.release_front(1) };
        assert_eq!((d.base, d.chunks.len(), d.free.len()), (1, 2, 1));

        unsafe { d.release_front(2) };
        assert_eq!((d.base, d.chunks.len(), d.free.len()), (2, 1, 2));

        // idempotent at the same floor
        unsafe { d.release_front(2) };
        assert_eq!((d.base, d.chunks.len(), d.free.len()), (2, 1, 2));
    }

    // T-2: survivors stay intact through chop and recycled-chunk reuse.
    #[test]
    fn survivors_intact_after_chop_and_reuse() {
        let (mut d, hs) = filled(24);
        unsafe { d.release_front(2) };
        for (i, h) in hs.iter().enumerate().skip(16) {
            assert_eq!(unsafe { *h.get() }, i as u64);
        }

        // Refill: pops both recycled chunks back off the free list.
        let new: Vec<Handle<u64>> = (100..116u64).map(|i| d.alloc(i)).collect();
        assert_eq!(d.free.len(), 0);
        // Ids keep counting past the chopped prefix.
        assert_eq!((new[0].chunk(), new[8].chunk()), (3, 4));

        for (i, h) in hs.iter().enumerate().skip(16) {
            assert_eq!(unsafe { *h.get() }, i as u64);
        }
        for (i, h) in new.iter().enumerate() {
            assert_eq!(unsafe { *h.get() }, 100 + i as u64);
        }
    }

    // T-3: no-op edges, including calls on an empty Domain.
    #[test]
    fn noop_edges() {
        let mut d = Domain::new(CS);
        unsafe { d.release_front(0) }; // empty, floor == base
        assert_eq!((d.base, d.chunks.len()), (0, 0));

        let (mut d, _hs) = filled(24);
        unsafe { d.release_front(0) }; // floor == base: no-op
        assert_eq!((d.base, d.chunks.len(), d.free.len()), (0, 3, 0));

        unsafe { d.release_front(d.back_id()) }; // frees all but the open chunk
        assert_eq!((d.base, d.chunks.len()), (2, 1));
    }

    // T-4: a mis-derived floor fails loudly in debug builds.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "chop floor")]
    fn floor_above_open_chunk_asserts() {
        let mut d = Domain::new(CS);
        d.alloc(1u64);
        unsafe { d.release_front(5) };
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "chop floor")]
    fn floor_below_base_asserts() {
        let (mut d, _hs) = filled(24);
        unsafe { d.release_front(2) };
        unsafe { d.release_front(1) }; // derived from a fossil record
    }

    // T-5: cursor capture, including on a fresh Domain.
    #[test]
    fn cursor_capture() {
        let mut d = Domain::new(CS);
        assert_eq!(d.cursor(), Cursor { chunk: 0, offset: 0 });
        d.alloc(7u64);
        assert_eq!(d.cursor(), Cursor { chunk: 0, offset: 8 });
        for i in 0..8u64 {
            d.alloc(i);
        }
        assert_eq!(d.cursor(), Cursor { chunk: 1, offset: 8 });
    }

    // T-6: mixed standard/oversize run frees the right kinds.
    #[test]
    fn mixed_chop_dealloc_vs_recycle() {
        let mut d = Domain::new(CS);
        let a = d.alloc(1u64); // chunk 0
        let big = d.alloc([7u8; 100]); // oversize: own chunk 1, reopens chunk 2
        let b = d.alloc(2u64); // chunk 2
        assert_eq!((a.chunk(), big.chunk(), b.chunk()), (0, 1, 2));

        unsafe { d.release_front(2) };
        // Chunk 0 recycled; exact-fit chunk 1 deallocated, not pooled.
        assert_eq!((d.base, d.chunks.len(), d.free.len()), (2, 1, 1));
        assert_eq!(unsafe { *b.get() }, 2);
    }

    // C-4: offsets travel as u32, so oversized chunk_size is rejected up front.
    #[test]
    #[should_panic(expected = "fit u32")]
    fn new_rejects_chunk_size_above_u32() {
        Domain::new(u32::MAX as usize + 1);
    }
}