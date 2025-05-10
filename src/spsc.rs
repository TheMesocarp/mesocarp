use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};

use crate::error::Error;

#[derive(Debug)]
pub struct BufferWheel<const N: usize, T> {
    buffers: [AtomicPtr<T>; N],
    read: AtomicUsize,
    write: AtomicUsize,
    full: AtomicBool,
}

impl<const N: usize, T> Default for BufferWheel<N, T> {
    fn default() -> Self {
        BufferWheel::new()
    }
}

impl<const N: usize, T> BufferWheel<N, T> {
    pub fn new() -> Self {
        let buffers = array_init::array_init(|_| AtomicPtr::new(ptr::null_mut()));
        Self {
            buffers,
            read: AtomicUsize::new(0),
            write: AtomicUsize::new(0),
            full: AtomicBool::new(false),
        }
    }

    pub fn write_heuristics(&self, edge_info: T) -> Result<(), Error> {
        let write = self.write.load(Relaxed);
        if self.full.load(Acquire) {
            return Err(Error::BuffersFull);
        }
        let new_ptr = Box::into_raw(Box::new(edge_info));
        let old_ptr = self.buffers[write].swap(new_ptr, AcqRel);
        if !old_ptr.is_null() {
            unsafe {
                drop(Box::from_raw(old_ptr));
            }
        }
        let next = (write + 1) % N;
        self.write.store(next, Release);

        if next == self.read.load(Acquire) {
            self.full.store(true, Release);
        }
        Ok(())
    }

    pub fn read_heuristics(&self) -> Result<T, Error> {
        let read = self.read.load(Relaxed);
        if read == self.write.load(Acquire) && !self.full.load(Acquire) {
            return Err(Error::NoPendingUpdates);
        }
        let null = ptr::null_mut();
        let old_ptr = self.buffers[read].swap(null, AcqRel);
        if old_ptr.is_null() {
            return Err(Error::ExpectedUpdate);
        }
        let edge = unsafe { Box::from_raw(old_ptr) };
        let info = *edge;

        let next = (read + 1) % N;
        self.read.store(next, Release);

        self.full.store(false, Release);
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn sequential_write_read() {
        let buf = BufferWheel::<3, i32>::default();

        // reading from an empty buffer should complain
        assert_eq!(buf.read_heuristics().unwrap_err(), Error::NoPendingUpdates);

        // write two items
        buf.write_heuristics(42).expect("first write okay");
        buf.write_heuristics(1337).expect("second write okay");

        // read them back in order
        assert_eq!(buf.read_heuristics().unwrap(), 42);
        assert_eq!(buf.read_heuristics().unwrap(), 1337);

        // and now it's empty again
        assert_eq!(buf.read_heuristics().unwrap_err(), Error::NoPendingUpdates);
    }

    #[test]
    fn capacity_full_and_recover() {
        // capacity 2 -> on the 2nd write 'full' flips on,
        // 3rd write errors until we read something.
        let buf = BufferWheel::<2, u8>::default();

        assert!(buf.write_heuristics(10).is_ok());
        assert!(buf.write_heuristics(20).is_ok());

        // buffer is full now
        let e = buf.write_heuristics(30).unwrap_err();
        assert_eq!(e, Error::BuffersFull);

        // read one slot, which should clear `full`
        assert_eq!(buf.read_heuristics().unwrap(), 10);

        // now we can write again
        buf.write_heuristics(30).expect("recovered after read");

        // drain the rest
        assert_eq!(buf.read_heuristics().unwrap(), 20);
        assert_eq!(buf.read_heuristics().unwrap(), 30);

        // finally empty
        assert_eq!(buf.read_heuristics().unwrap_err(), Error::NoPendingUpdates);
    }

    #[test]
    fn spsc_concurrent_spinning() {
        // tiny buffer of size 1; writer must spin until reader catches up
        let buf = Arc::new(BufferWheel::<1, usize>::default());
        let prod = Arc::clone(&buf);
        let cons = Arc::clone(&buf);

        let writer = thread::spawn(move || {
            for i in 0..100 {
                // spin on full
                loop {
                    match prod.write_heuristics(i) {
                        Ok(_) => break,
                        Err(Error::BuffersFull) => continue,
                        Err(e) => panic!("unexpected write error: {e:?}"),
                    }
                }
            }
        });

        let reader = thread::spawn(move || {
            for expected in 0..100 {
                // spin on empty
                loop {
                    match cons.read_heuristics() {
                        Ok(v) => {
                            assert_eq!(v, expected);
                            break;
                        }
                        Err(Error::NoPendingUpdates) => continue,
                        Err(e) => panic!("unexpected read error: {e:?}"),
                    }
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }
}
