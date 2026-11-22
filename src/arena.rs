//! Memory for the per-thread partition buffers of the parse, dedup and resolve stages.
//!
//! One anonymous mapping (2 MB aligned, `MADV_HUGEPAGE`) is carved into fixed 16 KB blocks.
//! A stage that has consumed a partition hands its blocks back, and the next stage writes its
//! output into them, so fresh pages (kernel zeroing, page faults, teardown at exit) are paid
//! about once for the peak live data instead of once per stage. Buffers are lists of blocks,
//! so no buffer is ever copied to grow.
//!
//! When the mapping is exhausted, blocks come from the heap (never freed; correctness does not
//! depend on the reservation being large enough).

use std::alloc::Layout;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

pub const BLOCK: usize = 16 << 10;
const HUGE: usize = 2 << 20;

pub struct Arena {
    base: usize,
    cap: usize,
    map: (usize, usize), // the whole mapping, for munmap
    bump: AtomicUsize,
    free: Mutex<Vec<usize>>,
}

static ARENA: OnceLock<Arena> = OnceLock::new();

/// Reserves `bytes` of address space (nothing is touched until a block is written).
pub fn init(bytes: usize) {
    let cap = bytes.div_ceil(HUGE) * HUGE;
    let len = cap + HUGE;
    // SAFETY: a fresh private anonymous mapping; no existing memory is affected
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    let a = if p == libc::MAP_FAILED {
        Arena { base: 0, cap: 0, map: (0, 0), bump: AtomicUsize::new(0), free: Mutex::new(Vec::new()) }
    } else {
        let base = (p as usize).div_ceil(HUGE) * HUGE;
        // SAFETY: advice on our own mapping; failure only means 4 KB pages
        unsafe { libc::madvise(base as *mut libc::c_void, cap, libc::MADV_HUGEPAGE) };
        Arena {
            base,
            cap,
            map: (p as usize, len),
            bump: AtomicUsize::new(0),
            free: Mutex::new(Vec::with_capacity(1 << 16)),
        }
    };
    let _ = ARENA.set(a);
}

#[inline]
fn arena() -> &'static Arena {
    ARENA.get().expect("arena::init not called")
}

/// One 16 KB block, 4 KB aligned: a recycled one if any, else fresh.
fn get() -> *mut u8 {
    let a = arena();
    if let Some(b) = a.free.lock().unwrap().pop() {
        return b as *mut u8;
    }
    let off = a.bump.fetch_add(BLOCK, Ordering::Relaxed);
    if off + BLOCK <= a.cap {
        return (a.base + off) as *mut u8;
    }
    let layout = Layout::from_size_align(BLOCK, 4096).unwrap();
    // SAFETY: non-zero size; the block is never freed (it may be recycled through `free`)
    let p = unsafe { std::alloc::alloc(layout) };
    if p.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    p
}

fn put(blocks: impl Iterator<Item = *mut u8>) {
    let mut f = arena().free.lock().unwrap();
    f.extend(blocks.map(|b| b as usize));
}

/// Returns the address space to the kernel. SAFETY: no block may be used afterwards.
pub unsafe fn unmap() {
    let a = arena();
    if a.map.1 != 0 {
        libc::munmap(a.map.0 as *mut libc::c_void, a.map.1);
    }
}

/// Bytes of the reservation handed out so far (fresh blocks, not counting recycling).
pub fn used() -> usize {
    arena().bump.load(Ordering::Relaxed).min(arena().cap)
}

/// Per-thread output of a stage: one growable list of blocks per partition.
pub struct PartBuf<T: Copy> {
    /// current block of each partition and how many records it holds (null = none yet)
    heads: Vec<(*mut T, u32)>,
    full: Vec<Vec<*mut T>>,
}

// SAFETY: the buffer owns its blocks exclusively; T is plain data
unsafe impl<T: Copy + Send> Send for PartBuf<T> {}
unsafe impl<T: Copy + Sync> Sync for PartBuf<T> {}

impl<T: Copy> PartBuf<T> {
    pub const CAP: usize = BLOCK / std::mem::size_of::<T>();

    pub fn new(nparts: usize) -> Self {
        assert!(Self::CAP >= 1 && Self::CAP <= 4096 && std::mem::align_of::<T>() <= 4096);
        PartBuf { heads: vec![(std::ptr::null_mut(), Self::CAP as u32); nparts], full: (0..nparts).map(|_| Vec::new()).collect() }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        if self.heads[p].1 as usize == Self::CAP {
            self.next_block(p);
        }
        let h = &mut self.heads[p];
        // SAFETY: h.0 is a block of CAP records and h.1 < CAP
        unsafe { h.0.add(h.1 as usize).write(x) };
        h.1 += 1;
    }

    #[cold]
    #[inline(never)]
    fn next_block(&mut self, p: usize) {
        let h = &mut self.heads[p];
        if !h.0.is_null() {
            self.full[p].push(h.0);
        }
        *h = (get() as *mut T, 0);
    }

    /// Records of partition p, in insertion order.
    pub fn slices(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        let h = self.heads[p];
        let head: &[T] = if h.0.is_null() {
            &[]
        } else {
            // SAFETY: the first h.1 records of the current block were written
            unsafe { std::slice::from_raw_parts(h.0, h.1 as usize) }
        };
        self.full[p]
            .iter()
            // SAFETY: full blocks hold CAP written records
            .map(|&b| unsafe { std::slice::from_raw_parts(b as *const T, Self::CAP) })
            .chain(std::iter::once(head))
            .filter(|s| !s.is_empty())
    }

    pub fn len(&self, p: usize) -> usize {
        let h = self.heads[p];
        self.full[p].len() * Self::CAP + if h.0.is_null() { 0 } else { h.1 as usize }
    }

    /// Hands partition p's blocks to the next stage. SAFETY: partition p of this buffer must
    /// never be read again (each partition is consumed exactly once, by one thread).
    pub unsafe fn release(&self, p: usize) {
        let h = self.heads[p];
        put(self.full[p].iter().map(|&b| b as *mut u8).chain((!h.0.is_null()).then_some(h.0 as *mut u8)));
    }
}
