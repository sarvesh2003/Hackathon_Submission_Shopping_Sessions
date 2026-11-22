//! Memory for the per-thread partition buffers of the parse, dedup and resolve stages.
//!
//! One anonymous mapping (2 MB aligned, `MADV_HUGEPAGE`) is carved into fixed 16 KB blocks.
//! A stage that has consumed a partition hands its blocks back, and the next stage writes its
//! output into them, so fresh pages (kernel zeroing, page faults, teardown at exit) are paid
//! about once for the peak live data instead of once per stage. Buffers are lists of blocks,
//! so no buffer is ever copied to grow.
//!
//! Allocation is lock-free: each thread takes 2 MB slabs from the shared bump pointer and keeps
//! its own list of recycled blocks (a thread recycles the blocks of the partitions it consumed).
//!
//! When the mapping is exhausted, blocks come from the heap (never freed; correctness does not
//! depend on the reservation being large enough).

use std::alloc::Layout;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::RefCell;
use std::sync::OnceLock;

pub const BLOCK: usize = 16 << 10;
const HUGE: usize = 2 << 20;

pub struct Arena {
    base: usize,
    cap: usize,
    map: (usize, usize), // the whole mapping, for munmap
    bump: AtomicUsize,
}

struct Local {
    free: Vec<usize>,
    cur: usize,
    end: usize,
}

thread_local! {
    static LOCAL: RefCell<Local> = const { RefCell::new(Local { free: Vec::new(), cur: 0, end: 0 }) };
}

static ARENA: OnceLock<Arena> = OnceLock::new();
static POPULATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

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
        Arena { base: 0, cap: 0, map: (0, 0), bump: AtomicUsize::new(0) }
    } else {
        let base = (p as usize).div_ceil(HUGE) * HUGE;
        // SAFETY: advice on our own mapping; failure only means 4 KB pages
        unsafe { libc::madvise(base as *mut libc::c_void, cap, libc::MADV_HUGEPAGE) };
        Arena { base, cap, map: (p as usize, len), bump: AtomicUsize::new(0) }
    };
    let _ = ARENA.set(a);
}

#[inline]
fn arena() -> &'static Arena {
    ARENA.get().expect("arena::init not called")
}

/// One 16 KB block, 16 KB aligned: one this thread recycled if any, else fresh.
fn get() -> *mut u8 {
    LOCAL.with(|l| {
        let mut l = l.borrow_mut();
        if let Some(b) = l.free.pop() {
            return b as *mut u8;
        }
        if l.cur == l.end {
            let a = arena();
            let off = a.bump.fetch_add(HUGE, Ordering::Relaxed);
            if off + HUGE > a.cap {
                return heap_block();
            }
            l.cur = a.base + off;
            l.end = l.cur + HUGE;
            if POPULATE.load(Ordering::Relaxed) {
                // one call maps the whole slab (one huge page, or 512 small ones without THP)
                // instead of a page-fault trap per 4 KB page; EINVAL on kernels before 5.14
                // SAFETY: advice on our own mapping
                unsafe { libc::madvise(l.cur as *mut libc::c_void, HUGE, 23 /* MADV_POPULATE_WRITE */) };
            }
        }
        let b = l.cur;
        l.cur += BLOCK;
        b as *mut u8
    })
}

#[cold]
fn heap_block() -> *mut u8 {
    // BLOCK-aligned like the arena's blocks (PartBuf finds a block from a pointer into it)
    let layout = Layout::from_size_align(BLOCK, BLOCK).unwrap();
    // SAFETY: non-zero size; the block is never freed (it may be recycled through `free`)
    let p = unsafe { std::alloc::alloc(layout) };
    if p.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    p
}

fn put(blocks: impl Iterator<Item = *mut u8>) {
    LOCAL.with(|l| l.borrow_mut().free.extend(blocks.map(|b| b as usize)));
}

/// Turns off slab pre-faulting (measurement switch).
pub fn set_populate(on: bool) {
    POPULATE.store(on, Ordering::Relaxed);
}

/// Releases the pages of part `i` of `n` of the used range (MADV_DONTNEED works under the
/// shared mmap lock, so the threads zap in parallel). SAFETY: no block may be used afterwards.
pub unsafe fn zap_part(i: usize, n: usize) {
    let a = arena();
    let used = used();
    let step = used.div_ceil(n * HUGE) * HUGE;
    let lo = (i * step).min(used);
    let hi = ((i + 1) * step).min(used);
    if hi > lo {
        libc::madvise((a.base + lo) as *mut libc::c_void, hi - lo, libc::MADV_DONTNEED);
    }
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
    /// write position in each partition's current block (null = no block yet). Blocks are
    /// BLOCK-aligned, so the block and the fill level follow from the pointer alone.
    cur: Vec<*mut T>,
    full: Vec<Vec<*mut T>>,
}

// SAFETY: the buffer owns its blocks exclusively; T is plain data
unsafe impl<T: Copy + Send> Send for PartBuf<T> {}
unsafe impl<T: Copy + Sync> Sync for PartBuf<T> {}

impl<T: Copy> PartBuf<T> {
    pub const CAP: usize = BLOCK / std::mem::size_of::<T>();
    const SIZE: usize = std::mem::size_of::<T>();

    pub fn new(nparts: usize) -> Self {
        assert!(Self::CAP >= 1 && Self::CAP <= 4096 && std::mem::align_of::<T>() <= 4096);
        PartBuf { cur: vec![std::ptr::null_mut(); nparts], full: (0..nparts).map(|_| Vec::new()).collect() }
    }

    /// Start of the block a non-null write position belongs to (the position is one past a
    /// written record, so it can equal the block end but never the block start).
    #[inline(always)]
    fn block_of(w: *mut T) -> *mut T {
        ((w as usize - 1) & !(BLOCK - 1)) as *mut T
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        let w = self.cur[p];
        // full when another record does not fit after w; a null w (no block yet) counts as full
        let w = if ((w as usize).wrapping_sub(1) & (BLOCK - 1)) >= BLOCK - Self::SIZE { self.next_block(p) } else { w };
        // SAFETY: w has room for one record inside its block
        unsafe {
            w.write(x);
            self.cur[p] = w.add(1);
        }
    }

    #[cold]
    #[inline(never)]
    fn next_block(&mut self, p: usize) -> *mut T {
        let w = self.cur[p];
        if !w.is_null() {
            self.full[p].push(Self::block_of(w));
        }
        get() as *mut T
    }

    fn head(&self, p: usize) -> &[T] {
        let w = self.cur[p];
        if w.is_null() {
            return &[];
        }
        let b = Self::block_of(w);
        // SAFETY: the records from the block start up to w were written
        unsafe { std::slice::from_raw_parts(b, (w as usize - b as usize) / Self::SIZE) }
    }

    /// Records of partition p, in insertion order.
    pub fn slices(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        self.full[p]
            .iter()
            // SAFETY: full blocks hold CAP written records
            .map(|&b| unsafe { std::slice::from_raw_parts(b as *const T, Self::CAP) })
            .chain(std::iter::once(self.head(p)))
            .filter(|s| !s.is_empty())
    }

    pub fn len(&self, p: usize) -> usize {
        self.full[p].len() * Self::CAP + self.head(p).len()
    }

    /// Hands partition p's blocks to the next stage. SAFETY: partition p of this buffer must
    /// never be read again (each partition is consumed exactly once, by one thread).
    pub unsafe fn release(&self, p: usize) {
        let w = self.cur[p];
        put(self.full[p].iter().map(|&b| b as *mut u8).chain((!w.is_null()).then(|| Self::block_of(w) as *mut u8)));
    }
}
