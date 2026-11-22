//! Shopping Sessions: the fast, multi-threaded pipeline.
//! Must produce exactly the same tables as src/bin/reference.rs (the oracle).
//!
//! Stages:
//!  1. mmap every input file, cut into chunks of whole lines.
//!  2. Parse links (parallel). Then N worker threads: worker 0 first runs union-find over the
//!     links, the others start parsing events (scattered into per-thread partitions by event_id);
//!     clicks are parsed last (they need the person map) and scattered by person.
//!  3. Per event_id partition: dedup with a hash table (smallest ingest_ts wins), map each
//!     surviving valid event to its person, scatter by person.
//!  4. Per person partition: sort, sessionize, credit (two pointers), assists (sliding window).
//!  5. Merge campaign counters, write Parquet in parallel.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use hashbrown::HashMap;
use memmap2::Mmap;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;

#[path = "../pq.rs"]
mod pq;
#[path = "../arena.rs"]
mod arena;
use arena::PartBuf;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const SESSION_GAP_MS: i64 = 1_800_000;
const PRICE_MAX: u32 = 10_000_000;

/// log2 of the partition count, chosen once from the input size before anything is
/// partitioned (every stage must agree; any value gives the same output).
static PART_BITS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(9);

#[inline(always)]
fn part_bits() -> u32 {
    PART_BITS.load(AtOrd::Relaxed)
}

#[inline(always)]
fn npart() -> usize {
    1 << part_bits()
}
/// How far ahead of the parse position the splitter prefetches the input. The inputs are
/// page-cache pages mapped 4 KB at a time, and the hardware prefetcher stops at every 4 KB
/// boundary; prefetching two lines per record keeps the stream ahead (parse -8% at 8 threads).
const PREF_DIST: usize = 4096;
const CHUNK_BYTES: usize = 2 << 20;

// ======================================================================= IDs
// Byte-order-preserving packing: each byte of [0-9A-Z_a-z] maps to a 6-bit code (1..=63)
// in byte order, left-aligned, big-endian, zero-padded, up to 10 bytes -> 60 bits.
// Comparing two packed keys as integers equals comparing the strings byte by byte.
// Anything else (other bytes, longer, empty) is interned exactly with bit 63 set.

const FALLBACK: u64 = 1 << 63;
const MAX_PACK: usize = 10;
/// marks an invalid event copy (never a real key: a fallback index this large cannot exist)
const INVALID: u64 = u64::MAX;

const fn build_codes() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        i += 1;
    }
    t[b'_' as usize] = 37;
    i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 38 + i as u8;
        i += 1;
    }
    t
}
static CODE: [u8; 256] = build_codes();

const fn build_decode() -> [u8; 64] {
    let codes = build_codes();
    let mut d = [0u8; 64];
    let mut b = 0;
    while b < 256 {
        if codes[b] != 0 {
            d[codes[b] as usize] = b as u8;
        }
        b += 1;
    }
    d
}
static DECODE: [u8; 64] = build_decode();

/// Reference version of `pack` (kept for the equivalence test).
#[cfg(test)]
fn pack_scalar(id: &[u8]) -> Option<u64> {
    if id.is_empty() || id.len() > MAX_PACK {
        return None;
    }
    let mut k = 0u64;
    let mut bad = 0u8;
    for &b in id {
        let c = CODE[b as usize];
        bad |= (c == 0) as u8;
        k = (k << 6) | c as u64;
    }
    if bad != 0 {
        return None;
    }
    Some(k << (6 * (MAX_PACK - id.len()) as u32))
}

/// Same result as `pack_scalar`: a fixed, fully unrolled 10-step loop with no data-dependent
/// exit (ID lengths vary, so a length-controlled loop mispredicts its exit), bytes past the end
/// count as padding (code 0) without being invalid.
#[inline(always)]
fn pack(id: &[u8]) -> Option<u64> {
    let n = id.len();
    if n.wrapping_sub(1) >= MAX_PACK {
        return None; // empty or longer than 10
    }
    let mut k = 0u64;
    let mut bad = 0u8;
    let mut i = 0;
    while i < MAX_PACK {
        let inside = i < n;
        // SAFETY: the index is read only when i < n = id.len()
        let b = if inside { unsafe { *id.get_unchecked(i) } } else { 0 };
        let c = CODE[b as usize];
        bad |= (inside & (c == 0)) as u8;
        k = (k << 6) | c as u64;
        i += 1;
    }
    if bad != 0 {
        None
    } else {
        Some(k)
    }
}

/// Bytes of the IDs that cannot be packed (key -> bytes), filled by the thread that met them.
/// Only IDs whose bytes are ever needed are noted: users and click IDs (output names, person
/// naming, the click tie-break) and both sides of every link. Event IDs are only compared for
/// equality, so they are never noted.
struct Dict {
    map: HashMap<u64, (u32, u32)>,
    bytes: Vec<u8>,
}

impl Dict {
    fn new() -> Self {
        Dict { map: HashMap::new(), bytes: Vec::new() }
    }
    #[inline]
    fn note(&mut self, k: u64, id: &[u8]) {
        if k & FALLBACK != 0 {
            self.note_slow(k, id);
        }
    }
    #[cold]
    #[inline(never)]
    fn note_slow(&mut self, k: u64, id: &[u8]) {
        let bytes = &mut self.bytes;
        self.map.entry(k).or_insert_with(|| {
            let s = bytes.len();
            bytes.extend_from_slice(id);
            (s as u32, id.len() as u32)
        });
    }
}

/// Every thread's dictionary, published after links and after parsing (lookups are rare).
static DICTS: std::sync::RwLock<Vec<Dict>> = std::sync::RwLock::new(Vec::new());

/// 64-bit hash of a byte string (folded 64x64->128 multiplies, length-seeded, fmix64 finish).
#[inline]
fn hash_bytes(s: &[u8]) -> u64 {
    #[inline(always)]
    fn fold(a: u64, b: u64) -> u64 {
        let r = (a as u128) * (b as u128);
        (r as u64) ^ ((r >> 64) as u64)
    }
    let n = s.len();
    let mut h = 0x243F_6A88_85A3_08D3u64 ^ (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut i = 0;
    while i + 8 <= n {
        let w = u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
        h = fold(h ^ w, 0xA076_1D64_78BD_642F) ^ w.rotate_left(29);
        i += 8;
    }
    if i < n {
        let mut t = [0u8; 8];
        t[..n - i].copy_from_slice(&s[i..]);
        let w = u64::from_le_bytes(t);
        h = fold(h ^ w, 0xE703_7ED1_A0B4_28DB) ^ w.rotate_left(29);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    h ^= h >> 33;
    h = h.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    h ^ (h >> 33)
}

static LEN_MASK: [u8; 32] = {
    let mut m = [0u8; 32];
    let mut i = 0;
    while i < 16 {
        m[i] = 0xFF;
        i += 1;
    }
    m
};

/// Same result as `pack` for the `len` bytes at `p`, in SSE registers: per-byte range checks
/// give the 6-bit codes, two multiply-adds fold them into 24-bit groups.
/// SAFETY: 16 bytes must be readable at p (bytes past `len` are ignored).
#[inline(always)]
unsafe fn pack16(p: *const u8, len: usize) -> Option<u64> {
    use std::arch::x86_64::*;
    if len.wrapping_sub(1) >= MAX_PACK {
        return None;
    }
    let keep = _mm_loadu_si128(LEN_MASK.as_ptr().add(16 - len) as *const __m128i);
    let x = _mm_and_si128(_mm_loadu_si128(p as *const __m128i), keep);
    // signed compares are exact here: every bound is below 0x80 and bytes >= 0x80 are negative
    let rng = |lo: u8, hi: u8| {
        _mm_and_si128(
            _mm_cmpgt_epi8(x, _mm_set1_epi8(lo as i8 - 1)),
            _mm_cmpgt_epi8(_mm_set1_epi8(hi as i8 + 1), x),
        )
    };
    let (d, u, l) = (rng(b'0', b'9'), rng(b'A', b'Z'), rng(b'a', b'z'));
    let us = _mm_cmpeq_epi8(x, _mm_set1_epi8(b'_' as i8));
    let valid = _mm_or_si128(_mm_or_si128(d, u), _mm_or_si128(us, l));
    if _mm_movemask_epi8(_mm_andnot_si128(valid, keep)) != 0 {
        return None;
    }
    // code = byte - offset: '0'..'9' -> 1..10, 'A'..'Z' -> 11..36, '_' -> 37, 'a'..'z' -> 38..63
    let off = _mm_or_si128(
        _mm_or_si128(_mm_and_si128(d, _mm_set1_epi8(0x2F)), _mm_and_si128(u, _mm_set1_epi8(0x36))),
        _mm_or_si128(_mm_and_si128(us, _mm_set1_epi8(0x3A)), _mm_and_si128(l, _mm_set1_epi8(0x3B))),
    );
    let c = _mm_sub_epi8(x, off); // 0 past the end
    let w = _mm_maddubs_epi16(c, _mm_set1_epi16(0x0140)); // c[2i] * 64 + c[2i+1]
    let v = _mm_madd_epi16(w, _mm_set1_epi32(0x0001_1000)); // 4 codes per 32-bit lane
    let lo = _mm_cvtsi128_si64(v) as u64;
    let hi = _mm_extract_epi32::<2>(v) as u32 as u64;
    Some((lo & 0xFF_FFFF) << 36 | (lo >> 32) << 12 | hi >> 12)
}

/// `pack` through `pack16` when 16 bytes from the field start lie before `end` (the address
/// one past the chunk the field was split from).
#[inline(always)]
fn pack_at(id: &[u8], end: usize) -> Option<u64> {
    if id.as_ptr() as usize + 16 <= end {
        // SAFETY: id lies in the chunk ending at `end`, so 16 bytes from its start are readable
        unsafe { pack16(id.as_ptr(), id.len()) }
    } else {
        pack(id)
    }
}

#[inline(always)]
fn id_key_at(id: &[u8], end: usize) -> u64 {
    match pack_at(id, end) {
        Some(k) => k,
        None => long_key(id),
    }
}

#[inline]
fn id_key(id: &[u8]) -> u64 {
    match pack(id) {
        Some(k) => k,
        None => long_key(id),
    }
}

/// Key of an ID that cannot be packed (longer than 10 bytes or outside [0-9A-Z_a-z]): bit 63
/// set (never equal to a packed key) and 63 bits of a strong hash of its bytes; never INVALID.
/// Equal IDs get equal keys; two different IDs collide with probability ~2^-63 per pair.
#[inline]
fn long_key(id: &[u8]) -> u64 {
    let v = hash_bytes(id) >> 1;
    FALLBACK | if v == (1u64 << 63) - 1 { 0 } else { v }
}

fn id_bytes(k: u64, out: &mut Vec<u8>) {
    if k & FALLBACK != 0 {
        if let Some(names) = NAMES.get() {
            if let Some(&r) = names[part_of(k)].get(&k) {
                // SAFETY: r points at r >> 48 bytes of an input file mapped until the process exits
                out.extend_from_slice(unsafe { std::slice::from_raw_parts((r & 0xFFFF_FFFF_FFFF) as *const u8, (r >> 48) as usize) });
                return;
            }
        }
        let g = DICTS.read().unwrap();
        for d in g.iter() {
            if let Some(&(s, l)) = d.map.get(&k) {
                out.extend_from_slice(&d.bytes[s as usize..(s + l) as usize]);
                return;
            }
        }
        panic!("no bytes noted for ID key {k:#x}");
    } else {
        for i in 0..MAX_PACK {
            let c = (k >> (6 * (MAX_PACK - 1 - i))) & 63;
            if c == 0 {
                break;
            }
            out.push(DECODE[c as usize]);
        }
    }
}

/// The bytes of packed key `k` (no FALLBACK bit), written as 16 bytes at `dst` (only the first
/// `len` are the ID; the rest is scratch); returns `len`. BMI2 spreads the ten 6-bit codes into
/// bytes, SSE maps codes to ASCII ('0'-'9' = 1-10, 'A'-'Z' = 11-36, '_' = 37, 'a'-'z' = 38-63).
/// SAFETY: 16 bytes must be writable at dst.
#[inline(always)]
unsafe fn decode_packed16(k: u64, dst: *mut u8) -> usize {
    use std::arch::x86_64::*;
    let hi = _pdep_u64(k >> 12, 0x3F3F_3F3F_3F3F_3F3F).swap_bytes(); // codes 0..7, code 0 first
    let lo = (_pdep_u64(k & 0xFFF, 0x3F3F) as u16).swap_bytes(); // codes 8, 9
    let c = _mm_set_epi64x(lo as i64, hi as i64);
    let gt = |v: i8| _mm_cmpgt_epi8(c, _mm_set1_epi8(v));
    let off = _mm_add_epi8(
        _mm_add_epi8(_mm_set1_epi8(0x2F), _mm_and_si128(gt(10), _mm_set1_epi8(7))),
        _mm_add_epi8(_mm_and_si128(gt(36), _mm_set1_epi8(4)), _mm_and_si128(gt(37), _mm_set1_epi8(1))),
    );
    _mm_storeu_si128(dst as *mut __m128i, _mm_add_epi8(c, off));
    // codes are left-aligned: the ID ends at the first zero code (bytes 10..16 are always zero)
    (_mm_movemask_epi8(_mm_cmpeq_epi8(c, _mm_setzero_si128())) as u32).trailing_zeros() as usize
}

/// Appends the ID of key `k` to `out` in Parquet PLAIN form (u32 length, then the bytes).
#[inline]
fn id_plain(k: u64, out: &mut Vec<u8>) {
    if k & FALLBACK == 0 {
        out.reserve(20);
        // SAFETY: 20 bytes are reserved past len: 4 for the length, 16 for decode_packed16
        unsafe {
            let d = out.as_mut_ptr().add(out.len());
            let n = decode_packed16(k, d.add(4));
            (d as *mut u32).write_unaligned(n as u32);
            out.set_len(out.len() + 4 + n);
        }
    } else {
        let at = out.len();
        out.extend_from_slice(&[0; 4]);
        id_bytes(k, out);
        let n = (out.len() - at - 4) as u32;
        out[at..at + 4].copy_from_slice(&n.to_le_bytes());
    }
}

/// Byte-wise comparison of the strings behind two keys.
#[inline]
fn id_cmp(a: u64, b: u64) -> std::cmp::Ordering {
    if (a | b) & FALLBACK == 0 {
        a.cmp(&b)
    } else {
        let (mut x, mut y) = (Vec::new(), Vec::new());
        id_bytes(a, &mut x);
        id_bytes(b, &mut y);
        x.cmp(&y)
    }
}

#[inline]
fn part_of(k: u64) -> usize {
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - part_bits())) as usize
}

// ======================================================================= CSV dialect

/// Splits one line into exactly N fields (quotes around whole fields removed).
/// Returns false if the line is malformed. Used for the small files.
fn split_fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut n = 0;
    for raw in line.split(|&b| b == b',') {
        let v = if raw.first() == Some(&b'"') {
            if raw.len() < 2 || raw[raw.len() - 1] != b'"' {
                return false;
            }
            let inner = &raw[1..raw.len() - 1];
            if inner.contains(&b'"') {
                return false;
            }
            inner
        } else {
            if raw.contains(&b'"') {
                return false;
            }
            raw
        };
        if n == N {
            return false;
        }
        out[n] = v;
        n += 1;
    }
    n == N
}

/// Bit i set <=> p[i] is ',' or '\n' or '"' (64 bytes).
#[cfg(target_feature = "avx2")]
#[inline(always)]
fn special_mask(p: &[u8; 64]) -> u64 {
    use std::arch::x86_64::*;
    // SAFETY: avx2 is enabled at compile time; loads are unaligned and within the 64-byte array.
    unsafe {
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let c = _mm256_set1_epi8(b',' as i8);
        let n = _mm256_set1_epi8(b'\n' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let ma = _mm256_or_si256(
            _mm256_or_si256(_mm256_cmpeq_epi8(a, c), _mm256_cmpeq_epi8(a, n)),
            _mm256_cmpeq_epi8(a, q),
        );
        let mb = _mm256_or_si256(
            _mm256_or_si256(_mm256_cmpeq_epi8(b, c), _mm256_cmpeq_epi8(b, n)),
            _mm256_cmpeq_epi8(b, q),
        );
        (_mm256_movemask_epi8(ma) as u32 as u64) | ((_mm256_movemask_epi8(mb) as u32 as u64) << 32)
    }
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
fn special_mask(p: &[u8; 64]) -> u64 {
    let mut m = 0u64;
    for (i, &b) in p.iter().enumerate() {
        if b == b',' || b == b'\n' || b == b'"' {
            m |= 1 << i;
        }
    }
    m
}

/// Calls `f` with the N field values of every well-formed line of `chunk` (lines split on '\n'
/// only; a field is either free of quotes or wrapped in exactly one pair of quotes, which are
/// removed). Malformed and empty lines are skipped. Same semantics as `split_fields`.
#[inline(always)]
#[allow(unused_assignments)]
fn for_each_record<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = chunk.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize;
    let mut fstart = 0usize;
    let mut lstart = 0usize;
    let mut q = 0u32;
    let mut q0 = 0usize;
    let mut q1 = 0usize;
    let mut bad = false;
    let mut base = 0usize;
    let mut tail = [0u8; 64];

    macro_rules! end_field {
        ($pos:expr) => {{
            let pos = $pos;
            if !bad {
                let v: Option<&'a [u8]> = if q == 0 {
                    Some(&chunk[fstart..pos])
                } else if q == 2 && q0 == fstart && q1 + 1 == pos {
                    Some(&chunk[fstart + 1..pos - 1])
                } else {
                    None
                };
                match v {
                    Some(v) if nf < N => {
                        fields[nf] = v;
                        nf += 1;
                    }
                    _ => bad = true,
                }
            }
            q = 0;
            fstart = pos + 1;
        }};
    }

    while base < len {
        let mut m = if base + 64 <= len {
            special_mask(chunk[base..base + 64].try_into().unwrap())
        } else {
            tail = [0u8; 64];
            tail[..len - base].copy_from_slice(&chunk[base..]);
            special_mask(&tail)
        };
        while m != 0 {
            let pos = base + m.trailing_zeros() as usize;
            m &= m - 1;
            let b = chunk[pos];
            if b == b'"' {
                if q == 0 {
                    q0 = pos;
                }
                q1 = pos;
                q += 1;
                continue;
            }
            end_field!(pos);
            if b == b'\n' {
                if !bad && nf == N && pos > lstart {
                    f(&fields);
                }
                nf = 0;
                bad = false;
                lstart = pos + 1;
            }
        }
        base += 64;
    }
    let _ = tail;
    if lstart < len {
        // last line without a trailing '\n'
        end_field!(len);
        if !bad && nf == N {
            f(&fields);
        }
    }
}

/// (commas, newlines, quotes) masks of 64 bytes; bit i <=> p[i].
#[cfg(target_feature = "avx2")]
#[inline(always)]
fn masks3(p: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    // SAFETY: avx2 is enabled at compile time; loads are unaligned and within the 64-byte array.
    unsafe {
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |x: __m256i, y: __m256i| {
            (_mm256_movemask_epi8(x) as u32 as u64) | ((_mm256_movemask_epi8(y) as u32 as u64) << 32)
        };
        let vc = _mm256_set1_epi8(b',' as i8);
        let vn = _mm256_set1_epi8(b'\n' as i8);
        let vq = _mm256_set1_epi8(b'"' as i8);
        (
            m(_mm256_cmpeq_epi8(a, vc), _mm256_cmpeq_epi8(b, vc)),
            m(_mm256_cmpeq_epi8(a, vn), _mm256_cmpeq_epi8(b, vn)),
            m(_mm256_cmpeq_epi8(a, vq), _mm256_cmpeq_epi8(b, vq)),
        )
    }
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
fn masks3(p: &[u8; 64]) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for (i, &b) in p.iter().enumerate() {
        c |= ((b == b',') as u64) << i;
        n |= ((b == b'\n') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (c, n, q)
}

/// Same contract as `for_each_record`, one whole line per step (lines < 128 bytes): a line is
/// well-formed iff it has N-1 commas and its quotes are exactly the first and last byte of some
/// fields (each such field at least 2 bytes long), checked for all fields at once on 128-bit
/// masks. Longer lines and the last 128 bytes of the chunk go through `split_fields`.
#[inline(always)]
fn for_each_record_v5<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = chunk.len();
    let mut pos = 0usize;
    let mut fields: [&'a [u8]; N] = [&[]; N];
    while pos + 128 <= len {
        // SAFETY: a prefetch is only a hint and never faults, even past the end of the mapping
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch::<_MM_HINT_T0>(chunk.as_ptr().wrapping_add(pos + PREF_DIST) as *const i8);
            _mm_prefetch::<_MM_HINT_T0>(chunk.as_ptr().wrapping_add(pos + PREF_DIST + 64) as *const i8);
        }
        let (c0, n0, q0) = masks3(chunk[pos..pos + 64].try_into().unwrap());
        let (nl, c, q): (usize, u128, u128) = if n0 != 0 {
            let nl = n0.trailing_zeros() as usize;
            let m = (1u64 << nl) - 1;
            (nl, (c0 & m) as u128, (q0 & m) as u128)
        } else {
            let (c1, n1, q1) = masks3(chunk[pos + 64..pos + 128].try_into().unwrap());
            if n1 == 0 {
                // a line of 128 bytes or more: exact slow path
                let end = match memchr::memchr(b'\n', &chunk[pos + 128..]) {
                    Some(d) => pos + 128 + d,
                    None => len,
                };
                if split_fields(&chunk[pos..end], &mut fields) {
                    f(&fields);
                }
                pos = end + 1;
                continue;
            }
            let nl1 = n1.trailing_zeros() as usize;
            let m1 = (1u64 << nl1) - 1;
            (64 + nl1, c0 as u128 | (((c1 & m1) as u128) << 64), q0 as u128 | (((q1 & m1) as u128) << 64))
        };
        let ls = pos;
        pos += nl + 1;
        let term = c | (1u128 << nl); // one past the end of every field
        let starts = 1u128 | (c << 1); // first byte of every field
        let ends = term >> 1; // last byte of every non-empty field
        let open_q = q & starts;
        let close_q = q & ends;
        // every check at once with non-short-circuit `|`: one branch per line, not one per check
        // (an empty line has no comma, so it fails the field count)
        let bad = (c.count_ones() != (N - 1) as u32)
            | (q != (open_q | close_q))
            | ((open_q & close_q) != 0)
            | (open_q.count_ones() != close_q.count_ones())
            | ((close_q.wrapping_sub(open_q) & term) != 0);
        if bad {
            continue; // empty line, wrong field count or misplaced quotes
        }
        let line = &chunk[ls..ls + nl];
        let mut cm = c;
        let mut s = 0usize;
        for (k, slot) in fields.iter_mut().enumerate() {
            let e = if k + 1 < N {
                let e = cm.trailing_zeros() as usize;
                cm &= cm - 1;
                e
            } else {
                nl
            };
            let qk = ((open_q >> s) & 1) as usize;
            *slot = &line[s + qk..e - qk];
            s = e + 1;
        }
        f(&fields);
    }
    // the last < 128 bytes (and a final line without '\n'): exact slow path
    while pos < len {
        let end = match memchr::memchr(b'\n', &chunk[pos..]) {
            Some(d) => pos + d,
            None => len,
        };
        if split_fields(&chunk[pos..end], &mut fields) {
            f(&fields);
        }
        pos = end + 1;
    }
}

// ======================================================================= timestamps

#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some((a * 10 + b) as i64)
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Per-thread cache of validated dates. Key = the exact 10 bytes "YYYY-MM-DD"; a hit returns
/// the day number computed when those same bytes were validated.
struct DateCache {
    a: [u64; 32],
    b: [u16; 32],
    days: [i64; 32],
    ok: [bool; 32],
    /// one past the current chunk (0 = unknown): lets ID packing read 16 bytes at a time
    end: usize,
}

impl DateCache {
    fn new() -> Self {
        DateCache { a: [0; 32], b: [0; 32], days: [0; 32], ok: [false; 32], end: 0 }
    }
}

/// F1 / F2 / F3 -> epoch ms. None = bad (including an F3 value too large for i64).
fn parse_ts(s: &[u8]) -> Option<i64> {
    parse_ts_c(s, &mut DateCache::new())
}

/// `parse_ts` with a per-thread date cache.
#[inline(always)]
fn parse_ts_c(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso(s, dc);
    }
    if n == 0 {
        return None;
    }
    if n == 13 {
        // the usual F3 width: 8 + 4 + 1 digits with SWAR
        let hi = le64(s, 0);
        let mid = u32::from_le_bytes([s[8], s[9], s[10], s[11]]) as u64;
        let lo = s[12].wrapping_sub(b'0');
        if !swar_digits(hi, u64::MAX) || !swar_digits(mid, 0xFFFF_FFFF) || lo > 9 {
            return None;
        }
        return Some((swar8(hi) * 100_000 + swar4(mid) * 10 + lo as u64) as i64);
    }
    if n <= 18 {
        // at most 18 digits cannot overflow i64
        let mut v: i64 = 0;
        let mut bad = 0u8;
        for &c in s {
            let d = c.wrapping_sub(b'0');
            bad |= (d > 9) as u8;
            v = v.wrapping_mul(10).wrapping_add(d as i64);
        }
        return if bad == 0 { Some(v) } else { None };
    }
    let mut v: i64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

#[inline(always)]
fn le64(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

/// Value of 8 ASCII digits (validated by the caller), first digit in the lowest byte.
#[inline(always)]
fn swar8(x: u64) -> u64 {
    let d = x - 0x3030_3030_3030_3030;
    let p2 = (d.wrapping_mul(10 << 8 | 1) >> 8) & 0x00FF_00FF_00FF_00FF;
    let p4 = (p2.wrapping_mul(100 << 16 | 1) >> 16) & 0x0000_FFFF_0000_FFFF;
    p4.wrapping_mul(10_000 << 32 | 1) >> 32
}

/// Value of 4 ASCII digits in the low 4 bytes (validated by the caller).
#[inline(always)]
fn swar4(x: u64) -> u64 {
    let d = x - 0x3030_3030;
    let p2 = (d.wrapping_mul(10 << 8 | 1) >> 8) & 0x00FF_00FF;
    (p2.wrapping_mul(100 << 16 | 1) >> 16) & 0xFFFF
}

/// True if every byte selected by `mask` (0xFF per byte) is an ASCII digit.
#[inline(always)]
fn swar_digits(x: u64, mask: u64) -> bool {
    const HI: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    const THREES: u64 = 0x3030_3030_3030_3030;
    const SIXES: u64 = 0x0606_0606_0606_0606;
    let d = x & mask;
    // high nibble must be 3, and adding 6 must not carry into it (rejects 0x3A..0x3F)
    (d & HI & mask) == (THREES & mask) && (d.wrapping_add(SIXES & mask) & HI & mask) == (THREES & mask)
}

/// Validates "YYYY-MM-DD" (s[0..10]) and returns its day number.
#[inline(never)]
fn validate_date(s: &[u8]) -> Option<i64> {
    let a = le64(s, 0); // YYYY-MM-
    const A_SEP_MASK: u64 = 0xFF00_00FF_0000_0000;
    const A_SEP: u64 = 0x2D00_002D_0000_0000;
    const A_DIG: u64 = 0x00FF_FF00_FFFF_FFFF;
    if (a & A_SEP_MASK) != A_SEP || !swar_digits(a, A_DIG) {
        return None;
    }
    let day = d2(s, 8)?;
    let da = a ^ 0x3030_3030_3030_3030;
    let by = |i: u32| ((da >> (8 * i)) & 0xFF) as i64;
    let year = by(0) * 1000 + by(1) * 100 + by(2) * 10 + by(3);
    let month = by(5) * 10 + by(6);
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

#[inline(always)]
fn date_days(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    let a = le64(s, 0);
    let b = u16::from_le_bytes([s[8], s[9]]);
    let e = ((s[8].wrapping_sub(b'0') as usize) * 10 + s[9].wrapping_sub(b'0') as usize) & 31;
    if dc.ok[e] && dc.a[e] == a && dc.b[e] == b {
        return Some(dc.days[e]);
    }
    let d = validate_date(s)?;
    dc.a[e] = a;
    dc.b[e] = b;
    dc.days[e] = d;
    dc.ok[e] = true;
    Some(d)
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` or F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` (length 20 / 25, s[4] == '-').
#[inline(always)]
fn parse_iso(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    let days = date_days(s, dc)?;
    if s[10] != b'T' {
        return None;
    }
    let t = le64(s, 11); // HH:MM:SS
    if t & 0x0000_FF00_00FF_0000 != 0x0000_3A00_003A_0000 || !swar_digits(t, 0xFFFF_00FF_FF00_FFFF) {
        return None;
    }
    let dt = t ^ 0x3030_3030_3030_3030;
    let by = |i: u32| ((dt >> (8 * i)) & 0xFF) as i64;
    let hour = by(0) * 10 + by(1);
    let minute = by(3) * 10 + by(4);
    let second = by(6) * 10 + by(7);
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset_ms = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 3600 + om * 60) * 1000
    };
    Some((days * 86_400 + hour * 3600 + minute * 60 + second) * 1000 - offset_ms)
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u32 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u32;
        if v > PRICE_MAX {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v)
    }
}

// ======================================================================= inputs

fn read_manifest_ts(text: &[u8], key: &str) -> Res<i64> {
    let pat = format!("\"{key}\"");
    let pos = text
        .windows(pat.len())
        .position(|w| w == pat.as_bytes())
        .ok_or_else(|| format!("manifest: missing {key}"))?;
    let rest = &text[pos + pat.len()..];
    let colon = rest.iter().position(|&b| b == b':').ok_or("manifest: missing colon")?;
    let mut rest = &rest[colon + 1..];
    while let Some(&b) = rest.first() {
        if b.is_ascii_whitespace() {
            rest = &rest[1..];
        } else {
            break;
        }
    }
    let value: &[u8] = if rest.first() == Some(&b'"') {
        let end = rest[1..].iter().position(|&b| b == b'"').ok_or("manifest: unterminated")?;
        &rest[1..1 + end]
    } else {
        let end = rest.iter().position(|b| !b.is_ascii_digit()).unwrap_or(rest.len());
        &rest[..end]
    };
    parse_ts(value).ok_or_else(|| format!("manifest: bad {key}").into())
}

fn list_csv(dir: &Path) -> Res<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let is_csv = path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".csv"))
                .unwrap_or(false);
            if is_csv && path.is_file() {
                paths.push(path);
            }
        }
    }
    paths.sort();
    Ok(paths)
}

fn map_files(paths: &[PathBuf]) -> Res<Vec<Mmap>> {
    paths
        .par_iter()
        .filter_map(|p| {
            let f = match fs::File::open(p) {
                Ok(f) => f,
                Err(e) => return Some(Err(e.into())),
            };
            match f.metadata() {
                Ok(m) if m.len() == 0 => return None,
                Err(e) => return Some(Err(e.into())),
                _ => {}
            }
            // SAFETY: inputs are read-only and not modified while we run.
            let mut opts = memmap2::MmapOptions::new();
            if std::env::var_os("SS_POPULATE").is_some() {
                opts.populate();
            }
            Some(unsafe { opts.map(&f) }.map_err(|e| e.into()))
        })
        .collect()
}

/// Splits every file (header line skipped) into chunks of whole lines; files in parallel (each
/// boundary search touches a fresh input page), chunks in file order.
fn chunk_files(maps: &[Mmap], target: usize) -> Vec<&[u8]> {
    let per_file: Vec<Vec<&[u8]>> = maps
        .par_iter()
        .map(|m| {
            let data: &[u8] = m;
            let mut chunks = Vec::new();
            let mut pos = match memchr::memchr(b'\n', data) {
                Some(p) => p + 1,
                None => return chunks, // header only
            };
            let len = data.len();
            while pos < len {
                let mut end = (pos + target).min(len);
                if end < len {
                    end = match memchr::memchr(b'\n', &data[end..]) {
                        Some(p) => end + p + 1,
                        None => len,
                    };
                }
                chunks.push(&data[pos..end]);
                pos = end;
            }
            chunks
        })
        .collect();
    per_file.into_iter().flatten().collect()
}

fn data_lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    buf.split(|&b| b == b'\n').skip(1)
}

// ======================================================================= records

/// One parsed copy of an event (before dedup). 32 bytes.
#[derive(Clone, Copy)]
struct EvRec {
    eid: u64,
    /// person-independent user key, or INVALID if this copy fails validation
    user: u64,
    ingest: i64,
    /// ms since window_start
    ts: u32,
    /// price (low 24 bits, > 0 only for a purchase) | category << 24
    pc: u32,
}

/// A surviving event, keyed by person. 16 bytes.
#[derive(Clone, Copy, Default)]
struct PEv {
    person: u64,
    ts: u32,
    pc: u32,
}

/// A valid click, keyed by person. 24 bytes.
#[derive(Clone, Copy, Default)]
struct PCl {
    person: u64,
    cid: u64,
    /// ms since window_start - 7 days
    ts: u32,
    camp: u16,
}

struct Ctx {
    ws: i64,
    we: i64,
    cws: i64, // click window start
    ws_day: i64,
    ndays: usize,
    prod_packed: HashMap<u64, u8>,
    prod_bytes: HashMap<Vec<u8>, u8>,
    unknown_cat: u8,
    camp_packed: HashMap<u64, u16>,
    camp_bytes: HashMap<Vec<u8>, u16>,
    ncamp: usize, // index ncamp = organic
}

impl Ctx {
    #[inline]
    fn category(&self, product: &[u8], end: usize) -> u8 {
        match pack_at(product, end) {
            Some(k) => self.prod_packed.get(&k).copied().unwrap_or(self.unknown_cat),
            None => self.prod_bytes.get(product).copied().unwrap_or(self.unknown_cat),
        }
    }
    #[inline]
    fn campaign(&self, c: &[u8], end: usize) -> Option<u16> {
        match pack_at(c, end) {
            Some(k) => self.camp_packed.get(&k).copied(),
            None => self.camp_bytes.get(c).copied(),
        }
    }
}

/// `ingest_ts`, which the rules guarantee is a valid F1 (`YYYY-MM-DDTHH:MM:SSZ`): the date goes
/// through the validated-date cache (a miss validates it in full); the time is read without
/// re-checking separators or ranges. Any other length takes the general parser.
#[inline(always)]
fn parse_ingest(s: &[u8], dc: &mut DateCache) -> i64 {
    if s.len() == 20 {
        if let Some(days) = date_days(s, dc) {
            let dt = le64(s, 11) ^ 0x3030_3030_3030_3030;
            let by = |i: u32| ((dt >> (8 * i)) & 0xFF) as i64;
            let secs = (by(0) * 10 + by(1)) * 3600 + (by(3) * 10 + by(4)) * 60 + by(6) * 10 + by(7);
            return (days * 86_400 + secs) * 1000;
        }
    }
    parse_ts_c(s, dc).unwrap_or(i64::MAX)
}

/// Fields already split; event_id is non-empty.
#[inline(always)]
fn parse_event(f: &[&[u8]; 7], ctx: &Ctx, dc: &mut DateCache) -> EvRec {
    let eid = id_key_at(f[0], dc.end);
    let ingest = parse_ingest(f[3], dc);
    let mut rec = EvRec { eid, user: INVALID, ingest, ts: 0, pc: 0 };
    let (user, ts_raw, etype, product, price_raw) = (f[1], f[2], f[4], f[5], f[6]);
    if user.is_empty() || product.is_empty() {
        return rec;
    }
    let ts = match parse_ts_c(ts_raw, dc) {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => return rec,
    };
    // event type by one integer compare: "view" / "cart" (4 bytes) or "purchase" (8 bytes)
    let price = if etype.len() == 4 {
        let w = u32::from_le_bytes([etype[0], etype[1], etype[2], etype[3]]);
        if w != u32::from_le_bytes(*b"view") && w != u32::from_le_bytes(*b"cart") {
            return rec;
        }
        0
    } else if etype.len() == 8 && u64::from_le_bytes(etype[..8].try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
        match parse_price(price_raw) {
            Some(p) => p,
            None => return rec,
        }
    } else {
        return rec;
    };
    if ingest.saturating_sub(ts) > DAY_MS {
        return rec;
    }
    rec.user = id_key_at(user, dc.end);
    rec.ts = (ts - ctx.ws) as u32;
    // the category is only ever read for purchases (price > 0)
    let cat = if price > 0 { ctx.category(product, dc.end) } else { 0 };
    rec.pc = price | ((cat as u32) << 24);
    rec
}

/// (user key, ts since click window start, click_id key, campaign) of a valid click.
#[inline(always)]
fn parse_click(f: &[&[u8]; 4], ctx: &Ctx, dc: &mut DateCache) -> Option<(u64, u32, u64, u16)> {
    let (cid, user, camp, ts_raw) = (f[0], f[1], f[2], f[3]);
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let camp = ctx.campaign(camp, dc.end)?;
    let ts = parse_ts_c(ts_raw, dc)?;
    if ts < ctx.cws || ts >= ctx.we {
        return None;
    }
    Some((id_key_at(user, dc.end), (ts - ctx.cws) as u32, id_key_at(cid, dc.end), camp))
}

// ======================================================================= links

/// One chunk of links: its pairs, the bytes of its unpackable IDs (for output names), and
/// (key, `name_ref`) of those IDs (for union-find; the input stays mapped until the process exits).
type LinkOut = (Vec<(u64, u64)>, Dict, Vec<(u64, u64)>);

fn parse_link_chunk(chunk: &[u8]) -> LinkOut {
    let mut v = Vec::new();
    let mut d = Dict::new();
    let mut refs = Vec::new();
    for_each_record_v5::<2>(chunk, |f| {
        if !f[0].is_empty() && !f[1].is_empty() {
            let (a, b) = (id_key(f[0]), id_key(f[1]));
            for (k, id) in [(a, f[0]), (b, f[1])] {
                if k & FALLBACK != 0 {
                    d.note(k, id);
                    if let Some(r) = name_ref(id) {
                        refs.push((k, r));
                    }
                }
            }
            v.push((a, b));
        }
    });
    (v, d, refs)
}

/// All link chunks' results: pairs per chunk, and the refs sorted by key without duplicates.
/// The dictionaries go to DICTS (output names).
fn gather_links(outs: Vec<LinkOut>) -> (Vec<Vec<(u64, u64)>>, Vec<(u64, u64)>) {
    let mut pairs = Vec::with_capacity(outs.len());
    let mut refs = Vec::new();
    let mut dicts = Vec::with_capacity(outs.len());
    for (v, d, r) in outs {
        pairs.push(v);
        dicts.push(d);
        refs.extend(r);
    }
    DICTS.write().unwrap().extend(dicts);
    refs.sort_unstable_by_key(|r| r.0);
    refs.dedup_by_key(|r| r.0);
    (pairs, refs)
}

/// Maps every linked ID whose person is not itself to its person's key.
/// Partitioned by `part_of(id)` so the maps can be built in parallel.
struct PersonMap {
    parts: Vec<HashMap<u64, u64>>,
}


fn build_person_map(pairs: &[Vec<(u64, u64)>], refs: &[(u64, u64)]) -> PersonMap {
    let tt = Instant::now();
    let tm = std::env::var_os("SS_TIMERS").is_some();
    macro_rules! uft {
        ($n:expr) => {
            if tm {
                eprintln!("uf: {:>7.1} ms  {}", tt.elapsed().as_secs_f64() * 1e3, $n);
            }
        };
    }
    // dense ids: sort (key, endpoint slot) pairs once, then number distinct keys in order;
    // slot 2j / 2j+1 = the two sides of link j (in `pairs` order)
    let mut offs = Vec::with_capacity(pairs.len());
    let mut total = 0usize;
    for v in pairs {
        offs.push(total);
        total += v.len();
    }
    let mut ends: Vec<(u64, u32)> = pairs
        .par_iter()
        .zip(offs.par_iter())
        .flat_map_iter(|(v, &base)| {
            v.iter().enumerate().flat_map(move |(j, &(a, b))| {
                let s = ((base + j) * 2) as u32;
                [(a, s), (b, s + 1)]
            })
        })
        .collect();
    uft!("collected endpoints");
    ends.par_sort_unstable_by_key(|e| e.0);
    uft!("sorted endpoints");
    let mut keys: Vec<u64> = Vec::with_capacity(ends.len() / 2 + 1);
    let mut ids: Vec<u32> = vec![0; ends.len()];
    for &(k, slot) in &ends {
        if keys.last() != Some(&k) {
            keys.push(k);
        }
        ids[slot as usize] = (keys.len() - 1) as u32;
    }
    drop(ends);
    uft!(format!("dense ids ({} keys)", keys.len()));
    let n = keys.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[parent[x as usize] as usize];
            parent[x as usize] = p;
            x = p;
        }
        x
    }
    // Unions hang the root with the larger dense id under the other: no byte comparisons here.
    for j in 0..total {
        let ra = find(&mut parent, ids[2 * j]);
        let rb = find(&mut parent, ids[2 * j + 1]);
        if ra != rb {
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    uft!("unions");
    // A person is the byte-wise smallest ID of its set, found in one pass. Dense ids follow
    // sorted u64 key order: byte order among packed keys, all of which come before the fallback
    // keys (bit 63). Each fallback key's bytes are fetched once, up front.
    let fb = keys.partition_point(|&k| k & FALLBACK == 0);
    // keys[fb..] and refs are both sorted by key: one merge walk finds every key's bytes
    let mut fb_slices: Vec<&[u8]> = Vec::with_capacity(n - fb);
    let mut ri = 0usize;
    let mut slow = Vec::new();
    let mut slow_off: Vec<(usize, usize, usize)> = Vec::new(); // (index in fb_slices, start, end)
    for &k in &keys[fb..] {
        while ri < refs.len() && refs[ri].0 < k {
            ri += 1;
        }
        if ri < refs.len() && refs[ri].0 == k {
            let r = refs[ri].1;
            // SAFETY: r points at r >> 48 bytes of a links file, mapped until the process exits
            fb_slices.push(unsafe { std::slice::from_raw_parts((r & 0xFFFF_FFFF_FFFF) as *const u8, (r >> 48) as usize) });
        } else {
            // an ID of 64 KB or more: through the dictionaries
            let st = slow.len();
            id_bytes(k, &mut slow);
            slow_off.push((fb_slices.len(), st, slow.len()));
            fb_slices.push(&[]);
        }
    }
    for &(i, a, b) in &slow_off {
        fb_slices[i] = &slow[a..b];
    }
    let (mut ta, mut tb) = (Vec::new(), Vec::new());
    let mut less = |a: usize, b: usize| -> bool {
        if a < fb && b < fb {
            return a < b;
        }
        let sa: &[u8] = if a >= fb {
            fb_slices[a - fb]
        } else {
            ta.clear();
            id_bytes(keys[a], &mut ta);
            &ta
        };
        let sb: &[u8] = if b >= fb {
            fb_slices[b - fb]
        } else {
            tb.clear();
            id_bytes(keys[b], &mut tb);
            &tb
        };
        sa < sb
    };
    // Unions always hang the larger root under the smaller, so every parent pointer points to
    // a smaller id: one ascending pass turns parent[] into root[] (parent[i] is final already).
    for i in 0..n {
        let p = parent[i] as usize;
        parent[i] = parent[p];
    }
    let root = parent;
    // best[r] = smallest member seen so far of the set rooted at r (a root starts as itself);
    // the root is its set's smallest dense id, so only sets with fallback keys ever change
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in fb..n {
        let r = root[i] as usize;
        if less(i, best[r] as usize) {
            best[r] = i as u32;
        }
    }
    uft!("persons named");
    // scatter (id, person) by partition, then build the small maps in parallel
    let mut by_part: Vec<Vec<(u64, u64)>> = new_parts();
    for i in 0..n as u32 {
        let pi = best[root[i as usize] as usize];
        if pi != i {
            // IDs that are their own person need no entry
            by_part[part_of(keys[i as usize])].push((keys[i as usize], keys[pi as usize]));
        }
    }
    uft!("scattered");
    let parts = by_part
        .into_par_iter()
        .map(|v| {
            let mut m = HashMap::with_capacity(v.len());
            m.extend(v);
            m
        })
        .collect();
    uft!("maps built");
    PersonMap { parts }
}

// ======================================================================= per-person work

#[derive(Clone, Copy, Default)]
struct CampAcc {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

struct URow {
    person: u64,
    day: u16,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: i32, // category index, -1 = null
}

#[derive(Clone, Copy)]
struct Sess {
    start: i64, // ms since window_start
    day: u16,
    purchase: bool,
    revenue: u64,
}

struct Worker {
    ndays: usize,
    ncamp: usize,
    day0_ms: i64, // ms from window_start back to the start of day 0 (<= 0)
    // per (campaign, day), split hot/cold so a thread's counters stay small in L2:
    camp_assisted: Vec<u32>, // touched on every assist run
    camp_sp: Vec<[u32; 2]>,  // credited sessions, purchasing sessions
    camp_rev: Vec<u64>,      // credited revenue (written only for purchasing sessions)
    rows: Vec<URow>,
    sessions: Vec<Sess>,
    cat_sum: Vec<u64>,
    cnt: Vec<u32>,
    act: Vec<u32>,
    day_first: Vec<u32>,
}

impl Worker {
    fn new(ctx: &Ctx, ncat: usize) -> Self {
        Worker {
            ndays: ctx.ndays,
            ncamp: ctx.ncamp,
            day0_ms: ctx.ws_day * DAY_MS - ctx.ws,
            camp_assisted: vec![0; (ctx.ncamp + 1) * ctx.ndays],
            camp_sp: vec![[0; 2]; (ctx.ncamp + 1) * ctx.ndays],
            camp_rev: vec![0; (ctx.ncamp + 1) * ctx.ndays],
            rows: Vec::new(),
            sessions: Vec::new(),
            cat_sum: vec![0; ncat],
            cnt: vec![0; ctx.ncamp],
            act: vec![0; ctx.ncamp],
            day_first: vec![0; ctx.ndays + 1],
        }
    }

    fn flush_row(&mut self, person: u64, day: u16, sessions: u32, events: u32, revenue: u64) {
        let mut top = -1i32;
        if revenue > 0 {
            let mut best = 0u64;
            for (i, v) in self.cat_sum.iter_mut().enumerate() {
                if *v > best {
                    best = *v;
                    top = i as i32;
                }
                *v = 0;
            }
        }
        self.rows.push(URow { person, day, sessions, events, revenue, top });
    }

    /// Adds `assisted` for campaign c over sessions [a, b), split by day.
    #[inline]
    fn flush_assist(&mut self, c: usize, a: usize, b: usize) {
        let mut s = a;
        while s < b {
            let d = self.sessions[s].day as usize;
            let e = b.min(self.day_first[d + 1] as usize);
            self.camp_assisted[c * self.ndays + d] += (e - s) as u32;
            s = e;
        }
    }

    fn person(&mut self, person: u64, evs: &[PEv], cls: &[PCl]) {
        // pass 1: sessions and user_daily rows
        self.sessions.clear();
        let n = evs.len();
        let mut i = 0;
        let mut cur_day: u16 = u16::MAX;
        let (mut d_sess, mut d_events, mut d_rev) = (0u32, 0u32, 0u64);
        while i < n {
            let start = evs[i].ts as i64;
            let day = (((start - self.day0_ms) as u64) / DAY_MS as u64) as u16;
            if day != cur_day {
                if cur_day != u16::MAX {
                    self.flush_row(person, cur_day, d_sess, d_events, d_rev);
                }
                cur_day = day;
                d_sess = 0;
                d_events = 0;
                d_rev = 0;
            }
            let mut rev = 0u64;
            let mut j = i;
            loop {
                let e = &evs[j];
                let price = e.pc & 0x00FF_FFFF;
                if price > 0 {
                    rev += price as u64;
                    self.cat_sum[(e.pc >> 24) as usize] += price as u64;
                }
                j += 1;
                if j == n || (evs[j].ts - evs[j - 1].ts) as i64 > SESSION_GAP_MS {
                    break;
                }
            }
            d_sess += 1;
            d_events += (j - i) as u32;
            d_rev += rev;
            self.sessions.push(Sess { start, day, purchase: rev > 0, revenue: rev });
            i = j;
        }
        if cur_day != u16::MAX {
            self.flush_row(person, cur_day, d_sess, d_events, d_rev);
        }

        let organic = self.ncamp;
        let nd = self.ndays;
        if cls.is_empty() {
            for s in &self.sessions {
                let k = organic * nd + s.day as usize;
                self.camp_sp[k][0] += 1;
                if s.purchase {
                    self.camp_sp[k][1] += 1;
                    self.camp_rev[k] += s.revenue;
                }
            }
            return;
        }

        // first session index of each day (sessions are sorted by start), for the days this
        // person has: flush_assist reads day_first[d + 1] for days d of its sessions only
        {
            let d0 = self.sessions[0].day as usize;
            let dl = (self.sessions[self.sessions.len() - 1].day as usize + 1).min(nd);
            let mut k = 0usize;
            for d in d0..=dl {
                while k < self.sessions.len() && (self.sessions[k].day as usize) < d {
                    k += 1;
                }
                self.day_first[d] = k as u32;
            }
        }

        // click ts are relative to window_start - 7 days: click.ts - WEEK is window-relative.
        // Qualifying: start - 7d <= click <= start  <=>  start <= cts <= start + 7d (cts = raw)
        // pass 2: credit (two pointers) and assists (sliding window of campaign counts)
        let ns = self.sessions.len();
        let (mut lo, mut hi) = (0usize, 0usize);
        for si in 0..ns {
            let s = self.sessions[si];
            while hi < cls.len() && cls[hi].ts as i64 - WEEK_MS <= s.start {
                let c = cls[hi].camp as usize;
                if self.cnt[c] == 0 {
                    self.act[c] = si as u32;
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cls[lo].ts as i64) < s.start {
                let c = cls[lo].camp as usize;
                self.cnt[c] -= 1;
                if self.cnt[c] == 0 {
                    let a = self.act[c] as usize;
                    self.flush_assist(c, a, si);
                }
                lo += 1;
            }
            let credited = if lo < hi {
                let t = cls[hi - 1].ts;
                let mut best = hi - 1;
                let mut k = hi - 1;
                while k > lo && cls[k - 1].ts == t {
                    k -= 1;
                    if id_cmp(cls[k].cid, cls[best].cid) == std::cmp::Ordering::Less {
                        best = k;
                    }
                }
                cls[best].camp as usize
            } else {
                organic
            };
            let k = credited * nd + s.day as usize;
            self.camp_sp[k][0] += 1;
            if s.purchase {
                self.camp_sp[k][1] += 1;
                self.camp_rev[k] += s.revenue;
            }
        }
        for k in lo..hi {
            let c = cls[k].camp as usize;
            if self.cnt[c] > 0 {
                self.cnt[c] = 0;
                let a = self.act[c] as usize;
                self.flush_assist(c, a, ns);
            }
        }
    }
}

// ======================================================================= sampling profiler
// Measurement only (SS_PROF=<file>): ITIMER_PROF delivers SIGPROF to whichever thread is
// using CPU; the handler stores that thread's instruction pointer. At exit the samples are
// written as offsets into the executable, for `addr2line -f -i`. Needs no perf permissions.

const PROF_N: usize = 1 << 20;
static PROF_IDX: AtomicUsize = AtomicUsize::new(0);
static mut PROF_BUF: [u64; PROF_N] = [0; PROF_N];

extern "C" fn on_sigprof(_sig: libc::c_int, _info: *mut libc::siginfo_t, ctx: *mut libc::c_void) {
    // SAFETY: called by the kernel with a valid ucontext; slots are claimed atomically.
    unsafe {
        let uc = ctx as *const libc::ucontext_t;
        let rip = (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] as u64;
        let i = PROF_IDX.fetch_add(1, AtOrd::Relaxed);
        if i < PROF_N {
            std::ptr::addr_of_mut!(PROF_BUF).cast::<u64>().add(i).write(rip);
        }
    }
}

fn prof_start() {
    if std::env::var_os("SS_PROF").is_none() {
        return;
    }
    // SAFETY: installing a signal handler and an interval timer for this process.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_sigprof as *const () as usize;
        sa.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
        libc::sigaction(libc::SIGPROF, &sa, std::ptr::null_mut());
        let tv = libc::timeval { tv_sec: 0, tv_usec: 200 };
        let it = libc::itimerval { it_interval: tv, it_value: tv };
        libc::setitimer(libc::ITIMER_PROF, &it, std::ptr::null_mut());
    }
}

fn prof_dump() {
    let Some(path) = std::env::var_os("SS_PROF") else { return };
    // SAFETY: stop the timer before reading the buffer.
    unsafe {
        let z = libc::itimerval {
            it_interval: libc::timeval { tv_sec: 0, tv_usec: 0 },
            it_value: libc::timeval { tv_sec: 0, tv_usec: 0 },
        };
        libc::setitimer(libc::ITIMER_PROF, &z, std::ptr::null_mut());
    }
    let n = PROF_IDX.load(AtOrd::Relaxed).min(PROF_N);
    // load address of the executable: first mapping of /proc/self/exe in /proc/self/maps
    let exe = std::fs::read_link("/proc/self/exe").unwrap_or_default();
    let maps = std::fs::read_to_string("/proc/self/maps").unwrap_or_default();
    let mut base = 0u64;
    for line in maps.lines() {
        if line.ends_with(exe.to_str().unwrap_or("?")) {
            let start = line.split('-').next().unwrap_or("0");
            base = u64::from_str_radix(start, 16).unwrap_or(0);
            break;
        }
    }
    let mut out = String::new();
    for i in 0..n {
        // SAFETY: slots below n were written by the handler; the timer is stopped.
        let rip = unsafe { std::ptr::addr_of!(PROF_BUF).cast::<u64>().add(i).read() };
        out.push_str(&format!("0x{:x}\n", rip.wrapping_sub(base)));
    }
    let _ = std::fs::write(path, out);
    eprintln!("shopping: profile: {n} samples");
}

// ======================================================================= main

fn main() {
    match run() {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("shopping: error: {e}");
            std::process::exit(1);
        }
    }
}

macro_rules! stage {
    ($t0:expr, $name:expr) => {
        eprintln!("shopping: {:>8.1} ms  {}", $t0.elapsed().as_secs_f64() * 1000.0, $name)
    };
}

fn new_parts<T>() -> Vec<Vec<T>> {
    (0..npart()).map(|_| Vec::new()).collect()
}

/// What one parse thread produces.
struct ParseOut {
    ev: PartBuf<EvRec>, // by event_id partition
    cl: PartBuf<PCl>,   // by user partition
    /// (key, where its bytes are in the input) of every unpackable user / click ID met, by key
    /// partition; turned into per-partition dictionaries during the resolve stage
    names: PartBuf<(u64, u64)>,
}

/// Where an ID's bytes are: address (48 bits) | length << 48. The input stays mapped until the
/// dictionaries are built. IDs of 64 KB or more go to the shared slow dictionary instead.
#[inline(always)]
fn name_ref(id: &[u8]) -> Option<u64> {
    (id.len() < 1 << 16).then(|| id.as_ptr() as u64 | (id.len() as u64) << 48)
}

#[cold]
fn note_huge(k: u64, id: &[u8]) {
    let mut d = Dict::new();
    d.note(k, id);
    DICTS.write().unwrap().push(d);
}

#[inline(always)]
fn note_name(names: &mut PartBuf<(u64, u64)>, k: u64, id: &[u8]) {
    if k & FALLBACK != 0 && k != INVALID {
        match name_ref(id) {
            Some(r) => names.push(part_of(k), (k, r)),
            None => note_huge(k, id),
        }
    }
}

/// Per key partition: where the bytes of each unpackable user / click ID are in the input
/// (a `name_ref`), built in the resolve stage; the input stays mapped until the process exits.
static NAMES: std::sync::OnceLock<Vec<HashMap<u64, u64>>> = std::sync::OnceLock::new();

/// A one-shot value that threads can wait for.
struct Gate<T> {
    v: Mutex<Option<Arc<T>>>,
    cv: Condvar,
}
impl<T> Gate<T> {
    fn new() -> Self {
        Gate { v: Mutex::new(None), cv: Condvar::new() }
    }
    fn set(&self, t: T) {
        *self.v.lock().unwrap() = Some(Arc::new(t));
        self.cv.notify_all();
    }
    fn wait(&self) -> Arc<T> {
        let mut g = self.v.lock().unwrap();
        loop {
            if let Some(t) = g.as_ref() {
                return t.clone();
            }
            g = self.cv.wait(g).unwrap();
        }
    }
}

extern "C" {
    fn mi_option_set(option: i32, value: std::ffi::c_long);
}

fn run() -> Res<()> {
    let t0 = Instant::now();
    if std::env::var_os("SS_MI_DEFAULT").is_none() {
        // SAFETY: mimalloc option setter (v3: 15 = purge_delay; -1 = never return memory to
        // the OS), so buffers freed by one stage are reused, not faulted in again, by the next.
        unsafe { mi_option_set(15, -1) };
    }
    prof_start();
    let args: Vec<String> = std::env::args().collect();
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into())),
        )
    };
    let nthreads = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new().num_threads(nthreads).build_global()?;

    // ---- small inputs
    let manifest = fs::read(data_dir.join("manifest.json"))?;
    let ws = read_manifest_ts(&manifest, "window_start")?;
    let we = read_manifest_ts(&manifest, "window_end")?;
    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY_MS) - ws_day + 1) as usize } else { 1 };
    if we - ws + WEEK_MS >= u32::MAX as i64 {
        return Err("window longer than 42 days is not supported".into());
    }

    let products_buf = fs::read(data_dir.join("products.csv"))?;
    let campaigns_buf = fs::read(data_dir.join("campaigns.csv"))?;
    // The lookup tables (products, campaigns) are built while the big inputs are mapped and
    // cut into chunks: the two are independent, and each alone leaves most threads idle.
    let build_ctx = || -> Res<(Ctx, Vec<&[u8]>, Vec<(&[u8], &[u8])>)> {
        let mut prod_rows: Vec<(&[u8], &[u8])> = Vec::with_capacity(products_buf.len() / 12 + 16);
        for line in data_lines(&products_buf) {
            let mut f: [&[u8]; 2] = [&[]; 2];
            if split_fields(line, &mut f) {
                prod_rows.push((f[0], f[1]));
            }
        }
        // distinct categories first (a few dozen), then sort only those
        let mut seen_cats: std::collections::HashSet<&[u8]> = prod_rows.iter().map(|r| r.1).collect();
        seen_cats.insert(b"unknown");
        let mut cat_names: Vec<&[u8]> = seen_cats.into_iter().collect();
        cat_names.sort_unstable();
        if cat_names.len() > 255 {
            return Err("more than 255 categories is not supported".into());
        }
        let cat_index = |c: &[u8]| cat_names.binary_search(&c).unwrap() as u8;
        let mut prod_packed = HashMap::with_capacity(prod_rows.len());
        let mut prod_bytes = HashMap::new();
        for &(p, c) in &prod_rows {
            let ci = cat_index(c);
            match pack(p) {
                Some(k) => {
                    prod_packed.entry(k).or_insert(ci);
                }
                None => {
                    prod_bytes.entry(p.to_vec()).or_insert(ci);
                }
            }
        }
        let mut camp_names: Vec<(&[u8], &[u8])> = Vec::new();
        let mut camp_packed = HashMap::new();
        let mut camp_bytes = HashMap::new();
        for line in data_lines(&campaigns_buf) {
            let mut f: [&[u8]; 2] = [&[]; 2];
            if split_fields(line, &mut f) {
                let exists = match pack(f[0]) {
                    Some(k) => camp_packed.contains_key(&k),
                    None => camp_bytes.contains_key(f[0]),
                };
                if exists {
                    continue;
                }
                let idx = camp_names.len() as u16;
                camp_names.push((f[0], f[1]));
                match pack(f[0]) {
                    Some(k) => {
                        camp_packed.insert(k, idx);
                    }
                    None => {
                        camp_bytes.insert(f[0].to_vec(), idx);
                    }
                }
            }
        }
        if camp_names.len() >= u16::MAX as usize {
            return Err("too many campaigns".into());
        }
        let ctx = Ctx {
            ws,
            we,
            cws: ws - WEEK_MS,
            ws_day,
            ndays,
            prod_packed,
            prod_bytes,
            unknown_cat: cat_index(b"unknown"),
            camp_packed,
            camp_bytes,
            ncamp: camp_names.len(),
        };
        Ok((ctx, cat_names, camp_names))
    };
    // ---- map the big inputs (the mappings live until the process exits: leaked, so the chunks
    // borrowing them can come out of the join)
    type Mapped = (&'static [Mmap], &'static [Mmap], &'static [Mmap]);
    type Chunks = (Vec<&'static [u8]>, Vec<&'static [u8]>, Vec<&'static [u8]>);
    let map_all = || -> Res<(Mapped, Chunks)> {
        let leak = |v: Vec<Mmap>| -> &'static [Mmap] { Box::leak(v.into_boxed_slice()) };
        let link_maps = leak(map_files(&list_csv(&data_dir.join("links"))?)?);
        let event_maps = leak(map_files(&list_csv(&data_dir.join("events"))?)?);
        let click_maps = leak(map_files(&list_csv(&data_dir.join("clicks"))?)?);
        let link_chunks = chunk_files(link_maps, 256 << 10);
        let event_chunks = chunk_files(event_maps, CHUNK_BYTES);
        let click_chunks = chunk_files(click_maps, CHUNK_BYTES);
        Ok(((link_maps, event_maps, click_maps), (link_chunks, event_chunks, click_chunks)))
    };
    let (ctx_r, maps_r) = rayon::join(build_ctx, map_all);
    let (ctx, cat_names, camp_names) = ctx_r?;
    let ((link_maps, event_maps, click_maps), (link_chunks, event_chunks, click_chunks)) = maps_r?;
    // Partitions sized for the caches: per-partition tables and sort buffers were tuned at ~2 GB
    // of event+click input (512 partitions); one more bit per doubling beyond ~2.5 GB.
    {
        let bytes: usize = event_chunks.iter().chain(click_chunks.iter()).map(|c| c.len()).sum();
        let mut bits = 9u32;
        while bits < 10 && bytes > (5usize << 29) << (bits - 9) {
            bits += 1;
        }
        if let Some(b) = std::env::var("SS_PART_BITS").ok().and_then(|v| v.parse::<u32>().ok()) {
            bits = b.clamp(6, 12);
        }
        PART_BITS.store(bits, AtOrd::Relaxed);
    }
    stage!(t0, "mapped inputs");

    if std::env::var_os("SS_SCAN_ONLY").is_some() {
        // measurement mode: how fast can we just touch every byte with all threads?
        let lines: usize = event_chunks
            .par_iter()
            .chain(click_chunks.par_iter())
            .chain(link_chunks.par_iter())
            .map(|c| memchr::memchr_iter(b'\n', c).count())
            .sum();
        stage!(t0, format!("scan only: {lines} lines"));
        return Ok(());
    }

    if std::env::var_os("SS_MICRO").is_some() {
        micro(&event_chunks, &ctx);
        return Ok(());
    }

    // ---- one work queue: link chunks first, then events, then clicks. The worker that finishes
    // the last link chunk runs union-find while the others parse events.
    let gate: Gate<PersonMap> = Gate::new();
    let next = AtomicUsize::new(0);
    let n_link = link_chunks.len();
    let link_slots: Vec<Mutex<Option<LinkOut>>> = (0..n_link).map(|_| Mutex::new(None)).collect();
    let links_done = AtomicUsize::new(0);
    if n_link == 0 {
        // no link lines at all (or header-only files): nobody would finish a last link chunk
        let (pairs, refs) = gather_links(Vec::new());
        gate.set(build_person_map(&pairs, &refs));
    }
    let n_ev = event_chunks.len();
    let n_all = n_link + n_ev + click_chunks.len();
    // address space for all partition buffers: parse output is ~0.35 B per input byte, plus a
    // partly filled block per (thread, partition, kind); untouched reservation costs nothing
    let in_bytes: usize = event_chunks.iter().chain(click_chunks.iter()).map(|c| c.len()).sum();
    arena::set_populate(std::env::var_os("SS_NO_POPULATE").is_none());
    arena::init(in_bytes * 3 / 4 + nthreads * npart() * 4 * arena::BLOCK + (256 << 20));
    // union-find runs inside the global pool: nobody steals its parallel parts until the other
    // workers run out of chunks, so no second pool oversubscribes the cores
    let outs: Vec<ParseOut> = rayon::broadcast(|_bc| {
        {
                let (gate, next, ctx, link_slots, links_done) = (&gate, &next, &ctx, &link_slots, &links_done);
                let (event_chunks, click_chunks, link_chunks) = (&event_chunks, &click_chunks, &link_chunks);
                (move || {
                    let mut out = ParseOut {
                        ev: PartBuf::new(npart()),
                        cl: PartBuf::new(npart()),
                        names: PartBuf::new(npart()),
                    };
                    let mut dc = DateCache::new();
                    loop {
                        let i = next.fetch_add(1, AtOrd::Relaxed);
                        if i >= n_all {
                            break;
                        }
                        if i < n_link {
                            *link_slots[i].lock().unwrap() = Some(parse_link_chunk(link_chunks[i]));
                            if links_done.fetch_add(1, AtOrd::AcqRel) + 1 == n_link {
                                // the last link chunk: every other one is in its slot
                                let lo: Vec<LinkOut> = link_slots.iter().map(|m| m.lock().unwrap().take().unwrap()).collect();
                                let (pairs, refs) = gather_links(lo);
                                stage!(t0, "parsed links");
                                gate.set(build_person_map(&pairs, &refs));
                                stage!(t0, "union-find done");
                            }
                            continue;
                        }
                        let i = i - n_link;
                        if i < n_ev {
                            dc.end = event_chunks[i].as_ptr() as usize + event_chunks[i].len();
                            for_each_record_v5::<7>(event_chunks[i], |f| {
                                if !f[0].is_empty() {
                                    let r = parse_event(f, ctx, &mut dc);
                                    note_name(&mut out.names, r.user, f[1]);
                                    out.ev.push(part_of(r.eid), r);
                                }
                            });
                        } else {
                            // by user partition; the person is resolved later (partitioned join)
                            let ch = click_chunks[i - n_ev];
                            dc.end = ch.as_ptr() as usize + ch.len();
                            for_each_record_v5::<4>(ch, |f| {
                                if let Some((user, ts, cid, camp)) = parse_click(f, ctx, &mut dc) {
                                    note_name(&mut out.names, user, f[1]);
                                    note_name(&mut out.names, cid, f[0]);
                                    out.cl.push(part_of(user), PCl { person: user, cid, ts, camp });
                                }
                            });
                        }
                    }
                    out
                })()
        }
    });
    let pmap = gate.wait();
    stage!(t0, "parsed events + clicks");
    // Nothing references the input files any more (records hold packed keys; odd IDs were
    // copied into the interner). Unmap them on a background thread now, overlapped with the
    // rest of the run, instead of during the single-threaded teardown after exit: this cut
    // the time between our last write and process exit from ~160 ms to ~60 ms locally.
    drop(event_chunks);
    drop(click_chunks);
    drop(link_chunks);
    let maps = (event_maps, click_maps, link_maps);
    // (unmapped after the resolve stage, which copies the bytes of the unpackable IDs)

    // ---- dedup events per event_id partition, scatter survivors by person
    let next = AtomicUsize::new(0);
    let ue_outs: Vec<PartBuf<PEv>> = rayon::broadcast(|_bc| {
        {
                let (next, outs) = (&next, &outs);
                (move || {
                    let mut parts: PartBuf<PEv> = PartBuf::new(npart());
                    let mut table: Vec<u64> = Vec::new();
                    let mut table32: Vec<u32> = Vec::new();
                    let mut emitted: Vec<*mut PEv> = Vec::new();
                    let mut alive: Vec<u64> = Vec::new();
                    let mut base: Vec<u32> = Vec::with_capacity(outs.len());
                    let mut srcs: Vec<&[EvRec]> = Vec::with_capacity(outs.len());
                    loop {
                        let p = next.fetch_add(1, AtOrd::Relaxed);
                        if p >= npart() {
                            break;
                        }
                        // records are addressed in place as (source block << 12 | index)
                        srcs.clear();
                        srcs.extend(outs.iter().flat_map(|o| o.ev.slices(p)));
                        assert!(srcs.len() < 1 << 20 && srcs.iter().all(|v| v.len() <= 1 << 12));
                        let n: usize = srcs.iter().map(|v| v.len()).sum();
                        if n == 0 {
                            continue;
                        }
                        // a record is addressed as (source block << 12 | index); its dense number
                        // g = base[thread] + index selects its bit in `alive`
                        let rec = |id: u32| -> &EvRec { &srcs[(id >> 12) as usize][(id & 0xFFF) as usize] };
                        base.clear();
                        let mut acc = 0u32;
                        for v in srcs.iter() {
                            base.push(acc);
                            acc += v.len() as u32;
                        }
                        let dense = |id: u32| (base[(id >> 12) as usize] + (id & 0xFFF)) as usize;
                        alive.clear();
                        alive.resize(n.div_ceil(64), 0);
                        // load factor 1/3..2/3 (a 256 KB table of 4-byte slots at 20M)
                        let bits = (n + n / 2).next_power_of_two().trailing_zeros().max(4);
                        let mask = (1usize << bits) - 1;
                        if n < (1 << 20) - 1 {
                            // 4-byte slots: 12 hash bits below the index bits << 20 | (dense number
                            // + 1); 0 = empty. A tag match finds its record by a binary search over
                            // the source blocks (duplicates are a few percent). Single pass: the
                            // current winner of each event_id is emitted at once and its output
                            // record remembered; a later, better copy retracts it in place (person
                            // = INVALID, skipped by the resolve stage). Each record is read once.
                            table32.clear();
                            table32.resize(1 << bits, 0);
                            emitted.clear();
                            emitted.resize(n, std::ptr::null_mut());
                            let mut g = 0u32;
                            for src in srcs.iter() {
                                for cur in src.iter() {
                                    let k = cur.eid;
                                    let mixed = k.wrapping_mul(0xD6E8_FEB8_6659_FD93);
                                    let tag = (((mixed << bits) >> 52) as u32) << 20;
                                    let mut h = (mixed >> (64 - bits)) as usize;
                                    let wins = loop {
                                        let sl = table32[h];
                                        if sl == 0 {
                                            table32[h] = tag | (g + 1);
                                            break true;
                                        }
                                        if sl & 0xFFF0_0000 == tag {
                                            let og = (sl & 0x000F_FFFF) - 1;
                                            let t = base.partition_point(|&x| x <= og) - 1;
                                            let r = &srcs[t][(og - base[t]) as usize];
                                            if r.eid == k {
                                                // keep the smallest ingest_ts; on a tie, the first seen
                                                if cur.ingest < r.ingest {
                                                    table32[h] = tag | (g + 1);
                                                    let e = emitted[og as usize];
                                                    if !e.is_null() {
                                                        // SAFETY: e points at a PEv this thread wrote into
                                                        // `parts`; blocks never move
                                                        unsafe { (*e).person = INVALID };
                                                    }
                                                    break true;
                                                }
                                                break false;
                                            }
                                        }
                                        h = (h + 1) & mask;
                                    };
                                    if wins && cur.user != INVALID {
                                        // person resolved in the next pass (partitioned join)
                                        emitted[g as usize] = parts.push_ptr(part_of(cur.user), PEv { person: cur.user, ts: cur.ts, pc: cur.pc });
                                    }
                                    g += 1;
                                }
                            }
                        } else {
                            // slot = (32 hash bits below the index bits) << 32 | id; EMPTY = u64::MAX
                            const EMPTY: u64 = u64::MAX;
                            table.clear();
                            table.resize(1 << bits, EMPTY);
                            for (t, src) in srcs.iter().enumerate() {
                                for (i, cur) in src.iter().enumerate() {
                                    let id = ((t as u32) << 12) | i as u32;
                                    let k = cur.eid;
                                    let mixed = k.wrapping_mul(0xD6E8_FEB8_6659_FD93);
                                    let tag = (mixed << bits) & 0xFFFF_FFFF_0000_0000;
                                    let mut h = (mixed >> (64 - bits)) as usize;
                                    loop {
                                        let s = table[h];
                                        if s == EMPTY {
                                            table[h] = tag | id as u64;
                                            let g = dense(id);
                                            alive[g >> 6] |= 1u64 << (g & 63);
                                            break;
                                        }
                                        if s & 0xFFFF_FFFF_0000_0000 == tag {
                                            let old = s as u32;
                                            let r = rec(old);
                                            if r.eid == k {
                                                // keep the smallest ingest_ts; on a tie, the first seen
                                                if cur.ingest < r.ingest {
                                                    table[h] = tag | id as u64;
                                                    let go = dense(old);
                                                    alive[go >> 6] &= !(1u64 << (go & 63));
                                                    let g = dense(id);
                                                    alive[g >> 6] |= 1u64 << (g & 63);
                                                }
                                                break;
                                            }
                                        }
                                        h = (h + 1) & mask;
                                    }
                                }
                            }
                            // emit survivors in memory order
                            let mut g = 0usize;
                            for src in srcs.iter() {
                                for r in src.iter() {
                                    if (alive[g >> 6] >> (g & 63)) & 1 != 0 && r.user != INVALID {
                                        // person resolved in the next pass (partitioned join)
                                        parts.push(part_of(r.user), PEv { person: r.user, ts: r.ts, pc: r.pc });
                                    }
                                    g += 1;
                                }
                            }
                        }
                        // SAFETY: partition p is consumed; its blocks take later output
                        for o in outs.iter() {
                            unsafe { o.ev.release(p) };
                        }
                    }
                    parts
                })()
        }
    });
    let (cu_outs, nm_outs): (Vec<PartBuf<PCl>>, Vec<PartBuf<(u64, u64)>>) = outs.into_iter().map(|o| (o.cl, o.names)).unzip();
    stage!(t0, "dedup events");

    // ---- resolve persons: user partition q only needs the small map pmap.parts[q]
    let next = AtomicUsize::new(0);
    // largest partitions first (a whale's partition claimed last would be the stage's tail)
    let mut rorder: Vec<(usize, usize)> = (0..npart())
        .map(|q| (ue_outs.iter().map(|o| o.len(q)).sum::<usize>() + cu_outs.iter().map(|o| o.len(q)).sum::<usize>(), q))
        .collect();
    rorder.sort_unstable_by(|a, b| b.cmp(a));
    let rorder: Vec<usize> = rorder.into_iter().map(|x| x.1).collect();
    let resolved: Vec<(PartBuf<PEv>, PartBuf<PCl>, Vec<(usize, HashMap<u64, u64>)>)> = rayon::broadcast(|_bc| {
        {
                let (next, ue_outs, cu_outs, nm_outs, pmap, rorder) = (&next, &ue_outs, &cu_outs, &nm_outs, &pmap, &rorder);
                (move || {
                    let mut parts: PartBuf<PEv> = PartBuf::new(npart());
                    let mut cparts: PartBuf<PCl> = PartBuf::new(npart());
                    let mut dicts: Vec<(usize, HashMap<u64, u64>)> = Vec::new();
                    loop {
                        let qi = next.fetch_add(1, AtOrd::Relaxed);
                        if qi >= npart() {
                            break;
                        }
                        let q = rorder[qi];
                        let m = &pmap.parts[q];
                        for o in ue_outs.iter() {
                            for e in o.slices(q).flatten() {
                                if e.person == INVALID {
                                    continue; // a copy retracted by dedup
                                }
                                let person = m.get(&e.person).copied().unwrap_or(e.person);
                                parts.push(part_of(person), PEv { person, ts: e.ts, pc: e.pc });
                            }
                        }
                        for o in cu_outs.iter() {
                            for c in o.slices(q).flatten() {
                                let person = m.get(&c.person).copied().unwrap_or(c.person);
                                cparts.push(part_of(person), PCl { person, ..*c });
                            }
                        }
                        // where key partition q's unpackable IDs are (no input byte is read here)
                        let mut d: HashMap<u64, u64> = HashMap::new();
                        for o in nm_outs.iter() {
                            for &(k, r) in o.slices(q).flatten() {
                                d.entry(k).or_insert(r);
                            }
                        }
                        if !d.is_empty() {
                            dicts.push((q, d));
                        }
                        // SAFETY: user partition q is consumed, by this thread only
                        for o in ue_outs.iter() {
                            unsafe { o.release(q) };
                        }
                        for o in cu_outs.iter() {
                            unsafe { o.release(q) };
                        }
                        for o in nm_outs.iter() {
                            unsafe { o.release(q) };
                        }
                    }
                    (parts, cparts, dicts)
                })()
        }
    });
    drop(ue_outs);
    drop(cu_outs);
    drop(nm_outs);
    let mut pe_outs: Vec<PartBuf<PEv>> = Vec::with_capacity(resolved.len());
    let mut cl_outs: Vec<PartBuf<PCl>> = Vec::with_capacity(resolved.len());
    let mut names: Vec<HashMap<u64, u64>> = (0..npart()).map(|_| HashMap::new()).collect();
    for (e, c, ds) in resolved {
        pe_outs.push(e);
        cl_outs.push(c);
        for (q, d) in ds {
            names[q] = d;
        }
    }
    let _ = NAMES.set(names);
    // the name dictionaries point into the input files: they stay mapped until after the write
    stage!(t0, "resolved persons");

    // ---- per person partition: sessions, credit, assists
    let ncat = cat_names.len();
    let next = AtomicUsize::new(0);
    let mut order: Vec<(usize, usize)> = (0..npart())
        .map(|p| (pe_outs.iter().map(|o| o.len(p)).sum::<usize>(), p))
        .collect();
    // output rows: about one per 9 surviving events, spread over the workers
    let rows_est = order.iter().map(|o| o.0).sum::<usize>() / 8 / nthreads + 1024;
    order.sort_unstable_by(|a, b| b.cmp(a));
    let order: Vec<usize> = order.into_iter().map(|x| x.1).collect();
    let workers: Vec<Worker> = rayon::broadcast(|_bc| {
        {
                let (next, pe_outs, cl_outs, ctx, order) = (&next, &pe_outs, &cl_outs, &ctx, &order);
                (move || {
                    let mut w = Worker::new(ctx, ncat);
                    w.rows.reserve(rows_est);
                    let (mut evs2, mut cls2): (Vec<PEv>, Vec<PCl>) = (Vec::new(), Vec::new());
                    let mut evs3: Vec<PEv> = Vec::new();
                    let mut evs_pre: Vec<PEv> = Vec::new();
                    let mut soff: Vec<u32> = Vec::new();
                    let (mut eoff, mut coff): (Vec<u32>, Vec<u32>) = (Vec::new(), Vec::new());
                    let (mut tg, mut tso, mut tp) = (0u64, 0u64, 0u64);
                    loop {
                        let i = next.fetch_add(1, AtOrd::Relaxed);
                        if i >= npart() {
                            break;
                        }
                        let p = order[i];
                        let ta = Instant::now();
                        let esrc: Vec<&[PEv]> = pe_outs.iter().flat_map(|o| o.slices(p)).collect();
                        let csrc: Vec<&[PCl]> = cl_outs.iter().flat_map(|o| o.slices(p)).collect();
                        let n_e: usize = esrc.iter().map(|v| v.len()).sum();
                        let tb = Instant::now();
                        // Group by person. The person hash's top `kbits` bits split the partition
                        // into sub-ranges of ~16K events (a sub-range's sort stays inside half an
                        // L2 at any data size); the next `bbits` bits pick a bucket within the
                        // sub-range (~4 events per bucket, so few buckets hold two persons). A
                        // sub-range is radix-sorted by (bucket, ts), so each bucket is in ts order.
                        let kbits = n_e.div_ceil(8 << 10).next_power_of_two().trailing_zeros();
                        let nsub = 1usize << kbits;
                        let per_sub = n_e >> kbits;
                        let bbits = (per_sub / 4).clamp(16, 1 << 14).next_power_of_two().trailing_zeros();
                        let nb = 1usize << bbits;
                        let gbits = kbits + bbits;
                        let hp = |k: u64| k.wrapping_mul(0xA24B_AED4_963E_E407);
                        let gbucket = |k: u64| (hp(k) >> (64 - gbits)) as usize;
                        let sub_of = |k: u64| (hp(k) >> (64 - gbits)) as usize >> bbits;
                        let local = |k: u64| (hp(k) >> (64 - gbits)) as usize & (nb - 1);
                        counting_sort(&esrc, &mut evs_pre, &mut soff, nsub, |e| sub_of(e.person));
                        counting_sort(&csrc, &mut cls2, &mut coff, nsub * nb, |c| gbucket(c.person));
                        let tc = Instant::now();
                        tg += (tb - ta).as_nanos() as u64;
                        tso += (tc - tb).as_nanos() as u64;
                        for sb in 0..nsub {
                            let part = &evs_pre[soff[sb] as usize..soff[sb + 1] as usize];
                            if part.is_empty() {
                                continue;
                            }
                            sort_by_bucket_ts(&[part], &mut evs2, &mut evs3, &mut eoff, nb, local);
                            // visit only the non-empty buckets, in order, by walking the array
                            let mut at = 0usize;
                            while at < evs2.len() {
                                let b = local(evs2[at].person);
                                let end = eoff[b + 1] as usize;
                                let es = &mut evs2[at..end];
                                at = end;
                                if es.len() > 1 {
                                    // the bucket is in ts order; when it holds several persons, a
                                    // stable sort by person keeps each person's events in ts order
                                    let p0 = es[0].person;
                                    if !es.iter().all(|e| e.person == p0) {
                                        es.sort_by_key(|e| e.person);
                                    }
                                }
                                let g = sb * nb + b;
                                let cs = &mut cls2[coff[g] as usize..coff[g + 1] as usize];
                                if cs.len() > 1 {
                                    let p0 = cs[0].person;
                                    if cs.iter().all(|c| c.person == p0) {
                                        cs.sort_unstable_by_key(|c| c.ts);
                                    } else {
                                        cs.sort_unstable_by_key(|c| ((c.person as u128) << 32) | c.ts as u128);
                                    }
                                }
                                let (es, cs) = (&*es, &*cs);
                                let mut ci = 0usize;
                                let mut i = 0usize;
                                while i < es.len() {
                                    let person = es[i].person;
                                    let mut j = i + 1;
                                    while j < es.len() && es[j].person == person {
                                        j += 1;
                                    }
                                    while ci < cs.len() && cs[ci].person < person {
                                        ci += 1;
                                    }
                                    let mut cj = ci;
                                    while cj < cs.len() && cs[cj].person == person {
                                        cj += 1;
                                    }
                                    w.person(person, &es[i..j], &cs[ci..cj]);
                                    ci = cj;
                                    i = j;
                                }
                            }
                        }
                        tp += tc.elapsed().as_nanos() as u64;
                    }
                    if std::env::var_os("SS_TIMERS").is_some() {
                        eprintln!("sessions thread: gather {} ms, sort {} ms, persons {} ms", tg / 1_000_000, tso / 1_000_000, tp / 1_000_000);
                    }
                    w
                })()
        }
    });
    stage!(t0, "sessions");
    // all partition buffers are dead: return the arena to the kernel off the critical path, so
    // neither the Parquet writers nor the exit teardown pay for it
    drop(pe_outs);
    drop(cl_outs);
    if std::env::var_os("SS_TIMERS").is_some() {
        eprintln!("arena: {} MB of fresh blocks", arena::used() >> 20);
    }
    // SAFETY: no PartBuf is alive any more; every worker zaps its share of the pages, so the
    // final munmap (and the exit teardown) has almost nothing left to walk
    if std::env::var_os("SS_NO_ZAP").is_none() {
        rayon::broadcast(|bc| unsafe { arena::zap_part(bc.index(), bc.num_threads()) });
    }
    std::thread::spawn(|| unsafe { arena::unmap() });

    // ---- merge campaign accumulators
    let mut camp_total = vec![CampAcc::default(); (ctx.ncamp + 1) * ndays];
    for w in &workers {
        for (k, t) in camp_total.iter_mut().enumerate() {
            t.sessions += w.camp_sp[k][0] as u64;
            t.purchasing += w.camp_sp[k][1] as u64;
            t.revenue += w.camp_rev[k];
            t.assisted += w.camp_assisted[k] as u64;
        }
    }

    // ---- write Parquet
    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&ud_dir)?;
    fs::create_dir_all(&cd_dir)?;

    let cd_job = || -> Res<()> {
        let mut c_campaign = Vec::new();
        let mut c_channel = Vec::new();
        let mut c_day = Vec::new();
        let mut c_sessions = Vec::new();
        let mut c_purch = Vec::new();
        let mut c_revenue = Vec::new();
        let mut c_assisted = Vec::new();
        for c in 0..=ctx.ncamp {
            for d in 0..ndays {
                let a = &camp_total[c * ndays + d];
                if a.sessions == 0 && a.assisted == 0 {
                    continue;
                }
                let (name, channel): (&[u8], &[u8]) =
                    if c == ctx.ncamp { (b"organic", b"organic") } else { camp_names[c] };
                c_campaign.push(ByteArray::from(name.to_vec()));
                c_channel.push(ByteArray::from(channel.to_vec()));
                c_day.push((ws_day + d as i64) as i32);
                c_sessions.push(a.sessions as i64);
                c_purch.push(a.purchasing as i64);
                c_revenue.push(a.revenue as i64);
                c_assisted.push(a.assisted as i64);
            }
        }
        write_parquet(
            &cd_dir.join("part-0.parquet"),
            CD_SCHEMA,
            c_campaign.len(),
            vec![
                Col::Bytes(c_campaign, None),
                Col::Bytes(c_channel, None),
                Col::I32(c_day),
                Col::I64(c_sessions),
                Col::I64(c_purch),
                Col::I64(c_revenue),
                Col::I64(c_assisted),
            ],
        )
    };

    let ud_job = |wi: usize, w: &Worker| -> Res<()> {
        let mut u_user = Vec::with_capacity(w.rows.len());
        let mut u_day = Vec::with_capacity(w.rows.len());
        let mut u_sessions = Vec::with_capacity(w.rows.len());
        let mut u_events = Vec::with_capacity(w.rows.len());
        let mut u_revenue = Vec::with_capacity(w.rows.len());
        let mut u_top = Vec::new();
        let mut u_def = Vec::with_capacity(w.rows.len());
        let mut buf = Vec::new();
        for r in &w.rows {
            buf.clear();
            id_bytes(r.person, &mut buf);
            u_user.push(ByteArray::from(buf.clone()));
            u_day.push((ws_day + r.day as i64) as i32);
            u_sessions.push(r.sessions as i64);
            u_events.push(r.events as i64);
            u_revenue.push(r.revenue as i64);
            if r.top >= 0 {
                u_top.push(ByteArray::from(cat_names[r.top as usize].to_vec()));
                u_def.push(1i16);
            } else {
                u_def.push(0i16);
            }
        }
        write_parquet(
            &ud_dir.join(format!("part-{wi}.parquet")),
            UD_SCHEMA,
            u_user.len(),
            vec![
                Col::Bytes(u_user, None),
                Col::I32(u_day),
                Col::I64(u_sessions),
                Col::I64(u_events),
                Col::I64(u_revenue),
                Col::Bytes(u_top, Some(u_def)),
            ],
        )
    };

    // hand-written writer (default); SS_PQ_CRATE=1 uses the parquet crate instead
    let cd_job_hw = || -> Res<()> {
        let (mut cname, mut cname_end) = (Vec::new(), Vec::new());
        let (mut chan, mut chan_end) = (Vec::new(), Vec::new());
        let (mut day, mut sess, mut purch, mut rev, mut asst): (Vec<i32>, Vec<i32>, Vec<i32>, Vec<i64>, Vec<i32>) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for c in 0..=ctx.ncamp {
            for d in 0..ndays {
                let a = &camp_total[c * ndays + d];
                if a.sessions == 0 && a.assisted == 0 {
                    continue;
                }
                let (name, channel): (&[u8], &[u8]) =
                    if c == ctx.ncamp { (b"organic", b"organic") } else { camp_names[c] };
                cname.extend_from_slice(name);
                cname_end.push(cname.len() as u32);
                chan.extend_from_slice(channel);
                chan_end.push(chan.len() as u32);
                day.push((ws_day + d as i64) as i32);
                sess.push(a.sessions as i32);
                purch.push(a.purchasing as i32);
                rev.push(a.revenue as i64);
                asst.push(a.assisted as i32);
            }
        }
        let cols = [
            pq::Column::Str { name: "campaign_id", data: &cname, ends: &cname_end },
            pq::Column::Str { name: "channel", data: &chan, ends: &chan_end },
            pq::Column::Date { name: "day", values: &day },
            pq::Column::I32 { name: "sessions", values: &sess },
            pq::Column::I32 { name: "purchasing_sessions", values: &purch },
            pq::Column::I64 { name: "revenue_cents", values: &rev },
            pq::Column::I32 { name: "assisted_sessions", values: &asst },
        ];
        let mut f = fs::File::create(cd_dir.join("part-0.parquet"))?;
        pq::write_file(&mut f, day.len(), &cols)?;
        Ok(())
    };
    let ud_job_hw = |wi: usize, w: &Worker| -> Res<()> {
        let n = w.rows.len();
        let mut user: Vec<u8> = Vec::with_capacity(n * 16 + 64);
        let (mut top, mut top_end, mut present) = (Vec::new(), Vec::new(), Vec::with_capacity(n));
        let (mut day, mut sess, mut evs, mut rev) =
            (Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n));
        for r in &w.rows {
            id_plain(r.person, &mut user);
            day.push((ws_day + r.day as i64) as i32);
            sess.push(r.sessions as i32);
            evs.push(r.events as i32);
            rev.push(r.revenue as i64);
            if r.top >= 0 {
                top.extend_from_slice(cat_names[r.top as usize]);
                top_end.push(top.len() as u32);
                present.push(true);
            } else {
                present.push(false);
            }
        }
        let cols = [
            pq::Column::PlainStr { name: "user_id", body: &user },
            pq::Column::Date { name: "day", values: &day },
            pq::Column::I32 { name: "sessions", values: &sess },
            pq::Column::I32 { name: "events", values: &evs },
            pq::Column::I64 { name: "revenue_cents", values: &rev },
            pq::Column::OptStr { name: "top_category", data: &top, ends: &top_end, present: &present },
        ];
        let mut f = fs::File::create(ud_dir.join(format!("part-{wi}.parquet")))?;
        pq::write_file(&mut f, n, &cols)?;
        Ok(())
    };

    let (r1, r2) = if std::env::var_os("SS_PQ_CRATE").is_some() {
        rayon::join(cd_job, || {
            workers.par_iter().enumerate().map(|(i, w)| ud_job(i, w)).collect::<Res<Vec<()>>>()
        })
    } else {
        rayon::join(cd_job_hw, || {
            workers.par_iter().enumerate().map(|(i, w)| ud_job_hw(i, w)).collect::<Res<Vec<()>>>()
        })
    };
    r1?;
    r2?;
    stage!(t0, "written");
    prof_dump();
    if std::env::var_os("SS_TIMERS").is_some() {
        // SAFETY: getrusage fills a plain struct
        unsafe {
            let mut ru: libc::rusage = std::mem::zeroed();
            libc::getrusage(libc::RUSAGE_SELF, &mut ru);
            eprintln!(
                "shopping: rusage user {:.3} s, sys {:.3} s, minflt {}, majflt {}, maxrss {} MB, nvcsw {}, nivcsw {}",
                ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 / 1e6,
                ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1e6,
                ru.ru_minflt, ru.ru_majflt, ru.ru_maxrss / 1024, ru.ru_nvcsw, ru.ru_nivcsw
            );
        }
    }
    // Drop the input mappings' pages with all workers in parallel (MADV_DONTNEED runs under the
    // shared mmap lock), so the single-threaded exit teardown has almost nothing left to walk.
    if std::env::var_os("SS_NO_ZAP").is_none() {
        let regions: Vec<(usize, usize)> =
            maps.0.iter().chain(maps.1.iter()).chain(maps.2.iter()).map(|m| (m.as_ptr() as usize, m.len())).collect();
        rayon::broadcast(|bc| {
            for (i, &(a, l)) in regions.iter().enumerate() {
                if i % bc.num_threads() == bc.index() && l > 0 {
                    // SAFETY: nothing reads the inputs any more (every file is written)
                    unsafe { libc::madvise(a as *mut libc::c_void, l, libc::MADV_DONTNEED) };
                }
            }
        });
    }
    let _ = maps; // leaked: the mappings stay until exit
    // all files are closed; exit without freeing the heap
    // SAFETY: every output file is written and closed and stderr is unbuffered: skip the
    // atexit handlers (allocator teardown), unless SS_STD_EXIT is set (measurement)
    if std::env::var_os("SS_STD_EXIT").is_some() {
        std::process::exit(0)
    }
    unsafe { libc::_exit(0) }
}

/// Stable LSD radix sort of the concatenation of `srcs` by (bucket(person), ts): three passes
/// on ts (11, 11 and 10 bits), then one counting pass on the bucket, so every bucket comes out
/// in ts order. One read builds all four histograms. Result in `dst`, bucket b at
/// off[b]..off[b + 1]; `tmp` is scratch.
fn sort_by_bucket_ts(
    srcs: &[&[PEv]],
    dst: &mut Vec<PEv>,
    tmp: &mut Vec<PEv>,
    off: &mut Vec<u32>,
    nb: usize,
    bucket: impl Fn(u64) -> usize,
) {
    let mut h0 = [0u32; 2048];
    let mut h1 = [0u32; 2048];
    let mut h2 = [0u32; 1024];
    off.clear();
    off.resize(nb + 1, 0);
    let mut n = 0usize;
    for src in srcs {
        n += src.len();
        for e in src.iter() {
            h0[(e.ts & 2047) as usize] += 1;
            h1[((e.ts >> 11) & 2047) as usize] += 1;
            h2[(e.ts >> 22) as usize] += 1;
            off[bucket(e.person) + 1] += 1;
        }
    }
    fn excl(h: &mut [u32]) {
        let mut acc = 0u32;
        for x in h.iter_mut() {
            let c = *x;
            *x = acc;
            acc += c;
        }
    }
    excl(&mut h0);
    excl(&mut h1);
    excl(&mut h2);
    for b in 0..nb {
        off[b + 1] += off[b];
    }
    dst.clear();
    dst.reserve(n);
    tmp.clear();
    tmp.reserve(n);
    // SAFETY: PEv is Copy; each pass below writes every slot 0..n exactly once before the next
    // pass reads it (the histogram ranges partition 0..n)
    unsafe {
        dst.set_len(n);
        tmp.set_len(n);
    }
    for src in srcs {
        for e in src.iter() {
            let k = (e.ts & 2047) as usize;
            tmp[h0[k] as usize] = *e;
            h0[k] += 1;
        }
    }
    for e in tmp.iter() {
        let k = ((e.ts >> 11) & 2047) as usize;
        dst[h1[k] as usize] = *e;
        h1[k] += 1;
    }
    for e in dst.iter() {
        let k = (e.ts >> 22) as usize;
        tmp[h2[k] as usize] = *e;
        h2[k] += 1;
    }
    // off[b] is the cursor of bucket b; afterwards off[b] = end of b, shifted back below
    for e in tmp.iter() {
        let k = bucket(e.person);
        dst[off[k] as usize] = *e;
        off[k] += 1;
    }
    for b in (1..=nb).rev() {
        off[b] = off[b - 1];
    }
    off[0] = 0;
}

/// Counting sort of the concatenation of `srcs` into `dst` by `key` (< nb);
/// `off[b]..off[b+1]` = bucket b.
#[inline]
fn counting_sort<T: Copy>(
    srcs: &[&[T]],
    dst: &mut Vec<T>,
    off: &mut Vec<u32>,
    nb: usize,
    key: impl Fn(&T) -> usize,
) {
    off.clear();
    off.resize(nb + 1, 0);
    let mut n = 0usize;
    for src in srcs {
        n += src.len();
        for x in src.iter() {
            off[key(x) + 1] += 1;
        }
    }
    for b in 0..nb {
        off[b + 1] += off[b];
    }
    dst.clear();
    dst.reserve(n);
    // SAFETY: T is Copy (no drop); every slot 0..n is written exactly once below before any
    // read, because the bucket ranges off[b]..off[b+1] partition 0..n.
    unsafe { dst.set_len(n) };
    // scatter using off[b] as the cursor of bucket b, then shift the offsets back
    for src in srcs {
        for x in src.iter() {
            let k = key(x);
            dst[off[k] as usize] = *x;
            off[k] += 1;
        }
    }
    // now off[b] = end of bucket b = start of bucket b + 1
    for b in (1..=nb).rev() {
        off[b] = off[b - 1];
    }
    off[0] = 0;
}

/// Single-threaded component timings on ~300 MB of events held in memory (measurement only).
#[inline(never)]
fn micro(event_chunks: &[&[u8]], ctx: &Ctx) {
    let mut data: Vec<u8> = Vec::new();
    for c in event_chunks {
        if data.len() > 300 << 20 {
            break;
        }
        data.extend_from_slice(c);
    }
    let mb = data.len() as f64 / 1e6;
    let lines = memchr::memchr_iter(b'\n', &data).count();
    let run = |name: &str, f: &mut dyn FnMut() -> usize| {
        let mut best = f64::MAX;
        let mut x = 0;
        for _ in 0..3 {
            let t = Instant::now();
            x = f();
            best = best.min(t.elapsed().as_secs_f64());
        }
        eprintln!(
            "micro: {:>7.1} ms  {:>6.1} ns/line  {:>6.0} MB/s  {}  [{}]",
            best * 1e3,
            best * 1e9 / lines as f64,
            mb / best,
            name,
            x
        );
    };
    run("memchr newlines", &mut || memchr::memchr_iter(b'\n', &data).count());
    for _ in 0..3 {
        run("split v1", &mut || {
            let mut n = 0;
            for_each_record::<7>(&data, |f| n += std::hint::black_box(f).iter().map(|x| x.len()).sum::<usize>());
            n
        });
        run("split v5 (line at a time)", &mut || {
            let mut n = 0;
            for_each_record_v5::<7>(&data, |f| n += std::hint::black_box(f).iter().map(|x| x.len()).sum::<usize>());
            n
        });
    }
    run("split 7 fields", &mut || {
        let mut n = 0;
        for_each_record::<7>(&data, |f| n += f[0].len());
        n
    });
    run("+ pack eid", &mut || {
        let mut n = 0u64;
        for_each_record::<7>(&data, |f| n = n.wrapping_add(id_key(f[0])));
        n as usize
    });
    let dend = data.as_ptr() as usize + data.len();
    run("+ pack16 eid", &mut || {
        let mut n = 0u64;
        for_each_record::<7>(&data, |f| n = n.wrapping_add(id_key_at(f[0], dend)));
        n as usize
    });
    run("+ parse ts", &mut || {
        let mut n = 0i64;
        for_each_record::<7>(&data, |f| n = n.wrapping_add(parse_ts(f[2]).unwrap_or(0)));
        n as usize
    });
    run("+ parse ingest", &mut || {
        let mut n = 0i64;
        for_each_record::<7>(&data, |f| n = n.wrapping_add(parse_ts(f[3]).unwrap_or(0)));
        n as usize
    });
    run("+ pack user", &mut || {
        let mut n = 0u64;
        for_each_record::<7>(&data, |f| n = n.wrapping_add(id_key(f[1])));
        n as usize
    });
    run("+ product category", &mut || {
        let mut n = 0usize;
        for_each_record::<7>(&data, |f| n += ctx.category(f[5], 0) as usize);
        n
    });
    let mut dc = DateCache::new();
    dc.end = dend;
    run("full parse_event", &mut || {
        let mut n = 0u64;
        for_each_record::<7>(&data, |f| {
            if !f[0].is_empty() {
                let r = parse_event(f, ctx, &mut dc);
                n = n.wrapping_add(r.eid ^ r.user ^ r.pc as u64);
            }
        });
        n as usize
    });
    run("parse_event + scatter 512", &mut || {
        let mut parts: Vec<Vec<EvRec>> = (0..npart()).map(|_| Vec::with_capacity(lines / npart() + 64)).collect();
        for_each_record::<7>(&data, |f| {
            if !f[0].is_empty() {
                let r = parse_event(f, ctx, &mut dc);
                parts[part_of(r.eid)].push(r);
            }
        });
        parts.iter().map(|p| p.len()).sum()
    });
    // production splitter, cached dates, 16-byte packing: cumulative field costs
    let mut dc2 = DateCache::new();
    dc2.end = dend;
    run("v5: split", &mut || {
        let mut n = 0usize;
        for_each_record_v5::<7>(&data, |f| n += std::hint::black_box(f)[0].len());
        n
    });
    run("v5: + 2 packs", &mut || {
        let mut n = 0u64;
        for_each_record_v5::<7>(&data, |f| n = n.wrapping_add(id_key_at(f[0], dend) ^ id_key_at(f[1], dend)));
        n as usize
    });
    run("v5: + 2 packs + ts", &mut || {
        let mut n = 0u64;
        for_each_record_v5::<7>(&data, |f| {
            n = n.wrapping_add(id_key_at(f[0], dend) ^ id_key_at(f[1], dend));
            n = n.wrapping_add(parse_ts_c(f[2], &mut dc2).unwrap_or(0) as u64);
        });
        n as usize
    });
    run("v5: + 2 packs + ts + ingest", &mut || {
        let mut n = 0u64;
        for_each_record_v5::<7>(&data, |f| {
            n = n.wrapping_add(id_key_at(f[0], dend) ^ id_key_at(f[1], dend));
            n = n.wrapping_add(parse_ts_c(f[2], &mut dc2).unwrap_or(0) as u64);
            n = n.wrapping_add(parse_ts_c(f[3], &mut dc2).unwrap_or(0) as u64);
        });
        n as usize
    });
    run("v5: full parse_event", &mut || {
        let mut n = 0u64;
        for_each_record_v5::<7>(&data, |f| {
            if !f[0].is_empty() {
                let r = parse_event(f, ctx, &mut dc2);
                n = n.wrapping_add(r.eid ^ r.user ^ r.ingest as u64 ^ r.ts as u64 ^ r.pc as u64);
            }
        });
        n as usize
    });
}

const UD_SCHEMA: &str = "message user_daily {
    REQUIRED BYTE_ARRAY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CD_SCHEMA: &str = "message campaign_daily {
    REQUIRED BYTE_ARRAY campaign_id (UTF8);
    REQUIRED BYTE_ARRAY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

enum Col {
    Bytes(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn write_parquet(path: &Path, schema: &str, rows: usize, cols: Vec<Col>) -> Res<()> {
    let schema = Arc::new(parse_message_type(schema)?);
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(path)?;
    let mut writer = SerializedFileWriter::new(file, schema, props)?;
    if rows > 0 {
        let mut rg = writer.next_row_group()?;
        for col in cols {
            let mut cw = rg.next_column()?.ok_or("schema has fewer columns than data")?;
            match col {
                Col::Bytes(v, def) => {
                    cw.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None)?;
                }
                Col::I32(v) => {
                    cw.typed::<Int32Type>().write_batch(&v, None, None)?;
                }
                Col::I64(v) => {
                    cw.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
            }
            cw.close()?;
        }
        rg.close()?;
    }
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference implementation of timestamp parsing (same logic as src/bin/reference.rs).
    fn ref_parse_ts(s: &[u8]) -> Option<i64> {
        fn num(b: &[u8]) -> Option<i64> {
            if b.is_empty() {
                return None;
            }
            let mut v: i64 = 0;
            for &c in b {
                if !c.is_ascii_digit() {
                    return None;
                }
                v = v * 10 + (c - b'0') as i64;
            }
            Some(v)
        }
        if s.is_empty() {
            return None;
        }
        if s.iter().all(|c| c.is_ascii_digit()) {
            let mut v: i64 = 0;
            for &c in s {
                v = v.checked_mul(10)?.checked_add((c - b'0') as i64)?;
            }
            return Some(v);
        }
        let offset_ms = match s.len() {
            20 => {
                if s[19] != b'Z' {
                    return None;
                }
                0
            }
            25 => {
                let sign = match s[19] {
                    b'+' => 1,
                    b'-' => -1,
                    _ => return None,
                };
                if s[22] != b':' {
                    return None;
                }
                let oh = num(&s[20..22])?;
                let om = num(&s[23..25])?;
                if oh > 23 || om > 59 {
                    return None;
                }
                sign * (oh * 3600 + om * 60) * 1000
            }
            _ => return None,
        };
        if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let (y, mo, d) = (num(&s[0..4])?, num(&s[5..7])?, num(&s[8..10])?);
        let (h, mi, se) = (num(&s[11..13])?, num(&s[14..16])?, num(&s[17..19])?);
        if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se) * 1000 - offset_ms)
    }


    fn split_reference<const N: usize>(chunk: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for line in chunk.split(|&b| b == b'\n') {
            let mut f: [&[u8]; N] = [&[]; N];
            if split_fields(line, &mut f) {
                out.push(f.iter().map(|v| v.to_vec()).collect());
            }
        }
        out
    }

    #[test]
    fn splitters_match_reference() {
        let alphabet: &[u8] = b"aab,,\"\"\n ";
        let mut rng: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        // 7-field lines need about 6 commas per line: commas common, newlines rarer
        let alpha7: &[u8] = b"aaabbcc,,,,,,\"\"\n ";
        for round in 0..100_000 {
            let n = (next() % 700) as usize;
            let chunk: Vec<u8> = (0..n).map(|_| alphabet[(next() as usize) % alphabet.len()]).collect();
            let want = split_reference::<3>(&chunk);
            let mut got1 = Vec::new();
            for_each_record::<3>(&chunk, |f| got1.push(f.iter().map(|v| v.to_vec()).collect::<Vec<_>>()));
            assert_eq!(got1, want, "v1 round {round}: {:?}", String::from_utf8_lossy(&chunk));
            let mut got5 = Vec::new();
            for_each_record_v5::<3>(&chunk, |f| got5.push(f.iter().map(|v| v.to_vec()).collect::<Vec<_>>()));
            assert_eq!(got5, want, "v5 round {round}: {:?}", String::from_utf8_lossy(&chunk));
            let chunk7: Vec<u8> = (0..n * 2).map(|_| alpha7[(next() as usize) % alpha7.len()]).collect();
            let want7 = split_reference::<7>(&chunk7);
            let mut got7 = Vec::new();
            for_each_record_v5::<7>(&chunk7, |f| got7.push(f.iter().map(|v| v.to_vec()).collect::<Vec<_>>()));
            assert_eq!(got7, want7, "v5/7 round {round}: {:?}", String::from_utf8_lossy(&chunk7));
            let mut got71 = Vec::new();
            for_each_record::<7>(&chunk7, |f| got71.push(f.iter().map(|v| v.to_vec()).collect::<Vec<_>>()));
            assert_eq!(got71, want7, "v1/7 round {round}: {:?}", String::from_utf8_lossy(&chunk7));
        }
    }


    #[test]
    fn plain_decode_matches_id_bytes() {
        let alphabet: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
        let mut rng: u64 = 0x1234_5678_9abc_def1;
        for _ in 0..200_000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let len = 1 + (rng % 10) as usize;
            let id: Vec<u8> = (0..len).map(|i| alphabet[((rng >> (i * 6 % 58)) as usize + i * 7) % alphabet.len()]).collect();
            let k = pack(&id).unwrap();
            let mut a = Vec::new();
            id_bytes(k, &mut a);
            assert_eq!(a, id);
            let mut b = vec![9u8; 3];
            id_plain(k, &mut b);
            assert_eq!(&b[..3], &[9, 9, 9]);
            assert_eq!(u32::from_le_bytes(b[3..7].try_into().unwrap()) as usize, len);
            assert_eq!(&b[7..], &id[..]);
        }
    }

    #[test]
    fn pack_matches_scalar() {
        let mut rng: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let alphabet: &[u8] = b"0189AZaz_ \t\"\x00\xff-/:@[`{";
        for _ in 0..2_000_000 {
            let n = (next() % 13) as usize;
            let v: Vec<u8> = (0..n)
                .map(|_| if next() % 4 == 0 { alphabet[(next() as usize) % alphabet.len()] } else { b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"[(next() % 63) as usize] })
                .collect();
            assert_eq!(pack(&v), pack_scalar(&v), "{:?}", v);
            let mut buf = v.clone();
            buf.extend((0..16).map(|_| next() as u8));
            // SAFETY: buf has v.len() + 16 bytes
            assert_eq!(unsafe { pack16(buf.as_ptr(), v.len()) }, pack_scalar(&v), "{:?}", v);
        }
    }

    #[test]
    fn timestamps_match_reference() {
        let templates: &[&[u8]] = &[
            b"2026-10-03T23:58:10Z",
            b"2028-02-29T00:00:00Z",
            b"2027-02-28T23:59:59Z",
            b"2026-10-03T19:58:10-04:00",
            b"2026-10-04T01:30:00+02:00",
            b"2026-10-04T01:30:00+23:59",
            b"2026-10-04T01:30:00-00:00",
            b"1791071890000",
            b"0001791021600000",
        ];
        let alphabet: &[u8] = b"0123456789-+:TZtz /.\x00\xff9\x2f\x3a\x3f";
        let mut rng: u64 = 0x1234_5678_9abc_def1;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut checked = 0u64;
        let mut shared = DateCache::new();
        for t in templates {
            assert_eq!(parse_ts(t), ref_parse_ts(t), "{:?}", String::from_utf8_lossy(t));
            for _ in 0..400_000 {
                let mut v = t.to_vec();
                for _ in 0..(1 + next() % 2) {
                    let i = (next() as usize) % v.len();
                    v[i] = alphabet[(next() as usize) % alphabet.len()];
                }
                assert_eq!(parse_ts(&v), ref_parse_ts(&v), "{:?}", String::from_utf8_lossy(&v));
                assert_eq!(parse_ts_c(&v, &mut shared), ref_parse_ts(&v), "cached {:?}", String::from_utf8_lossy(&v));
                checked += 1;
            }
        }
        // every day of 2024..=2029 at a few times, both forms
        for y in 2024..=2029 {
            for m in 0..=13 {
                for d in 0..=32 {
                    for hms in ["00:00:00", "23:59:59", "12:30:00", "24:00:00", "23:60:00", "23:59:60"] {
                        let f1 = format!("{y:04}-{m:02}-{d:02}T{hms}Z");
                        let f2 = format!("{y:04}-{m:02}-{d:02}T{hms}+05:30");
                        assert_eq!(parse_ts(f1.as_bytes()), ref_parse_ts(f1.as_bytes()), "{f1}");
                        assert_eq!(parse_ts(f2.as_bytes()), ref_parse_ts(f2.as_bytes()), "{f2}");
                        assert_eq!(parse_ts_c(f1.as_bytes(), &mut shared), ref_parse_ts(f1.as_bytes()), "cached {f1}");
                        assert_eq!(parse_ts_c(f2.as_bytes(), &mut shared), ref_parse_ts(f2.as_bytes()), "cached {f2}");
                        checked += 2;
                    }
                }
            }
        }
        eprintln!("checked {checked} timestamps");
    }
}
