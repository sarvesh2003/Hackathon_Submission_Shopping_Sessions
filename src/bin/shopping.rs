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
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::Instant;

use hashbrown::HashMap;
use memmap2::Mmap;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const SESSION_GAP_MS: i64 = 1_800_000;
const PRICE_MAX: u32 = 10_000_000;

const PART_BITS: u32 = 9;
const NPART: usize = 1 << PART_BITS;
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

#[inline]
fn pack(id: &[u8]) -> Option<u64> {
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

struct Interner {
    map: std::collections::HashMap<Vec<u8>, u64>,
    names: Vec<Vec<u8>>,
}

static INTERNER: LazyLock<Mutex<Interner>> = LazyLock::new(|| {
    Mutex::new(Interner { map: std::collections::HashMap::new(), names: Vec::new() })
});

#[inline]
fn id_key(id: &[u8]) -> u64 {
    match pack(id) {
        Some(k) => k,
        None => intern(id),
    }
}

#[cold]
fn intern(id: &[u8]) -> u64 {
    let mut g = INTERNER.lock().unwrap();
    if let Some(&k) = g.map.get(id) {
        return k;
    }
    let k = FALLBACK | g.names.len() as u64;
    g.names.push(id.to_vec());
    g.map.insert(id.to_vec(), k);
    k
}

fn id_bytes(k: u64, out: &mut Vec<u8>) {
    if k & FALLBACK != 0 {
        let g = INTERNER.lock().unwrap();
        out.extend_from_slice(&g.names[(k & !FALLBACK) as usize]);
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
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - PART_BITS)) as usize
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

#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// F1 / F2 / F3 -> epoch ms. None = bad (including an F3 value too large for i64).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso(s);
    }
    if n == 0 {
        return None;
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

#[inline]
fn parse_iso(s: &[u8]) -> Option<i64> {
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
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d2(s, 0)? * 100 + d2(s, 2)?;
    let month = d2(s, 5)?;
    let day = d2(s, 8)?;
    let hour = d2(s, 11)?;
    let minute = d2(s, 14)?;
    let second = d2(s, 17)?;
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
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

/// Splits every file (header line skipped) into chunks of whole lines.
fn chunk_files(maps: &[Mmap], target: usize) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    for m in maps {
        let data: &[u8] = m;
        let mut pos = match memchr::memchr(b'\n', data) {
            Some(p) => p + 1,
            None => continue, // header only
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
    }
    chunks
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
#[derive(Clone, Copy)]
struct PEv {
    person: u64,
    ts: u32,
    pc: u32,
}

/// A valid click, keyed by person. 24 bytes.
#[derive(Clone, Copy)]
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
    fn category(&self, product: &[u8]) -> u8 {
        match pack(product) {
            Some(k) => self.prod_packed.get(&k).copied().unwrap_or(self.unknown_cat),
            None => self.prod_bytes.get(product).copied().unwrap_or(self.unknown_cat),
        }
    }
    #[inline]
    fn campaign(&self, c: &[u8]) -> Option<u16> {
        match pack(c) {
            Some(k) => self.camp_packed.get(&k).copied(),
            None => self.camp_bytes.get(c).copied(),
        }
    }
}

/// Fields already split; event_id is non-empty.
#[inline(always)]
fn parse_event(f: &[&[u8]; 7], ctx: &Ctx) -> EvRec {
    let eid = id_key(f[0]);
    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
    let mut rec = EvRec { eid, user: INVALID, ingest, ts: 0, pc: 0 };
    let (user, ts_raw, etype, product, price_raw) = (f[1], f[2], f[4], f[5], f[6]);
    if user.is_empty() || product.is_empty() {
        return rec;
    }
    let ts = match parse_ts(ts_raw) {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => return rec,
    };
    let price = match etype {
        b"view" | b"cart" => 0,
        b"purchase" => match parse_price(price_raw) {
            Some(p) => p,
            None => return rec,
        },
        _ => return rec,
    };
    if ingest.saturating_sub(ts) > DAY_MS {
        return rec;
    }
    rec.user = id_key(user);
    rec.ts = (ts - ctx.ws) as u32;
    rec.pc = price | ((ctx.category(product) as u32) << 24);
    rec
}

/// (user key, ts since click window start, click_id key, campaign) of a valid click.
#[inline(always)]
fn parse_click(f: &[&[u8]; 4], ctx: &Ctx) -> Option<(u64, u32, u64, u16)> {
    let (cid, user, camp, ts_raw) = (f[0], f[1], f[2], f[3]);
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let camp = ctx.campaign(camp)?;
    let ts = parse_ts(ts_raw)?;
    if ts < ctx.cws || ts >= ctx.we {
        return None;
    }
    Some((id_key(user), (ts - ctx.cws) as u32, id_key(cid), camp))
}

// ======================================================================= links

fn parse_links(link_chunks: &[&[u8]]) -> Vec<Vec<(u64, u64)>> {
    link_chunks
        .par_iter()
        .map(|chunk| {
            let mut v = Vec::new();
            for_each_record::<2>(chunk, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((id_key(f[0]), id_key(f[1])));
                }
            });
            v
        })
        .collect()
}

/// Maps every linked ID whose person is not itself to its person's key.
/// Partitioned by `part_of(id)` so the maps can be built in parallel.
struct PersonMap {
    parts: Vec<HashMap<u64, u64>>,
}

impl PersonMap {
    #[inline]
    fn person(&self, id: u64) -> u64 {
        self.parts[part_of(id)].get(&id).copied().unwrap_or(id)
    }
}

fn build_person_map(pairs: &[Vec<(u64, u64)>]) -> PersonMap {
    // dense indices: sorted unique keys
    let mut keys: Vec<u64> = pairs.par_iter().flat_map_iter(|v| v.iter().flat_map(|&(a, b)| [a, b])).collect();
    keys.par_sort_unstable();
    keys.dedup();
    let idx: Vec<Vec<(u32, u32)>> = pairs
        .par_iter()
        .map(|v| {
            v.iter()
                .map(|&(a, b)| {
                    (keys.binary_search(&a).unwrap() as u32, keys.binary_search(&b).unwrap() as u32)
                })
                .collect()
        })
        .collect();
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
    for v in &idx {
        for &(a, b) in v {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    // smallest ID per root (byte-wise; integer order for packed keys)
    let mut best: Vec<u64> = vec![INVALID; n];
    let mut roots: Vec<u32> = Vec::with_capacity(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        roots.push(r as u32);
        if best[r] == INVALID || id_cmp(keys[i], best[r]) == std::cmp::Ordering::Less {
            best[r] = keys[i];
        }
    }
    // scatter (id, person) by partition, then build the small maps in parallel
    let mut by_part: Vec<Vec<(u64, u64)>> = new_parts();
    for i in 0..n {
        let b = best[roots[i] as usize];
        if b != keys[i] {
            by_part[part_of(keys[i])].push((keys[i], b)); // own-person IDs need no entry
        }
    }
    let parts = by_part
        .into_par_iter()
        .map(|v| {
            let mut m = HashMap::with_capacity(v.len());
            m.extend(v);
            m
        })
        .collect();
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
    camp: Vec<CampAcc>, // (ncamp + 1) * ndays
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
            camp: vec![CampAcc::default(); (ctx.ncamp + 1) * ctx.ndays],
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
            self.camp[c * self.ndays + d].assisted += (e - s) as u64;
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
            let day = ((start - self.day0_ms) / DAY_MS) as u16;
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
                let a = &mut self.camp[organic * nd + s.day as usize];
                a.sessions += 1;
                a.purchasing += s.purchase as u64;
                a.revenue += s.revenue;
            }
            return;
        }

        // first session index of each day (sessions are sorted by start)
        {
            let mut k = 0usize;
            for d in 0..=nd {
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
            let a = &mut self.camp[credited * nd + s.day as usize];
            a.sessions += 1;
            a.purchasing += s.purchase as u64;
            a.revenue += s.revenue;
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
    (0..NPART).map(|_| Vec::new()).collect()
}

/// What one parse thread produces.
struct ParseOut {
    ev: Vec<Vec<EvRec>>, // by event_id partition
    cl: Vec<Vec<PCl>>,   // by person partition
}

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

fn run() -> Res<()> {
    let t0 = Instant::now();
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
    let mut prod_rows: Vec<(&[u8], &[u8])> = Vec::new();
    for line in data_lines(&products_buf) {
        let mut f: [&[u8]; 2] = [&[]; 2];
        if split_fields(line, &mut f) {
            prod_rows.push((f[0], f[1]));
        }
    }
    let mut cat_names: Vec<&[u8]> = prod_rows.iter().map(|r| r.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    if cat_names.len() > 255 {
        return Err("more than 255 categories is not supported".into());
    }
    let cat_index = |c: &[u8]| cat_names.binary_search(&c).unwrap() as u8;
    let mut prod_packed = HashMap::new();
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

    // ---- map the big inputs
    let link_maps = map_files(&list_csv(&data_dir.join("links"))?)?;
    let event_maps = map_files(&list_csv(&data_dir.join("events"))?)?;
    let click_maps = map_files(&list_csv(&data_dir.join("clicks"))?)?;
    let link_chunks = chunk_files(&link_maps, CHUNK_BYTES);
    let event_chunks = chunk_files(&event_maps, CHUNK_BYTES);
    let click_chunks = chunk_files(&click_maps, CHUNK_BYTES);
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

    // ---- links (parallel parse)
    let link_pairs = parse_links(&link_chunks);
    stage!(t0, "parsed links");

    // ---- worker 0: union-find, then parse; others: events first, clicks once the map exists
    let gate: Gate<PersonMap> = Gate::new();
    let next = AtomicUsize::new(0);
    let n_ev = event_chunks.len();
    let n_all = n_ev + click_chunks.len();
    let cl_est = click_chunks.iter().map(|c| c.len()).sum::<usize>() / 50 / nthreads / NPART * 9 / 8 + 16;
    let est = event_chunks.iter().map(|c| c.len()).sum::<usize>() / 90 / nthreads / NPART + 16;
    let outs: Vec<ParseOut> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..nthreads)
            .map(|w| {
                let (gate, next, ctx, link_pairs) = (&gate, &next, &ctx, &link_pairs);
                let (event_chunks, click_chunks) = (&event_chunks, &click_chunks);
                s.spawn(move || {
                    if w == 0 {
                        gate.set(build_person_map(link_pairs));
                        stage!(t0, "union-find done");
                    }
                    let mut out = ParseOut {
                        ev: (0..NPART).map(|_| Vec::with_capacity(est)).collect(),
                        cl: (0..NPART).map(|_| Vec::with_capacity(cl_est)).collect(),
                    };
                    let mut pmap: Option<Arc<PersonMap>> = None;
                    let exp: u32 = std::env::var("SS_EXP").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
                    let mut dummy = 0usize;
                    loop {
                        let i = next.fetch_add(1, AtOrd::Relaxed);
                        if i >= n_all {
                            break;
                        }
                        if i < n_ev {
                            match exp {
                                1 => for_each_record::<7>(event_chunks[i], |f| {
                                    dummy += f[0].len();
                                }),
                                2 => for_each_record::<7>(event_chunks[i], |f| {
                                    if !f[0].is_empty() {
                                        let r = parse_event(f, ctx);
                                        dummy += r.ts as usize + r.eid as usize;
                                    }
                                }),
                                _ => for_each_record::<7>(event_chunks[i], |f| {
                                    if !f[0].is_empty() {
                                        let r = parse_event(f, ctx);
                                        out.ev[part_of(r.eid)].push(r);
                                    }
                                }),
                            }
                        } else {
                            let pm = pmap.get_or_insert_with(|| gate.wait()).clone();
                            for_each_record::<4>(click_chunks[i - n_ev], |f| {
                                if let Some((user, ts, cid, camp)) = parse_click(f, ctx) {
                                    let person = pm.person(user);
                                    out.cl[part_of(person)].push(PCl { person, cid, ts, camp });
                                }
                            });
                        }
                    }
                    if dummy == 1 {
                        eprintln!("dummy");
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let pmap = gate.wait();
    stage!(t0, "parsed events + clicks");

    // ---- dedup events per event_id partition, scatter survivors by person
    let next = AtomicUsize::new(0);
    let n_ev_recs: usize = outs.iter().map(|o| o.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
    let pe_est = n_ev_recs / nthreads / NPART * 9 / 8 + 16;
    let pe_outs: Vec<Vec<Vec<PEv>>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..nthreads)
            .map(|_| {
                let (next, outs, pmap) = (&next, &outs, &pmap);
                s.spawn(move || {
                    let mut parts: Vec<Vec<PEv>> =
                        (0..NPART).map(|_| Vec::with_capacity(pe_est)).collect();
                    let mut recs: Vec<EvRec> = Vec::new();
                    let nolookup = std::env::var_os("SS_NOLOOKUP").is_some();
                    let mut table: Vec<u32> = Vec::new();
                    loop {
                        let p = next.fetch_add(1, AtOrd::Relaxed);
                        if p >= NPART {
                            break;
                        }
                        recs.clear();
                        for o in outs.iter() {
                            recs.extend_from_slice(&o.ev[p]);
                        }
                        let n = recs.len();
                        if n == 0 {
                            continue;
                        }
                        let bits = (2 * n).next_power_of_two().trailing_zeros().max(4);
                        let mask = (1usize << bits) - 1;
                        table.clear();
                        table.resize(1 << bits, u32::MAX);
                        for i in 0..n {
                            let k = recs[i].eid;
                            let mut h = (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> (64 - bits)) as usize;
                            loop {
                                let s = table[h];
                                if s == u32::MAX {
                                    table[h] = i as u32;
                                    break;
                                }
                                let r = &recs[s as usize];
                                if r.eid == k {
                                    if recs[i].ingest < r.ingest {
                                        table[h] = i as u32;
                                    }
                                    break;
                                }
                                h = (h + 1) & mask;
                            }
                        }
                        for &s in table.iter() {
                            if s != u32::MAX {
                                let r = &recs[s as usize];
                                if r.user != INVALID {
                                    let person = if nolookup { r.user } else { pmap.person(r.user) };
                                    parts[part_of(person)].push(PEv { person, ts: r.ts, pc: r.pc });
                                }
                            }
                        }
                    }
                    parts
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let cl_outs: Vec<Vec<Vec<PCl>>> = outs.into_iter().map(|o| o.cl).collect();
    stage!(t0, "dedup events");

    // ---- per person partition: sessions, credit, assists
    let ncat = cat_names.len();
    let next = AtomicUsize::new(0);
    let workers: Vec<Worker> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..nthreads)
            .map(|_| {
                let (next, pe_outs, cl_outs, ctx) = (&next, &pe_outs, &cl_outs, &ctx);
                s.spawn(move || {
                    let mut w = Worker::new(ctx, ncat);
                    let mut evs: Vec<PEv> = Vec::new();
                    let mut cls: Vec<PCl> = Vec::new();
                    loop {
                        let p = next.fetch_add(1, AtOrd::Relaxed);
                        if p >= NPART {
                            break;
                        }
                        evs.clear();
                        for o in pe_outs.iter() {
                            evs.extend_from_slice(&o[p]);
                        }
                        cls.clear();
                        for o in cl_outs.iter() {
                            cls.extend_from_slice(&o[p]);
                        }
                        evs.sort_unstable_by_key(|e| ((e.person as u128) << 32) | e.ts as u128);
                        cls.sort_unstable_by_key(|c| ((c.person as u128) << 32) | c.ts as u128);
                        let mut ci = 0usize;
                        let mut i = 0usize;
                        while i < evs.len() {
                            let person = evs[i].person;
                            let mut j = i + 1;
                            while j < evs.len() && evs[j].person == person {
                                j += 1;
                            }
                            while ci < cls.len() && cls[ci].person < person {
                                ci += 1;
                            }
                            let mut cj = ci;
                            while cj < cls.len() && cls[cj].person == person {
                                cj += 1;
                            }
                            w.person(person, &evs[i..j], &cls[ci..cj]);
                            ci = cj;
                            i = j;
                        }
                    }
                    w
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    stage!(t0, "sessions");

    // ---- merge campaign accumulators
    let mut camp_total = vec![CampAcc::default(); (ctx.ncamp + 1) * ndays];
    for w in &workers {
        for (t, a) in camp_total.iter_mut().zip(&w.camp) {
            t.sessions += a.sessions;
            t.purchasing += a.purchasing;
            t.revenue += a.revenue;
            t.assisted += a.assisted;
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

    let (r1, r2) = rayon::join(cd_job, || {
        workers.par_iter().enumerate().map(|(i, w)| ud_job(i, w)).collect::<Res<Vec<()>>>()
    });
    r1?;
    r2?;
    stage!(t0, "written");
    Ok(())
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
