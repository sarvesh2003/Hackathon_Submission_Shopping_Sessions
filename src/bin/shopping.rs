//! Shopping Sessions: the fast, multi-threaded pipeline.
//! Must produce exactly the same tables as src/bin/reference.rs (the oracle).
//!
//! Stages: mmap + chunk -> links/union-find -> parse events (scatter by event_id) and clicks
//! -> dedup events per partition (scatter survivors by person) -> per person partition:
//! sort, sessionize, credit, assists -> merge -> Parquet.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
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

const PART_BITS: u32 = 8;
const NPART: usize = 1 << PART_BITS;
const CHUNK_BYTES: usize = 4 << 20;

// ======================================================================= IDs
// Byte-order-preserving packing: each byte of [0-9A-Z_a-z] maps to a 6-bit code (1..=63)
// in byte order, left-aligned, big-endian, zero-padded, up to 10 bytes -> 60 bits.
// Comparing two packed keys as integers equals comparing the strings byte by byte.
// Anything else (other bytes, longer, empty) is interned exactly with bit 63 set.

const FALLBACK: u64 = 1 << 63;
const MAX_PACK: usize = 10;

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
    for &b in id {
        let c = CODE[b as usize];
        if c == 0 {
            return None;
        }
        k = (k << 6) | c as u64;
    }
    Some(k << (6 * (MAX_PACK - id.len()) as u32))
}

struct Interner {
    map: std::collections::HashMap<Vec<u8>, u64>,
    names: Vec<Vec<u8>>,
}

static INTERNER: LazyLock<Mutex<Interner>> = LazyLock::new(|| {
    Mutex::new(Interner {
        map: std::collections::HashMap::new(),
        names: Vec::new(),
    })
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
/// Returns false if the line is malformed.
#[inline]
fn split_fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let has_quote = memchr::memchr(b'"', line).is_some();
    let mut n = 0;
    let mut start = 0;
    let len = line.len();
    loop {
        let end = match memchr::memchr(b',', &line[start..]) {
            Some(p) => start + p,
            None => len,
        };
        let raw = &line[start..end];
        let v = if has_quote {
            if raw.first() == Some(&b'"') {
                if raw.len() < 2 || raw[raw.len() - 1] != b'"' {
                    return false;
                }
                let inner = &raw[1..raw.len() - 1];
                if memchr::memchr(b'"', inner).is_some() {
                    return false;
                }
                inner
            } else {
                if memchr::memchr(b'"', raw).is_some() {
                    return false;
                }
                raw
            }
        } else {
            raw
        };
        if n == N {
            return false;
        }
        out[n] = v;
        n += 1;
        if end == len {
            break;
        }
        start = end + 1;
    }
    n == N
}

/// Calls f on every line of a chunk (split on '\n' only; the empty tail is skipped).
#[inline]
fn for_each_line(chunk: &[u8], mut f: impl FnMut(&[u8])) {
    let mut pos = 0;
    let len = chunk.len();
    while pos < len {
        let end = match memchr::memchr(b'\n', &chunk[pos..]) {
            Some(p) => pos + p,
            None => len,
        };
        f(&chunk[pos..end]);
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
    if n == 20 || n == 25 {
        if s[4] == b'-' {
            return parse_iso(s);
        }
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
            Some(unsafe { Mmap::map(&f) }.map_err(|e| e.into()))
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

/// One parsed copy of an event (before dedup).
#[derive(Clone, Copy)]
struct EvRec {
    eid: u64,
    ingest: i64,
    user: u64,
    ts: i64,
    price: u32, // > 0 only for a valid purchase
    cat: u16,
    valid: bool,
}

/// A surviving event, keyed by person.
#[derive(Clone, Copy)]
struct PEv {
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
}

/// A valid click, keyed by person.
#[derive(Clone, Copy)]
struct PCl {
    person: u64,
    ts: i64,
    cid: u64,
    camp: u16,
}

struct Ctx {
    ws: i64,
    we: i64,
    ws_day: i64,
    ndays: usize,
    prod_packed: HashMap<u64, u16>,
    prod_bytes: HashMap<Vec<u8>, u16>,
    unknown_cat: u16,
    camp_packed: HashMap<u64, u16>,
    camp_bytes: HashMap<Vec<u8>, u16>,
    ncamp: usize, // index ncamp = organic
}

impl Ctx {
    #[inline]
    fn category(&self, product: &[u8]) -> u16 {
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

fn parse_event(line: &[u8], ctx: &Ctx) -> Option<EvRec> {
    let mut f: [&[u8]; 7] = [&[]; 7];
    if !split_fields(line, &mut f) || f[0].is_empty() {
        return None;
    }
    let eid = id_key(f[0]);
    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
    let mut rec = EvRec { eid, ingest, user: 0, ts: 0, price: 0, cat: 0, valid: false };
    let (user, ts_raw, etype, product, price_raw) = (f[1], f[2], f[4], f[5], f[6]);
    if user.is_empty() || product.is_empty() {
        return Some(rec);
    }
    let ts = match parse_ts(ts_raw) {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => return Some(rec),
    };
    let price = match etype {
        b"view" | b"cart" => 0,
        b"purchase" => match parse_price(price_raw) {
            Some(p) => p,
            None => return Some(rec),
        },
        _ => return Some(rec),
    };
    if ingest.saturating_sub(ts) > DAY_MS {
        return Some(rec);
    }
    rec.user = id_key(user);
    rec.ts = ts;
    rec.price = price;
    rec.cat = ctx.category(product);
    rec.valid = true;
    Some(rec)
}

/// (user key, ts, click_id key, campaign) of a valid click.
fn parse_click(line: &[u8], ctx: &Ctx) -> Option<(u64, i64, u64, u16)> {
    let mut f: [&[u8]; 4] = [&[]; 4];
    if !split_fields(line, &mut f) {
        return None;
    }
    let (cid, user, camp, ts_raw) = (f[0], f[1], f[2], f[3]);
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let camp = ctx.campaign(camp)?;
    let ts = parse_ts(ts_raw)?;
    if ts < ctx.ws - WEEK_MS || ts >= ctx.we {
        return None;
    }
    Some((id_key(user), ts, id_key(cid), camp))
}

// ======================================================================= links

fn build_person_map(link_chunks: &[&[u8]]) -> HashMap<u64, u64> {
    let pairs: Vec<Vec<(u64, u64)>> = link_chunks
        .par_iter()
        .map(|chunk| {
            let mut v = Vec::new();
            for_each_line(chunk, |line| {
                let mut f: [&[u8]; 2] = [&[]; 2];
                if split_fields(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                    v.push((id_key(f[0]), id_key(f[1])));
                }
            });
            v
        })
        .collect();
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut index: HashMap<u64, u32> = HashMap::with_capacity(total * 2);
    let mut keys: Vec<u64> = Vec::with_capacity(total * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(total * 2);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[parent[x as usize] as usize];
            parent[x as usize] = p;
            x = p;
        }
        x
    }
    for v in &pairs {
        for &(a, b) in v {
            let mut idx = [0u32; 2];
            for (slot, k) in idx.iter_mut().zip([a, b]) {
                *slot = *index.entry(k).or_insert_with(|| {
                    keys.push(k);
                    parent.push(parent.len() as u32);
                    (parent.len() - 1) as u32
                });
            }
            let ra = find(&mut parent, idx[0]);
            let rb = find(&mut parent, idx[1]);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    // smallest ID per root
    let n = keys.len();
    let mut best: Vec<u64> = vec![u64::MAX; n];
    let mut has: Vec<bool> = vec![false; n];
    let mut roots: Vec<u32> = Vec::with_capacity(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        roots.push(r as u32);
        if !has[r] || id_cmp(keys[i], best[r]) == std::cmp::Ordering::Less {
            best[r] = keys[i];
            has[r] = true;
        }
    }
    let mut person: HashMap<u64, u64> = HashMap::with_capacity(n);
    for i in 0..n {
        person.insert(keys[i], best[roots[i] as usize]);
    }
    person
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
    start: i64,
    day: u16,
    purchase: bool,
    revenue: u64,
}

struct Worker {
    ndays: usize,
    ncamp: usize,
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
        let mut best = 0u64;
        for (i, v) in self.cat_sum.iter_mut().enumerate() {
            if *v > best {
                best = *v;
                top = i as i32;
            }
            *v = 0;
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

    fn person(&mut self, ctx: &Ctx, person: u64, evs: &[PEv], cls: &[PCl]) {
        // pass 1: sessions and user_daily rows
        self.sessions.clear();
        let n = evs.len();
        let mut i = 0;
        let mut cur_day: u16 = u16::MAX;
        let (mut d_sess, mut d_events, mut d_rev) = (0u32, 0u32, 0u64);
        while i < n {
            let start = evs[i].ts;
            let day = (start.div_euclid(DAY_MS) - ctx.ws_day) as u16;
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
                if e.price > 0 {
                    rev += e.price as u64;
                    self.cat_sum[e.cat as usize] += e.price as u64;
                }
                j += 1;
                if j == n || evs[j].ts - evs[j - 1].ts > SESSION_GAP_MS {
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

        // pass 2: credit (two pointers) and assists (sliding window of campaign counts)
        let ns = self.sessions.len();
        let (mut lo, mut hi) = (0usize, 0usize);
        for si in 0..ns {
            let s = self.sessions[si];
            while hi < cls.len() && cls[hi].ts <= s.start {
                let c = cls[hi].camp as usize;
                if self.cnt[c] == 0 {
                    self.act[c] = si as u32;
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && cls[lo].ts < s.start - WEEK_MS {
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
    let cat_index = |c: &[u8]| cat_names.binary_search(&c).unwrap() as u16;
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
    let ctx = Ctx {
        ws,
        we,
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

    // ---- links -> person of every linked ID
    let person_map = build_person_map(&link_chunks);
    let person_of = |u: u64| -> u64 { person_map.get(&u).copied().unwrap_or(u) };
    stage!(t0, "links / union-find");

    // ---- parse events, scatter copies by event_id partition
    let ev_parts: Vec<Vec<Vec<EvRec>>> = event_chunks
        .par_iter()
        .map(|chunk| {
            let mut parts: Vec<Vec<EvRec>> = (0..NPART).map(|_| Vec::new()).collect();
            for_each_line(chunk, |line| {
                if let Some(r) = parse_event(line, &ctx) {
                    parts[part_of(r.eid)].push(r);
                }
            });
            parts
        })
        .collect();
    stage!(t0, "parsed events");

    // ---- parse clicks, scatter by person partition
    let cl_parts: Vec<Vec<Vec<PCl>>> = click_chunks
        .par_iter()
        .map(|chunk| {
            let mut parts: Vec<Vec<PCl>> = (0..NPART).map(|_| Vec::new()).collect();
            for_each_line(chunk, |line| {
                if let Some((user, ts, cid, camp)) = parse_click(line, &ctx) {
                    let person = person_of(user);
                    parts[part_of(person)].push(PCl { person, ts, cid, camp });
                }
            });
            parts
        })
        .collect();
    stage!(t0, "parsed clicks");

    // ---- dedup events per event_id partition, scatter survivors by person
    let pe_parts: Vec<Vec<Vec<PEv>>> = (0..NPART)
        .into_par_iter()
        .map(|p| {
            let mut recs: Vec<EvRec> = Vec::with_capacity(ev_parts.iter().map(|c| c[p].len()).sum());
            for c in &ev_parts {
                recs.extend_from_slice(&c[p]);
            }
            recs.sort_unstable_by_key(|r| (r.eid, r.ingest));
            let mut out: Vec<Vec<PEv>> = (0..NPART).map(|_| Vec::new()).collect();
            let mut i = 0;
            while i < recs.len() {
                let r = recs[i];
                let mut j = i + 1;
                while j < recs.len() && recs[j].eid == r.eid {
                    j += 1;
                }
                i = j;
                if r.valid {
                    let person = person_of(r.user);
                    out[part_of(person)].push(PEv { person, ts: r.ts, price: r.price, cat: r.cat });
                }
            }
            out
        })
        .collect();
    drop(ev_parts);
    stage!(t0, "dedup events");

    // ---- per person partition: sessions, credit, assists
    let ncat = cat_names.len();
    let workers: Vec<Worker> = (0..NPART)
        .into_par_iter()
        .fold(
            || Worker::new(&ctx, ncat),
            |mut w, p| {
                let mut evs: Vec<PEv> = Vec::with_capacity(pe_parts.iter().map(|c| c[p].len()).sum());
                for c in &pe_parts {
                    evs.extend_from_slice(&c[p]);
                }
                let mut cls: Vec<PCl> = Vec::with_capacity(cl_parts.iter().map(|c| c[p].len()).sum());
                for c in &cl_parts {
                    cls.extend_from_slice(&c[p]);
                }
                evs.sort_unstable_by_key(|e| (e.person, e.ts));
                cls.sort_unstable_by_key(|c| (c.person, c.ts));
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
                    w.person(&ctx, person, &evs[i..j], &cls[ci..cj]);
                    ci = cj;
                    i = j;
                }
                w
            },
        )
        .collect();
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
        workers
            .par_iter()
            .enumerate()
            .map(|(i, w)| ud_job(i, w))
            .collect::<Res<Vec<()>>>()
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
