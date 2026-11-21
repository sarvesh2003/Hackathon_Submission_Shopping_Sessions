# PLAN.md: Shopping Sessions

Cutoff: **Sun Nov 22, 11:00 AM ET**. Target times are ET. Work top to bottom and keep PROGRESS.md current.

The order: a simple, correct Rust version first (it is our first submission and then our oracle), then a fast version checked against it, then speed.

## M0. Setup (about 30 min)
- [ ] Ask the human to keep the laptop awake while it is plugged in.
- [ ] Check tools: `docker version`, `docker buildx version`, `cargo --version` (Rust 1.85 or newer), `python3 -m venv --help`, `make`, `unzip`, `curl`. Ask the human to install anything missing (sudo).
- [ ] Record in PROGRESS.md: `docker info --format '{{.OSType}} {{.Architecture}} {{.NCPU}} {{.MemTotal}}'`, CPU model, AVX-512 yes/no (`grep -c avx512f /proc/cpuinfo`), free disk (`df -h ~`).
- [ ] `python3 -m venv .venv && .venv/bin/pip install polars pyarrow` (for the official test scripts); pin the versions in `requirements-dev.txt`.
- [ ] `make kit`, then `make data` (dev set, small cross-check sets, and the 20M set, about 1.9 GB). The 60M set (about 5.7 GB) waits until the fast version works.
- [ ] Docker check with the scorer's flags, using the CPUS and MEM that Docker really has: `docker run --rm --network none --read-only --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 debian:bookworm-slim true`.
- Done when: the datasets exist, `kit/dev-answer/*.csv` exists, and the Docker check passes.

## M1. Simple Rust reference = first submission (about 3 h; submit by Sat about 2:30 PM)
- [ ] Cargo project at the repo root with `src/bin/reference.rs`: single-threaded, std `HashMap`, IDs as byte strings, no packing, no unsafe, written step by step from docs/rules.md. It reads `DATA_DIR` and `OUTPUT_DIR`, and also accepts the two folders as its last two arguments (that is how `scripts/run_fixtures.py` calls it).
- [ ] Parquet output with the `parquet` crate (no arrow) or a small hand-written writer: `day` as DATE, nullable `top_category`, at least one file per table even when empty.
- [ ] Multi-stage `Dockerfile` at the top: pinned `rust:<version>-slim-bookworm` builder, then a slim runtime image holding only the binary. `ARG BIN=reference`, `ARG TARGET_CPU=x86-64-v3` passed via `RUSTFLAGS`. Runs as uid 1000; `CMD` runs `/app/$BIN` with no arguments. Measure a clean `docker build --no-cache .` and record it (must be well under 10 minutes).
- [ ] Gate: `make ref-fixtures` 36/36, `make ref-dev` MATCH, `make fixtures` 36/36 and `make run-dev check-dev` MATCH with the image, `make bench20` finishes in under 60 s.
- [ ] Commit, push, READY TO SUBMIT. When the result shows 0 missing, extra and differing rows on the hidden data, the reference is proven at 20M scale. From then on it is the oracle: never optimize it.

## M2. Fast version v1 (about 4 h; submit by Sat about 7 PM)
- [ ] `src/bin/shopping.rs`; switch the Dockerfile default to `BIN=shopping`. Design:
    - Read all part files with all threads (mmap, or `pread` chunks split at `\n`); no per-row heap allocation; 64-bit offsets.
    - Compact integer IDs with the byte-order-preserving packing from CLAUDE.md, plus an exact fallback table.
    - Global event dedup first (partition by event_id hash), then union-find the links, then partition rows by person so each thread owns its partitions.
    - Per person: sort by ts, sessionize, credit with two pointers over the person's sorted clicks, sliding window for assists.
    - Merge per-thread aggregates so every key is unique; write several Parquet files in parallel.
- [ ] Gate: `make fixtures` 36/36, `make run-dev check-dev` MATCH, `make check20` identical, `make crosscheck` identical.
- [ ] Commit, push, READY TO SUBMIT.

## M3. Speed (Sat evening to Sun about 6 AM)
Target: the best time on the board (0.31 s on Saturday morning; ask the human for the current one).
- [ ] Per-stage timers on stderr. First measure a pure scan of the input (how fast can we just read the bytes with all threads?), then work on the slowest measured stage.
- [ ] Ideas, roughly by expected payoff (prove each with the timers, don't assume):
    1. Input: compare mmap (page faults) with parallel `pread` into reused per-thread buffers; find newlines and commas with `memchr`.
    2. Byte-order-preserving integer IDs; never hash full strings in the hot path.
    3. Radix partitions sized to fit in L2 cache; per-thread state; no locks, no global sort.
    4. Skip click dedup (identical copies cannot change results).
    5. Write several Parquet files in parallel, no compression.
    6. Exit right after the last file is flushed and closed (`std::process::exit(0)`; don't spend time freeing a big heap). mimalloc.
- [ ] After every kept change: `make fixtures`, `make run-dev check-dev`, `make check20`. Every few changes: `make data60` (once), then `make check60` and peak memory.
- [ ] Submit a candidate that passes the gate and is faster locally at both 20M and 60M (at most 2 per hour). Board times vary by about 20%, so resubmit the best commit before trusting a small difference.
- [ ] Optional late experiment: a `TARGET_CPU=cascadelake` build as its own submission. Keep it only if the board shows a clear gain.
- [ ] Midnight checkpoint: tell the human our board time and the stage profile. If we are far from the top with no measured path to a big gain, the human may move to help Blast Radius.

## M4. Final hardening (Sun 6:00 to 9:30 AM)
- [ ] No new optimizations after 8:00 AM.
- [ ] On the best candidate: `make fixtures`, `make run-dev check-dev`, `make check20`, `make check60`, `make crosscheck`, peak memory at 60M, and a clean `docker build --no-cache .` from a fresh clone.
- [ ] README: approach, how to build, run and test, every library used, MIT license.
- [ ] Recommend the final SHA by 10:00 AM: the fastest commit that passed everything and scored on the board. The human selects it on the track page before 11:00 AM.

## Datasets (`make data`, `make data60`)
- dev: seed 1, 1M events, 250k clicks (official answer in `kit/dev-answer/`).
- cross-check: seeds 101 to 110 at 20,000 events; seeds 2 and 3 at 500,000 events.
- p20m: seed 7, 20M events, 5M clicks (about 1.9 GB). f60m: seed 11, 60M events, 15M clicks (about 5.7 GB).
- Reference outputs are cached per dataset in `out/ref-cache/` and recomputed only when the reference binary changes.
