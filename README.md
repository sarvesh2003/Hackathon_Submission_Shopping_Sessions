# Shopping Sessions: Builders Buildathon, Databricks track

Turns the messy click-stream CSVs in `/data` into two exact daily Parquet tables (`user_daily`, `campaign_daily`), as fast as possible.

- Rules: [docs/rules.md](docs/rules.md). Track facts: [docs/track-page.md](docs/track-page.md).
- Plan and status: [PLAN.md](PLAN.md), [PROGRESS.md](PROGRESS.md). Instructions for Claude Code: [CLAUDE.md](CLAUDE.md).

## Approach
Two Rust binaries in one Cargo project; the Dockerfile's `BIN` build argument picks one (default `shopping`).

- `src/bin/reference.rs`: a simple, single-threaded implementation written step by step from the rules (std `HashMap`, byte-string IDs, the `parquet` crate). It is never optimized; it is the oracle the fast version is checked against.
- `src/bin/shopping.rs`: the fast pipeline.
  1. Memory-map every input file and cut it into chunks of whole lines (64-bit offsets throughout).
  2. Parse fields with an AVX2 scanner that finds `,`, `\n` and `"` 64 bytes at a time, applying the custom CSV dialect exactly (quotes only around whole fields, `\r` is data, wrong field counts and empty lines dropped).
  3. IDs become 64-bit keys with a byte-order-preserving 6-bit packing of `[0-9A-Z_a-z]` (up to 10 bytes, left-aligned, big-endian, zero-padded), so integer order is byte order; anything else goes to an exact interning table.
  4. Links: parallel parse, union-find over densified IDs, person = byte-wise smallest ID; this runs on one thread while the others parse events.
  5. Events are scattered by `event_id` hash into per-thread partitions; each partition is deduplicated with a hash table (smallest `ingest_ts` wins, and only that copy is validated). Survivors are joined to their person partition by partition, then scattered by person; clicks are scattered by person directly (click dedup is skipped: identical copies cannot change any result).
  6. Per person partition: group by person (counting sort into hash buckets), sort by timestamp, sessionize, credit with two pointers over the person's clicks, and count assists with a sliding window of per-campaign counts that adds whole runs of sessions per day.
  7. Per-thread daily campaign counters are merged; Parquet files are written in parallel by a small hand-written writer (PLAIN encoding, no compression, `day` as `DATE`).
- Timestamp parsing (all three forms, full calendar validation) and the field splitter are fuzz-tested against the reference logic (`cargo test --release --bin shopping`).

## Build and test
```sh
make kit                 # official generator and dev answer into kit/
make data                # local datasets into data/
make fixtures            # 36 edge-case fixtures, Docker image with the scorer's flags
make run-dev check-dev   # public dev set vs the official answer
make check20             # fast image vs the simple Rust reference on 20M events
make bench20             # median of 3 timed runs on 20M events
make crosscheck          # fast image vs reference on 12 small generated datasets
make check60             # fast image vs reference on 60M events (make data60 first)
scripts/stage_bench.sh data/p20m 5   # native per-stage medians (NCPU=8)
```
Run the image by hand: `docker build -t shopping-sessions . && scripts/docker_run.sh DATA_DIR OUTPUT_DIR`.

## Third-party material
- Official Buildathon starter material: `fixtures/`, `scripts/run_fixtures.py`, `scripts/compare.py`, `scripts/docker_run.sh`, `docs/rules.md`.
- Rust crates (versions pinned in `Cargo.lock`): `parquet` (Apache Arrow Rust project, default features off, no `arrow`), `rayon`, `memmap2`, `memchr`, `hashbrown` (with its default `foldhash` hasher), `mimalloc`, `libc`, and their transitive dependencies.
- Docker base images: `rust:1.99-slim-bookworm` (build) and `debian:bookworm-slim` (runtime), pinned by digest.
- Python (official test scripts only): polars, pyarrow.

## License
MIT. See [LICENSE](LICENSE).
