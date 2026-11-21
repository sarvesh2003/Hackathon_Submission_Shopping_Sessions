# CLAUDE.md: Shopping Sessions (Databricks track, Builders Buildathon)

You are the engineer on this repo. Read this file, then PLAN.md (imported below), then PROGRESS.md. Work through PLAN.md one milestone at a time.

@PLAN.md

## Mission
Write the **fastest exactly-correct** pipeline that turns the click-stream CSVs in `/data` into two daily Parquet tables.
- Score = wall time from container start to container exit on a hidden practice dataset of about 20M events (about 1.9 GB of CSV). Lower wins. Baseline: 172.00 s (PySpark starter).
- Every submission first runs on the public 1M-event dev set (3-minute cap). If it is wrong there, we get the differing rows and our logs. On hidden data we only get the exit code, whether memory ran out, and counts of missing, extra and differing rows.
- A practice run under 20 s is timed 3 times and scored on the median. Practice machines vary by about 20%, so small gaps on the board mean little.
- Every run's tables must match the reference **exactly**. One wrong row means the run scores nothing.
- The bar is extreme: on Saturday morning (Nov 21, 269 teams) #1 and #2 were at 0.31 s and #13 at 0.64 s. Equal scores rank the earlier submission first.
- Final (after the Sun Nov 22, 11:00 AM ET cutoff): our chosen commit is rebuilt and timed 3 times on about 60M new events (about 5.7 GB), each run capped at 3 minutes. If any run goes over 3 minutes we get no final score; otherwise the median counts. At least the 10 fastest are then retimed 7 times alone on one idle machine, and that median replaces their score. The winner takes $5,000; second place gets nothing.
- Never special-case or cache anything about a particular dataset: the final uses new data, and the organizers verify top results.

## Source of truth
- `docs/rules.md`: the official rules, verbatim. If anything disagrees with it (a starter, a comment, this file), rules.md wins. The gotchas below are a summary, not the full rules.
- `docs/track-page.md`: the official limits, scoring and final rules.
- `fixtures/README.md`: what each of the 36 hand-written edge-case fixtures tests.
- Unsure what a rule means? Do not guess. Ask the human right away; they can ask the organizers in the leaderboard chat.

## Fixed decisions
- Submission: **Rust** (stable). Pin the toolchain image in the Dockerfile, commit `Cargo.lock`, build with `--locked` in Docker.
- Two binaries in one Cargo project: `reference` (simple, single-threaded, obviously correct, never optimized: it is our first submission and then our oracle) and `shopping` (the fast one). The Dockerfile's `ARG BIN` picks which one `CMD` runs.
- Crates: `rayon`, `memmap2`, `memchr`, a fast hash map (`hashbrown` with `foldhash` or `ahash`), `mimalloc`. Parquet: the `parquet` crate with default features off (no `arrow`), or a small hand-written writer (PLAIN encoding, no compression) if that is simpler or faster. Avoid the `arrow` crate: it makes builds slow.
- No CSV library for the inputs: the dialect is custom (see Gotchas). Write the byte parser yourself.
- Python is used only to run the official test scripts (`scripts/run_fixtures.py`, `scripts/compare.py`, which need polars).

## How the scorer runs us (match this exactly)
- linux/amd64. Build: network on, at most 10 minutes, image at most 8 GB. The final rebuilds our commit, so pin every version the build downloads.
- Run: no network; 8 vCPUs that are hyperthreads (4 physical cores) of an Intel Xeon, Cascade Lake or newer; 24 GB RAM, no swap.
- `/data` is read-only input. Write `/output/user_daily/` and `/output/campaign_daily/` as Parquet. `/scratch` is empty writable disk; `/output` and `/scratch` together may hold 50 GB.
- Root filesystem read-only except `/tmp` (64 MB, in memory). Runs as uid 1000. At most 4,096 processes and threads. At most 50 MB on stdout, so log to stderr.
- Env: `DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24`.
- The repo at the submitted commit must be at most 50 MB. Never commit datasets (`data/`, `kit/`, `out/` are git-ignored).
- Exit code 0 means done. The clock includes process start, reading the CSVs and writing the Parquet.

## Build settings
- Release profile: start with `lto = "thin"` and the default codegen units. Try fat LTO later only if a clean `docker build --no-cache .` stays well under 10 minutes (measure it; keep a 5-minute margin).
- CPU target: default `TARGET_CPU=x86-64-v3` (AVX2). It runs on both the laptop and the scorer, so the binary we test is the binary we submit. A `cascadelake` build is only a late, measured leaderboard experiment: it cannot run on a laptop without AVX-512. Never `target-cpu=native`, and no hand-written AVX-512 intrinsics.

## Local Docker limits
Use the scorer's flags as `scripts/docker_run.sh` sets them (read-only filesystem, no network, uid 1000, `--memory-swap` equal to `--memory`). Check `docker info --format '{{.NCPU}} {{.MemTotal}}'`: if Docker has fewer than 8 CPUs or less than 24 GB, set `CPUS` and `MEM` to what it really has and record that in PROGRESS.md. Local times only compare our own versions; the leaderboard is the real measure.

## Correctness gate: all must pass before any READY TO SUBMIT
1. `make fixtures`: 36/36 (the Docker image with the scorer's flags).
2. `make run-dev check-dev`: `MATCH` against `kit/dev-answer` (public dev set: seed 1, 1M events, 250k clicks).
3. `make check20`: the image's output is identical to the reference's on `data/p20m`.
4. `make crosscheck` (small generated sets): for shortlisted candidates and before the final, not every iteration.
5. `make check60`: before the final pick, and after any change to parsing, ID packing, partitioning or byte offsets.

## Rules gotchas (docs/rules.md has the full text)
- Read every part file in `events/`, `clicks/` and `links/`; each has its own header line. A later line equal to the header is data.
- Only `\n` ends a line. `\r` is ordinary data (`1000\r` is a bad price).
- Quotes wrap whole fields only and are syntax: `"u1"` is `u1`; `""` is empty, which means null. A quote anywhere else, the wrong number of fields, or an empty line makes the line malformed, and it is dropped. No value contains a comma, a quote or a newline.
- Values are never trimmed or case-folded: ` u1`, `u1 `, `U1` and `u1` are four different IDs. Compare byte by byte everywhere. Digits means ASCII `0` to `9` only.
- Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`; F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` or `-HH:MM` (hour 00 to 23, minute 00 to 59, colon required, `-00:00` is UTC; subtract the offset); F3 digits only = epoch milliseconds (leading zeros fine; a value too large for i64 is outside the window, not an error). Validate the calendar (leap years, including 2028-02-29; no hour 24, no second 60). Anything else is a bad timestamp.
- Windows come from `manifest.json`. Event window `[window_start, window_end)`. Click window `[window_start - 7 days, window_end)`.
- Drop malformed lines and lines with an empty `event_id` or `click_id` before dedup.
- Event dedup is global and comes before grouping by person: copies of one event can name different users. Keep the copy with the smallest `ingest_ts`; validate only that copy (if it is invalid, the event is dropped). Then drop if `ingest_ts - ts > 24 h` (exactly 24 h is kept). A purchase needs a price of digits only, 1 to 10,000,000.
- People: union-find over valid links (transitive across all files; self-links and repeats are harmless; a line with an empty side is dropped). A person's name is their byte-wise smallest ID, including IDs that appear only in links (uppercase sorts before lowercase). An unlinked ID is its own person.
- Sessions: per person, all IDs together, sorted by ts. A new session starts when the gap between distinct timestamps is over 30 minutes (1,800,000 ms). Equal timestamps always share a session. The session's day is the UTC date of its start.
- Credit: the person's latest valid click with `ts <= start` and `start - ts <= 7 days`; on a tie, the smallest `click_id` byte-wise. No such click means campaign `organic`, channel `organic`. One click can credit many sessions.
- Copies of a click have identical values, so click dedup cannot change credit or assists (skipping it is a legal optimization).
- Assists: every campaign with at least one qualifying click in `[start - 7 days, start]` assists once, including the crediting one; `organic` never assists. A campaign with only assists still gets a row, with 0 in the credited columns.
- A product missing from `products.csv` has category `unknown`, which competes for `top_category` (highest summed purchase cents per person and day; tie: byte-wise smallest; null with no purchases).
- Every output key must be unique: merge per-thread partial results for (person, day) and (campaign, day) before writing.
- Output: exactly the listed columns; `day` is a Parquet DATE; any integer width; file names must not start with `.` or `_`. Always write at least one Parquet file per table, even when the table has no rows.
- Use 64-bit byte offsets: the 60M dataset is about 5.7 GB.
- The generator's IDs are one letter plus up to 8 base-36 characters (values below 2^40), but the fixtures contain IDs with spaces, quotes, uppercase letters and a tab. Pack IDs into integers only with a byte-order-preserving encoding (left-aligned, big-endian, zero-padded); anything containing a NUL byte or too long for the key goes to an exact fallback table.
- Flush and close every Parquet writer before `std::process::exit(0)`.
- DuckDB's CSV reader does not follow this dialect (the starter warns about it). Don't use it on the inputs.

## Working rules
- Follow PLAN.md. Ask the human before changing the plan, a fixed decision, or a rule interpretation.
- After every milestone: run its checks, update PROGRESS.md, commit, `git push`, and post a summary of at most 5 lines.
- Never force-push, rebase pushed commits, delete branches or rewrite history: every submitted commit must stay fetchable until results are published. Keep the repo public.
- The human submits. When a candidate passes the gate: commit, push, print exactly `READY TO SUBMIT: <full 40-character SHA>` with what changed, the local timing and the local Docker limits. Then keep working on the next independent task while the human submits; don't sit idle.
- Limits: 2 submissions per hour; rejected ones count; one submission in the queue at a time.
- Time-box dead ends to 20 minutes. Raise rule questions and access problems with the human immediately.
- Confirm the final SHA with the human by Sun 10:00 AM ET.
- Everything we write is MIT-licensed. Name every open-source library we use in README.md.
- Windows/WSL: work under the WSL home folder, never under `/mnt/c` or `/mnt/d`. `.gitattributes` turns off line-ending conversion; never re-save fixtures with CRLF.
