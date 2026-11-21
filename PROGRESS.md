# PROGRESS.md: Shopping Sessions

Claude updates this after every milestone. The human reads it to see where things stand.

Last updated: Sat Nov 21, 4:15 PM ET

## Status
| Milestone | State | Notes |
| --- | --- | --- |
| M0 Setup | done | Repo lives in WSL at `~/shopping_sessions` (not /mnt/c), pushed to GitHub. Kit, dev, cross-check, p20m and f60m datasets generated. |
| M1 Simple Rust reference (first submission) | gate passed except local bench20 time (see notes) | 36/36 fixtures (native and image), dev MATCH (native and image). 20M: 175 s natively on this laptop. |
| M2 Fast version v1 | gate passed | Image BIN=shopping: fixtures 36/36, dev MATCH, check20 identical, crosscheck 12/12. 6.6 s on p20m in Docker. |
| M3 Speed | in progress | ~1.2 s native on p20m (from 5-5.7 s for v1). Two external reviews (Fable, Gemini) applied where they measured faster. |
| M4 Final hardening | not started | README approach section already written. |

## Machine
CPU: Intel Core Ultra 5 225U (12 cores / 14 threads, laptop) | AVX-512: no (AVX2 yes) | docker NCPU: 14 | docker MemTotal: 10.4 GB (WSL capped at 10 GB; laptop has 15.5 GB) | free disk: 815 GB | clean build time: 69 s with fat LTO (builder cache pruned) | Docker limits used: CPUS=8 MEM=8g

Notes on local timing:
- On battery the CPU is about 3x slower (Windows parks WSL on efficiency cores). Only time on AC power.
- Runs vary about 20%; compare versions with interleaved medians (`scripts/stage_bench.sh DATA 7 BIN`), not single runs.
- Local Docker is about 2x slower than native here (Docker Desktop bind mount): use native NCPU=8 for speed, Docker for correctness.
- The 60M reference needs more memory than this laptop has: it ran 1h40m swapping and then froze WSL (WSL had to be restarted at 2:30 PM). check60 has NOT run.
- After the WSL restart, Windows-exe interop is broken inside WSL (binfmt entry missing), so the Docker and git credential helpers fail. Workarounds: DOCKER_CONFIG=~/.docker-nocreds for builds; pushes go through Windows git. Fix (sudo): `echo ':WSLInterop:M::MZ::/init:PF' | sudo tee /proc/sys/fs/binfmt_misc/register`.

## Correctness gate (latest run)
reference: ref-fixtures 36/36, ref-dev MATCH, image fixtures 36/36, image dev MATCH
shopping image (037bec2): fixtures 36/36 | dev: MATCH | check20: identical | crosscheck: 12/12 | check60: not run (see above)

## Local timings (native, NCPU=8, data/p20m, medians of 5-7 runs unless noted)
| Commit | Change | Time | Peak memory | Notes |
| --- | --- | --- | --- | --- |
| bc75a66 | reference (native) | 175 s | 4.4 GB | single run, AC power |
| bc75a66 | reference image, bench20 (CPUS=8 MEM=8g) | 123 s | under 8g | single run, output MATCH vs native reference |
| 2ef8cd9 | shopping v1 | 5.0-5.7 s | 4.1 GB | single runs |
| beb2920 | AVX2 scanner, per-thread partitions, hash dedup | 1.9-2.3 s | ~2.5 GB | |
| 41b8e4f | + hand-written Parquet, SWAR timestamps, partitioned person join, bucketed grouping | 1.85 s (min 1.53) | | |
| 71874c0 | + review fixes: buffer sizing, purchase-only category, date cache, single-person bucket sort | 1.13-1.57 s | | minflt ~32k (was ~250k) |
| 037bec2 | + no zero-fill counting sort, biggest partitions first, SWAR F3 | ~same as 71874c0 | | neutral locally; less work at 60M |
| 15e88b3+ | + alive-bitset dedup, fat LTO | ~1.20 s | | dedup stage ~190 ms (was 290-470) |
| f60m | shopping 4342a0e, 60M | 14.7 s (first run 29 s) | 6.0 GB RSS | laptop swapping; far under the 3-minute cap |

## Tried and rejected (measured, not faster here)
- Three alternative CSV splitters (branch-free popcount, two-stage index array): 1.4-2.5x slower than the bit-iteration splitter.
- Person lookup fused into dedup via one global table with batched prefetch (both reviews suggested it): dedup+lookup ~500 ms vs ~255 ms for the partitioned join.
- Union-find before parsing on all threads: +260 ms on the critical path; overlapping it on worker 0 is better. Union-find without rayon: too slow, delays the parse stage.
- Freeing the big buffers before writing Parquet: doubles page faults.
- Bitmap filter in front of the person map; mimalloc purge/large-page options; MAP_POPULATE.

## Submissions
| Time (ET) | Commit | Leaderboard result | Notes |
| --- | --- | --- | --- |
| ? | ? | ? | Human submits; results not reported back yet. Top of the board: 0.3 s. |

## Open questions and blockers
- Leaderboard score of our submissions (needed to know the real gap to 0.3 s).
- Profiling needs: echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid.
- check60 needs a machine with more memory, or a lower-memory way to run the reference.

## Next step
- Gemini rank 9 (Parquet columns written directly, INT32 counts), rank 1 (line-at-a-time u128 splitter), rank 7 (PEXT ID packing), rank 2 (write-combining scatter, 24-byte EvRec).
