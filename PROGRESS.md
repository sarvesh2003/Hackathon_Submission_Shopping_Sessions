# PROGRESS.md: Shopping Sessions

Claude updates this after every milestone. The human reads it to see where things stand.

Last updated: Sat Nov 21, 12:35 PM ET

## Status
| Milestone | State | Notes |
| --- | --- | --- |
| M0 Setup | done | Repo lives in WSL at `~/shopping_sessions` (not /mnt/c), pushed to GitHub. Kit, dev, cross-check and p20m datasets generated. |
| M1 Simple Rust reference (first submission) | gate passed except local bench20 time (see notes) | 36/36 fixtures (native and image), dev MATCH (native and image). 20M: 175 s natively on this laptop. |
| M2 Fast version v1 | gate passed | Image BIN=shopping: fixtures 36/36, dev MATCH, check20 identical, crosscheck 12/12. 6.6 s on p20m in Docker. |
| M3 Speed | in progress | AVX2 field scanner, per-thread partitions, hash dedup, parallel union-find densify. ~2 s native on p20m (from 5-5.7 s). |
| M4 Final hardening | not started | |

## Machine
CPU: Intel Core Ultra 5 225U (12 cores / 14 threads, laptop) | AVX-512: no (AVX2 yes) | docker NCPU: 14 | docker MemTotal: 10.4 GB (WSL capped at 10 GB; laptop has 15.5 GB) | free disk: 817 GB | clean build time: 86 s (builder cache pruned) | Docker limits used: CPUS=8 MEM=8g

Notes on local timing:
- On battery the CPU is about 3x slower (Windows parks WSL on efficiency cores). Only time on AC power.
- Large working sets are very slow here (the WSL VM shares 10 GB with Docker): the reference reads p20m at about 57 MB/s and its 20M-entry hash map runs at 2-3 us per operation. The reference took 175 s on p20m natively, so the "bench20 under 60 s" gate cannot be met on this laptop; on the scorer (Xeon, 24 GB) it should be several times faster, and the practice cap is 300 s.

## Correctness gate (latest run)
reference: ref-fixtures 36/36, ref-dev MATCH, image fixtures 36/36, image dev MATCH
shopping image: fixtures 36/36 | dev: MATCH | check20: identical | crosscheck: 12/12 | check60: not yet

## Local timings (median of 3 on data/p20m; data/f60m where noted)
| Commit | Change | Time | Peak memory | Notes |
| --- | --- | --- | --- | --- |
| bc75a66 | reference (native) | 175 s | 4.4 GB | single run, AC power |
| bc75a66 | reference image, bench20 (CPUS=8 MEM=8g) | 123 s | under 8g | single run, output MATCH vs native reference |
| M2 v1 | shopping v1 (native, warm cache) | 5.0-5.7 s | 4.1 GB | single runs; first cold run 14 s |
| M2 v1 | shopping v1 image (check20 run) | 6.6 s | | single run |
| M3a beb2920 | shopping, NCPU=8 native | 1.9-2.3 s | ~2.5 GB | runs vary 20%; image 3.7-5.0 s (Docker Desktop bind mount is slow here) |
| M3b | + hand-written Parquet, SWAR timestamps, partitioned person join, bucketed grouping | 1.85 s median (min 1.53) | | stage_bench 5 runs NCPU=8 |

## Submissions
| Time (ET) | Commit | Leaderboard result | Notes |
| --- | --- | --- | --- |

## Open questions and blockers
- Profiling needs: echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid (asked the human).
- Local Docker is much slower than native here (bind mount); use native NCPU=8 for speed comparisons, Docker for correctness.

## Next step
- M3: keep cutting parse cost (timestamps, IDs), dedup gather, person lookups; hand-written Parquet writer.
