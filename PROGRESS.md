# PROGRESS.md: Shopping Sessions

Claude updates this after every milestone. The human reads it to see where things stand.

Last updated: Sat Nov 21, 11:45 AM ET

## Status
| Milestone | State | Notes |
| --- | --- | --- |
| M0 Setup | done | Repo lives in WSL at `~/shopping_sessions` (not /mnt/c), pushed to GitHub. Kit, dev, cross-check and p20m datasets generated. |
| M1 Simple Rust reference (first submission) | gate passed except local bench20 time (see notes) | 36/36 fixtures (native and image), dev MATCH (native and image). 20M: 175 s natively on this laptop. |
| M2 Fast version v1 | in progress | `src/bin/shopping.rs` already 36/36, dev MATCH, and identical to the reference on p20m (native run). |
| M3 Speed | not started | |
| M4 Final hardening | not started | |

## Machine
CPU: Intel Core Ultra 5 225U (12 cores / 14 threads, laptop) | AVX-512: no (AVX2 yes) | docker NCPU: 14 | docker MemTotal: 10.4 GB (WSL capped at 10 GB; laptop has 15.5 GB) | free disk: 817 GB | clean build time: ? | Docker limits used: CPUS=8 MEM=8g

Notes on local timing:
- On battery the CPU is about 3x slower (Windows parks WSL on efficiency cores). Only time on AC power.
- Large working sets are very slow here (the WSL VM shares 10 GB with Docker): the reference reads p20m at about 57 MB/s and its 20M-entry hash map runs at 2-3 us per operation. The reference took 175 s on p20m natively, so the "bench20 under 60 s" gate cannot be met on this laptop; on the scorer (Xeon, 24 GB) it should be several times faster, and the practice cap is 300 s.

## Correctness gate (latest run)
reference: ref-fixtures 36/36, ref-dev MATCH, image fixtures 36/36, image dev MATCH
shopping (native, not yet in the image): fixtures 36/36, dev MATCH, p20m identical to reference

## Local timings (median of 3 on data/p20m; data/f60m where noted)
| Commit | Change | Time | Peak memory | Notes |
| --- | --- | --- | --- | --- |
| bc75a66 | reference (native) | 175 s | 4.4 GB | single run, AC power |
| bc75a66 | reference image, bench20 (CPUS=8 MEM=8g) | 123 s | under 8g | single run, output MATCH vs native reference |
| (next) | shopping v1 (native, warm cache) | 5.7 s | 4.1 GB | single run; first cold run 14 s |

## Submissions
| Time (ET) | Commit | Leaderboard result | Notes |
| --- | --- | --- | --- |

## Open questions and blockers
- none yet

## Next step
- Submit the reference (M1), then put shopping into the image and run the M2 gate.
