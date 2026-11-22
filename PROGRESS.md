# PROGRESS.md: Shopping Sessions

Claude updates this after every milestone. The human reads it to see where things stand.

Last updated: Sat Nov 21, ~10:45 PM ET

## Status
| Milestone | State | Notes |
| --- | --- | --- |
| M0 Setup | done | Repo lives in WSL at `~/shopping_sessions` (not /mnt/c), pushed to GitHub. |
| M1 Simple Rust reference (first submission) | done | 36/36 fixtures, dev MATCH (native and image). |
| M2 Fast version v1 | done | |
| M3 Speed | in progress | v5 data: ~0.78 s end-to-end locally (8 threads); best scorer result 0.952 s (v4 data), 1.04-1.06 s on v5. |
| M4 Final hardening | not started | check60 still impossible locally (the reference needs more memory than the laptop has). |

## Rules change (organizers, Sat 9:08 PM): v5
- 1 in 8 user/event/click IDs is a letter + 32 hex characters; IDs pass through a per-seed keyed bijection. Pipeline rules unchanged. kit/ and docs/rules.md updated; all data regenerated (old data in data_v4/, kit_v4/, git-ignored).
- Every team's best entry is rescored on v5. Final: top 30, interleaved rounds over 3 fresh 60M datasets (9 runs).
- Our old code interned long IDs behind a global lock: 9-10 s on v5. Fixed in d96850b: unpackable IDs get a 63-bit hashed key (collision probability ~1e-5 per 60M dataset), bytes kept only where needed.

## Machine
CPU: Intel Core Ultra 5 225U (12 cores / 14 threads, laptop) | AVX-512: no | WSL capped at 10 GB (laptop 15.5 GB) | Docker limits used: CPUS=8 MEM=8g
- Only time on AC power (battery throttles ~2x). Background Docker containers distort timings badly.
- Compare builds only with interleaved runs (`~/scratch_bench/e2e.sh ROUNDS DATA name...`: end-to-end best/q1/median).
- Windows-exe interop inside WSL is broken since the WSL restart: DOCKER_CONFIG=~/.docker-nocreds for builds; pushes go through Windows git.

## Correctness gate (latest: dc4444a, v5 data)
shopping: unit tests | fixtures 36/36 | dev MATCH | check20 identical | crosscheck 12/12 | check60: not run (no 60M reference possible locally). 60M output passes the rules' self-checks (sessions and revenue equal across tables, assisted >= non-organic, no duplicate keys).

## What the scorer measures (established with two diagnostic submissions)
- ade74e6 (delay-encoded facts): real Linux (not gVisor), THP madvise, MADV_POPULATE_WRITE supported.
- 5649428 (sleep 10x our own running time): ~0.96 s of the 1.04 s score is inside our process; only ~80 ms outside.
- So the score tracks our 8-thread end-to-end wall time (critical path), not total CPU work. Practice scores also vary ~10-20% across machines, so single submissions cannot resolve <10% differences.

## Submissions
| Commit | Data | Scorer | Notes |
| --- | --- | --- | --- |
| 2133c20 | v4 | 1.12 s | |
| 8904915 | v4 | 1.08 s | |
| 19fd217 | v4 | 1.05 s | |
| f4b00c4 | v4 | 1.05 s | |
| 3c7c9d7 (exp-pread) | v4 | 1.08 s | pread instead of mmap |
| 173b98a / ac22c3b | v4 | 0.952 s | arena + lock-free blocks, no second rayon pool, SIMD ID packing |
| d96850b | v5 | 1.06 s | hashed keys for long IDs |
| 3537898 | v5 | 1.06 s | name pointers instead of per-thread maps (in-process -15%, but +100 ms exit; since fixed) |
| ade74e6 | v5 | 1.04 s | diagnostic (delay 0) |
| 5649428 | v5 | 10.63 s | diagnostic (in-process ~0.96 s) |

## Local end-to-end (v5 20M, NCPU=8, 11 interleaved rounds)
| Commit | best | median |
| --- | --- | --- |
| d96850b | 807 ms | 869 ms |
| 3537898 | 760 ms | 815 ms |
| dc4444a | 741 ms | 797 ms |

## Kept since the 0.952 s submission
- Pre-fault arena slabs (MADV_POPULATE_WRITE), parallel arena zap, `_exit(0)`.
- Sessions: LSD radix sort by (person bucket, ts); partitions sub-split so the sort stays in half an L2 at 60M.
- Splitter: one branch for all malformed-line checks. Event type by one integer compare.
- v5: hashed keys; long-ID names as (key, input pointer) records turned into per-partition maps in the resolve stage; inputs dropped in parallel after the write.
- Union-find: no byte comparisons in unions; roots in one pass; persons named in one pass (v5 ~300 -> ~175 ms on worker 0; would have neared the parse stage at 60M).
- PartBuf: one write pointer per partition. Startup: parallel chunking, categories deduplicated before sorting.

## Tried and rejected (measured, not faster here)
- Splitters: popcount/two-stage index (1.4-2.5x slower); 3-block sliding window with masks once per aligned 64 B (28.6 -> 39.7 ns/line); byte-load quote detection.
- Branch-free timestamps (64 vs 54 ns/line); dropping one radix pass with a fix-up check (sessions 797 vs 740 ms).
- Flat open-addressing person maps in resolve (no change: data-movement bound); pre-sized per-thread maps.
- Person lookup fused into dedup / during parse (DRAM misses); union-find before parsing; two records per event; non-temporal stores; 256/1024/2048 partitions.
- MADV_POPULATE_READ on input chunks; pre-touching the input; pread input; MAP_POPULATE; mimalloc options; glibc malloc.

## Open questions and blockers
- check60 needs a machine with more memory, or a lower-memory way to run the reference.
- Exact (collision-free) long-ID keys would cost ~3-5%; currently hashed (~1e-5 risk per dataset). Human's call.

## Next step
- Critical-path work (8-thread wall): exit path, sessions per-person work, parse, union-find on worker 0. A fresh design review is running.
