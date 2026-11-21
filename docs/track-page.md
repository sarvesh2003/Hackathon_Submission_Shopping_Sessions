# Databricks: Shopping Sessions (official track page, copied 2026-11-21)

Source: https://www.joinbuilders.org/buildathon/tracks/shopping-sessions. Rules: [rules.md](rules.md). Prize: $5,000, winner takes all.

> Turn 60 million messy click-stream events into two exact daily tables. Fastest correct pipeline wins.

## The problem
An online shop logs every page view, add-to-cart and purchase, and every ad click, as CSV files, spread over files of very different sizes. The logs are dirty: duplicated events, malformed lines, bad timestamps in three formats, events that arrive a day late. One shopper can use many user IDs, and a third file links them: chains hundreds of links long, and shared devices that tie thousands of IDs into one person.

Clean them, join each person's IDs, group each person's events into sessions, credit each session to the ad click that brought them in, count every campaign that assisted it in the week before, and write two daily tables: one per person, one per campaign. The rules are exact, so there is exactly one right answer. A fast answer that is almost right scores nothing. The fastest correct pipeline wins.

## What you hand in
A public GitHub repository with a Dockerfile at its top, and the commit to score. The repository at that commit may be at most 50 MB, so keep the data out of it: it is mounted at run time. Any engine, any language, any amount of AI.

The starters (PySpark, DuckDB, Polars) each build a linux/amd64 image, run under the scorer's limits and write both tables with the right columns but no rows. Each comes with the 36 edge-case fixtures and scripts that run your image the way the scorer does and show which rows differ. dev-answer.zip holds the right answer for the public dev set, so you can check yourself without spending a submission.

## How it runs
- The image must be linux/amd64. The scorers are Google Cloud n2 machines: Intel Xeon, Cascade Lake or newer, so AVX-512 is there, and every scoring machine has the same CPU. It builds with the network in at most 10 minutes; the run has no network.
- 8 vCPUs (hyperthreads) pinned to your run, and 24 GB of memory, no swap.
- The dataset is in /data, read-only. Write /output/user_daily/ and /output/campaign_daily/ as Parquet. /scratch is empty, writable disk for spills.
- The root filesystem is read-only apart from /tmp, 64 MB in memory, and your process runs as uid 1000.
- The image may be at most 8 GB, and /output and /scratch together may hold 50 GB.
- Environment: DATA_DIR=/data, OUTPUT_DIR=/output, SCRATCH_DIR=/scratch, HOME=/scratch, NCPU=8 and MEM_GB=24. At most 4,096 processes and threads, and 50 MB printed to stdout: log to stderr.

## Docs and starter files (all at the source URL above, under /files/)
- rules.md: inputs, the CSV dialect, cleaning, linked IDs, sessions, attribution, assists and the two output tables (copied to docs/rules.md).
- starter-pyspark.zip, starter-duckdb.zip, starter-polars.zip: skeletons that write empty tables. Their fixtures/ and scripts/ are copied into this repo.
- dev-answer.zip: the right answer for the public dev set (seed 1, 1M events), as user_daily.csv and campaign_daily.csv. `make kit` downloads it into kit/dev-answer/.
- generator.zip: `generate`, the dataset generator (Rust source). Makes the public 1M-event dev set, or any size and seed. `make kit` builds it into kit/bin/generate.

## Scoring
- Metric: run time, lower is better. Baseline to beat: 172.00 s (it was 171.91 s at kickoff) (starter: PySpark, local mode).
- Formula: one run on hidden data (median of 3 when under 20 s), container start to container exit.
- Ties: equal scores rank the earlier submission first.
- We build your repository's Dockerfile, check it on the public dev set, then time it once on a hidden practice dataset of about 20 million events, in a new container each time; a run under 20 seconds is timed 3 times and scored on the middle time. Every run's tables must match the reference exactly. The final is timed 3 times on a new dataset of about 60 million events; the fastest entries are then timed again on one idle machine (at least the 10 fastest, 7 times each, per the live page), and that median decides the winner.

## Rules
- Where a starter and rules.md disagree, rules.md wins; ask the organizers if rules.md leaves you unsure.
- Each submission first runs on the public 1M-event dev set (at most 3 minutes). If it is wrong there you get the differing rows and your logs, and it never reaches the timed runs.
- Then it is timed once on hidden data, or 3 times if the first run is under 20 seconds. The repeats share the time the first run was allowed; one cut off counts as the full time limit. A wrong table, a crash, or a run over the memory, disk or time limit stops the submission. A practice run may take 2x the baseline's time, and at most 5 minutes.
- The final caps each run at 3 minutes on about 3x the practice data. Time grows at least in step with the data, so a practice time over about 60 seconds will probably time out in the final and score nothing there, even though practice accepts it.
- The same image can score up to about 20% apart depending on which scoring machine runs it, and a few percent apart on the same machine. Gaps under about 20% between teams are not meaningful on the practice board.
- A busy scoring machine runs slower than an idle one. That is why the fastest final entries are timed again on one machine with nothing else running.
- On hidden data we never show your logs or any value from the data: only the exit code, whether memory ran out, and counts of missing, extra and differing rows.
- The clock runs from your container starting to it exiting: starting the engine and reading the CSV files count.
- A rejected submission never changes your best score.
- Your team may submit 2 times an hour.
- Keep your repository public, with every commit you submitted still in it, until the results are published: the final builds your chosen commit again, and an entry we can neither restore nor fetch scores nothing.

## The final
Updated on the live page (checked Sat Nov 21): at the cutoff, we rebuild your chosen commit (your best, unless you pick another) and time it 3 times on about 60 million new events made after submissions close, each run capped at 3 minutes. If any run goes over 3 minutes, you get no final score; otherwise your score is the median. At least the 10 fastest entries are then retimed 7 times alone on one idle machine, and that median replaces their first score.

The cutoff is Sun Nov 22, 11:00 AM ET.

## General Buildathon rules that apply
- Teams of up to 2. Any tools, AI included. Code written before the weekend and open-source libraries are allowed; name them in the README.
- Everything built must be released under the MIT License.
- Every submission counts toward the hourly limit, even a rejected one. One entry per team per track at a time, including entries waiting in line.
- Pick your final submission on the track page before the cutoff; if you don't, your best-scoring entry is used.
