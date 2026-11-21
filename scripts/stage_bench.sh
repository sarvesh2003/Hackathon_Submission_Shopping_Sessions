#!/usr/bin/env bash
# Usage: scripts/stage_bench.sh DATA_DIR [RUNS] [BIN]
# Runs the native binary RUNS times and prints the median time at which each stage ended.
# Local-only tool for comparing our own versions (the leaderboard is the real measure).
set -euo pipefail
cd "$(dirname "$0")/.."
data="$1"; runs="${2:-5}"; bin="${3:-target/release/shopping}"
export NCPU="${NCPU:-8}"   # the scorer gives us 8 vCPUs
tmp=$(mktemp -d)
for i in $(seq 1 "$runs"); do
  rm -rf "$tmp/out"
  "$bin" "$data" "$tmp/out" 2>"$tmp/log.$i" >/dev/null
done
python3 - "$tmp" "$runs" <<'EOF'
import sys, re, statistics, collections
tmp, runs = sys.argv[1], int(sys.argv[2])
stages = collections.OrderedDict()
for i in range(1, runs + 1):
    for line in open(f"{tmp}/log.{i}"):
        m = re.match(r"shopping:\s+([\d.]+) ms\s+(.*)", line.strip())
        if m:
            stages.setdefault(m.group(2), []).append(float(m.group(1)))
for name, v in stages.items():
    print(f"{statistics.median(v):9.1f} ms  (min {min(v):8.1f})  {name}")
EOF
rm -rf "$tmp"
