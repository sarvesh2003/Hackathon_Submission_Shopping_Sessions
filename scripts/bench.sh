#!/usr/bin/env bash
# Usage: scripts/bench.sh DATA_DIR [RUNS]
# Runs the image with the scorer's flags RUNS times and prints each wall time and the median.
# Includes a little wrapper overhead, so compare our own versions only.
set -euo pipefail
cd "$(dirname "$0")/.."
data="$1"; runs="${2:-3}"
mkdir -p out
ms=()
for i in $(seq 1 "$runs"); do
  rm -rf out/bench
  t0=$(date +%s%N)
  scripts/docker_run.sh "$data" out/bench >/dev/null 2>out/bench.stderr.log
  t1=$(date +%s%N)
  t=$(( (t1 - t0) / 1000000 ))
  ms+=("$t")
  echo "run $i: ${t} ms"
done
printf '%s\n' "${ms[@]}" | sort -n | awk '{a[NR]=$1} END {m=(NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2; printf "median: %d ms over %d runs\n", m, NR}'
