#!/usr/bin/env bash
# Usage: scripts/crosscheck.sh [DATASET_DIR...]   (default: every dataset under data/x/)
# Runs the Docker image (scorer flags) and the reference binary on each dataset and compares the
# outputs exactly. Reference outputs are cached in out/ref-cache/ until the reference binary changes.
set -uo pipefail
cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python}"
REF="${REF:-target/release/reference}"
[ -x "$REF" ] || { echo "missing $REF (run: make ref)"; exit 1; }
if [ "$#" -eq 0 ]; then set -- data/x/*/; fi
fail=0; n=0
for d in "$@"; do
  d="${d%/}"
  [ -d "$d" ] || { echo "skip $d (missing; run make data / make data60)"; continue; }
  name=$(basename "$d"); n=$((n + 1))
  img="out/check/$name"; ref="out/ref-cache/$name"
  rm -rf "${img:?}"; mkdir -p "$(dirname "$img")" "$(dirname "$ref")"
  if [ ! -f "$ref/.done" ] || [ "$REF" -nt "$ref/.done" ]; then
    rm -rf "${ref:?}"; mkdir -p "$ref"
    if ! DATA_DIR="$d" OUTPUT_DIR="$ref" "$REF" "$d" "$ref" 2>"$ref.log"; then
      echo "FAIL $name: reference exited non-zero (see $ref.log)"; fail=1; continue
    fi
    touch "$ref/.done"
  fi
  if ! scripts/docker_run.sh "$d" "$img" >/dev/null 2>"$img.log"; then
    echo "FAIL $name: image exited non-zero (see $img.log)"; fail=1; continue
  fi
  if "$PY" scripts/compare.py "$img" "$ref" >"$img.compare.txt" 2>&1; then
    echo "ok   $name"
  else
    echo "FAIL $name: outputs differ (see $img.compare.txt)"; fail=1
  fi
done
[ "$n" -gt 0 ] || { echo "no datasets checked"; exit 1; }
[ "$fail" -eq 0 ] && echo "crosscheck: all $n identical" || echo "crosscheck: FAILED"
exit $fail
