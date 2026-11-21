#!/bin/sh
# Diagnostic run (diag branch only): prints the scoring machine's shape and our stage timings
# at ~20M-event input volume, then exits non-zero on purpose so the dev-set check fails and the
# scorer shows these logs. Not a scoring submission.
exec 1>&2
echo "=== machine"
grep -m1 "model name" /proc/cpuinfo
grep -m1 "flags" /proc/cpuinfo | tr ' ' '\n' | grep -E "^(avx2|avx512f|avx512bw|bmi2|erms|fsrm)$" | tr '\n' ' '; echo
echo "nproc: $(nproc)  NCPU=$NCPU  MEM_GB=$MEM_GB"
grep -E "^(cpu MHz|cache size|siblings|cpu cores)" /proc/cpuinfo | sort | uniq -c | head -8
uname -r
cat /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null
grep -E "MemTotal|MemAvailable|Cached|AnonHugePages" /proc/meminfo
cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max 2>/dev/null
df -h /scratch /data 2>/dev/null | tail -2
mount | grep -E " /data | /scratch | /output " | head -3

now() { date +%s%N; }
echo "=== dev set (data as mounted)"
t=$(now); SS_TIMERS=1 /app/run; rc=$?; echo "dev run rc=$rc wall $(( ($(now) - t) / 1000000 )) ms"

echo "=== building ~20x dev volume in /scratch"
t=$(now)
mkdir -p /scratch/big/events /scratch/big/clicks /scratch/big/links /scratch/out
cp /data/manifest.json /data/products.csv /data/campaigns.csv /scratch/big/
for d in events clicks links; do
  for f in /data/$d/*.csv; do
    b=$(basename "$f" .csv)
    if [ "$d" = links ]; then
      cp "$f" "/scratch/big/$d/$b.csv"
    else
      i=0; while [ $i -lt 20 ]; do cp "$f" "/scratch/big/$d/$b-r$i.csv"; i=$((i+1)); done
    fi
  done
done
du -sh /scratch/big; echo "copy took $(( ($(now) - t) / 1000000 )) ms"
# warm the page cache like the scored runs 2 and 3
cat /scratch/big/*/*.csv > /dev/null

for k in 1 2 3; do
  echo "=== 20x run $k"
  rm -rf /scratch/out; mkdir -p /scratch/out
  t=$(now)
  DATA_DIR=/scratch/big OUTPUT_DIR=/scratch/out SS_TIMERS=1 /app/run /scratch/big /scratch/out
  echo "20x run $k rc=$? wall $(( ($(now) - t) / 1000000 )) ms"
done
grep -E "VmHWM|VmRSS" /proc/self/status
echo "=== done; failing on purpose so these logs are shown"
exit 3
