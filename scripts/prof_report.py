#!/usr/bin/env python3
"""Aggregate SS_PROF samples (offsets into the executable) by innermost function and line.
Usage: prof_report.py BINARY SAMPLES_FILE [TOP]"""
import collections
import subprocess
import sys

binary, samples = sys.argv[1], sys.argv[2]
top = int(sys.argv[3]) if len(sys.argv) > 3 else 30
offs = [l.strip() for l in open(samples) if l.strip()]
uniq = sorted(set(offs))
out = subprocess.run(["addr2line", "-a", "-f", "-i", "-C", "-e", binary],
                     input="\n".join(uniq) + "\n", capture_output=True, text=True).stdout.splitlines()
# output: address line, then (function, file:line) pairs, innermost first
info = {}
i = 0
while i < len(out):
    addr = out[i]
    i += 1
    frames = []
    while i < len(out) and not out[i].startswith("0x"):
        fn, loc = out[i], out[i + 1] if i + 1 < len(out) else "?"
        frames.append((fn, loc.split("/")[-1].split(" ")[0]))
        i += 2
    info[addr] = frames
by_fn = collections.Counter()
by_line = collections.Counter()
by_outer = collections.Counter()
for o in offs:
    key = "0x%016x" % int(o, 16)
    fr = info.get(key) or [("?", "?")]
    fn, loc = fr[0]
    by_fn[fn[:90]] += 1
    by_line["%s  %s" % (loc, fn[:60])] += 1
    by_outer[fr[-1][0][:90]] += 1
n = len(offs)
print(f"{n} samples\n\n== innermost function")
for k, v in by_fn.most_common(top):
    print(f"{100 * v / n:5.1f}%  {k}")
print("\n== source line (innermost)")
for k, v in by_line.most_common(top):
    print(f"{100 * v / n:5.1f}%  {k}")
print("\n== outermost frame (stage)")
for k, v in by_outer.most_common(12):
    print(f"{100 * v / n:5.1f}%  {k}")
