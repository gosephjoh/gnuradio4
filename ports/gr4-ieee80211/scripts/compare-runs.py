#!/usr/bin/env python3
"""Compare a GR3 latency.csv with a GR4 latency.csv on the same cell, packet for packet.

    compare-runs.py GR3/latency.csv GR4/latency.csv [--chain K]

Reports, per chain: frames, decoded by each, decoded by both, GR3-only and
GR4-only sequence numbers (a GR4-only frame with correct=1 is a frame GR3's
decoder gambled away -- gotchas #36/#38 of the GR3 project), and the
last-sample-to-decode percentiles side by side.  Offline analysis, so Python.
"""
import csv
import sys


def load(path):
    rows = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.setdefault(int(r["chain"]), {})[int(r["seq"])] = r
    return rows


def pct(v, q):
    if not v:
        return float("nan")
    v = sorted(v)
    i = max(0, min(len(v) - 1, int(-(-q * len(v) // 1)) - 1))
    return v[i]


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) != 2:
        print(__doc__)
        return 2
    a, b = load(args[0]), load(args[1])
    chains = sorted(set(a) | set(b))
    rc = 0
    for k in chains:
        ra, rb = a.get(k, {}), b.get(k, {})
        seqs = sorted(set(ra) | set(rb))
        da = {s for s in ra if ra[s]["decoded"] == "1"}
        db = {s for s in rb if rb[s]["decoded"] == "1"}
        both = da & db
        only_a, only_b = sorted(da - db), sorted(db - da)
        wrong_b = sorted(s for s in db if rb[s].get("correct", "") == "0")
        la = [float(ra[s]["lat_last_us"]) for s in da]
        lb = [float(rb[s]["lat_last_us"]) for s in db]
        print(f"chain {k}: frames {len(seqs)}; decoded GR3 {len(da)} GR4 {len(db)} both {len(both)}")
        print(f"  GR3-only {len(only_a)} {only_a[:20]}")
        print(f"  GR4-only {len(only_b)} {only_b[:20]}  (GR4 wrong payload: {len(wrong_b)} {wrong_b[:20]})")
        print("  last->decode us     p50      p95      p99      max")
        print(f"  GR3            {pct(la,.5):8.1f} {pct(la,.95):8.1f} {pct(la,.99):8.1f} {max(la) if la else float('nan'):8.1f}")
        print(f"  GR4            {pct(lb,.5):8.1f} {pct(lb,.95):8.1f} {pct(lb,.99):8.1f} {max(lb) if lb else float('nan'):8.1f}")
        if only_a or wrong_b:
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
