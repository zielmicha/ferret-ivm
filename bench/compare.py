#!/usr/bin/env python3
"""Summarize and compare ivm-bench results.

Each argument is a JSON file written by `ivm-bench --json FILE`; files are
grouped into sets by a `--` separator (or by `A:`/`B:` prefixes), e.g.

    bench/compare.py base-1.json base-2.json -- new-1.json new-2.json

For every measurement and metric the minimum and median over the runs of
each set are printed, and with two sets the change of the medians. CPU
times on a shared machine are noisy: interleave the runs of the variants
(A B A B ...) and look at medians and minimums together.
"""

import json
import statistics
import sys

# metrics shown per measurement (when present)
METRICS = [
    "wall_s",
    "engine_cpu_s",
    "source_cpu_s",
    "sequencer_cpu_s",
    "session_cpu_s",
    "peak_rss_mb",
    "engine_us_per_txn",
    "engine_us_per_row",
    "engine_allocs",
    "engine_allocs_per_txn",
    "engine_allocs_per_row",
    "source_allocs",
    "source_us_per_row",
    "mean_ms",
    "p99_ms",
    "cpu_ms_per_query",
    "catchup_after_commit_s",
    "first_progress_s",
    "lag_after_upstream_s",
    "store_mb",
    "first_read_s",
]


def load(path):
    with open(path) as f:
        return json.load(f)


def collect(paths):
    """phase -> metric -> [values]"""
    out = {}
    order = []
    for p in paths:
        for rec in load(p):
            ph = rec["phase"]
            if ph not in out:
                out[ph] = {}
                order.append(ph)
            for k, v in rec.items():
                if k != "phase" and isinstance(v, (int, float)):
                    out[ph].setdefault(k, []).append(float(v))
    return out, order


def fmt(v):
    if v is None:
        return "-"
    if abs(v) >= 100:
        return f"{v:.0f}"
    if abs(v) >= 10:
        return f"{v:.1f}"
    return f"{v:.3f}"


def main(argv):
    sets = [[]]
    for a in argv:
        if a == "--":
            sets.append([])
        else:
            sets[-1].append(a)
    sets = [s for s in sets if s]
    if not sets:
        print(__doc__)
        return 2
    data = [collect(s) for s in sets]
    phases = []
    for _, order in data:
        for ph in order:
            if ph not in phases:
                phases.append(ph)
    hdr = f"{'measurement':<24} {'metric':<24}"
    for i, s in enumerate(sets):
        hdr += f" {'set' + str(i + 1) + ' min':>10} {'median':>10}"
    if len(sets) == 2:
        hdr += f" {'change':>8}"
    print(hdr)
    for ph in phases:
        for m in METRICS:
            vals = [d.get(ph, {}).get(m) for d, _ in data]
            if all(v is None for v in vals):
                continue
            line = f"{ph:<24} {m:<24}"
            meds = []
            for v in vals:
                if v:
                    line += f" {fmt(min(v)):>10} {fmt(statistics.median(v)):>10}"
                    meds.append(statistics.median(v))
                else:
                    line += f" {'-':>10} {'-':>10}"
                    meds.append(None)
            if len(sets) == 2 and meds[0] and meds[1] is not None:
                line += f" {100.0 * (meds[1] - meds[0]) / meds[0]:>+7.1f}%"
            print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
