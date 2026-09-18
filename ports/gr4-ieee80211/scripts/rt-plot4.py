#!/usr/bin/env python3
"""rt-plot4.py -- the 0036 figures from a sweep's batch_rt.json files.

    ./scripts/rt-plot4.py SWEEP_DIR [--out-dir DIR] [--x max_worker|total|receivers|rate] [--gr3 RUN_DIR ...]

Reads every <SWEEP_DIR>/*/batch_rt.json (rt-sweep4 + trace-batch-rt.py wrote
them) and draws, one panel per batch size N and one line per policy:

  batch_rt.png       batch response time (0036 item 16): mean with a +-1 std-dev
                     band and min/max whiskers, against the *measured* utilization
                     of the busiest worker (--x total: cores busy; --x receivers /
                     rate: the raw knob); points where the graph did not keep real
                     time are left out and counted in the footer
  edf_misses.png     EDF deadline-miss ratio against the same x
  frame_rt.png       the backup: frame response time (0035's last sample -> decoded
                     packet), same layout; --gr3 adds GR3 runs as a fourth series
                     (their latency.csv gives min/max/mean/std; their x is the
                     receiver count, so use --x receivers for that comparison)
  summary.csv        every point's numbers, one row per (N, policy, x, repeat)

Repeats at the same point are pooled: the mean/std/min/max are over all their
batches together, and the point's x is the mean of the repeats' utilizations.
Colors: the first three categorical slots of the data-viz reference palette
(validated all-pairs; RR blue, EDF orange, RM aqua); GR3, when present, is
drawn in grey with a dashed line so the three GR4 policies keep their hues.
"""
import argparse, csv, glob, json, os, sys
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SERIES = {"rr": ("#2a78d6", "RR (GR4 round robin)"), "edf": ("#eb6834", "EDF"), "rm": ("#1baf7a", "RM"), "gr3": ("#6b6a66", "GR3 (thread per block)")}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"


def load_points(sweep_dir):
    pts = []
    for p in sorted(glob.glob(os.path.join(sweep_dir, "*", "batch_rt.json"))):
        b = json.load(open(p))
        if not b.get("per_chain"):
            continue
        u = b.get("utilization") or {}
        # saturation: the run's own flag, or the throttle publishing ten batch periods late at the
        # 95th percentile (runs analysed before that rule was added carry only the first flag)
        per_us = b.get("batch_period_us")
        sat = bool(b.get("saturated"))
        if per_us:
            for pc in b["per_chain"]:
                tl = pc.get("throttle_lag") or {}
                if tl.get("n") and tl.get("p95_us", 0) > 10 * per_us:
                    sat = True
        pts.append({"dir": os.path.dirname(p), "N": b["fixed_batch"], "policy": b["policy"], "threads": b["threads"], "chains": b["chains"], "rate": b["rate"],
                    "traced": bool(b.get("trace", {}).get("written")), "u_max": u.get("max_worker"), "u_total": u.get("total_cores_busy"), "saturated": sat,
                    "per_chain": b["per_chain"], "edf": b.get("edf"), "lost": (b.get("trace_header") or {}).get("lost_count", 0), "batch_period_us": b.get("batch_period_us")})
    return pts


def pooled(chains, key):
    """pool a stat across chains and repeats from the per-chain summaries (n, mean, std, min, max):
    exact for min/max/mean; the pooled std uses the per-group n, mean, std (parallel variance)."""
    n = np.array([c[key]["n"] for c in chains if key in c and c[key].get("n")], dtype=float)
    if n.size == 0:
        return None
    m = np.array([c[key]["mean_us"] for c in chains if key in c and c[key].get("n")])
    s = np.array([c[key]["std_us"] for c in chains if key in c and c[key].get("n")])
    mn = min(c[key]["min_us"] for c in chains if key in c and c[key].get("n"))
    mx = max(c[key]["max_us"] for c in chains if key in c and c[key].get("n"))
    N = n.sum()
    mean = float((n * m).sum() / N)
    var = float((((n - 1) * s ** 2).sum() + (n * (m - mean) ** 2).sum()) / max(N - 1, 1))
    return {"n": int(N), "mean": mean, "std": var ** 0.5, "min": mn, "max": mx}


def gr3_points(run_dirs):
    out = []
    for rd in run_dirs:
        if not (os.path.exists(os.path.join(rd, "latency_summary.json")) and os.path.exists(os.path.join(rd, "latency.csv"))):
            print(f"rt-plot4: --gr3 {rd}: no latency_summary.json/latency.csv, skipped", file=sys.stderr)
            continue
        summ = json.load(open(os.path.join(rd, "latency_summary.json")))
        lat = list(csv.DictReader(open(os.path.join(rd, "latency.csv"))))
        v = np.array([float(r["lat_last_us"]) for r in lat if r.get("decoded") == "1" and r.get("lat_last_us")])
        if v.size == 0:
            continue
        out.append({"dir": rd, "policy": "gr3", "chains": summ["chains"], "rate": summ["latency"]["rate"], "N": None,
                    "frame": {"n": int(v.size), "mean": float(v.mean()), "std": float(v.std(ddof=1)) if v.size > 1 else 0.0, "min": float(v.min()), "max": float(v.max())}})
    return out


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, loc="left", fontsize=11, color=INK)
    ax.set_xlabel(xlabel, color=INK2)
    ax.set_ylabel(ylabel, color=INK2)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9)
    ax.grid(True, axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def draw(ax, groups, xkey, ykey, policies, direct_label=True):
    for pol in policies:
        rows = sorted([g for g in groups if g["policy"] == pol and g.get(ykey)], key=lambda g: g[xkey] if g[xkey] is not None else 0)
        if not rows:
            continue
        col, name = SERIES[pol]
        x = np.array([r[xkey] for r in rows], dtype=float)
        mean = np.array([r[ykey]["mean"] for r in rows])
        std = np.array([r[ykey]["std"] for r in rows])
        lo = np.array([r[ykey]["min"] for r in rows])
        hi = np.array([r[ykey]["max"] for r in rows])
        ls = "--" if pol == "gr3" else "-"
        ax.fill_between(x, mean - std, mean + std, color=col, alpha=0.18, linewidth=0)
        ax.vlines(x, lo, hi, color=col, linewidth=1.0, alpha=0.7)
        ax.plot(x, mean, ls, color=col, linewidth=2, marker="o", markersize=6, markeredgecolor="white", markeredgewidth=1.2, label=name)
        if direct_label and x.size:
            ax.annotate(name.split(" (")[0], (x[-1], mean[-1]), xytext=(6, 0), textcoords="offset points", fontsize=9, color=INK, va="center")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sweep_dir")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--x", default="max_worker", choices=["max_worker", "total", "receivers", "rate"])
    ap.add_argument("--gr3", nargs="*", default=[])
    ap.add_argument("--include-controls", action="store_true", help="also plot untraced runs (they have frame RT only)")
    a = ap.parse_args()
    out_dir = a.out_dir or a.sweep_dir
    os.makedirs(out_dir, exist_ok=True)
    pts = load_points(a.sweep_dir)
    if not pts:
        sys.exit(f"rt-plot4: no batch_rt.json under {a.sweep_dir}")
    xkey = {"max_worker": "u_max", "total": "u_total", "receivers": "chains", "rate": "rate"}[a.x]
    xlabel = {"max_worker": "measured utilization of the busiest worker", "total": "measured cores busy (sum over blocks)", "receivers": "receivers", "rate": "sample rate (Msps)"}[a.x]

    # group repeats: same (N, policy, threads, chains, rate, traced)
    groups = {}
    for p in pts:
        if not p["traced"] and not a.include_controls:
            continue
        k = (p["N"], p["policy"], p["threads"], p["chains"], p["rate"], p["traced"])
        groups.setdefault(k, []).append(p)
    rows = []
    for k, ps in groups.items():
        N, pol, T, R, rate, traced = k
        chains = [c for p in ps for c in p["per_chain"]]
        xs = [p[xkey] for p in ps if p.get(xkey) is not None]
        g = {"N": N, "policy": pol, "threads": T, "chains": R, "rate": rate / 1e6 if a.x == "rate" else rate, "traced": traced, "repeats": len(ps), "saturated": any(p["saturated"] for p in ps),
             "x": float(np.mean(xs)) if xs else None, "batch": pooled(chains, "batch_rt"), "frame": pooled(chains, "frame_rt"), "frame_batch": pooled(chains, "frame_batch_rt"),
             "lost": sum(p["lost"] or 0 for p in ps), "batch_period_us": ps[0]["batch_period_us"]}
        if a.x == "rate":
            g["x"] = rate / 1e6
        elif a.x == "receivers":
            g["x"] = R
        e = [p["edf"] for p in ps if p.get("edf")]
        if e:
            # pipeline blocks only: the source and throttle carry a 1 us deadline on purpose
            rel = sum(x.get("releases_pipeline", x["releases"]) for x in e)
            mis = sum(x.get("misses_pipeline", x["misses"]) for x in e)
            g["miss_ratio"] = mis / rel if rel else None
        rows.append(g)
    Ns = sorted({g["N"] for g in rows})
    policies = [p for p in ("rr", "edf", "rm") if any(g["policy"] == p for g in rows)]

    def figure(ykey, title, ylabel, fname, extra=None, drop_saturated=False, logy=False):
        fig, axes = plt.subplots(1, len(Ns), figsize=(5.2 * len(Ns), 4.2), squeeze=False, sharey=False)
        dropped = 0
        for ax, N in zip(axes[0], Ns):
            gs = [dict(g, **{"_x": g["x"]}) for g in rows if g["N"] == N and g["x"] is not None]
            if drop_saturated:
                dropped += sum(1 for g in gs if g["saturated"])
                gs = [g for g in gs if not g["saturated"]]
            if logy:
                ax.set_yscale("log")
            for g in gs:
                g[xkey if xkey in g else "x"] = g["x"]
            draw(ax, [dict(g, **{xkey: g["x"]}) for g in gs], xkey, ykey, policies)
            per = gs[0]["batch_period_us"] if gs else None
            sub = f"N = {N}" + (f" ({per:.0f} µs per batch)" if per else "")
            style(ax, sub, xlabel, ylabel)
            if extra:
                extra(ax, gs)
        axes[0][0].legend(loc="upper left", frameon=False, fontsize=9)
        fig.suptitle(title, x=0.01, ha="left", fontsize=13, color=INK)
        note = "mean, ±1 std-dev band, min–max whiskers over all batches of the pooled repeats; x = " + xlabel
        if drop_saturated and dropped:
            note += f"; {dropped} saturated points (the graph did not keep real time) left out — their response times are unbounded"
        fig.text(0.01, 0.005, note, fontsize=8, color=INK2)
        fig.tight_layout(rect=(0, 0.03, 1, 0.95))
        path = os.path.join(out_dir, fname)
        fig.savefig(path, dpi=150, facecolor="#fcfcfb")
        plt.close(fig)
        print("rt-plot4: wrote " + path)

    figure("batch", "Batch response time — nominal arrival of batch j to sync_short consuming it", "µs", "batch_rt.png", drop_saturated=True)
    figure("frame", "Frame response time (backup metric) — last sample released to decoded packet", "µs (log)", "frame_rt.png", logy=True)
    figure("frame_batch", "Frame response time anchored at the batch release", "µs", "frame_batch_rt.png", drop_saturated=True)

    # EDF miss ratio
    if any(g.get("miss_ratio") is not None for g in rows):
        fig, axes = plt.subplots(1, len(Ns), figsize=(5.2 * len(Ns), 3.6), squeeze=False)
        for ax, N in zip(axes[0], Ns):
            gs = sorted([g for g in rows if g["N"] == N and g["policy"] == "edf" and g.get("miss_ratio") is not None and g["x"] is not None], key=lambda g: g["x"])
            if gs:
                ax.plot([g["x"] for g in gs], [g["miss_ratio"] for g in gs], "-", color=SERIES["edf"][0], linewidth=2, marker="o", markersize=6, markeredgecolor="white")
            style(ax, f"N = {N}", xlabel, "deadline-miss ratio (misses / releases)")
            ax.set_ylim(bottom=0)
        fig.suptitle("EDF: jobs that finished after their implicit deadline", x=0.01, ha="left", fontsize=13, color=INK)
        fig.tight_layout(rect=(0, 0.02, 1, 0.95))
        fig.savefig(os.path.join(out_dir, "edf_misses.png"), dpi=150, facecolor="#fcfcfb")
        plt.close(fig)
        print("rt-plot4: wrote " + os.path.join(out_dir, "edf_misses.png"))

    # GR3 backup comparison on frame RT vs receivers
    if a.gr3:
        g3 = gr3_points(a.gr3)
        fig, ax = plt.subplots(figsize=(6.4, 4.2))
        gs = [dict(g, x=g["chains"]) for g in rows if g["frame"]]
        for N in Ns:
            pass
        draw(ax, [dict(g, chains=g["chains"]) for g in gs], "chains", "frame", policies, direct_label=False)
        g3r = [dict(p, chains=p["chains"], x=p["chains"]) for p in g3]
        draw(ax, g3r, "chains", "frame", ["gr3"], direct_label=False)
        style(ax, "all batch sizes pooled per policy", "receivers", "µs")
        ax.legend(loc="upper left", frameon=False, fontsize=9)
        fig.suptitle("Frame response time, GR3 against the GR4 policies", x=0.01, ha="left", fontsize=13, color=INK)
        fig.tight_layout(rect=(0, 0.02, 1, 0.94))
        fig.savefig(os.path.join(out_dir, "frame_rt_vs_gr3.png"), dpi=150, facecolor="#fcfcfb")
        plt.close(fig)
        print("rt-plot4: wrote " + os.path.join(out_dir, "frame_rt_vs_gr3.png"))

    with open(os.path.join(out_dir, "summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["N", "policy", "threads", "receivers", "rate", "traced", "repeats", "saturated", "x", "batch_n", "batch_mean_us", "batch_std_us", "batch_min_us", "batch_max_us", "frame_n", "frame_mean_us", "frame_std_us", "frame_min_us", "frame_max_us", "edf_miss_ratio", "lost_records"])
        for g in sorted(rows, key=lambda g: (g["N"], g["policy"], g["x"] or 0)):
            b, fr = g["batch"] or {}, g["frame"] or {}
            w.writerow([g["N"], g["policy"], g["threads"], g["chains"], g["rate"], g["traced"], g["repeats"], g["saturated"], g["x"], b.get("n"), b.get("mean"), b.get("std"), b.get("min"), b.get("max"), fr.get("n"), fr.get("mean"), fr.get("std"), fr.get("min"), fr.get("max"), g.get("miss_ratio"), g["lost"]])
    print("rt-plot4: wrote " + os.path.join(out_dir, "summary.csv"))


if __name__ == "__main__":
    main()
