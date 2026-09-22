#!/usr/bin/env python3
"""rt-paper-figs.py -- the range figures of a 0036 sweep, as PDF and PNG, plus the numbers they show.

    ./scripts/rt-paper-figs.py SWEEP_DIR --out-dir DIR [--gr3 RUN_DIR ...]

Every figure shows, per configuration and policy, the mean (dot), +-1 sample
standard deviation (thick bar) and the minimum and maximum (thin whisker) of a
response-time population -- the range plot decision 0036 item 16 asks for.
Populations are all batches (or frames) of all chains of all repeats of a point.

Figures (each <name>.pdf and <name>.png):
  batch_rt_N<N>            batch response time per configuration, unsaturated points, linear axis
  batch_rt_saturated       the saturated configurations, log axis, all N
  batch_rt_vs_demand       batch RT against measured demand, one panel per (N, workers), lines per policy
  frame_rt_N<N>            frame response time (0035) per configuration, all points, log axis
  frame_rt_vs_gr3          frame RT against the receiver count with the GR3 runs given by --gr3
  edf_misses               EDF pipeline deadline-miss ratio per configuration
  pipeline_N<N>            where a batch's time goes: completion offset per pre-gate block at the
                           one-receiver, one-worker, 5 Msps point, per policy
  numbers.json / numbers.md  every value the figures show, for the text
"""
import argparse, csv, glob, json, os, sys, collections
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

POL = ["rr", "edf", "rm"]
COL = {"rr": "#2a78d6", "edf": "#eb6834", "rm": "#1baf7a", "gr3": "#6b6a66"}
NAME = {"rr": "RR", "edf": "EDF", "rm": "RM", "gr3": "GR3"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
PRE_GATE = ["mag2", "avgpow", "dly16", "conj", "mul", "avgcor", "mag", "div", "syncshort"]
plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9, "pdf.fonttype": 42, "ps.fonttype": 42})


def pooled(stats_list):
    """exact min/max/mean over groups given (n, mean, std, min, max); pooled sample std by parallel variance."""
    g = [s for s in stats_list if s and s.get("n")]
    if not g:
        return None
    n = np.array([s["n"] for s in g], float)
    m = np.array([s["mean_us"] for s in g])
    sd = np.array([s["std_us"] for s in g])
    N = n.sum()
    mean = float((n * m).sum() / N)
    var = float((((n - 1) * sd ** 2).sum() + (n * (m - mean) ** 2).sum()) / max(N - 1, 1))
    return {"n": int(N), "mean": mean, "std": var ** 0.5, "min": float(min(s["min_us"] for s in g)), "max": float(max(s["max_us"] for s in g))}


def saturated(b):
    if b.get("saturated"):
        return True
    per = b.get("batch_period_us")
    if per:
        for pc in b["per_chain"]:
            tl = pc.get("throttle_lag") or {}
            if tl.get("n") and tl.get("p95_us", 0) > 10 * per:
                return True
    return False


def load(sweep):
    runs = []
    for p in sorted(glob.glob(os.path.join(sweep, "*", "batch_rt.json"))):
        b = json.load(open(p))
        tag = os.path.basename(os.path.dirname(p))
        if tag.startswith("probe_"):
            continue
        runs.append(b)
    groups = collections.defaultdict(list)
    for b in runs:
        traced = bool((b.get("trace") or {}).get("written"))
        groups[(b["fixed_batch"], b["threads"], b["chains"], float(b["rate"]), b["policy"], traced)].append(b)
    pts = {}
    for k, bs in groups.items():
        N, T, R, rate, pol, traced = k
        chains = [pc for b in bs for pc in b["per_chain"]]
        u = [b["utilization"]["max_worker"] for b in bs if (b.get("utilization") or {}).get("max_worker") is not None]
        e = [b["edf"] for b in bs if b.get("edf")]
        per_block = {}
        for role in PRE_GATE:
            per_block[role] = pooled([pc.get("per_block_done", {}).get(role) for pc in chains])
        pts[k] = {"N": N, "T": T, "R": R, "rate": rate, "policy": pol, "traced": traced, "repeats": len(bs), "period_us": N / rate * 1e6,
                  "saturated": any(saturated(b) for b in bs), "demand": float(np.mean(u)) if u else None,
                  "batch": pooled([pc.get("batch_rt") for pc in chains]), "frame": pooled([pc.get("frame_rt") for pc in chains]),
                  "frame_batch": pooled([pc.get("frame_batch_rt") for pc in chains]), "per_block": per_block,
                  "edf_rel": sum(x.get("releases_pipeline", 0) for x in e), "edf_mis": sum(x.get("misses_pipeline", 0) for x in e),
                  "lost": sum(((b.get("trace_header") or {}).get("lost_count", 0) > 0) for b in bs)}
    return pts


def style(ax, title=None, xlabel=None, ylabel=None):
    if title:
        ax.set_title(title, loc="left", color=INK)
    if xlabel:
        ax.set_xlabel(xlabel, color=INK2)
    if ylabel:
        ax.set_ylabel(ylabel, color=INK2)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2)
    ax.grid(True, axis="y", color=GRID, linewidth=0.7)
    ax.set_axisbelow(True)


def range_marks(ax, x, st, color, label=None, width=0.22):
    """one range mark: min-max whisker (thin), +-1 sd bar (thick), mean dot."""
    ax.vlines(x, st["min"], st["max"], color=color, linewidth=0.9, alpha=0.75)
    lo, hi = st["mean"] - st["std"], st["mean"] + st["std"]
    ax.vlines(x, max(lo, st["min"]), min(hi, st["max"]), color=color, linewidth=4.5, alpha=0.9)
    ax.plot([x], [st["mean"]], "o", color=color, markersize=5.5, markeredgecolor="white", markeredgewidth=1.0, label=label, zorder=5)


def config_label(p):
    return f"{p['T']}w · {p['R']}rx · {p['rate']/1e6:g} Msps"


def save(fig, out, name):
    fig.savefig(os.path.join(out, name + ".pdf"), facecolor="white")
    fig.savefig(os.path.join(out, name + ".png"), dpi=160, facecolor="white")
    plt.close(fig)
    print("wrote", name)


def per_config_figure(pts, N, key, out, name, title, ylabel, only_unsat=True, logy=False, note=None):
    cfgs = sorted({(p["T"], p["R"], p["rate"]) for p in pts.values() if p["N"] == N and p["traced"]}, key=lambda c: (c[0], c[1], c[2]))
    if only_unsat:
        cfgs = [c for c in cfgs if any(not pts[(N, c[0], c[1], c[2], pol, True)]["saturated"] for pol in POL if (N, c[0], c[1], c[2], pol, True) in pts)]
    if not cfgs:
        return []
    fig, ax = plt.subplots(figsize=(max(6.5, 0.9 * len(cfgs) + 2.5), 4.0))
    shown = []
    labelled = set()
    for i, c in enumerate(cfgs):
        for j, pol in enumerate(POL):
            k = (N, c[0], c[1], c[2], pol, True)
            p = pts.get(k)
            if not p or not p[key] or (only_unsat and p["saturated"]):
                continue
            range_marks(ax, i + (j - 1) * 0.26, p[key], COL[pol], None if pol in labelled else NAME[pol])
            labelled.add(pol)
            shown.append((c, pol, p[key], p["saturated"]))
    ax.set_xticks(range(len(cfgs)))
    ax.set_xticklabels([f"{c[0]}w · {c[1]}rx\n{c[2]/1e6:g} Msps" for c in cfgs], fontsize=8)
    if logy:
        ax.set_yscale("log")
    style(ax, title, "configuration (workers · receivers · sample rate)", ylabel)
    ax.legend(loc="upper left", frameon=False, ncol=3)
    foot = "dot = mean, thick bar = ±1 sample std-dev, thin whisker = min–max, over all batches of all chains and repeats"
    if note:
        foot += "; " + note
    fig.text(0.01, 0.005, foot, fontsize=7, color=INK2)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    save(fig, out, name)
    return shown


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sweep")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--gr3", nargs="*", default=[])
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    pts = load(a.sweep)
    Ns = sorted({p["N"] for p in pts.values()})
    numbers = {"points": [], "figures": {}}

    # 1. per-configuration batch RT, unsaturated, per N
    for N in Ns:
        rates = sorted({p["rate"] for p in pts.values() if p["N"] == N})
        per_txt = ", ".join(f"{N / r * 1e6:.0f} µs at {r/1e6:g} Msps" for r in rates)
        shown = per_config_figure(pts, N, "batch", a.out_dir, f"batch_rt_N{N}", f"Batch response time, N = {N} (one batch period = {per_txt})", "µs",
                                  note="configurations where the graph did not keep real time are left out (see batch_rt_saturated)")
        numbers["figures"][f"batch_rt_N{N}"] = [{"cfg": config_label({"T": c[0], "R": c[1], "rate": c[2]}), "policy": pol, **st} for c, pol, st, _ in shown]

    # 2. saturated configurations, log axis
    sat = [(k, p) for k, p in pts.items() if p["traced"] and p["saturated"] and p["batch"]]
    if sat:
        cfgs = sorted({(p["N"], p["T"], p["R"], p["rate"]) for _, p in sat})
        fig, ax = plt.subplots(figsize=(max(6.5, 0.9 * len(cfgs) + 2.5), 4.0))
        labelled = set()
        for i, c in enumerate(cfgs):
            for j, pol in enumerate(POL):
                p = pts.get((c[0], c[1], c[2], c[3], pol, True))
                if p and p["saturated"] and p["batch"]:
                    range_marks(ax, i + (j - 1) * 0.26, p["batch"], COL[pol], None if pol in labelled else NAME[pol])
                    labelled.add(pol)
        ax.set_xticks(range(len(cfgs)))
        ax.set_xticklabels([f"N={c[0]}\n{c[1]}w·{c[2]}rx\n{c[3]/1e6:g} Msps" for c in cfgs], fontsize=7)
        ax.set_yscale("log")
        style(ax, "Batch response time where the graph did not keep real time (unbounded regime)", "configuration", "µs (log)")
        ax.legend(loc="upper left", frameon=False, ncol=3)
        fig.text(0.01, 0.005, "dot = mean, thick bar = ±1 std-dev, thin whisker = min–max; these populations grow with the run length and are not response times of a schedulable system", fontsize=7, color=INK2)
        fig.tight_layout(rect=(0, 0.03, 1, 1))
        save(fig, a.out_dir, "batch_rt_saturated")

    # 3. batch RT against demand: panel per (N, workers)
    Ts = sorted({p["T"] for p in pts.values()})
    fig, axes = plt.subplots(len(Ns), len(Ts), figsize=(4.2 * len(Ts), 3.1 * len(Ns)), squeeze=False, sharex=False)
    for r, N in enumerate(Ns):
        for c, T in enumerate(Ts):
            ax = axes[r][c]
            for pol in POL:
                ps = sorted([p for k, p in pts.items() if p["N"] == N and p["T"] == T and p["policy"] == pol and p["traced"] and not p["saturated"] and p["batch"] and p["demand"] is not None], key=lambda p: p["demand"])
                if not ps:
                    continue
                x = np.array([p["demand"] for p in ps]); m = np.array([p["batch"]["mean"] for p in ps]); s = np.array([p["batch"]["std"] for p in ps])
                lo = np.array([p["batch"]["min"] for p in ps]); hi = np.array([p["batch"]["max"] for p in ps])
                ax.fill_between(x, np.maximum(m - s, lo), np.minimum(m + s, hi), color=COL[pol], alpha=0.15, linewidth=0)
                ax.vlines(x, lo, hi, color=COL[pol], linewidth=0.8, alpha=0.6)
                ax.plot(x, m, "-o", color=COL[pol], linewidth=1.8, markersize=4.5, markeredgecolor="white", markeredgewidth=0.8, label=NAME[pol])
            per = next((p["period_us"] for p in pts.values() if p["N"] == N), None)
            style(ax, f"N = {N}, {T} worker{'s' if T > 1 else ''}", "measured demand of the busiest worker" if r == len(Ns) - 1 else None, "µs" if c == 0 else None)
            ax.set_yscale("log")
            if r == 0 and c == 0:
                ax.legend(loc="upper left", frameon=False, fontsize=8)
    fig.suptitle("Batch response time against measured demand (unsaturated configurations; log axis)", x=0.01, ha="left", fontsize=11, color=INK)
    fig.text(0.01, 0.005, "line = mean, band = ±1 std-dev, whiskers = min–max; each point pools all batches of all chains and repeats", fontsize=7, color=INK2)
    fig.tight_layout(rect=(0, 0.02, 1, 0.96))
    save(fig, a.out_dir, "batch_rt_vs_demand")

    # 4. frame RT per configuration, all points, log
    for N in Ns:
        per_config_figure(pts, N, "frame", a.out_dir, f"frame_rt_N{N}", f"Frame response time (last sample released → decoded packet), N = {N}", "µs (log)", only_unsat=False, logy=True,
                          note="all configurations; saturated ones show the depth of the buffers, not a latency")

    # 5. frame RT vs GR3 by receivers (pooled over N per policy)
    g3 = []
    for rd in a.gr3:
        try:
            summ = json.load(open(os.path.join(rd, "latency_summary.json")))
            lat = list(csv.DictReader(open(os.path.join(rd, "latency.csv"))))
        except FileNotFoundError:
            continue
        v = np.array([float(r["lat_last_us"]) for r in lat if r.get("decoded") == "1" and r.get("lat_last_us")])
        if v.size:
            g3.append({"R": summ["chains"], "rate": summ["latency"]["rate"], "n": int(v.size), "mean": float(v.mean()), "std": float(v.std(ddof=1)), "min": float(v.min()), "max": float(v.max())})
    if g3:
        fig, ax = plt.subplots(figsize=(6.5, 4.0))
        Rs = sorted({p["R"] for p in pts.values()} | {g["R"] for g in g3})
        for j, pol in enumerate(POL + ["gr3"]):
            for i, R in enumerate(Rs):
                if pol == "gr3":
                    st = pooled([{"n": g["n"], "mean_us": g["mean"], "std_us": g["std"], "min_us": g["min"], "max_us": g["max"]} for g in g3 if g["R"] == R])
                else:
                    st = pooled([{"n": p["frame"]["n"], "mean_us": p["frame"]["mean"], "std_us": p["frame"]["std"], "min_us": p["frame"]["min"], "max_us": p["frame"]["max"]}
                                 for p in pts.values() if p["policy"] == pol and p["traced"] and p["R"] == R and not p["saturated"] and p["frame"]])
                if st:
                    range_marks(ax, i + (j - 1.5) * 0.2, st, COL[pol], NAME[pol] if i == 0 else None)
        ax.set_xticks(range(len(Rs))); ax.set_xticklabels([str(R) for R in Rs])
        ax.set_yscale("log")
        style(ax, "Frame response time by receiver count: GR3 (10 Msps) against the GR4 policies\n(unsaturated GR4 points, all N and rates pooled)", "receivers", "µs (log)")
        ax.legend(loc="upper left", frameon=False, ncol=4)
        fig.tight_layout()
        save(fig, a.out_dir, "frame_rt_vs_gr3")
        numbers["gr3"] = g3

    # 6. EDF miss ratio
    e = sorted([p for p in pts.values() if p["policy"] == "edf" and p["traced"] and p["edf_rel"]], key=lambda p: (p["N"], p["T"], p["R"], p["rate"]))
    if e:
        fig, ax = plt.subplots(figsize=(max(6.5, 0.45 * len(e) + 2), 3.6))
        x = np.arange(len(e)); y = np.array([p["edf_mis"] / p["edf_rel"] for p in e])
        ax.bar(x, np.maximum(y, 1e-7), color=[COL["edf"] if not p["saturated"] else "#c3c2b7" for p in e], width=0.7)
        ax.set_yscale("log"); ax.set_ylim(1e-7, 1)
        ax.set_xticks(x); ax.set_xticklabels([f"N{p['N']}\n{p['T']}w·{p['R']}rx\n{p['rate']/1e6:g}M" for p in e], fontsize=6.5)
        style(ax, "EDF: pipeline deadline-miss ratio per configuration (grey = saturated; floor of the axis = no miss)", "configuration", "misses / releases (log)")
        fig.tight_layout()
        save(fig, a.out_dir, "edf_misses")
        numbers["edf"] = [{"cfg": f"N{p['N']} {config_label(p)}", "releases": p["edf_rel"], "misses": p["edf_mis"], "saturated": p["saturated"]} for p in e]

    # 7. pipeline build-up at the one-receiver, one-worker, 5 Msps point
    for N in Ns:
        ref = [(pol, pts.get((N, 1, 1, 5e6, pol, True))) for pol in POL]
        ref = [(pol, p) for pol, p in ref if p and all(p["per_block"].get(r) for r in PRE_GATE)]
        if not ref:
            continue
        fig, ax = plt.subplots(figsize=(8.0, 4.0))
        for i, role in enumerate(PRE_GATE):
            for j, (pol, p) in enumerate(ref):
                range_marks(ax, i + (j - 1) * 0.26, p["per_block"][role], COL[pol], NAME[pol] if i == 0 else None)
        ax.set_xticks(range(len(PRE_GATE))); ax.set_xticklabels(PRE_GATE)
        ax.set_yscale("log")
        style(ax, f"Where a batch's time goes: completion of each pre-gate block after the batch's nominal arrival, N = {N}, 1 receiver, 1 worker, 5 Msps", "block (in dataflow order; sync_short is the gate)", "µs after arrival (log)")
        ax.legend(loc="upper left", frameon=False, ncol=3)
        fig.tight_layout()
        save(fig, a.out_dir, f"pipeline_N{N}")
        numbers["figures"][f"pipeline_N{N}"] = {pol: {r: p["per_block"][r] for r in PRE_GATE} for pol, p in ref}

    # numbers
    for k, p in sorted(pts.items()):
        numbers["points"].append({"N": p["N"], "workers": p["T"], "receivers": p["R"], "rate": p["rate"], "policy": p["policy"], "traced": p["traced"], "repeats": p["repeats"],
                                  "saturated": p["saturated"], "demand": p["demand"], "batch": p["batch"], "frame": p["frame"], "frame_batch": p["frame_batch"],
                                  "edf_releases": p["edf_rel"], "edf_misses": p["edf_mis"], "runs_with_lost_records": p["lost"]})
    json.dump(numbers, open(os.path.join(a.out_dir, "numbers.json"), "w"), indent=1)
    with open(os.path.join(a.out_dir, "numbers.md"), "w") as f:
        f.write("| N | workers | rx | Msps | policy | traced | saturated | demand | batch mean | std | min | max | frame mean | std | min | max | EDF misses/releases |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
        for p in numbers["points"]:
            b = p["batch"] or {}; fr = p["frame"] or {}
            g = lambda d, k: f"{d[k]:.0f}" if d and k in d else "-"
            f.write(f"| {p['N']} | {p['workers']} | {p['receivers']} | {p['rate']/1e6:g} | {p['policy']} | {'y' if p['traced'] else 'n'} | {'y' if p['saturated'] else 'n'} | {p['demand']:.2f} | {g(b,'mean')} | {g(b,'std')} | {g(b,'min')} | {g(b,'max')} | {g(fr,'mean')} | {g(fr,'std')} | {g(fr,'min')} | {g(fr,'max')} | {p['edf_misses']}/{p['edf_releases'] if p['policy']=='edf' else ''} |\n" if p["demand"] is not None else "")
    print("wrote numbers.json, numbers.md")


if __name__ == "__main__":
    main()
