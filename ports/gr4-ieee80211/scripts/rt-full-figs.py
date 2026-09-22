#!/usr/bin/env python3
"""rt-full-figs.py -- box plots, tables and numbers of a full rt-prelim4 sweep (repeats).

    ./scripts/rt-full-figs.py RUN_DIR --out-dir DIR [--fence 3] [--stall-ms 2]

Reads every <workload>_<policy>_rep<k>/ (or <workload>_<policy>/) under RUN_DIR: batch_rt.json for the
receivers' rates and periods, batch_rt.csv for every batch (release time, response time).  Batches of
all repeats are pooled per cell (workload, policy, receiver).

Outlier filter (reported, never silent): a batch is a *stall* when its response time exceeds
STALL_FACTOR x the median of its cell AND every other receiver of the same run also has such a batch
released within STALL_MS ms of it -- the whole process stopped, which is the host (a descheduled
VM, an interrupt), not the policy.  Stalls are excluded from the box statistics; the plots draw the
excluded maximum as a small cross so the reader sees what was cut, and TABLE.md gives the excluded
share and the raw maximum.  A one-receiver run has no second receiver to coincide with: there a batch
above SOLO_FACTOR x the median is excluded instead (expected: none).  A policy effect that hits one
receiver only -- rate monotonic starving a slow receiver -- is never a stall and is never cut.

Figures (PDF + PNG; box = 25th-75th percentile, line = median, dot = mean, whiskers = min-max after the
stall filter, x = maximum before it):
  full_simple            the simple workloads
  full_mix_<mix>         one multi-rate mix per figure; full_mixes all three side by side
  full_late              per mix and receiver, the share of batches later than the receiver's own period:
                         mean over repeats, error bar = +-1 sd over repeats (raw, unfiltered)
  full_fast_slow         the last (5 Msps) receiver of each mix; the slow receivers at the knee
  full_repeats           per-repeat means of the last receiver: run-to-run variation
  numbers.json, TABLE.md
"""
import argparse, collections, glob, json, os, re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

POL = ["rr", "edf", "rm"]; COL = {"rr": "#2a78d6", "edf": "#eb6834", "rm": "#1baf7a"}; NAME = {"rr": "RR", "edf": "EDF", "rm": "RM"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
plt.rcParams.update({"font.size": 8.5, "axes.titlesize": 9.5, "pdf.fonttype": 42, "ps.fonttype": 42, "font.family": "DejaVu Sans"})
SIMPLE = ["simple1", "simple2"]; MIXES = ["light", "mid", "knee"]
TITLE = {"simple1": "one receiver, one worker, 2.5 Msps", "simple2": "two equal receivers, two workers, 2.5 Msps each"}


def style(ax, title=None, xlabel=None, ylabel=None):
    if title: ax.set_title(title, loc="left", color=INK)
    if xlabel: ax.set_xlabel(xlabel, color=INK2)
    if ylabel: ax.set_ylabel(ylabel, color=INK2)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    for s in ("left", "bottom"): ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8); ax.grid(True, axis="y", color=GRID, linewidth=0.6); ax.set_axisbelow(True)


def save(fig, out, name):
    fig.savefig(os.path.join(out, name + ".pdf"), facecolor="white"); fig.savefig(os.path.join(out, name + ".png"), dpi=170, facecolor="white"); plt.close(fig); print("wrote", name)


def load(run_dir):
    """-> meta[(w, pol)] = {chains: [{chain, rate, period_us}], reps, windows}, data[(w, pol)] = DataFrame(rep, chain, release_ns, rt)"""
    meta, frames = {}, collections.defaultdict(list)
    for d in sorted(glob.glob(os.path.join(run_dir, "*_*"))):
        m = re.match(r"(simple1|simple2|light|mid|knee)_(rr|edf|rm)(?:_rep(\d+))?$", os.path.basename(d))
        if not m or not os.path.exists(os.path.join(d, "batch_rt.json")): continue
        w, pol, rep = m.group(1), m.group(2), int(m.group(3) or 0)
        b = json.load(open(os.path.join(d, "batch_rt.json")))
        mt = meta.setdefault((w, pol), {"chains": [{"chain": pc["chain"], "rate": pc.get("rate"), "period_us": pc.get("batch_period_us")} for pc in b["per_chain"]], "reps": 0, "windows": [], "saturated": []})
        mt["reps"] += 1; mt["windows"].append(b.get("window_covered_s")); mt["saturated"].append(bool(b.get("saturated")))
        df = pd.read_csv(os.path.join(d, "batch_rt.csv"), usecols=["chain", "release_ns", "batch_rt_us"]); df["rep"] = rep
        frames[(w, pol)].append(df)
    data = {k: pd.concat(v, ignore_index=True) for k, v in frames.items()}
    return meta, data


def flag_stalls(df, factor, solo_factor, stall_ns):
    """df: one (workload, policy) frame with every receiver and repeat -> boolean Series 'stall'"""
    med = df.groupby("chain").batch_rt_us.median()
    hot = df.batch_rt_us > df.chain.map(med) * factor
    chains = sorted(df.chain.unique())
    if len(chains) == 1:
        return df.batch_rt_us > med.iloc[0] * solo_factor
    stall = pd.Series(False, index=df.index)
    for rep, g in df[hot].groupby("rep"):
        times = {c: np.sort(g[g.chain == c].release_ns.to_numpy()) for c in chains}
        for c in chains:
            mine = g[g.chain == c]
            if not len(mine): continue
            ok = np.ones(len(mine), bool)
            for o in chains:
                if o == c: continue
                t = times[o]
                if not len(t): ok[:] = False; break
                x = mine.release_ns.to_numpy(); i = np.searchsorted(t, x)
                near = np.minimum(np.abs(t[np.clip(i, 0, len(t) - 1)] - x), np.abs(t[np.clip(i - 1, 0, len(t) - 1)] - x))
                ok &= near <= stall_ns
            stall.loc[mine.index[ok]] = True
    return stall


def cell_stats(df, period_us):
    """df: one cell's batches (all reps) with a 'stall' column"""
    rt = df.batch_rt_us.to_numpy(); keep = ~df.stall.to_numpy(); f = rt[keep]
    fd = df[keep]
    return {"n": int(len(rt)), "excluded": int((~keep).sum()), "excluded_frac": float((~keep).mean()),
            "min": float(f.min()), "q25": float(np.percentile(f, 25)), "median": float(np.median(f)), "mean": float(f.mean()), "std": float(f.std()), "q75": float(np.percentile(f, 75)), "max": float(f.max()),
            "raw_max": float(rt.max()), "raw_mean": float(rt.mean()), "late": float((rt > period_us).mean()), "late_filtered": float((f > period_us).mean()),
            "per_rep_mean": {int(r): float(g.batch_rt_us.mean()) for r, g in fd.groupby("rep")}, "per_rep_late": {int(r): float((g.batch_rt_us > period_us).mean()) for r, g in df.groupby("rep")},
            "stall_episodes": int(df[~keep].groupby("rep").release_ns.apply(lambda t: int((np.diff(np.sort(t.to_numpy())) > 5e6).sum()) + 1).sum()) if (~keep).any() else 0}


def box(ax, x, st, color, width=0.2, label=None):
    ax.vlines(x, st["min"], st["max"], color=color, linewidth=0.8, alpha=0.8, zorder=3)
    ax.add_patch(Rectangle((x - width / 2, st["q25"]), width, st["q75"] - st["q25"], facecolor=color, edgecolor="white", linewidth=0.6, alpha=0.9, zorder=4))
    ax.hlines(st["median"], x - width / 2, x + width / 2, color="white", linewidth=1.1, zorder=5)
    ax.plot([x], [st["mean"]], "o", color=color, markeredgecolor="white", markersize=4.5, markeredgewidth=0.9, zorder=6, label=label)
    if st["raw_max"] > st["max"] * 1.02:
        ax.plot([x], [st["raw_max"]], "x", color=color, markersize=4, markeredgewidth=0.8, alpha=0.7, zorder=3)


FOOT = "box = 25th–75th percentile, line = median, dot = mean, whiskers = min–max without whole-process stalls (all receivers > {f}× their median within {ms} ms), × = max with them;\ndotted = the receiver's batch period (its deadline); batches of all repeats pooled"


def fit_y(axes, stats):
    lo = min(st["min"] for st in stats); hi = max(st["raw_max"] for st in stats)
    for ax in np.atleast_1d(axes): ax.set_ylim(lo * 0.8, hi * 1.35)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("run_dir"); ap.add_argument("--out-dir", required=True)
    ap.add_argument("--stall-factor", type=float, default=5.0, help="a batch is 'hot' above this multiple of its cell's median")
    ap.add_argument("--stall-ms", type=float, default=2.0, help="hot batches of every receiver of a run within this many ms of each other = a stall")
    ap.add_argument("--solo-factor", type=float, default=10.0, help="one-receiver runs: exclude above this multiple of the median")
    a = ap.parse_args(); os.makedirs(a.out_dir, exist_ok=True)
    meta, data = load(a.run_dir)
    S = {}  # (w, pol, chain) -> stats
    for (w, pol), df in data.items():
        df["stall"] = flag_stalls(df, a.stall_factor, a.solo_factor, a.stall_ms * 1e6)
        for pc in meta[(w, pol)]["chains"]:
            S[(w, pol, pc["chain"])] = cell_stats(df[df.chain == pc["chain"]], pc["period_us"])
    foot = FOOT.format(f=f"{a.stall_factor:g}", ms=f"{a.stall_ms:g}")
    reps = {k: v["reps"] for k, v in meta.items()}

    def chains_of(w):
        return sorted({pc["chain"] for p in POL if (w, p) in meta for pc in meta[(w, p)]["chains"]})

    def ref(w):
        return next((meta[(w, p)]["chains"] for p in POL if (w, p) in meta), [])

    # --- simple
    if any((w, p) in meta for w in SIMPLE for p in POL):
        fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.3), gridspec_kw={"width_ratios": [1, 1.6]})
        for ax, w in zip(axes, SIMPLE):
            ch = chains_of(w)
            for i, c in enumerate(ch):
                for j, pol in enumerate(POL):
                    if (w, pol, c) in S: box(ax, i + (j - 1) * 0.26, S[(w, pol, c)], COL[pol], label=NAME[pol] if i == 0 else None)
            per = ref(w)[0]["period_us"] if ref(w) else None
            if per: ax.hlines(per, -0.45, len(ch) - 0.55, color=INK2, linewidth=0.7, linestyles="dotted")
            ax.set_xticks(range(len(ch))); ax.set_xticklabels([f"rx {c}" for c in ch]); ax.set_yscale("log")
            style(ax, TITLE[w], None, "batch response time, µs (log)" if w == SIMPLE[0] else None)
            if w == SIMPLE[0]: ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=7.5, handletextpad=0.2, columnspacing=0.8)
        fit_y(axes, [S[k] for k in S if k[0] in SIMPLE])
        fig.text(0.01, 0.004, foot, fontsize=5.8, color=INK2); fig.tight_layout(rect=(0, 0.06, 1, 1)); save(fig, a.out_dir, "full_simple")

    # --- mixes, one per figure and all three side by side
    def draw_mix(ax, mix, ylabel=True, legend=True, short=False):
        ch = chains_of(mix); r = ref(mix); lab = set()
        for i, c in enumerate(ch):
            for j, pol in enumerate(POL):
                if (mix, pol, c) in S:
                    box(ax, i + (j - 1) * 0.26, S[(mix, pol, c)], COL[pol], label=None if pol in lab else NAME[pol]); lab.add(pol)
            ax.hlines(r[c]["period_us"], i - 0.42, i + 0.42, color=INK2, linewidth=0.7, linestyles="dotted")
        ax.set_xticks(range(len(ch))); ax.set_xticklabels([(f"rx {c}\n{r[c]['rate']/1e6:g} M\n{r[c]['period_us']:.0f} µs" if short else f"rx {c}: {r[c]['rate']/1e6:g} Msps\n{r[c]['period_us']:.0f} µs period") for c in ch], fontsize=7 if short else 7.5); ax.set_yscale("log")
        fit_y(ax, [S[k] for k in S if k[0] == mix])
        style(ax, f"mix '{mix}'", None, "batch response time, µs (log)" if ylabel else None)
        if legend: ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=7.5, handletextpad=0.2, columnspacing=0.8)
    have = [m for m in MIXES if any((m, p) in meta for p in POL)]
    for mix in have:
        fig, ax = plt.subplots(figsize=(7.2, 3.4)); draw_mix(ax, mix)
        ax.set_title(f"Batch response time, mix '{mix}', 4 workers, N = 1024, receivers in graph order (fastest last)", loc="left", color=INK)
        fig.text(0.01, 0.004, foot, fontsize=5.8, color=INK2); fig.tight_layout(rect=(0, 0.06, 1, 1)); save(fig, a.out_dir, f"full_mix_{mix}")
    if have:
        fig, axes = plt.subplots(1, len(have), figsize=(7.2, 3.1), sharey=True)
        for k, (ax, mix) in enumerate(zip(np.atleast_1d(axes), have)): draw_mix(ax, mix, ylabel=(k == 0), legend=(k == 0), short=True)
        fit_y(axes, [S[k] for k in S if k[0] in MIXES])
        fig.text(0.01, 0.004, foot, fontsize=5.8, color=INK2); fig.tight_layout(rect=(0, 0.06, 1, 1)); save(fig, a.out_dir, "full_mixes")

    # --- late share with repeat error bars
    if have:
        fig, axes = plt.subplots(1, len(have), figsize=(7.2, 2.9), sharey=True)
        for k, (ax, mix) in enumerate(zip(np.atleast_1d(axes), have)):
            ch = chains_of(mix); r = ref(mix)
            for i, c in enumerate(ch):
                for j, pol in enumerate(POL):
                    st = S.get((mix, pol, c))
                    if not st: continue
                    v = np.array(list(st["per_rep_late"].values())); m_, sd = v.mean(), v.std()
                    ax.bar(i + (j - 1) * 0.27, max(m_, 1e-5), width=0.25, color=COL[pol], label=NAME[pol] if (i == 0 and k == 0) else None, zorder=3)
                    if m_ > 1e-5: ax.errorbar(i + (j - 1) * 0.27, m_, yerr=[[min(sd, m_ - 1e-5)], [sd]], color=INK, elinewidth=0.7, capsize=2, zorder=4)
            ax.set_xticks(range(len(ch))); ax.set_xticklabels([f"rx {c}\n{r[c]['rate']/1e6:g} M" for c in ch], fontsize=7.5)
            ax.set_yscale("log"); ax.set_ylim(1e-5, 1.5); style(ax, f"mix '{mix}'", None, "share of batches later than own period (log)" if k == 0 else None)
            if k == 0: ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=7.5, handletextpad=0.2, columnspacing=0.8)
        fig.text(0.01, 0.004, "bar = mean over repeats of the per-repeat share, error bar = ±1 sd over repeats; unfiltered", fontsize=6, color=INK2)
        fig.tight_layout(rect=(0, 0.04, 1, 1)); save(fig, a.out_dir, "full_late")

    # --- fastest receiver per mix; slow receivers at the knee
    if have:
        fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.2), gridspec_kw={"width_ratios": [1.4, 1]})
        ax = axes[0]
        for i, mix in enumerate(have):
            r = ref(mix); fast = max(r, key=lambda x: (x["rate"] or 0, x["chain"]))["chain"]
            for j, pol in enumerate(POL):
                if (mix, pol, fast) in S: box(ax, i + (j - 1) * 0.26, S[(mix, pol, fast)], COL[pol], label=NAME[pol] if i == 0 else None)
            ax.hlines(r[fast]["period_us"], i - 0.42, i + 0.42, color=INK2, linewidth=0.7, linestyles="dotted")
        ax.set_xticks(range(len(have))); ax.set_xticklabels([f"mix '{m}'\nrx {max(ref(m), key=lambda x: (x['rate'] or 0, x['chain']))['chain']}, 5 Msps" for m in have], fontsize=7.5); ax.set_yscale("log")
        style(ax, "the last receiver of each mix, 5 Msps (205 µs period)", None, "batch response time, µs (log)"); ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=7.5, handletextpad=0.2, columnspacing=0.8)
        ax = axes[1]
        if "knee" in have:
            r = ref("knee"); slow = [pc["chain"] for pc in r if pc["rate"] == min(x["rate"] for x in r)]
            for i, c in enumerate(slow):
                for j, pol in enumerate(POL):
                    if ("knee", pol, c) in S: box(ax, i + (j - 1) * 0.26, S[("knee", pol, c)], COL[pol])
                ax.hlines(r[c]["period_us"], i - 0.42, i + 0.42, color=INK2, linewidth=0.7, linestyles="dotted")
            ax.set_xticks(range(len(slow))); ax.set_xticklabels([f"rx {c}: {r[c]['rate']/1e6:g} Msps" for c in slow], fontsize=7.5); ax.set_yscale("log")
        style(ax, "slow receivers, mix 'knee' (410 µs)")
        fit_y(axes, [S[k] for k in S if k[0] in MIXES])
        fig.text(0.01, 0.004, foot, fontsize=5.8, color=INK2); fig.tight_layout(rect=(0, 0.06, 1, 1)); save(fig, a.out_dir, "full_fast_slow")

    # --- per-repeat means of the fastest receiver
    if have:
        fig, ax = plt.subplots(figsize=(7.2, 2.8))
        for i, mix in enumerate(have):
            r = ref(mix); fast = max(r, key=lambda x: (x["rate"] or 0, x["chain"]))["chain"]
            for j, pol in enumerate(POL):
                st = S.get((mix, pol, fast))
                if not st: continue
                v = np.array(list(st["per_rep_mean"].values())); x = i + (j - 1) * 0.26
                ax.plot(x + (np.random.RandomState(1).uniform(-0.06, 0.06, len(v))), v, "o", color=COL[pol], markersize=3.2, alpha=0.75, markeredgecolor="white", markeredgewidth=0.4, label=NAME[pol] if i == 0 else None, zorder=4)
                ax.hlines(v.mean(), x - 0.1, x + 0.1, color=COL[pol], linewidth=1.4, zorder=5)
            ax.hlines(r[fast]["period_us"], i - 0.42, i + 0.42, color=INK2, linewidth=0.7, linestyles="dotted")
        ax.set_xticks(range(len(have))); ax.set_xticklabels([f"mix '{m}', rx {max(ref(m), key=lambda x: (x['rate'] or 0, x['chain']))['chain']}" for m in have])
        style(ax, "the last (5 Msps) receiver: mean batch response time of each repeat (dots) and their mean (bar)", None, "µs"); ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=7.5, handletextpad=0.2, columnspacing=0.8)
        fig.tight_layout(); save(fig, a.out_dir, "full_repeats")

    # --- numbers and table
    numbers = {"stall_factor": a.stall_factor, "stall_ms": a.stall_ms, "solo_factor": a.solo_factor, "reps": {f"{w}|{p}": v for (w, p), v in reps.items()},
               "windows_s": {f"{w}|{p}": meta[(w, p)]["windows"] for (w, p) in meta}, "saturated_runs": {f"{w}|{p}": int(sum(meta[(w, p)]["saturated"])) for (w, p) in meta},
               "cells": {f"{w}|{p}|{c}": dict(st, rate=next(x["rate"] for x in meta[(w, p)]["chains"] if x["chain"] == c), period_us=next(x["period_us"] for x in meta[(w, p)]["chains"] if x["chain"] == c)) for (w, p, c), st in S.items()}}
    json.dump(numbers, open(os.path.join(a.out_dir, "numbers.json"), "w"), indent=1)
    md = ["Batch response time per cell, batches of all repeats pooled, whole-process stalls excluded: mean / median / 75th percentile / max µs, and the share of batches later than the receiver's own period (unfiltered, mean ± sd over repeats).\n\n",
          "| workload | receiver (Msps, period µs) | RR | EDF | RM |\n|---|---|---|---|---|\n"]
    for w in SIMPLE + MIXES:
        r = ref(w)
        for pc in r:
            cells = []
            for pol in POL:
                st = S.get((w, pol, pc["chain"]))
                if st:
                    v = np.array(list(st["per_rep_late"].values()))
                    cells.append(f"{st['mean']:.0f} / {st['median']:.0f} / {st['q75']:.0f} / {st['max']:.0f} ({100 * v.mean():.1f} ± {100 * v.std():.1f} %)")
                else: cells.append("-")
            md.append(f"| {w} | rx {pc['chain']} ({pc['rate']/1e6:g}, {pc['period_us']:.0f}) | " + " | ".join(cells) + " |\n")
    md.append("\nThe stall filter (a batch above %g× its cell's median while every other receiver of the run has one within %g ms; one-receiver runs: above %g× the median): batches excluded per cell, their share, the number of stall episodes (gaps > 5 ms apart), and the maximum before and after.\n\n| workload | receiver | RR excluded (share, episodes, raw max → max) | EDF | RM |\n|---|---|---|---|---|\n" % (a.stall_factor, a.stall_ms, a.solo_factor))
    for w in SIMPLE + MIXES:
        for pc in ref(w):
            cells = []
            for pol in POL:
                st = S.get((w, pol, pc["chain"]))
                cells.append(f"{st['excluded']} of {st['n']} ({100 * st['excluded_frac']:.3f} %, {st['stall_episodes']}, {st['raw_max']:.0f} → {st['max']:.0f})" if st else "-")
            md.append(f"| {w} | rx {pc['chain']} | " + " | ".join(cells) + " |\n")
    md.append("\nRepeats per cell and the analysis window each repeat retained (s):\n\n| workload | policy | repeats | windows |\n|---|---|---|---|\n")
    for (w, p), v in meta.items():
        md.append(f"| {w} | {p} | {v['reps']} | " + ", ".join(f"{x:.1f}" for x in v["windows"]) + " |\n")
    open(os.path.join(a.out_dir, "TABLE.md"), "w").write("".join(md)); print("".join(md[:6]))


if __name__ == "__main__":
    main()
