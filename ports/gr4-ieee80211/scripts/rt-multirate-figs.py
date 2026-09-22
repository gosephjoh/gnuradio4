#!/usr/bin/env python3
"""rt-multirate-figs.py -- range figures of the multi-rate receivers experiment (four workers).

    ./scripts/rt-multirate-figs.py SWEEP_DIR --out-dir DIR

Runs are tagged mr-<mix>_N<N>_t<T>_r<R>_<policy>_trace_rep<k>.  Each receiver has its own
rate, hence its own batch period and (implicit) deadline.  Figures (PDF + PNG):
  mr_batch_<mix>       batch response time per receiver (x: receiver, its rate and period), policies side by side
  mr_frame_<mix>       the same for the frame response time
  mr_over_period       per mix and receiver, the fraction of batches later than the receiver's own period
  mr_summary           every mix in one row: the fastest receiver's batch RT per policy
  numbers.json         every value
"""
import argparse, collections, glob, json, os, re
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

POL = ["rr", "edf", "rm"]; COL = {"rr": "#2a78d6", "edf": "#eb6834", "rm": "#1baf7a"}; NAME = {"rr": "RR", "edf": "EDF", "rm": "RM"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "pdf.fonttype": 42})


def pooled(gs):
    g = [s for s in gs if s and s.get("n")]
    if not g:
        return None
    n = np.array([s["n"] for s in g], float); m = np.array([s["mean_us"] for s in g]); sd = np.array([s["std_us"] for s in g])
    N = n.sum(); mean = float((n * m).sum() / N)
    var = float((((n - 1) * sd ** 2).sum() + (n * (m - mean) ** 2).sum()) / max(N - 1, 1))
    return {"n": int(N), "mean": mean, "std": var ** 0.5, "min": float(min(s["min_us"] for s in g)), "max": float(max(s["max_us"] for s in g))}


def load(sweep):
    pts = collections.defaultdict(lambda: collections.defaultdict(lambda: {"batch": [], "frame": [], "over": [], "sat": [], "rate": None, "period": None, "edf": []}))
    reps = collections.Counter()
    for p in sorted(glob.glob(os.path.join(sweep, "mr-*", "batch_rt.json"))):
        tag = os.path.basename(os.path.dirname(p))
        m = re.match(r"mr-(\w+)_N(\d+)_t(\d+)_r(\d+)_(\w+)_trace_rep(\d+)", tag)
        if not m:
            continue
        mix, pol = m.group(1), m.group(5)
        b = json.load(open(p))
        reps[(mix, pol)] += 1
        for pc in b["per_chain"]:
            d = pts[(mix, pol)][pc["chain"]]
            d["rate"] = pc.get("rate"); d["period"] = pc.get("batch_period_us")
            d["batch"].append(pc.get("batch_rt")); d["frame"].append(pc.get("frame_rt"))
            d["sat"].append(bool(pc.get("saturated")))
            br = pc.get("batch_rt") or {}
            if br.get("n") and br.get("over_period") is not None:
                d["over"].append((br["over_period"] / br["n"], br["n"]))
            if pc.get("edf"):
                d["edf"].append(pc["edf"])
    out = {}
    for k, chains in pts.items():
        out[k] = {}
        for c, d in chains.items():
            over = sum(r * n for r, n in d["over"]) / sum(n for _, n in d["over"]) if d["over"] else None
            e = {"releases": sum(x["releases"] for x in d["edf"]), "misses": sum(x["misses"] for x in d["edf"])} if d["edf"] else None
            out[k][c] = {"rate": d["rate"], "period": d["period"], "batch": pooled(d["batch"]), "frame": pooled(d["frame"]), "over_period": over, "saturated": any(d["sat"]), "edf": e, "reps": reps[k]}
    return out


def style(ax, title=None, xlabel=None, ylabel=None):
    if title: ax.set_title(title, loc="left", color=INK)
    if xlabel: ax.set_xlabel(xlabel, color=INK2)
    if ylabel: ax.set_ylabel(ylabel, color=INK2)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    for s in ("left", "bottom"): ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2); ax.grid(True, axis="y", color=GRID, linewidth=0.7); ax.set_axisbelow(True)


def mark(ax, x, st, color, label=None):
    ax.vlines(x, st["min"], st["max"], color=color, linewidth=0.9, alpha=0.7)
    ax.vlines(x, max(st["mean"] - st["std"], st["min"]), min(st["mean"] + st["std"], st["max"]), color=color, linewidth=4.5, alpha=0.9)
    ax.plot([x], [st["mean"]], "o", color=color, markeredgecolor="white", markersize=5.5, markeredgewidth=1.0, label=label, zorder=5)


def save(fig, out, name):
    fig.savefig(os.path.join(out, name + ".pdf"), facecolor="white"); fig.savefig(os.path.join(out, name + ".png"), dpi=160, facecolor="white"); plt.close(fig); print("wrote", name)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("sweep"); ap.add_argument("--out-dir", required=True); a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    pts = load(a.sweep)
    mixes = []
    for k in pts:
        if k[0] not in mixes:
            mixes.append(k[0])
    order = {"light": 0, "mid": 1, "knee": 2}
    mixes.sort(key=lambda m: order.get(m, 9))
    numbers = {}
    for mix in mixes:
        chains = sorted({c for pol in POL for c in pts.get((mix, pol), {})})
        for key, fname, title, ylabel in (("batch", "mr_batch", "Batch response time", "µs (log)"), ("frame", "mr_frame", "Frame response time", "µs (log)")):
            fig, ax = plt.subplots(figsize=(max(7, 1.7 * len(chains) + 2.5), 4.2)); lab = set()
            for i, c in enumerate(chains):
                for j, pol in enumerate(POL):
                    d = pts.get((mix, pol), {}).get(c)
                    if not d:
                        continue
                    if d[key] and not d["saturated"]:
                        mark(ax, i + (j - 1) * 0.26, d[key], COL[pol], None if pol in lab else NAME[pol]); lab.add(pol)
                    elif d["saturated"]:
                        ax.text(i + (j - 1) * 0.26, 1.0, "sat.", ha="center", va="bottom", fontsize=7, color=COL[pol], transform=ax.get_xaxis_transform())
                d0 = next((pts[(mix, p)][c] for p in POL if (mix, p) in pts and c in pts[(mix, p)]), None)
                if d0 and d0["period"] and key == "batch":
                    ax.hlines(d0["period"], i - 0.42, i + 0.42, color=INK2, linewidth=0.8, linestyles="dotted")
            ax.set_xticks(range(len(chains)))
            ax.set_xticklabels([f"rx {c}: {pts[(mix, POL[0])][c]['rate']/1e6:g} Msps\n{pts[(mix, POL[0])][c]['period']:.0f} µs period" if (mix, POL[0]) in pts and c in pts[(mix, POL[0])] else f"rx {c}" for c in chains], fontsize=8)
            ax.set_yscale("log")
            style(ax, f"{title}, mix '{mix}', 4 workers, N = 1024, receivers in graph order (rx {chains[-1]} last)", "receiver (its rate and batch period = its deadline)", ylabel)
            ax.legend(loc="upper left", frameon=False, ncol=3)
            fig.text(0.01, 0.005, "dot = mean, bar = ±1 sd, whisker = min–max over all batches of all repeats; dotted = the receiver's batch period (its implicit deadline); 'sat.' = that receiver did not keep real time", fontsize=7, color=INK2)
            fig.tight_layout(rect=(0, 0.03, 1, 1)); save(fig, a.out_dir, f"{fname}_{mix}")
        numbers[mix] = {pol: {str(c): pts[(mix, pol)][c] for c in pts[(mix, pol)]} for pol in POL if (mix, pol) in pts}
    # over-period fraction: one panel per mix
    fig, axes = plt.subplots(1, len(mixes), figsize=(5.2 * len(mixes), 3.8), squeeze=False)
    for ax, mix in zip(axes[0], mixes):
        chains = sorted({c for pol in POL for c in pts.get((mix, pol), {})})
        for i, c in enumerate(chains):
            for j, pol in enumerate(POL):
                d = pts.get((mix, pol), {}).get(c)
                if d and d["over_period"] is not None:
                    ax.bar(i + (j - 1) * 0.27, max(d["over_period"], 1e-5), width=0.25, color=COL[pol], alpha=0.35 if d["saturated"] else 1.0, label=NAME[pol] if i == 0 else None)
        ax.set_xticks(range(len(chains))); ax.set_xticklabels([f"rx {c}\n{pts[(mix, POL[0])][c]['rate']/1e6:g} M" for c in chains], fontsize=8)
        ax.set_yscale("log"); ax.set_ylim(1e-5, 1)
        style(ax, f"mix '{mix}'", "receiver", "fraction of batches later than own period (log)")
        ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=8)
    fig.suptitle("Batches later than the receiver's own batch period (its implicit deadline); faded = the receiver saturated", x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.01, 1, 0.94)); save(fig, a.out_dir, "mr_over_period")
    json.dump(numbers, open(os.path.join(a.out_dir, "numbers.json"), "w"), indent=1)
    print("wrote numbers.json")


if __name__ == "__main__":
    main()
