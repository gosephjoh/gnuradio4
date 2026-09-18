#!/usr/bin/env python3
"""rt-prelim-figs.py -- range figures and tables of the rt-prelim4 matrix.

    ./scripts/rt-prelim-figs.py RUN_DIR --out-dir DIR

Figures (PDF + PNG), each mark = mean (dot), +-1 sd (bar), min-max (whisker) over the batches
inside the analysis window of the one run:
  prelim_simple        the two simple workloads: batch RT per policy (and per receiver for simple2),
  prelim_mix_<mix>     the multi-rate mixes: batch RT per receiver, policies side by side, own period dotted
  prelim_late          per mix and receiver, the fraction of batches later than the receiver's own period
  prelim_fast_slow     left: the fastest receiver of every mix per policy; right: the slow receivers at the knee
  numbers.json, TABLE.md
"""
import argparse, json, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

POL = ["rr", "edf", "rm"]; COL = {"rr": "#2a78d6", "edf": "#eb6834", "rm": "#1baf7a"}; NAME = {"rr": "RR", "edf": "EDF", "rm": "RM"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "pdf.fonttype": 42})
SIMPLE = ["simple1", "simple2"]; MIXES = ["light", "mid", "knee"]


def style(ax, title=None, xlabel=None, ylabel=None):
    if title: ax.set_title(title, loc="left", color=INK)
    if xlabel: ax.set_xlabel(xlabel, color=INK2)
    if ylabel: ax.set_ylabel(ylabel, color=INK2)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    for s in ("left", "bottom"): ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2); ax.grid(True, axis="y", color=GRID, linewidth=0.7); ax.set_axisbelow(True)


def mark(ax, x, st, color, label=None):
    ax.vlines(x, st["min_us"], st["max_us"], color=color, linewidth=0.9, alpha=0.7)
    ax.vlines(x, max(st["mean_us"] - st["std_us"], st["min_us"]), min(st["mean_us"] + st["std_us"], st["max_us"]), color=color, linewidth=4.5, alpha=0.9)
    ax.plot([x], [st["mean_us"]], "o", color=color, markeredgecolor="white", markersize=5.5, markeredgewidth=1.0, label=label, zorder=5)


def save(fig, out, name):
    fig.savefig(os.path.join(out, name + ".pdf"), facecolor="white"); fig.savefig(os.path.join(out, name + ".png"), dpi=160, facecolor="white"); plt.close(fig); print("wrote", name)


def load(run_dir):
    runs = {}
    for w in SIMPLE + MIXES:
        for pol in POL:
            p = os.path.join(run_dir, f"{w}_{pol}", "batch_rt.json")
            if os.path.exists(p):
                b = json.load(open(p))
                u = b.get("utilization", {})
                runs[(w, pol)] = {"window_covered_s": b.get("window_covered_s"), "saturated": b.get("saturated"), "realtime_ratio": b.get("realtime_ratio"),
                                  "overhead": u.get("total_overhead"), "probe": u.get("total_probe"), "demand": u.get("total_cores_busy"), "demand_max": u.get("max_worker"), "occupancy": u.get("total_occupancy"), "threads": b.get("threads"),
                                  "chains": [{"chain": pc["chain"], "rate": pc.get("rate"), "period_us": pc.get("batch_period_us"), "batch": pc.get("batch_rt"), "frame": pc.get("frame_rt"), "saturated": pc.get("saturated"), "edf": pc.get("edf")} for pc in b["per_chain"]]}
    return runs


def late(pc):
    br = pc["batch"] or {}
    return (br.get("over_period", 0) / br["n"]) if br.get("n") else None


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("run_dir"); ap.add_argument("--out-dir", required=True); a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True); R = load(a.run_dir); md = []
    # --- simple workloads
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), gridspec_kw={"width_ratios": [1, 1.6]})
    for ax, w in zip(axes, SIMPLE):
        chains = sorted({pc["chain"] for pol in POL if (w, pol) in R for pc in R[(w, pol)]["chains"]})
        for i, c in enumerate(chains):
            for j, pol in enumerate(POL):
                r = R.get((w, pol))
                if not r: continue
                pc = r["chains"][c]
                if pc["batch"] and not pc["saturated"]:
                    mark(ax, i + (j - 1) * 0.25, pc["batch"], COL[pol], NAME[pol] if i == 0 else None)
        per = next((R[(w, p)]["chains"][0]["period_us"] for p in POL if (w, p) in R), None)
        if per: ax.hlines(per, -0.45, len(chains) - 0.55, color=INK2, linewidth=0.8, linestyles="dotted")
        ax.set_xticks(range(len(chains))); ax.set_xticklabels([f"rx {c}" for c in chains]); ax.set_yscale("log")
        t = {"simple1": "one receiver, one worker, 2.5 Msps", "simple2": "two equal receivers, two workers, 2.5 Msps each"}[w]
        style(ax, t, None, "batch response time µs (log)" if w == "simple1" else None)
        if w == "simple1": ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=8)
    fig.text(0.01, 0.005, "dot = mean, bar = ±1 sd, whisker = min–max of the batches in the window; dotted = the batch period (410 µs)", fontsize=6.8, color=INK2)
    fig.tight_layout(rect=(0, 0.04, 1, 1)); save(fig, a.out_dir, "prelim_simple")
    # --- mixes
    for mix in MIXES:
        chains = sorted({pc["chain"] for pol in POL if (mix, pol) in R for pc in R[(mix, pol)]["chains"]})
        if not chains: continue
        fig, ax = plt.subplots(figsize=(max(7, 1.7 * len(chains) + 2.5), 4.2)); lab = set()
        for i, c in enumerate(chains):
            for j, pol in enumerate(POL):
                r = R.get((mix, pol))
                if not r: continue
                pc = r["chains"][c]
                if pc["batch"] and not pc["saturated"]:
                    mark(ax, i + (j - 1) * 0.26, pc["batch"], COL[pol], None if pol in lab else NAME[pol]); lab.add(pol)
                elif pc["saturated"]:
                    ax.text(i + (j - 1) * 0.26, 1.0, "sat.", ha="center", va="bottom", fontsize=7, color=COL[pol], transform=ax.get_xaxis_transform())
            pc0 = next((R[(mix, p)]["chains"][c] for p in POL if (mix, p) in R), None)
            if pc0 and pc0["period_us"]: ax.hlines(pc0["period_us"], i - 0.42, i + 0.42, color=INK2, linewidth=0.8, linestyles="dotted")
        ref = next((R[(mix, p)]["chains"] for p in POL if (mix, p) in R), [])
        ax.set_xticks(range(len(chains))); ax.set_xticklabels([f"rx {c}: {ref[c]['rate']/1e6:g} Msps\n{ref[c]['period_us']:.0f} µs period" for c in chains], fontsize=8); ax.set_yscale("log")
        style(ax, f"Batch response time, mix '{mix}', 4 workers, N = 1024, receivers in graph order (rx {chains[-1]} last)", "receiver (its rate and batch period = its deadline)", "µs (log)")
        ax.legend(loc="upper left", frameon=False, ncol=3)
        fig.text(0.01, 0.005, "dot = mean, bar = ±1 sd, whisker = min–max of the batches in the window (one run); dotted = the receiver's batch period (its implicit deadline)", fontsize=7, color=INK2)
        fig.tight_layout(rect=(0, 0.03, 1, 1)); save(fig, a.out_dir, f"prelim_mix_{mix}")
    # --- late fraction
    fig, axes = plt.subplots(1, len(MIXES), figsize=(5.2 * len(MIXES), 3.8), squeeze=False)
    for ax, mix in zip(axes[0], MIXES):
        chains = sorted({pc["chain"] for pol in POL if (mix, pol) in R for pc in R[(mix, pol)]["chains"]})
        for i, c in enumerate(chains):
            for j, pol in enumerate(POL):
                r = R.get((mix, pol))
                if not r: continue
                lf = late(r["chains"][c])
                if lf is not None:
                    ax.bar(i + (j - 1) * 0.27, max(lf, 1e-5), width=0.25, color=COL[pol], alpha=0.35 if r["chains"][c]["saturated"] else 1.0, label=NAME[pol] if i == 0 else None)
        ref = next((R[(mix, p)]["chains"] for p in POL if (mix, p) in R), [])
        ax.set_xticks(range(len(chains))); ax.set_xticklabels([f"rx {c}\n{ref[c]['rate']/1e6:g} M" for c in chains], fontsize=8)
        ax.set_yscale("log"); ax.set_ylim(1e-5, 1); style(ax, f"mix '{mix}'", "receiver", "fraction of batches later than own period (log)"); ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=8)
    fig.suptitle("Batches later than the receiver's own batch period (its implicit deadline)", x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.01, 1, 0.94)); save(fig, a.out_dir, "prelim_late")
    # --- fastest receiver per mix, and the slow receivers at the knee
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.2), gridspec_kw={"width_ratios": [1.4, 1]})
    ax = axes[0]
    for i, mix in enumerate(MIXES):
        for j, pol in enumerate(POL):
            r = R.get((mix, pol))
            if not r: continue
            pc = max(r["chains"], key=lambda x: x["rate"] or 0)
            if pc["batch"] and not pc["saturated"]: mark(ax, i + (j - 1) * 0.25, pc["batch"], COL[pol], NAME[pol] if i == 0 else None)
        per = next((max(R[(mix, p)]["chains"], key=lambda x: x["rate"] or 0)["period_us"] for p in POL if (mix, p) in R), None)
        if per: ax.hlines(per, i - 0.42, i + 0.42, color=INK2, linewidth=0.8, linestyles="dotted")
    ax.set_xticks(range(len(MIXES))); ax.set_xticklabels([f"mix '{m}'\n5 Msps receiver (last)" for m in MIXES]); ax.set_yscale("log")
    style(ax, "The fastest receiver of each mix (205 µs period)", None, "batch response time µs (log)"); ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=8)
    ax = axes[1]; slow = []
    if any(("knee", p) in R for p in POL):
        ref = next(R[("knee", p)]["chains"] for p in POL if ("knee", p) in R)
        slow = [pc["chain"] for pc in ref if pc["rate"] == min(x["rate"] for x in ref)]
        for i, c in enumerate(slow):
            for j, pol in enumerate(POL):
                r = R.get(("knee", pol))
                if r and r["chains"][c]["batch"] and not r["chains"][c]["saturated"]: mark(ax, i + (j - 1) * 0.25, r["chains"][c]["batch"], COL[pol])
            ax.hlines(ref[c]["period_us"], i - 0.42, i + 0.42, color=INK2, linewidth=0.8, linestyles="dotted")
        ax.set_xticks(range(len(slow))); ax.set_xticklabels([f"rx {c}: {ref[c]['rate']/1e6:g} Msps" for c in slow]); ax.set_yscale("log")
    style(ax, "The slow receivers of the 'knee' mix (410 µs period)")
    fig.text(0.01, 0.005, "dot = mean, bar = ±1 sd, whisker = min–max of the batches in the window (one run); dotted = the receiver's batch period", fontsize=7, color=INK2)
    fig.tight_layout(rect=(0, 0.03, 1, 1)); save(fig, a.out_dir, "prelim_fast_slow")
    # --- numbers and table
    numbers = {f"{w}|{pol}": r for (w, pol), r in R.items()}
    json.dump(numbers, open(os.path.join(a.out_dir, "numbers.json"), "w"), indent=1)
    md.append("| workload | receiver (Msps, period µs) | RR mean / max (late) | EDF | RM | window s RR / EDF / RM |\n|---|---|---|---|---|---|\n")
    for w in SIMPLE + MIXES:
        if not any((w, p) in R for p in POL): continue
        ref = next(R[(w, p)]["chains"] for p in POL if (w, p) in R)
        for pc0 in ref:
            cells = []
            for pol in POL:
                r = R.get((w, pol)); pc = r["chains"][pc0["chain"]] if r else None
                if pc and pc["batch"]:
                    lf = late(pc); cells.append(f"{pc['batch']['mean_us']:.0f} / {pc['batch']['max_us']:.0f} ({100 * (lf or 0):.1f} %)" + (" sat." if pc["saturated"] else ""))
                else: cells.append("-")
            wins = " / ".join(f"{R[(w, p)]['window_covered_s']:.1f}" if (w, p) in R else "-" for p in POL)
            md.append(f"| {w} | rx {pc0['chain']} ({pc0['rate']/1e6:g}, {pc0['period_us']:.0f}) | " + " | ".join(cells) + f" | {wins} |\n")
    md.append("\nMeasured demand per worker (busiest worker's share of its time in productive block calls):\n\n| workload | RR | EDF | RM |\n|---|---|---|---|\n")
    for w in SIMPLE + MIXES:
        if not any((w, p) in R for p in POL): continue
        md.append(f"| {w} | " + " | ".join(f"{R[(w, p)]['demand_max']:.2f}" if (w, p) in R and R[(w, p)].get('demand_max') is not None else "-" for p in POL) + " |\n")
    open(os.path.join(a.out_dir, "TABLE.md"), "w").write("".join(md)); print("".join(md))


if __name__ == "__main__":
    main()
