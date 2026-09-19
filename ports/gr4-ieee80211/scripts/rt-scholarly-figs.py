#!/usr/bin/env python3
"""rt-scholarly-figs.py -- the three box plots in the classic style (pastel boxes, black frame, solid
median, dashed red mean, capped whiskers at min/max), each with and without a legend, PDF.

    ./scripts/rt-scholarly-figs.py --full FULL_RUN_DIR --prelim PRELIM_RUN_DIR --out-dir DIR [--png]

  fig1_simple_{legend,nolegend}.pdf     full sweep: one receiver / one worker; two equal receivers / two workers
  fig2_light_mix_{legend,nolegend}.pdf  preliminary run: the light mix, four receivers on four workers
  fig3_high_rate_{legend,nolegend}.pdf  full sweep: the last (5 Msps) receiver of the light, medium and heavy mixes
Statistics as in rt-full-figs.py (batches of all repeats pooled, whole-process stalls excluded).
"""
import argparse, importlib.util, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("full", os.path.join(HERE, "rt-full-figs.py")); full = importlib.util.module_from_spec(spec); spec.loader.exec_module(full)

POL = ["rr", "edf", "rm"]; NAME = {"rr": "RR", "edf": "EDF", "rm": "RM"}
FILL = {"rr": "#c6dbef", "edf": "#fdd0a2", "rm": "#c7e9c0"}
plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9.5, "pdf.fonttype": 42, "ps.fonttype": 42, "font.family": "DejaVu Sans"})
YLABEL = "Batch Response Time, µs (log)"


def stats_of(run_dir):
    meta, data = full.load(run_dir); S = {}
    for (w, pol), df in data.items():
        df["stall"] = full.flag_stalls(df, 5.0, 10.0, 2e6)
        for pc in meta[(w, pol)]["chains"]:
            S[(w, pol, pc["chain"])] = full.cell_stats(df[df.chain == pc["chain"]], pc["period_us"])
    return meta, S


def bxp_stats(st, label):
    return {"label": label, "med": st["median"], "q1": st["q25"], "q3": st["q75"], "whislo": st["min"], "whishi": st["max"], "mean": st["mean"], "fliers": []}


def draw_group(ax, groups, S, width=0.22):
    """groups: list of (xlabel, key_fn) with key_fn(pol) -> S key; one box per policy per group"""
    for i, (_, key) in enumerate(groups):
        for j, pol in enumerate(POL):
            k = key(pol)
            if k not in S: continue
            ax.bxp([bxp_stats(S[k], "")], positions=[i + (j - 1) * (width + 0.05)], widths=width, showmeans=True, meanline=True, showfliers=False, patch_artist=True,
                   boxprops={"facecolor": FILL[pol], "edgecolor": "black", "linewidth": 0.9},
                   medianprops={"color": "black", "linewidth": 1.2, "linestyle": "-"},
                   meanprops={"color": "red", "linewidth": 1.2, "linestyle": "--"},
                   whiskerprops={"color": "black", "linewidth": 0.9}, capprops={"color": "black", "linewidth": 1.1})
    ax.set_xticks(range(len(groups))); ax.set_xticklabels([g[0] for g in groups])
    ax.set_yscale("log"); ax.grid(True, axis="y", linestyle=":", linewidth=0.6, color="0.8"); ax.set_axisbelow(True)
    ax.tick_params(direction="out", length=3)


def legend(fig):
    handles = [Patch(facecolor=FILL[p], edgecolor="black", label=NAME[p]) for p in POL] + [Line2D([0], [0], color="black", linewidth=1.2, label="Median"), Line2D([0], [0], color="red", linewidth=1.2, linestyle="--", label="Mean")]
    fig.legend(handles=handles, loc="lower center", ncol=5, frameon=False, fontsize=8.5, handlelength=1.8, columnspacing=1.6)


def ticks(ax, lo, hi):
    ax.set_ylim(lo * 0.85, hi * 1.25)
    t = [v for v in (20, 50, 100, 200, 500, 1000, 2000, 5000, 10000) if lo * 0.85 <= v <= hi * 1.25]
    ax.set_yticks(t); ax.set_yticklabels([f"{v:g}" for v in t]); ax.minorticks_off()


def finish(fig, leg):
    if leg: legend(fig); fig.tight_layout(rect=(0, 0.07, 1, 1))
    else: fig.tight_layout()


def emit(fig, out, name, png):
    fig.savefig(os.path.join(out, name + ".pdf"))
    if png: fig.savefig(os.path.join(out, name + ".png"), dpi=170)
    plt.close(fig); print("wrote", name + ".pdf")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--full", required=True); ap.add_argument("--prelim", required=True); ap.add_argument("--out-dir", required=True); ap.add_argument("--png", action="store_true")
    a = ap.parse_args(); os.makedirs(a.out_dir, exist_ok=True)
    metaF, SF = stats_of(a.full); metaP, SP = stats_of(a.prelim)

    # fig 1: the simple workloads (full sweep)
    for leg in (True, False):
        fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.4), gridspec_kw={"width_ratios": [1, 1.7]})
        draw_group(axes[0], [("Rx 0", lambda p: ("simple1", p, 0))], SF)
        axes[0].set_title("One Receiver, One Worker, 2.5 Msps"); axes[0].set_ylabel(YLABEL)
        draw_group(axes[1], [("Rx 0", lambda p: ("simple2", p, 0)), ("Rx 1", lambda p: ("simple2", p, 1))], SF)
        axes[1].set_title("Two Equal Receivers, Two Workers, 2.5 Msps Each")
        lo = min(SF[k]["min"] for k in SF if k[0] in ("simple1", "simple2")); hi = max(SF[k]["max"] for k in SF if k[0] in ("simple1", "simple2"))
        for ax in axes: ticks(ax, lo, hi)
        finish(fig, leg); emit(fig, a.out_dir, f"fig1_simple_{'legend' if leg else 'nolegend'}", a.png)

    # fig 2: the light mix of the preliminary run
    ref = next(metaP[("light", p)]["chains"] for p in POL if ("light", p) in metaP)
    groups = [(f"Rx {c['chain']}\n{c['rate']/1e6:g} Msps\n{c['period_us']:.0f} µs", (lambda p, c=c["chain"]: ("light", p, c))) for c in ref]
    for leg in (True, False):
        fig, ax = plt.subplots(figsize=(7.2, 3.6))
        draw_group(ax, groups, SP)
        ax.set_title("Four Receivers, Four Workers"); ax.set_ylabel(YLABEL)
        lo = min(SP[k]["min"] for k in SP if k[0] == "light"); hi = max(SP[k]["max"] for k in SP if k[0] == "light"); ticks(ax, lo, hi)
        finish(fig, leg); emit(fig, a.out_dir, f"fig2_light_mix_{'legend' if leg else 'nolegend'}", a.png)

    # fig 3: the last (5 Msps) receiver of each mix (full sweep)
    label = {"light": "Light", "mid": "Medium", "knee": "Heavy"}
    groups = []
    for mix in ("light", "mid", "knee"):
        r = next(metaF[(mix, p)]["chains"] for p in POL if (mix, p) in metaF)
        last = max(r, key=lambda x: (x["rate"] or 0, x["chain"]))["chain"]
        groups.append((f"{label[mix]}\n5 Msps", (lambda p, m=mix, c=last: (m, p, c))))
    for leg in (True, False):
        fig, ax = plt.subplots(figsize=(5.6, 3.6))
        draw_group(ax, groups, SF)
        ax.set_title("High Rate Receivers Only"); ax.set_ylabel(YLABEL)
        keys = [g[1](p) for g in groups for p in POL]
        lo = min(SF[k]["min"] for k in keys); hi = max(SF[k]["max"] for k in keys); ticks(ax, lo, hi)
        finish(fig, leg); emit(fig, a.out_dir, f"fig3_high_rate_{'legend' if leg else 'nolegend'}", a.png)


if __name__ == "__main__":
    main()
