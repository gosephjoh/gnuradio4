#!/usr/bin/env python3
"""rt-multirate-compare.py -- the multi-rate experiment at two scheduler settings side by side.

    ./scripts/rt-multirate-compare.py --a SWEEP_A --a-label "ratio 16" --b SWEEP_B --b-label "ratio 4096" --out-dir DIR

Both sweeps are rt-sweep4 --multirate directories (runs tagged mr-<mix>_..._<policy>_trace_rep<k>).
Figures (PDF + PNG):
  mr_cmp_batch_<mix>   batch response time per receiver; per policy the A setting (hollow) next to the B setting (filled)
  mr_cmp_over_period   per mix and receiver, the fraction of batches later than the receiver's own period, A and B
  compare.json         every value, and TABLE.md with the pooled numbers as markdown
"""
import argparse, importlib.util, json, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("figs", os.path.join(HERE, "rt-multirate-figs.py")); figs = importlib.util.module_from_spec(spec); spec.loader.exec_module(figs)
POL, COL, NAME, INK, INK2 = figs.POL, figs.COL, figs.NAME, figs.INK, figs.INK2


def mark(ax, x, st, color, filled, label=None):
    ax.vlines(x, st["min"], st["max"], color=color, linewidth=0.9, alpha=0.45 if not filled else 0.8)
    ax.vlines(x, max(st["mean"] - st["std"], st["min"]), min(st["mean"] + st["std"], st["max"]), color=color, linewidth=4.5, alpha=0.45 if not filled else 0.95)
    ax.plot([x], [st["mean"]], "o", color=color if filled else "white", markeredgecolor=color, markersize=5.5, markeredgewidth=1.3, label=label, zorder=5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True); ap.add_argument("--a-label", default="A"); ap.add_argument("--b", required=True); ap.add_argument("--b-label", default="B"); ap.add_argument("--out-dir", required=True)
    a = ap.parse_args(); os.makedirs(a.out_dir, exist_ok=True)
    A, B = figs.load(a.a), figs.load(a.b)
    mixes = sorted({k[0] for k in list(A) + list(B)}, key=lambda m: {"light": 0, "mid": 1, "knee": 2}.get(m, 9))
    out = {}; md = []
    for mix in mixes:
        chains = sorted({c for S in (A, B) for pol in POL for c in S.get((mix, pol), {})})
        fig, ax = plt.subplots(figsize=(max(7, 2.4 * len(chains) + 2.5), 4.4)); lab = set()
        for i, c in enumerate(chains):
            for j, pol in enumerate(POL):
                for s, (S, filled) in enumerate(((A, False), (B, True))):
                    d = S.get((mix, pol), {}).get(c)
                    x = i + (j - 1) * 0.3 + (s - 0.5) * 0.11
                    if d and d["batch"] and not d["saturated"]:
                        lbl = f"{NAME[pol]}, {a.b_label if filled else a.a_label}"
                        mark(ax, x, d["batch"], COL[pol], filled, None if lbl in lab else lbl); lab.add(lbl)
                    elif d and d["saturated"]:
                        ax.text(x, 1.0, "sat.", ha="center", va="bottom", fontsize=6, color=COL[pol], transform=ax.get_xaxis_transform())
            d0 = next((S[(mix, p)][c] for S in (A, B) for p in POL if (mix, p) in S and c in S[(mix, p)]), None)
            if d0 and d0["period"]:
                ax.hlines(d0["period"], i - 0.46, i + 0.46, color=INK2, linewidth=0.8, linestyles="dotted")
        ref = next((S[(mix, p)] for S in (A, B) for p in POL if (mix, p) in S), {})
        ax.set_xticks(range(len(chains))); ax.set_xticklabels([f"rx {c}: {ref[c]['rate']/1e6:g} Msps\n{ref[c]['period']:.0f} µs period" if c in ref else f"rx {c}" for c in chains], fontsize=8)
        ax.set_yscale("log")
        figs.style(ax, f"Batch response time, mix '{mix}', 4 workers, N = 1024: {a.a_label} (hollow) against {a.b_label} (filled)", "receiver (its rate and batch period = its deadline)", "µs (log)")
        ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=7)
        fig.text(0.01, 0.005, "dot = mean, bar = ±1 sd, whisker = min–max over all batches of all repeats; dotted = the receiver's batch period; 'sat.' = did not keep real time", fontsize=7, color=INK2)
        fig.tight_layout(rect=(0, 0.03, 1, 1)); figs.save(fig, a.out_dir, f"mr_cmp_batch_{mix}")
        out[mix] = {}
        md.append(f"\n**Mix '{mix}'** (batch RT mean / max µs, and % of batches later than the receiver's own period; a = {a.a_label}, b = {a.b_label})\n\n| receiver | period µs | RR a | RR b | EDF a | EDF b | RM a | RM b |\n|---|---|---|---|---|---|---|---|\n")
        for c in chains:
            row = []; out[mix][str(c)] = {}
            for pol in POL:
                for lbl, S in ((a.a_label, A), (a.b_label, B)):
                    d = S.get((mix, pol), {}).get(c)
                    out[mix][str(c)][f"{pol}|{lbl}"] = d
                    if d and d["batch"]:
                        row.append(f"{d['batch']['mean']:.0f} / {d['batch']['max']:.0f} ({100 * (d['over_period'] or 0):.1f}%)" + (" sat." if d["saturated"] else ""))
                    else:
                        row.append("-")
            md.append(f"| rx {c} | {ref[c]['period']:.0f} | " + " | ".join(row) + " |\n")
    # over-period panel
    fig, axes = plt.subplots(1, len(mixes), figsize=(5.6 * len(mixes), 3.8), squeeze=False)
    for ax, mix in zip(axes[0], mixes):
        chains = sorted({c for S in (A, B) for pol in POL for c in S.get((mix, pol), {})})
        for i, c in enumerate(chains):
            for j, pol in enumerate(POL):
                for s, (S, lbl) in enumerate(((A, a.a_label), (B, a.b_label))):
                    d = S.get((mix, pol), {}).get(c)
                    if d and d["over_period"] is not None:
                        ax.bar(i + (j - 1) * 0.3 + (s - 0.5) * 0.12, max(d["over_period"], 1e-5), width=0.11, color=COL[pol], alpha=0.4 if s == 0 else 1.0, label=f"{NAME[pol]}, {lbl}" if i == 0 else None, hatch="//" if d["saturated"] else None)
        ref = next((S[(mix, p)] for S in (A, B) for p in POL if (mix, p) in S), {})
        ax.set_xticks(range(len(chains))); ax.set_xticklabels([f"rx {c}\n{ref[c]['rate']/1e6:g} M" for c in chains], fontsize=8)
        ax.set_yscale("log"); ax.set_ylim(1e-5, 1)
        figs.style(ax, f"mix '{mix}'", "receiver", "fraction later than own period (log)")
        ax.legend(loc="upper left", frameon=False, ncol=2, fontsize=6.5)
    fig.suptitle(f"Batches later than the receiver's own period: {a.a_label} (faded) against {a.b_label} (solid); hatched = saturated", x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.01, 1, 0.94)); figs.save(fig, a.out_dir, "mr_cmp_over_period")
    json.dump(out, open(os.path.join(a.out_dir, "compare.json"), "w"), indent=1)
    open(os.path.join(a.out_dir, "TABLE.md"), "w").write("".join(md)); print("".join(md))


if __name__ == "__main__":
    main()
