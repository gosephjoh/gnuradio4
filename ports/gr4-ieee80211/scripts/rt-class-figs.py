#!/usr/bin/env python3
"""rt-class-figs.py -- range figures of the deadline-structure sweep (modifications A and B).

    ./scripts/rt-class-figs.py SWEEP_DIR --out-dir DIR

Runs are tagged cls-<setting>_N<N>_t<T>_r<R>_rate<rate>_<policy>_trace_rep<k>.  Receiver 0 is
the control-channel receiver (the tight class under A/AB); receivers 1.. are the service
channels.  Figures (PDF + PNG):
  class_batch_N<N>_<setting>   batch RT per point: policies x {receiver 0 (filled), others (hollow)}
  class_frame_N<N>_<setting>   the same for the frame response time
  class_summary_batch          receiver 0's batch RT mean under every setting, per point and policy
  class_summary_frame          receiver 0's frame RT mean under every setting
  class_over_deadline          fraction of receiver 0's batches over its class deadline, per policy/setting
  numbers.json                 every value
"""
import argparse, collections, glob, json, os, re
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

POL = ["rr", "edf", "rm"]
COL = {"rr": "#2a78d6", "edf": "#eb6834", "rm": "#1baf7a"}
NAME = {"rr": "RR", "edf": "EDF", "rm": "RM"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
SETTINGS = ["base", "A25", "A50", "A25L", "A50L", "B25", "AB25"]
SETNAME = {"base": "equal deadlines", "A25": "A: control-channel receiver first in graph order, at 0.25", "A50": "A: control-channel receiver first, at 0.5",
           "A25L": "A: control-channel receiver LAST in graph order, at 0.25", "A50L": "A: control-channel receiver LAST, at 0.5", "B25": "B: frame path at 0.25", "AB25": "A+B at 0.25"}
plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "pdf.fonttype": 42})


def pooled(gs):
    g = [s for s in gs if s and s.get("n")]
    if not g:
        return None
    n = np.array([s["n"] for s in g], float); m = np.array([s["mean_us"] for s in g]); sd = np.array([s["std_us"] for s in g])
    N = n.sum(); mean = float((n * m).sum() / N)
    var = float((((n - 1) * sd ** 2).sum() + (n * (m - mean) ** 2).sum()) / max(N - 1, 1))
    return {"n": int(N), "mean": mean, "std": var ** 0.5, "min": float(min(s["min_us"] for s in g)), "max": float(max(s["max_us"] for s in g))}


def saturated(b):
    if b.get("saturated"):
        return True
    per = b.get("batch_period_us")
    for pc in b["per_chain"]:
        tl = pc.get("throttle_lag") or {}
        if per and tl.get("n") and tl.get("p95_us", 0) > 10 * per:
            return True
    return False


def load(sweep):
    pts = collections.defaultdict(lambda: {"c0": [], "rest": [], "c0f": [], "restf": [], "over": [], "sat": [], "edf0": [], "edfr": [], "reps": 0})
    for p in sorted(glob.glob(os.path.join(sweep, "cls-*", "batch_rt.json"))):
        tag = os.path.basename(os.path.dirname(p))
        m = re.match(r"cls-(\w+)_cls_N(\d+)_t(\d+)_r(\d+)_rate(\d+)_(\w+)_trace_rep(\d+)", tag)
        if not m:
            continue
        st, N, T, R, rate, pol, rep = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4)), float(m.group(5)), m.group(6), int(m.group(7))
        b = json.load(open(p))
        k = (N, T, R, rate, st, pol)
        d = pts[k]
        d["reps"] += 1
        d["sat"].append(saturated(b))
        classes = b.get("deadline_classes") or []
        tight = [i for i, c in enumerate(classes) if c < 1]
        tight_idx = tight[0] if tight else 0
        for pc in b["per_chain"]:
            tgt = "c0" if pc["chain"] == tight_idx else "rest"
            d[tgt].append(pc.get("batch_rt"))
            d[tgt + "f"].append(pc.get("frame_rt"))
            if pc["chain"] == tight_idx and (pc.get("batch_rt") or {}).get("over_class_deadline_ratio") is not None:
                d["over"].append((pc["batch_rt"]["over_class_deadline_ratio"], pc["batch_rt"]["n"]))
            if pc.get("edf"):
                d["edf0" if pc["chain"] == tight_idx else "edfr"].append(pc["edf"])
    out = {}
    for k, d in pts.items():
        over = sum(r * n for r, n in d["over"]) / sum(n for _, n in d["over"]) if d["over"] else None
        e0 = {"releases": sum(x["releases"] for x in d["edf0"]), "misses": sum(x["misses"] for x in d["edf0"])} if d["edf0"] else None
        er = {"releases": sum(x["releases"] for x in d["edfr"]), "misses": sum(x["misses"] for x in d["edfr"])} if d["edfr"] else None
        out[k] = {"c0": pooled(d["c0"]), "rest": pooled(d["rest"]), "c0f": pooled(d["c0f"]), "restf": pooled(d["restf"]), "over_c0": over, "saturated": any(d["sat"]), "reps": d["reps"], "edf_c0": e0, "edf_rest": er}
    return out


def style(ax, title=None, xlabel=None, ylabel=None):
    if title: ax.set_title(title, loc="left", color=INK)
    if xlabel: ax.set_xlabel(xlabel, color=INK2)
    if ylabel: ax.set_ylabel(ylabel, color=INK2)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    for s in ("left", "bottom"): ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2); ax.grid(True, axis="y", color=GRID, linewidth=0.7); ax.set_axisbelow(True)


def mark(ax, x, st, color, filled, label=None):
    ax.vlines(x, st["min"], st["max"], color=color, linewidth=0.9, alpha=0.7)
    ax.vlines(x, max(st["mean"] - st["std"], st["min"]), min(st["mean"] + st["std"], st["max"]), color=color, linewidth=4.5, alpha=0.9 if filled else 0.45)
    ax.plot([x], [st["mean"]], "o", color=color if filled else "white", markeredgecolor=color, markersize=5.5, markeredgewidth=1.4, label=label, zorder=5)


def save(fig, out, name):
    fig.savefig(os.path.join(out, name + ".pdf"), facecolor="white"); fig.savefig(os.path.join(out, name + ".png"), dpi=160, facecolor="white"); plt.close(fig); print("wrote", name)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("sweep"); ap.add_argument("--out-dir", required=True); a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    pts = load(a.sweep)
    Ns = sorted({k[0] for k in pts}); points = sorted({k[:4] for k in pts})
    settings = [s for s in SETTINGS if any(k[4] == s for k in pts)]
    numbers = {"points": []}
    for N in Ns:
        P = [p for p in points if p[0] == N]
        for st in settings:
            for key, fname, ylabel, title in (("c0", "class_batch", "µs (log)", "Batch response time"), ("c0f", "class_frame", "µs (log)", "Frame response time")):
                rest = "rest" if key == "c0" else "restf"
                fig, ax = plt.subplots(figsize=(max(6.5, 1.6 * len(P) + 2.5), 4.0))
                lab = set()
                for i, p in enumerate(P):
                    for j, pol in enumerate(POL):
                        d = pts.get(p + (st, pol))
                        if not d:
                            continue
                        x = i + (j - 1) * 0.28
                        if d[key] and not d["saturated"]:
                            mark(ax, x - 0.06, d[key], COL[pol], True, None if pol in lab else f"{NAME[pol]} control-channel rx"); lab.add(pol)
                        if d[rest] and not d["saturated"]:
                            mark(ax, x + 0.06, d[rest], COL[pol], False, None if (pol, "r") in lab else f"{NAME[pol]} service-channel rx")
                            lab.add((pol, "r"))
                        if d["saturated"]:
                            ax.text(x, 1.0, "sat.", ha="center", va="bottom", fontsize=7, color=INK2, transform=ax.get_xaxis_transform())
                ax.set_xticks(range(len(P))); ax.set_xticklabels([f"{p[1]}w · {p[2]}rx\n{p[3]/1e6:g} Msps" for p in P], fontsize=8)
                ax.set_yscale("log")
                style(ax, f"{title}, N = {N}, {SETNAME.get(st, st)}", "configuration", ylabel)
                ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=7.5)
                fig.text(0.01, 0.005, "filled = the control-channel receiver (the tight class; receiver 0 unless the setting says LAST), hollow = the service-channel receivers; dot mean, bar ±1 sd, whisker min–max; 'sat.' = the graph did not keep real time", fontsize=7, color=INK2)
                fig.tight_layout(rect=(0, 0.03, 1, 1)); save(fig, a.out_dir, f"{fname}_N{N}_{st}")
    # summaries: receiver 0 mean under every setting, per point and policy
    for key, fname, title in (("c0", "class_summary_batch", "Control-channel receiver: batch response time (mean, ±1 sd, min–max) under each deadline setting"), ("c0f", "class_summary_frame", "Control-channel receiver: frame response time under each deadline setting")):
        fig, axes = plt.subplots(len(Ns), 1, figsize=(max(7, 1.1 * len(points) / len(Ns) * len(settings) * 0.45 + 3), 3.6 * len(Ns)), squeeze=False)
        for r, N in enumerate(Ns):
            ax = axes[r][0]; P = [p for p in points if p[0] == N]; lab = set()
            xt, xl = [], []
            for i, p in enumerate(P):
                for si, st in enumerate(settings):
                    base = i * (len(settings) + 1) + si
                    xt.append(base); xl.append(f"{st}\n{p[1]}w·{p[2]}rx" if si == len(settings) // 2 else st)
                    for j, pol in enumerate(POL):
                        d = pts.get(p + (st, pol))
                        if d and d[key] and not d["saturated"]:
                            mark(ax, base + (j - 1) * 0.27, d[key], COL[pol], True, None if pol in lab else NAME[pol]); lab.add(pol)
                        elif d and d["saturated"]:
                            ax.text(base + (j - 1) * 0.27, 1.0, "s", ha="center", va="bottom", fontsize=6, color=INK2, transform=ax.get_xaxis_transform())
            ax.set_xticks(xt); ax.set_xticklabels(xl, fontsize=6.5); ax.set_yscale("log")
            style(ax, f"N = {N}", None, "µs (log)"); ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=8)
        fig.suptitle(title, x=0.01, ha="left", fontsize=11, color=INK); fig.tight_layout(rect=(0, 0.01, 1, 0.96)); save(fig, a.out_dir, fname)
    # over-deadline fraction of receiver 0
    fig, axes = plt.subplots(len(Ns), 1, figsize=(max(7, 0.5 * len(points) / len(Ns) * len(settings) + 3), 3.2 * len(Ns)), squeeze=False)
    for r, N in enumerate(Ns):
        ax = axes[r][0]; P = [p for p in points if p[0] == N]; xt, xl = [], []
        for i, p in enumerate(P):
            for si, st in enumerate(settings):
                base = i * (len(settings) + 1) + si; xt.append(base); xl.append(f"{st}\n{p[1]}w·{p[2]}rx" if si == len(settings) // 2 else st)
                for j, pol in enumerate(POL):
                    d = pts.get(p + (st, pol))
                    if d and d["over_c0"] is not None and not d["saturated"]:
                        ax.bar(base + (j - 1) * 0.27, max(d["over_c0"], 1e-6), width=0.25, color=COL[pol])
        ax.set_xticks(xt); ax.set_xticklabels(xl, fontsize=6.5); ax.set_yscale("log"); ax.set_ylim(1e-6, 1)
        style(ax, f"N = {N}", None, "fraction of receiver 0's batches over its class deadline (log)")
    fig.suptitle("Control-channel receiver: batches later than its class deadline (0.25 / 0.5 / 1 batch period), RR blue, EDF orange, RM aqua", x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.01, 1, 0.96)); save(fig, a.out_dir, "class_over_deadline")
    for k, d in sorted(pts.items()):
        numbers["points"].append({"N": k[0], "workers": k[1], "receivers": k[2], "rate": k[3], "setting": k[4], "policy": k[5], **d})
    json.dump(numbers, open(os.path.join(a.out_dir, "numbers.json"), "w"), indent=1)
    print("wrote numbers.json")


if __name__ == "__main__":
    main()
