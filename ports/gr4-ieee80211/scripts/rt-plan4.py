#!/usr/bin/env python3
"""rt-plan4.py -- turn probe runs into a sweep plan at the utilization targets (0036 items 12-13).

    ./scripts/rt-plan4.py PROFILE.json PROBE_RUN_DIR [PROBE_RUN_DIR ...] [--out PLAN.json]

Each probe run is one receiver, one worker, one batch size N, traced
(rt-sweep4 --probe makes them).  From its batch_rt.json the per-block busy
fractions are known.  For every candidate point of the profile (receivers R,
workers T, rate r, batch N) the plan predicts the utilization of the busiest
worker: every block's busy fraction scales with r / r_probe (invocations per
second scale with the rate; the per-call cost is the probe's), the R chains'
blocks are dealt to the T workers exactly as GR4's Simple<multiThreaded> deals
them (block i of the flattened graph to worker i mod T), and the worker sums
are taken.  The point nearest each utilization target is chosen per (N, T),
plus the profile's rate sweep.  The prediction is only a way to *choose*
points; every run measures its own utilization from its own trace.
"""
import argparse, json, sys


def load_probe(path):
    b = json.load(open(path.rstrip("/") + "/batch_rt.json"))
    u = b.get("utilization") or {}
    per = u.get("per_block") or []
    if not per:
        sys.exit(f"rt-plan4: {path} has no per-block utilization (traced probe needed)")
    # block order = the chain's build order (chain.cpp); one chain in a probe
    order = ["fsrc", "mag2", "avgpow", "dly16", "conj", "mul", "avgcor", "mag", "div", "syncshort", "dly320", "synclong", "fft", "eq", "decode", "stamp", "latsink", "throttle"]
    if b.get("saturated"):
        sys.exit(f"rt-plan4: the probe {path} ran {b['realtime_ratio']:.2f}x slower than real time (saturated); its busy fractions understate the demand -- re-run the probe at a lower probe_rate")
    if (b.get("trace_header") or {}).get("lost_count"):
        print(f"rt-plan4: WARNING {path} lost trace records; the window was moved to the retained span", file=sys.stderr)
    busy = {p["role"]: p["demand"] for p in per if p["chain"] == 0}
    return {"N": b["fixed_batch"], "rate": b["rate"], "busy": [busy.get(r, 0.0) for r in order], "roles": order, "total": sum(busy.values())}


def predict(probe, R, T, rate):
    scale = rate / probe["rate"]
    blocks = []
    for _ in range(R):
        blocks.extend(b * scale for b in probe["busy"])
    workers = [0.0] * T
    for i, b in enumerate(blocks):
        workers[i % T] += b
    return max(workers), sum(blocks), workers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("profile")
    ap.add_argument("probes", nargs="+")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    prof = json.load(open(a.profile))
    probes = {}
    for p in a.probes:
        pr = load_probe(p)
        probes[pr["N"]] = pr
    plan = {"profile": prof["name"], "points": [], "grid": []}
    targets = prof["utilization_targets"]
    for N in prof["batches"]:
        if N not in probes:
            print(f"rt-plan4: no probe for N={N}; skipping it", file=sys.stderr)
            continue
        pr = probes[N]
        for T in prof["workers"]:
            cands = []
            for R in prof["receivers"]:
                for rate in prof["rates"]:
                    umax, utot, per = predict(pr, R, T, rate)
                    cands.append({"N": N, "workers": T, "receivers": R, "rate": rate, "u_max_worker": umax, "u_total": utot})
                    plan["grid"].append(dict(cands[-1]))
            chosen = {}
            for tgt in targets:
                best = min(cands, key=lambda c: abs(c["u_max_worker"] - tgt))
                key = (best["receivers"], best["rate"])
                if key not in chosen:
                    chosen[key] = dict(best, target=tgt, kind="target")
                else:
                    chosen[key]["target"] = f"{chosen[key]['target']},{tgt}"
            plan["points"].extend(chosen.values())
    rs = prof.get("rate_sweep")
    if rs:
        for N in prof["batches"]:
            if N in probes:
                for rate in rs["rates"]:
                    umax, utot, _ = predict(probes[N], rs["receivers"], rs["workers"], rate)
                    plan["points"].append({"N": N, "workers": rs["workers"], "receivers": rs["receivers"], "rate": rate, "u_max_worker": umax, "u_total": utot, "target": None, "kind": "rate_sweep"})
    # de-duplicate identical points
    seen, pts = set(), []
    for p in plan["points"]:
        k = (p["N"], p["workers"], p["receivers"], p["rate"])
        if k not in seen:
            seen.add(k)
            pts.append(p)
    plan["points"] = pts
    print(f"{'N':>5} {'T':>2} {'R':>2} {'rate':>9} {'U_max':>6} {'U_tot':>6}  kind/target")
    for p in pts:
        print(f"{p['N']:5d} {p['workers']:2d} {p['receivers']:2d} {p['rate']/1e6:8.1f}M {p['u_max_worker']:6.2f} {p['u_total']:6.2f}  {p['kind']} {p['target'] or ''}")
    out = a.out or (a.profile.rsplit("/", 1)[-1].replace(".json", "") + "-plan.json")
    json.dump(plan, open(out, "w"), indent=2)
    print(f"rt-plan4: {len(pts)} points -> {out}")


if __name__ == "__main__":
    main()
