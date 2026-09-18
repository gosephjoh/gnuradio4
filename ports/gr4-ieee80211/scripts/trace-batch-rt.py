#!/usr/bin/env python3
"""trace-batch-rt.py -- batch response times of one rx_latency4 run (decision 0036).

    ./scripts/trace-batch-rt.py RUN_DIR [--warmup S] [--window S] [--trace-export BIN] [--quiet]

Reads, from RUN_DIR: trace_meta.json (written by rx_latency4: block names,
throttle anchors, knobs), latency.csv / latency_summary.json (the frame
response times of the arrival stamper), the manifest of the cell
(meta.run_dir/manifest.json: every frame's sample offsets) and the trace
capture named in trace_meta.json, flattened to CSV by build/trace_export
(done here if the CSVs are missing).  Writes RUN_DIR/batch_rt.json (every
statistic) and RUN_DIR/batch_rt.csv (one row per batch) and prints a summary.

Definitions (0036 items 1-3):
  batch j          samples [j*N, (j+1)*N) of the chain's input, N = fixed_batch
  release_j        nominal arrival: throttle_start + (j+1)*N/rate  (item 1)
  publish_j        when the throttle actually published chunk j (workExact end)
  done_<block>_j   end of the first invocation of <block> whose input span
                   covers the batch's last sample (j+1)*N-1
  batch RT         done_syncshort_j - release_j   (item 2, headline)
  frame batch RT   t_decode(frame) - release_{j(f)}, j(f) = batch holding the
                   frame's last sample (item 2, the sink variant)
  frame RT         lat_last_us of latency.csv (0035; works without a trace)
Under RM and RR a "job" is one invocation (item 3); the trace has no job
index, so j is assigned here from stream positions.

Utilization (0036 item 12) is measured, not predicted, and split three ways:
demand (work() calls that moved samples -- the load, scales with the rate),
overhead (calls counted productive that moved nothing) and probing (the
unproductive attempts of a spinning worker).  Per worker the sum over its
blocks; `total_cores_busy` and `max_worker` are demand, which the planner
scales and the plots put on x; `total_occupancy` is what a core actually did.
"""
import argparse, json, os, subprocess, sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRE_GATE = ["mag2", "avgpow", "dly16", "conj", "mul", "avgcor", "mag", "div", "syncshort"]
WRAP = 1 << 32


def die(msg):
    print("trace-batch-rt: " + msg, file=sys.stderr)
    sys.exit(2)


def stats(x, period_ns=None):
    x = np.asarray(x, dtype=np.float64)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {"n": 0}
    d = {"n": int(x.size), "min_us": float(x.min() / 1e3), "max_us": float(x.max() / 1e3), "mean_us": float(x.mean() / 1e3),
         "std_us": float(x.std(ddof=1) / 1e3) if x.size > 1 else 0.0,
         "p50_us": float(np.percentile(x, 50) / 1e3), "p95_us": float(np.percentile(x, 95) / 1e3), "p99_us": float(np.percentile(x, 99) / 1e3)}
    if period_ns:
        d["over_period"] = int((x > period_ns).sum())
        d["over_period_ratio"] = float((x > period_ns).mean())
    return d


def unwrap32(pos):
    """positions are the low 32 bits of a monotone stream index: unwrap."""
    pos = np.asarray(pos, dtype=np.int64)
    if pos.size == 0:
        return pos
    d = np.diff(pos)
    wraps = np.cumsum(np.concatenate([[0], (d < -(1 << 31)).astype(np.int64)]))
    return pos + wraps * WRAP


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--warmup", type=float, default=5.0, help="seconds after the throttle start to discard (0036: 5)")
    ap.add_argument("--window", type=float, default=20.0, help="analysis window length in seconds (0036: 20); 0 = to the end")
    ap.add_argument("--trace-export", default=os.path.join(HERE, "build", "trace_export"))
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    rd = a.run_dir.rstrip("/")

    meta = json.load(open(os.path.join(rd, "trace_meta.json")))
    summ = json.load(open(os.path.join(rd, "latency_summary.json")))
    N = int(meta["fixed_batch"])
    rate = float(meta["rate"])
    chain_rate = [float(c.get("rate", rate)) for c in meta["chains"]]  # multi-rate receivers
    out = {"run_dir": rd, "policy": meta["policy"], "policy_name": meta.get("policy_name"), "threads": meta["threads"], "chains": len(meta["chains"]),
           "rate": rate, "fixed_batch": N, "buffer": meta["buffer"], "warmup_s": a.warmup, "window_s": a.window,
           "trace": meta.get("trace", {}), "result": meta.get("result"), "per_chain": [], "utilization": None, "edf": None, "sync": None}
    period_ns = N / rate * 1e9 if (N > 0 and rate > 0) else None
    out["batch_period_us"] = period_ns / 1e3 if period_ns else None
    out["elapsed_s"] = meta.get("elapsed_s")

    # ---- the frames of the cell: last-sample offsets
    man = json.load(open(os.path.join(meta["run_dir"], "manifest.json")))
    pad_tail = man["phy"].get("pad_tail", 0)
    pad_front = man["phy"].get("pad_front", 0)
    f_last = np.array([f["sample_offset"] + f["samples"] - pad_tail - 1 for f in man["frames"]], dtype=np.int64)
    f_first = np.array([f["sample_offset"] + pad_front for f in man["frames"]], dtype=np.int64)
    total_samples = int(man.get("prediction", {}).get("total_samples", 0) or (f_last[-1] + pad_tail + 1))
    if meta.get("max_samples"):
        total_samples = min(total_samples, int(meta["max_samples"]))  # --max-samples: only that much was replayed
    out["air_s"] = total_samples / rate if rate > 0 else None
    if meta.get("run_s"):
        out["air_s"] = float(meta["run_s"])  # --run-s: every receiver replays that long at its own rate
    # slower than real time = the graph could not keep up = response times are unbounded (the
    # throttle publishes late because its output buffer stays full); the point is saturated
    out["realtime_ratio"] = (meta.get("elapsed_s") / out["air_s"]) if (out["air_s"] and meta.get("elapsed_s")) else None
    out["saturated"] = bool(out["realtime_ratio"] and out["realtime_ratio"] > 1.05)
    out["saturation_reason"] = "slower than 1.05x real time" if out["saturated"] else None
    if out["saturated"]:
        print(f"trace-batch-rt: WARNING the run took {out['realtime_ratio']:.2f}x its air time: the graph did not keep up; batch response times are unbounded here", file=sys.stderr)

    # ---- window in CLOCK_MONOTONIC ns
    starts = [c["throttle_start_ns"] for c in meta["chains"] if c["throttle_start_ns"]]
    if not starts:
        die("no throttle start anchor in trace_meta.json (unthrottled run?)")
    t0 = min(starts)
    w0 = t0 + int(a.warmup * 1e9)
    w1 = w0 + int(a.window * 1e9) if a.window > 0 else np.iinfo(np.int64).max
    out["window_ns"] = [int(w0), None if a.window <= 0 else int(w1)]

    # ---- frame response times (no trace needed)
    lat = pd.read_csv(os.path.join(rd, "latency.csv"))
    classes = meta.get("deadline_classes") or []
    frame_f = float(meta.get("frame_deadline", 1.0) or 1.0)
    out["deadline_classes"] = classes
    out["frame_deadline"] = frame_f
    for k, ch in enumerate(meta["chains"]):
        L = lat[lat.chain == k]
        dec = L[L.decoded == 1]
        inwin = dec[(dec.t_last_ns >= w0) & (dec.t_last_ns < w1)]
        cf = float(classes[k]) if k < len(classes) else 1.0
        pc = {"chain": k, "rate": chain_rate[k], "batch_period_us": N / chain_rate[k] * 1e6 if (N and chain_rate[k]) else None, "deadline_factor": cf, "frame_deadline_factor": min(cf, frame_f), "frames": int(len(L)), "decoded": int(len(dec)), "decoded_in_window": int(len(inwin)),
              "frame_rt": stats(inwin.lat_last_us.to_numpy() * 1e3), "frame_rt_all": stats(dec.lat_last_us.to_numpy() * 1e3)}
        out["per_chain"].append(pc)

    # ---- the trace
    tr = meta.get("trace") or {}
    cap = tr.get("path")
    have_trace = bool(cap) and tr.get("written") is not None and os.path.exists(cap) and N > 0
    if not have_trace:
        out["note"] = "no capture (or no fixed batch): frame response times only"
    else:
        inv_csv = os.path.join(rd, "trace_invocations.csv")
        stale = os.path.exists(inv_csv) and os.path.getmtime(inv_csv) < os.path.getmtime(cap)
        if not os.path.exists(inv_csv) or stale:
            if not os.path.exists(a.trace_export):
                die(f"{a.trace_export} missing; build it: cmake --build build --target trace_export")
            subprocess.run([a.trace_export, cap, "--out-dir", rd, "--meta", os.path.join(rd, "trace_meta.json")], check=True)
        hdr = json.load(open(os.path.join(rd, "trace_header.json")))
        out["trace_header"] = hdr
        if hdr.get("monotonic_anchor_ns", 0) == 0:
            print("trace-batch-rt: WARNING the capture's clock domain did not match CLOCK_MONOTONIC; trace and stamp times may not be comparable", file=sys.stderr)
        if hdr.get("lost_count", 0) > 0:
            print(f"trace-batch-rt: WARNING {hdr['lost_count']} records were lost (ring too small); every figure below understates", file=sys.stderr)
        inv = pd.read_csv(inv_csv, dtype={"role": str})
        inv = inv[inv.chain >= 0]
        # the rings keep the most recent records, and every worker's ring retains a different
        # span (a spinning worker overwrites faster): the window must lie inside the span that
        # *every* block of the batch path still has records for, or a batch published by the
        # throttle finds no consumer records (or the reverse).  Intersect the retained spans of
        # the throttle and the pre-gate blocks over all chains; slide/shrink the window into it.
        path = inv[inv.role.isin(["throttle"] + PRE_GATE)]
        spans = path.groupby(["chain", "role"]).start_ns.agg(["min", "max"]) if len(path) else None
        out["window_shifted"] = False
        if spans is not None and len(spans):
            span_start = int(spans["min"].max()) + int(0.2e9)
            span_end   = int(spans["max"].min())
            out["retained_span_ns"] = [span_start, span_end]
            # slide the window to the retained span, keep its length where the span allows
            new_w0 = max(w0, span_start)
            new_w1 = (new_w0 + int(a.window * 1e9)) if a.window > 0 else span_end
            new_w1 = min(new_w1, span_end)
            if new_w0 != w0 or new_w1 != w1:
                out["window_shifted"] = True
                print(f"trace-batch-rt: NOTE the rings' common retained span starts {(span_start - t0)/1e9:.1f} s after the throttle start and ends at {(span_end - t0)/1e9:.1f} s; window set to [{(new_w0 - t0)/1e9:.1f}, {(new_w1 - t0)/1e9:.1f}] s", file=sys.stderr)
            w0, w1 = new_w0, new_w1
            out["window_ns"] = [int(w0), int(w1)]
            if w1 <= w0:
                die("the rings' retained spans do not overlap inside the window; enlarge trace_buffer or shorten the window")
        rows = []
        for k, ch in enumerate(meta["chains"]):
            pc = out["per_chain"][k]
            thr_start = int(ch["throttle_start_ns"])
            rate = chain_rate[k]                       # this receiver's rate ...
            period_ns = N / rate * 1e9                  # ... and batch period
            I = inv[inv.chain == k]
            T = I[(I.role == "throttle") & (I["out"] > 0)].sort_values("start_ns")
            if len(T) == 0:
                pc["error"] = "no throttle invocations in the capture"
                continue
            tpos = unwrap32(T.pos.to_numpy())
            tout = T["out"].to_numpy(dtype=np.int64)
            tend = (T.start_ns.to_numpy(dtype=np.int64) + T.dur_ns.to_numpy(dtype=np.int64))
            # every chunk j published, with its publish instant
            j_first = tpos // N
            j_count = tout // N
            total = int(j_count.sum())
            J = np.empty(total, dtype=np.int64)
            PUB = np.empty(total, dtype=np.int64)
            o = 0
            for jf, jc, te in zip(j_first, j_count, tend):
                J[o:o + jc] = np.arange(jf, jf + jc)
                PUB[o:o + jc] = te
                o += jc
            REL = thr_start + ((J + 1) * N / rate * 1e9).astype(np.int64)
            sel = (REL >= w0) & (REL < w1)
            J, PUB, REL = J[sel], PUB[sel], REL[sel]
            last_sample = (J + 1) * N - 1
            done = {}
            begin = {}
            for role in PRE_GATE:
                B = I[(I.role == role) & (I["in"] > 0)].sort_values("start_ns")
                if len(B) == 0:
                    done[role] = np.full(J.size, np.nan)
                    begin[role] = np.full(J.size, np.nan)
                    continue
                bpos = unwrap32(B.pos.to_numpy())
                bin_ = B["in"].to_numpy(dtype=np.int64)
                bs = B.start_ns.to_numpy(dtype=np.int64)
                be = bs + B.dur_ns.to_numpy(dtype=np.int64)
                # the first invocation whose span [pos, pos+in) covers the batch's last sample:
                # positions are non-decreasing, so it is the last one with pos <= last_sample
                idx = np.searchsorted(bpos, last_sample, side="right") - 1
                ok = idx >= 0
                idx_c = np.clip(idx, 0, len(bpos) - 1)
                covered = ok & (last_sample < bpos[idx_c] + bin_[idx_c])
                d = np.where(covered, be[idx_c], np.nan).astype(np.float64)
                b = np.where(covered, bs[idx_c], np.nan).astype(np.float64)
                done[role] = d
                begin[role] = b
            rt = done["syncshort"] - REL
            rt_pub = done["syncshort"] - PUB
            pc["batches_in_window"] = int(J.size)
            pc["batches_matched"] = int(np.isfinite(rt).sum())
            pc["batch_rt"] = stats(rt, period_ns)
            # against this receiver's own class deadline (factor x period): the batch is late for its class
            if period_ns:
                dl = period_ns * pc["deadline_factor"]
                fin = rt[np.isfinite(rt)]
                pc["batch_rt"]["over_class_deadline"] = int((fin > dl).sum())
                pc["batch_rt"]["over_class_deadline_ratio"] = float((fin > dl).mean()) if fin.size else None
                pc["class_deadline_us"] = dl / 1e3
            pc["batch_rt_from_publish"] = stats(rt_pub, period_ns)
            pc["throttle_lag"] = stats(PUB - REL)  # publish - nominal: how late the arrival point itself was
            # a run can finish within 1.05x its air time and still carry a backlog of hundreds of
            # batches (a 0.4 s lag is 0.7 % of a 60 s run): the throttle publishing ten periods late
            # at the 95th percentile is the saturation criterion that matches what the plot shows
            pc["saturated"] = bool(period_ns and pc["throttle_lag"].get("n") and pc["throttle_lag"]["p95_us"] * 1e3 > 10 * period_ns)
            if pc["saturated"]:
                out["saturated"] = True
                out["saturation_reason"] = f"throttle lag p95 {pc['throttle_lag']['p95_us']:.0f} us > 10 batch periods (chain {k})"
            pc["per_block_done"] = {role: stats(done[role] - REL) for role in PRE_GATE}
            pc["per_block_begin"] = {role: stats(begin[role] - REL) for role in PRE_GATE}
            # frame-anchored batch response time: decode instant minus the release of the batch holding the frame's last sample
            L = lat[(lat.chain == k) & (lat.decoded == 1)]
            jf = f_last[L.seq.to_numpy(dtype=np.int64)] // N
            relf = thr_start + ((jf + 1) * N / rate * 1e9).astype(np.int64)
            fsel = (relf >= w0) & (relf < w1)
            pc["frame_batch_rt"] = stats(L.t_decode_ns.to_numpy(dtype=np.int64)[fsel] - relf[fsel])
            for i in range(J.size):
                rows.append((k, int(J[i]), int(REL[i]), int(PUB[i])) + tuple(int(done[r][i]) if np.isfinite(done[r][i]) else "" for r in PRE_GATE) + (float(rt[i] / 1e3) if np.isfinite(rt[i]) else "",))
        cols = ["chain", "batch", "release_ns", "publish_ns"] + [f"done_{r}_ns" for r in PRE_GATE] + ["batch_rt_us"]
        pd.DataFrame(rows, columns=cols).to_csv(os.path.join(rd, "batch_rt.csv"), index=False)

        # ---- utilization (0036 item 12), three parts that must not be confused:
        #   demand   = time in work() calls that moved samples (workExact with in>0 or out>0):
        #              the load itself, scales with the rate -- the planner's and the plot's figure
        #   overhead = calls the scheduler counted as productive that moved nothing (workEnd
        #              with performed == 0, e.g. a decoder polled between frames)
        #   probe    = the unproductive attempts (workProbe aggregates): a spinning worker's cost
        # per worker: the sum over its blocks (worker id from the scheduler-side workEnd records)
        wcsv = os.path.join(rd, "trace_work.csv")
        W = pd.read_csv(wcsv, dtype={"role": str}) if os.path.exists(wcsv) else pd.DataFrame()
        pcsv = os.path.join(rd, "trace_probes.csv")
        P = pd.read_csv(pcsv, dtype={"role": str}) if os.path.exists(pcsv) else pd.DataFrame()
        last_rec = int(inv.start_ns.max())
        win_len  = (min(w1, last_rec) - w0) / 1e9
        if win_len <= 0:
            die("no trace records inside the analysis window")
        out["window_covered_s"] = win_len
        util = {"window_s": win_len, "per_block": [], "per_worker": {}, "per_worker_occupancy": {}, "total_cores_busy": None, "max_worker": None,
                "total_overhead": None, "total_probe": None, "total_occupancy": None}
        worker_of = {}
        if len(W) > 0:
            for ent, grp in W.groupby("entity"):
                worker_of[int(ent)] = int(grp.worker.mode().iloc[0])
        In = inv[(inv.start_ns >= w0) & (inv.start_ns < w1)]
        prod = In[(In["in"] > 0) | (In["out"] > 0)]
        Wn = W[(W.start_ns >= w0) & (W.start_ns < w1)] if len(W) else W
        zero = Wn[Wn.performed == 0] if len(Wn) else Wn
        Pn = P[(P.start_ns >= w0) & (P.start_ns < w1)] if len(P) else P
        per_worker, per_worker_occ = {}, {}
        tot_d = tot_o = tot_p = 0.0
        keys = set(map(tuple, In[["chain", "role"]].drop_duplicates().itertuples(index=False)))
        for (ch, role) in sorted(keys, key=lambda k: (k[0], str(k[1]))):
            g = prod[(prod.chain == ch) & (prod.role == role)]
            z = zero[(zero.chain == ch) & (zero.role == role)] if len(zero) else zero
            q = Pn[(Pn.chain == ch) & (Pn.role == role)] if len(Pn) else Pn
            ent = int(In[(In.chain == ch) & (In.role == role)].entity.iloc[0])
            wk = worker_of.get(ent, -1)
            demand = float(g.dur_ns.sum()) / (win_len * 1e9)
            over = float(z.dur_ns.sum()) / (win_len * 1e9) if len(z) else 0.0
            pr = float(q.total_ns.sum()) / (win_len * 1e9) if len(q) else 0.0
            samples = int(g["in"].sum()) if role != "fsrc" else int(g["out"].sum())
            util["per_block"].append({"chain": int(ch), "role": role, "worker": wk, "calls": int(len(g)), "calls_per_s": len(g) / win_len,
                                      "mean_cost_us": float(g.dur_ns.mean() / 1e3) if len(g) else None, "max_cost_us": float(g.dur_ns.max() / 1e3) if len(g) else None,
                                      "samples": samples, "ns_per_sample": float(g.dur_ns.sum() / samples) if samples else None,
                                      "zero_work_calls": int(len(z)) if len(z) else 0, "probe_calls": int(q["count"].sum()) if len(q) else 0,
                                      "demand": demand, "overhead": over, "probe": pr, "busy": demand})
            per_worker[wk] = per_worker.get(wk, 0.0) + demand
            per_worker_occ[wk] = per_worker_occ.get(wk, 0.0) + demand + over + pr
            tot_d += demand
            tot_o += over
            tot_p += pr
        util["per_worker"] = {str(k): v for k, v in sorted(per_worker.items())}
        util["per_worker_occupancy"] = {str(k): v for k, v in sorted(per_worker_occ.items())}
        util["total_cores_busy"] = tot_d      # demand, the figure the plots and the planner use
        util["max_worker"] = max(per_worker.values()) if per_worker else None
        util["total_overhead"] = tot_o
        util["total_probe"] = tot_p
        util["total_occupancy"] = tot_d + tot_o + tot_p
        out["utilization"] = util

        # ---- EDF: releases, misses, discards
        rcsv, mcsv, scsv = (os.path.join(rd, f) for f in ("trace_releases.csv", "trace_misses.csv", "trace_sync.csv"))
        R = pd.read_csv(rcsv, dtype={"role": str}) if os.path.exists(rcsv) else pd.DataFrame()
        M = pd.read_csv(mcsv, dtype={"role": str}) if os.path.exists(mcsv) else pd.DataFrame()
        S = pd.read_csv(scsv) if os.path.exists(scsv) else pd.DataFrame()
        if len(R) > 0:
            Rn = R[(R.start_ns >= w0) & (R.start_ns < w1)]
            Mn = M[(M.start_ns >= w0) & (M.start_ns < w1)] if len(M) > 0 else M
            per = []
            for (ch, role), grp in Rn.groupby(["chain", "role"], dropna=False):
                mm = Mn[(Mn.chain == ch) & (Mn.role == role)] if len(Mn) > 0 else Mn
                per.append({"chain": int(ch), "role": role, "releases": int(len(grp)), "misses": int(len(mm)),
                            "miss_ratio": float(len(mm) / len(grp)) if len(grp) else None,
                            "max_late_us": float(mm.late_ns.max() / 1e3) if len(mm) else 0.0,
                            "max_queue_depth": int(grp.queue_depth.max())})
            # the source and the throttle carry a 1 us period/deadline on purpose (0036 item 8: always
            # most urgent, paced by the throttle's own clock), so every one of their jobs "misses";
            # the pipeline figure leaves them out
            pipe = ~Rn.role.isin(["fsrc", "throttle"])
            pipe_m = ~Mn.role.isin(["fsrc", "throttle"]) if len(Mn) > 0 else pipe[:0]
            n_rel_p, n_mis_p = int(pipe.sum()), int(pipe_m.sum()) if len(Mn) > 0 else 0
            per_chain_edf = {}
            for x in per:
                if x["role"] in ("fsrc", "throttle"):
                    continue
                d = per_chain_edf.setdefault(x["chain"], {"releases": 0, "misses": 0})
                d["releases"] += x["releases"]; d["misses"] += x["misses"]
            for ch, d in per_chain_edf.items():
                d["miss_ratio"] = d["misses"] / d["releases"] if d["releases"] else None
                if 0 <= ch < len(out["per_chain"]):
                    out["per_chain"][ch]["edf"] = d
            out["edf"] = {"releases": int(len(Rn)), "misses": int(len(Mn)), "miss_ratio_all": float(len(Mn) / len(Rn)) if len(Rn) else None,
                          "releases_pipeline": n_rel_p, "misses_pipeline": n_mis_p, "miss_ratio": float(n_mis_p / n_rel_p) if n_rel_p else None,
                          "job_response": stats(Mn[pipe_m].response_ns.to_numpy()) if n_mis_p else {"n": 0}, "per_block": per, "per_chain": per_chain_edf}
        if len(S) > 0:
            Sn = S[(S.start_ns >= w0) & (S.start_ns < w1)]
            out["sync"] = {"resyncs": int(len(Sn)), "jobs_discarded": int(Sn.discarded.sum()), "list_changed": int((Sn["flags"] & 1).sum())}

    json.dump(out, open(os.path.join(rd, "batch_rt.json"), "w"), indent=2)
    if not a.quiet:
        rt = out.get("realtime_ratio")
        print(f"{rd}: policy {out['policy']} N={N} rate={rate/1e6:g} Msps threads={out['threads']} chains={out['chains']} window {a.warmup}+{a.window} s" + (f"; elapsed/air {rt:.2f}" + (" SATURATED" if out["saturated"] else "") if rt else ""))
        for pc in out["per_chain"]:
            fr = pc["frame_rt"]
            line = f"  chain {pc['chain']}: frames {pc['decoded']}/{pc['frames']} decoded; frame RT (us) n={fr.get('n')} mean={fr.get('mean_us', float('nan')):.1f} std={fr.get('std_us', float('nan')):.1f} min={fr.get('min_us', float('nan')):.1f} max={fr.get('max_us', float('nan')):.1f}"
            if "batch_rt" in pc:
                b = pc["batch_rt"]
                line += f"\n           batch RT (us) n={b.get('n')} mean={b.get('mean_us', float('nan')):.1f} std={b.get('std_us', float('nan')):.1f} min={b.get('min_us', float('nan')):.1f} max={b.get('max_us', float('nan')):.1f} over-period={b.get('over_period')}"
            print(line)
        u = out.get("utilization")
        if u and u.get("total_cores_busy") is not None:
            print(f"  utilization (demand): total {u['total_cores_busy']:.3f} cores; per worker " + ", ".join(f"w{k}={v:.3f}" for k, v in u["per_worker"].items()) + f"; zero-work calls {u['total_overhead']:.3f}, probing {u['total_probe']:.3f}, occupancy {u['total_occupancy']:.3f}; covered {u['window_s']:.1f} s")
        if out.get("edf"):
            e = out["edf"]
            print(f"  EDF: pipeline {e['releases_pipeline']} releases, {e['misses_pipeline']} misses (ratio {e['miss_ratio']}); with source+throttle {e['releases']}/{e['misses']}")
        if out.get("sync"):
            print(f"  re-syncs in window: {out['sync']['resyncs']}, jobs discarded: {out['sync']['jobs_discarded']}")
        if out.get("trace_header", {}).get("lost_count"):
            print(f"  LOST {out['trace_header']['lost_count']} records -- figures understate")


if __name__ == "__main__":
    main()
