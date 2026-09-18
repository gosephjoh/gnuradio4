#!/usr/bin/env python3
"""trace-edf-oracle.py -- did the EDF worker run the earliest deadline among its ready jobs?

    ./scripts/trace-edf-oracle.py RUN_DIR [--warmup S] [--window S] [--tolerance-us T]

Replays a run's trace: every `jobRelease` pushes a job (release instant, absolute
deadline = release + relative deadline as recorded) on its block's FIFO; every
`workExact` of a block pops that block's front job (under EDF each job-backed
work() call retires the front job, productive or not).  At the instant a job
starts, the jobs at the front of the other queues on the same worker with a
release before that instant are the ready set; if one of them has an earlier
absolute deadline than the job that ran (by more than the tolerance), that start
is an inversion.  Reported over all starts, and split by whether the waiting job
belonged to a tighter class (smaller relative deadline) than the running one.
Only EDF leaves release records, so this is a check of EDF's selection, not a
comparison with RR or RM.  Needs the per-record tables (run rt-sweep4 with
--keep-tables, or trace_export by hand).
"""
import argparse, collections, json, os, sys
import numpy as np
import pandas as pd

UNSET = 0xFFFFFFFE


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--warmup", type=float, default=5.0)
    ap.add_argument("--window", type=float, default=20.0)
    ap.add_argument("--tolerance-us", type=float, default=1.0)
    a = ap.parse_args()
    rd = a.run_dir.rstrip("/")
    meta = json.load(open(os.path.join(rd, "trace_meta.json")))
    R = pd.read_csv(os.path.join(rd, "trace_releases.csv"), dtype={"role": str})
    I = pd.read_csv(os.path.join(rd, "trace_invocations.csv"), dtype={"role": str})
    W = pd.read_csv(os.path.join(rd, "trace_work.csv"), dtype={"role": str})
    if len(R) == 0:
        sys.exit("trace-edf-oracle: no jobRelease records (not an EDF run, or the release category was off)")
    worker_of = {int(e): int(g.worker.mode().iloc[0]) for e, g in W.groupby("entity")}
    t0 = min(c["throttle_start_ns"] for c in meta["chains"] if c["throttle_start_ns"])
    w0 = t0 + int(a.warmup * 1e9)
    w1 = w0 + int(a.window * 1e9) if a.window > 0 else np.iinfo(np.int64).max
    # both tables must be retained over the same span: use the later start and the earlier end
    lo = max(int(R.start_ns.min()), int(I.start_ns.min())) + int(0.2e9)
    hi = min(int(R.start_ns.max()), int(I.start_ns.max()))
    w0 = max(w0, lo)
    w1 = min(w1, hi) if hi > w0 else w1
    R = R[(R.start_ns >= w0) & (R.start_ns < w1)]
    I = I[(I.start_ns >= w0) & (I.start_ns < w1)]
    rel = R.rel_deadline_ns.to_numpy(dtype=np.int64)
    deadline = np.where(rel == UNSET, np.iinfo(np.int64).max, R.start_ns.to_numpy(dtype=np.int64) + rel)
    ev = pd.concat([
        pd.DataFrame({"t": R.start_ns.to_numpy(dtype=np.int64), "kind": 0, "entity": R.entity.to_numpy(), "deadline": deadline, "rel": rel}),
        pd.DataFrame({"t": I.start_ns.to_numpy(dtype=np.int64), "kind": 1, "entity": I.entity.to_numpy(), "deadline": 0, "rel": 0}),
    ]).sort_values(["t", "kind"], kind="stable")
    queues = collections.defaultdict(collections.deque)
    role = {int(e): (str(r), int(c)) for e, r, c in R[["entity", "role", "chain"]].drop_duplicates().itertuples(index=False)}
    by_worker = collections.defaultdict(set)
    for e, w in worker_of.items():
        by_worker[w].add(e)
    tol = int(a.tolerance_us * 1e3)
    starts = inversions = class_inv = pipeline_starts = pipeline_inv = 0
    no_job = 0
    wait_ns = []
    per_role_inv = collections.Counter()
    per_role_starts = collections.Counter()
    for t, kind, e, dl, rl in ev.itertuples(index=False):
        e = int(e)
        if kind == 0:
            queues[e].append((int(t), int(dl), int(rl)))
            continue
        q = queues[e]
        if not q:
            no_job += 1
            continue
        rt, rd_, rr = q.popleft()
        starts += 1
        rname = role.get(e, ("?", -1))[0]
        per_role_starts[rname] += 1
        is_pipe = rname not in ("fsrc", "throttle")
        pipeline_starts += is_pipe
        worst = None
        for o in by_worker.get(worker_of.get(e, -1), ()):
            if o == e or not queues[o]:
                continue
            ot, od, orl = queues[o][0]
            if ot <= t and od + tol < rd_:
                if worst is None or od < worst[1]:
                    worst = (o, od, orl, ot)
        if worst is not None:
            inversions += 1
            per_role_inv[rname] += 1
            pipeline_inv += is_pipe
            if worst[2] < rr:
                class_inv += 1
            wait_ns.append(t - worst[3])
    out = {"run_dir": rd, "window_ns": [int(w0), int(w1)], "starts": starts, "starts_without_release": no_job, "inversions": inversions,
           "inversion_ratio": inversions / starts if starts else None, "inversions_waiting_job_tighter_class": class_inv,
           "pipeline_starts": pipeline_starts, "pipeline_inversions": pipeline_inv, "pipeline_inversion_ratio": pipeline_inv / pipeline_starts if pipeline_starts else None,
           "inverted_wait_us": {"n": len(wait_ns), "mean": float(np.mean(wait_ns) / 1e3) if wait_ns else None, "max": float(np.max(wait_ns) / 1e3) if wait_ns else None},
           "per_role": {r: {"starts": per_role_starts[r], "inversions": per_role_inv[r]} for r in per_role_starts}}
    json.dump(out, open(os.path.join(rd, "edf_oracle.json"), "w"), indent=2)
    print(f"{rd}: {starts} job starts, {inversions} inversions ({out['inversion_ratio']:.2e}); pipeline {pipeline_inv}/{pipeline_starts} ({out['pipeline_inversion_ratio']:.2e}); waiting job of a tighter class in {class_inv}; starts without a release record {no_job}")


if __name__ == "__main__":
    main()
