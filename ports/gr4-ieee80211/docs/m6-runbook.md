# Running M6 on the machine that will report it

M6 is the microbenchmark that measures what a scheduler pass costs: for each
worker, how often it sweeps, how long a sweep, a house-keeping re-sync, a
message phase and a release scan take, and what share of the worker's life
each one claims — next to the batch response time each receiver actually got.
It is the measurement that explained why EDF lost every earlier comparison on
this branch, so it is worth running carefully rather than quickly.

This document is for whoever runs it on the measurement machine. Everything
here assumes a clean checkout of `benchmark-playground` and nothing else.

## 1. What the run consists of

Six runs of 20 s: three policies (RR, EDF, RM) × two values of the
scheduler's `process_stream_to_message_ratio` (16, its default, and 4096).
Each run is the same workload — four 802.11p receivers of different rates
(1.25, 1.25, 2.5 and 5 Msps) on four workers, fixed batches of 1024 samples,
the fastest receiver last in graph order, construction order rotated by one
slot per receiver — with every trace category live.

Allow about five minutes of running and another few of export and analysis.

## 2. Before you start

| Requirement | Why |
|---|---|
| GCC 15 (or Clang 20), CMake ≥ 3.27 | the tree is C++23 |
| Python 3 with `numpy` and `pandas` | the driver analyses the capture; without them it fails at the first import |
| ≈ 2.5 GB RAM for one translation unit | `src/chain.cpp`; build with `-j4` or lower on a small machine |
| ≈ 6 GB free disk | 4.7 GB for the stimulus cell, about 1 GB of capture per run (deleted after each analysis) |
| at least 8 hardware threads | four workers plus the tracing and the driver; fewer and the workers contend with the harness |
| the machine otherwise idle | this is a timing measurement: no other build, no other benchmark, nothing interactive |

Record what the machine is — CPU, core count, kernel, whether it is a virtual
machine, and whether frequency scaling is active. The published M6 ran on a
KVM/QEMU guest with 8 vCPUs and no frequency control, and numbers from
different machines are not comparable.

## 3. Build

```
cd ports/gr4-ieee80211
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-15
cmake --build build -j4 --target rx_latency4 gen4 trace_export microbench4
```

Release, not Debug: the whole point is how long a real pass takes. Tracing is
compiled in by default and costs a predicted branch per marker while no
category is live, so the same binary serves M6 and any untraced control.

## 4. Generate the stimulus cell

M6 replays the 60 s cell the rest of the experiments use. `gen4` writes it in
a few minutes and it is 4.7 GB, so generate it once and keep it:

```
./build/gen4 --out data/rt_300_300_102934_QPSK_1_2_s1 \
    --frames 102934 --payload-range 300,300 --mcs QPSK_1_2 --seed 1
```

Use this cell even though a 20 s run reads only the first fifth of it. The
published numbers came from it, and a shorter cell changes which frames each
receiver sees.

## 5. Run

```
./scripts/rt-microbench4 --out-dir results/sweep-<machine>/micro --only M6
```

Name `<machine>` after the machine, not after the branch — the results
directory is the record of where the numbers came from. The driver writes
`micro.json` and `MICRO.md` there, one `m6_<policy>_r<ratio>/` directory per
run, and `micro.log` with every command it issued.

Do not run the other microbenchmarks in the same invocation if you only want
M6: `--only M6` keeps an existing `micro.json` and replaces its M6 section
alone.

## 6. Check the runs before reading the numbers

A run that did not keep real time measures a backlog, not a schedule. For
each of the six `m6_*/latency_summary.json`:

- `"result": "ok"` — not `"timeout"` or `"error"`.
- `"max_pass_duration_us": 51` and `"sched_ratio"` 16 or 4096 as expected.
  If the budget reads 0, the run took the scheduler's auto rule and §7 says
  why that is wrong here.
- in every `per_chain` entry `decoded` equals `frames`, and at the top level
  `missing_total` and `wrong_payload_total` are 0: every frame replayed was
  decoded and checked byte for byte. (The top-level `frames` is the whole
  60 s cell, 102 934; a 20 s replay decodes 34 308 of them, so
  `decoded_total` is expected to be well below it.)
- `elapsed_s` close to 20: a run that took appreciably longer did not keep up.

Then in each `batch_rt.json`, no receiver should be marked `saturated`. The
light mix at four workers has ample capacity, so a saturated receiver means
something else was running on the machine.

## 7. Why the selection pass is bounded explicitly

The driver passes `--max-pass-duration 51`, a quarter of the fastest
receiver's batch period (1024 / 5 Msps = 204.8 µs). It is not a tuning knob
and should not be varied to make a policy look better.

Left at auto, the scheduler derives that budget from the shortest `period`
declared on the worker. This workload declares 1 µs on every block on
purpose, so that EDF's release gate never paces the pipeline and the real
timing is carried by `relative_deadline` instead (`src/chain.cpp`). The auto
rule would therefore read 250 ns, and every EDF pass would end after a single
`work()` call and return to the backstop release scan — a regime that says
nothing about what a pass costs. Stating the budget gives the rule the period
this workload actually means.

The budget reaches EDF alone: round robin sweeps unbounded and rate monotonic
keeps the count bound. `max_selections_per_pass` is left at its default for
all three.

## 8. Reading the result against the published table

`results/sweep-x86-8core/micro/MICRO.md` holds the earlier M6. Treat a new
run as a **new baseline, not a repeat**, for three reasons:

1. The scheduler has changed since. A selection pass is now bounded in time
   rather than by a count alone, blocks of equal period are treated as one
   rate class, equal priorities are ordered by data flow, and a graph is
   ordered so a producer precedes what it feeds. All of these move the
   quantities M6 reports.
2. The pass budget of §7 is new, and the earlier runs had no equivalent.
3. The earlier runs were on a KVM/QEMU guest; unless this machine is that
   guest, the platforms differ.

What has *not* changed is what M6 was built to expose: the house-keeping
re-sync still rebuilds EDF's release storage on the house-keeping cadence
whether or not the graph mutated. The contrast between ratio 16 and ratio
4096 is therefore still the subject of the measurement, and the quantity to
read first is `stateSync`'s mean duration and busy share under EDF against
the same rows under RR and RM.
