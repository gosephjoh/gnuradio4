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
| four CPUs the workers can have to themselves | on a guest set up as `docs/e2e-sweep-runbook.md` §13 describes (CPUs 4–7 isolated, everything else confined to 0–3) the workers get them only if pinned: pass `--cpus 4-7` (§5). On a machine without that isolation, omit it |
| the machine otherwise idle | this is a timing measurement: no other build, no other benchmark, nothing interactive |

Record what the machine is — CPU, core count, kernel, whether it is a virtual
machine, whether frequency scaling is active, and whether the workers' CPUs
were isolated and the workers pinned. The published M6 and every run up to
2026-09-23 ran on a KVM/QEMU guest with 8 vCPUs, no frequency control, a
generic kernel and no isolation or pinning; from the run of 2026-09-23
onwards the same guest runs an RT kernel (`7.0.11-rt`) with CPUs 4–7
isolated and the workers pinned to them. Numbers from different machines —
or from the same guest before and after that change — are not comparable.

## 3. Build

```
cd ports/gr4-ieee80211
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-15
cmake --build build -j4 --target rx_latency4 gen4 trace_export microbench4
```

Release, not Debug: the whole point is how long a real pass takes. Tracing is
compiled in by default, so the same binary serves M6 and any untraced control.
A marker whose category is off is meant to cost a predicted branch, but a
profile of this workload found the block-side scope constructor compiled out of
line inside `work()`: about 5 % of a round-robin worker with no category live.
An untraced control is therefore not quite a tracing-free one until that is
fixed on `lightweight-tracing`.

The port builds against the GR4 core at `GNURADIO4_DIR`, which defaults to this
checkout. Record the core's commit with the results: M6 measures the scheduler,
so the scheduler's commit is what a number belongs to.

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
./scripts/rt-microbench4 --out-dir results/sweep-<machine>/micro --only M6 --cpus 4-7
```

`--cpus 4-7` pins each run's four workers to CPUs 4, 5, 6, 7, one each,
under `SCHED_OTHER` (`rx_latency4 --cpus`; no real-time priority — see
`docs/e2e-sweep-runbook.md` §13 step 3b for why). Omit it on a machine
without the isolation of §2: pinning workers onto CPUs the rest of the
system also uses gains nothing. Whether a run was pinned is recorded as
`worker_cpus` in its `latency_summary.json` and in `micro.json`.

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
- `worker_cpus` reads `[4, 5, 6, 7]` when the run was meant to be pinned,
  and `rt_prio` 0.

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
all three. Do not set it to 1 for EDF: with exactly one selection per pass, an
EDF run whose source outgrows its output buffer has been seen never to finish.
That is a scheduler defect, not a property of the workload, and two selections
per pass do not show it.

## 8. Reading the result against the published table

`results/sweep-x86-8core/micro/MICRO.md` holds the first published M6, and
`results/sweep-x86-8core-m6-2026-09-23/micro/MICRO.md` the last run before the
per-pass fixes of item 4. Treat a new run as a **new baseline, not a repeat**,
for four reasons:

1. The scheduler has changed since. A selection pass is now bounded in time
   rather than by a count alone, blocks of equal period are treated as one
   rate class, equal priorities are ordered by data flow, and a graph is
   ordered so a producer precedes what it feeds. All of these move the
   quantities M6 reports.
2. The pass budget of §7 is new, and the earlier runs had no equivalent.
3. The earlier runs were on a KVM/QEMU guest; unless this machine is that
   guest, the platforms differ.
4. Every run up to and including the morning of 2026-09-23 predates the
   per-pass fixes this branch now carries (`ea42d87f`, `1e898ad4`,
   `3eb2f59c`, `d09a1e74`, `8fb0ed9f`). §9 says what they change, and which
   of M6's rows they move.
5. From the run of 2026-09-23 (afternoon) onwards the guest itself is
   different: an RT kernel (`7.0.11-rt`, `PREEMPT_RT`) instead of the
   generic one, CPUs 4–7 isolated (`isolcpus`, `nohz_full`, `rcu_nocbs`,
   `irqaffinity=0-3`), every other process confined to 0–3, and the four
   workers pinned one per isolated CPU. The earlier runs shared all eight
   vCPUs with the driver, the throttle timer threads, the trace export and
   the guest's own housekeeping. A report on a pinned run says so beside
   the platform line.

What M6 was built to expose has since been largely fixed. The re-sync that
rebuilt EDF's release storage on every message phase now reuses each worker's
successor lists unless the list or the topology changed (`a1be12a5`). It also
carries admitted jobs across a re-sync that changed nothing (`7af5453c`), and
the `stateSync` marker brackets the re-sync alone (`9126f2ac`); before that it
ran to the end of the message phase. Since then the re-sync no longer works out
a batch floor it then discards (`3eb2f59c`), and the costs that sat on every
pass, outside the message phase, are gone or reduced (§9). So the ratio-16
versus ratio-4096 contrast is no longer the headline. Read the pass period
first, per §9, and `stateSync` second.

## 9. What the numbers mean, and what they do not

### Busy shares are not savings

The workers never idle. A profile of this workload found 0 % of worker samples
in any wait, sleep or yield: every worker spins through passes as fast as it
can. Two consequences follow, and both trip up anyone comparing rows:

- **Overhead shows up as longer passes, not as spare CPU.** What a cheaper
  scheduler buys is earlier release detection and earlier reaction.
- **A busy share can rise because something else got cheaper.** Remove one
  per-pass cost and the time goes into more passes, so every other per-pass
  cost's share goes *up*. In one prototype, the release scan's input read went
  from 13 % to 22 % of a worker while getting no slower per call. Two runs'
  busy shares cannot be subtracted to get a saving.

The figure that means something is the **pass period**, per worker:
`1e6 / (passes per s)` µs. The *mean pass µs* column is the mean `sweep`
duration, which excludes the gap between sweeps, so it is not the period.
Compare worker to worker: round-robin block dealing (below) gives each worker a
different mix of blocks, and so a different pass period.

### The ratio amplifies everything else

`process_stream_to_message_ratio` counts passes, not time. The cheaper a pass,
the more message phases per second, so **the message phase's share rises
whenever passes get cheaper**, even when the phase itself is unchanged. In one
comparison EDF's fastest workers went from 17 % to about 20 % of their time in
message phases while their passes halved in length. Read a change in
`messagePhase` busy share next to the change in pass rate, never on its own.

### Markers see only what they bracket

Every row in `MICRO.md` comes from a trace marker, and a cost outside every
marker is invisible to them. The *mean gap between consecutive passes* is where
such a cost shows up: it is everything between one sweep's end and the next
one's start, the message phase every *n* passes included. Until `ea42d87f`
the gap carried about 1 µs per pass that no marker covered, beyond the message
phase's own amortised share. Most of it was `cleanupRemovedBlocks`, which built
and freed two hash sets on every pass to erase nothing: about 11–14 % of a
worker under every policy, and invisible in every earlier M6 table except as
that gap. With it and the other per-pass fixes now on this branch, the EDF gap
on the profiling machine fell from 1.67–1.95 µs to 0.50–0.62 µs, and the EDF
pass period from 6.0 µs to 3.0 µs, mean of four workers; round robin's period
fell from 5.9 µs to 4.6 µs. Those are single 6 s runs on a different machine
from the published M6, so they show the direction, not the figure a new run
will report. **Expect a new M6 to show a much smaller gap, a higher pass rate
under every policy, and, because of the amplifier above, a higher
message-phase share.** None of that last point is a regression.

When a gap or a total does not add up, profile it (§10) rather than trust the
markers to have covered it.

### Tracing every category is not free

M6 traces everything, and that costs about 17 % of an EDF worker on this
workload: clock reads rose from 3 % to 11.5 % of samples, and the trace layer
from 1 % to 10 %. Each marker's figure includes its own scope's two clock
reads, and the release scan's includes a record per release. So `releaseScan`'s
mean is the *traced* cost of release detection, not its cost. (Since
`d09a1e74` a backstop scan record's first payload word counts the blocks it
actually evaluated, not the length of the worker's list. The driver reads only
durations and counts, but anyone reading a capture directly should know.) For an absolute
cost, use an untraced profile, or a capture of `schedulerLoop` alone, which
adds one scope per pass and is the same in both builds being compared.

### A mean that disagrees with its median

Before `fa7e6d75`, each worker's first message phase absorbed the construction
of that thread's trace ring, about 1 ms. At ratio 4096 a worker sees few
phases, so that one phase set the mean and the p99. The fix is on this branch.
But the check it taught is general: if a kind's mean is far above what its
median would suggest, look for one-off outliers before reading the mean as a
cost.

### Blocks are dealt round-robin across the workers

`rx_latency4` uses `Simple<multiThreaded, …>`, which deals the flattened block
list across the workers in turn: block *j* to worker *j* mod 4. Consecutive
blocks of a chain therefore sit on different workers, and nearly every edge
crosses workers. Two things follow:

- **Event-driven release detection rarely applies.** A producer re-checks only
  consumers on its own worker, so here almost every block is found releasable
  by the per-pass backstop scan. That is why release detection is so large a
  share of an EDF worker on this workload, around a quarter untraced.
- **An optimisation that skips blocks waiting for data barely helps here.**
  `d09a1e74` skips such blocks, but only when their producer shares their
  worker; a block fed from another worker is checked every pass, because its
  producer must not write this worker's state. On this workload each backstop
  scan still evaluated 15.7–17.8 of a worker's 18 blocks. So most of the gain
  from the per-pass fixes on M6 comes from the others. A workload that kept
  each chain on one worker would see a different picture.

## 10. When a number needs explaining: profile it

The markers were designed around questions already asked. For one nobody has
asked yet, sample instead. This is a diagnostic, separate from the M6
measurement itself: it can use a shorter cell, because what it reports is
proportions, not the published numbers.

Build a separate copy with frame pointers, so call stacks survive `-O3`:

```
cmake -S . -B build-prof -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-15 \
    "-DCMAKE_CXX_FLAGS_RELEASE=-O3 -DNDEBUG -g -fno-omit-frame-pointer"
cmake --build build-prof -j4 --target rx_latency4 gen4
./build-prof/gen4 --out data/prof_cell --frames 15000 --payload-range 300,300 --mcs QPSK_1_2 --seed 1
```

Run one cell of M6 under `perf`, with the driver's flags but a 6 s replay, once
untraced and once traced if the tracer's share matters:

```
perf record -F 2999 --call-graph fp -o edf.perf -- ./build-prof/rx_latency4 \
    --run-dir data/prof_cell --out-dir /tmp/edf --chains 4 --threads 4 --policy edf \
    --fixed-batch 1024 --rates 1250000,1250000,2500000,5000000 --run-s 6 --rotate 1 \
    --sched-ratio 16 --max-pass-duration 51 --timeout-s 120
perf script -i edf.perf --no-inline -F comm,tid,ip,sym > edf.stacks.txt
```

Check the run's `latency_summary.json` as §6 describes before reading anything
from it. Then attribute samples on pool-worker stacks by call path (inclusive
counts per scheduler function), and compare policies side by side. Two
practical notes:

- `perf report` with inline expansion can spend tens of minutes in `addr2line`
  on this binary. `--no-inline` is fast.
- A frame's symbol can contain parentheses inside template arguments
  (`(gr::scheduler::ExecutionPolicy)1`), so a script that shortens names must
  cut at the *last* argument list, not the first.

To judge a candidate fix, measure the **pass period** with and without it,
per §9, from a `schedulerLoop`-only capture in both builds. Do not use the
shares from the profile: they move for the reasons §9 gives.
