# The end-to-end latency sweep: plan and runbook

*Status: a plan, not yet run. Section 12 lists the questions still open and
the decisions already taken (2026-09-23); open ones are marked ⚠ where they
arise. Every number in this document that is not quoted from a file came from
a script run on 2026-09-23 against the tree at `df99fb1`.*

The sweep repeats the shape of the published which-policy-for-which-workload
sweep (`results/sweep-x86-8core/full/REPORT.md`) with a fourth scheduler
configuration, 30 s runs, eight repeats, isolated worker CPUs, and a
different headline metric: the
end-to-end latency of the data, from the release of the source job that
carried it to the completion of the sink job that delivered it.

## 1. The matrix

| dimension | values | count |
|---|---|---|
| scheduler configuration | RR unbatched, RR, RM, EDF (all N = 1024 except the first), all under the `Simple` scheduler flavour | 4 |
| workload | one receiver, two same-rate receivers, light, medium, heavy | 5 |
| repeats | 8 | 8 |
| duration | 30 s per run | |

That is 4 × 5 × 8 = **160 runs** (confirmed 2026-09-23), 1 h 20 min of pure
run time.

**Wall time.** The previous full sweep ran 150 runs of 30 s in 5 h 50 min:
110 s of overhead per run beyond the run itself (start-up, the cell's 15 MB
manifest parse, the capture's export and analysis, the ring dump). At 30 s
per run that is 160 × 140 s ≈ **6.2 h**. Plan an overnight run.

The repeat index is the outermost loop, as before, so an interrupted sweep is
balanced across cells and `--skip-existing` resumes it.

## 2. Workloads

The five workloads of the previous sweep, unchanged (decided 2026-09-23),
under the names the request uses: medium is the driver's `mid`, heavy its
`knee`. The worker count: one per receiver for the one- and two-receiver
workloads, and for the three mixes **one count for all three, the ceiling of
the heavy workload's total demand** — 2.07 measured, so **3 workers** (decided
2026-09-23; the aim is to get close to full utilisation without
oversubscribing, not to stay far from it).

| request name | driver name | receivers (Msps, graph order) | workers | batch period of the fastest receiver |
|---|---|---|---|---|
| one receiver | `simple1` | 2.5 | 1 | 409.6 µs |
| two same-rate receivers | `simple2` | 2.5, 2.5 | 2 | 409.6 µs |
| light | `light` | 1.25, 1.25, 2.5, 5 | 3 | 204.8 µs |
| medium | `mid` | 1.25, 2.5, 2.5, 5 | 3 | 204.8 µs |
| heavy | `knee` | 2.5, 2.5, 5, 5 | 3 | 204.8 µs |

Batch periods: 819.2 µs at 1.25 Msps, 409.6 at 2.5, 204.8 at 5 (N = 1024).

Load, from the full sweep's trace utilisation (ten repeats per cell, demand =
share of a worker's time inside `work()`, medians): `simple1` 0.34 on its one
worker; `simple2` 0.69 in total, 0.35 on the busier of its two; the mixes at
four workers 1.38 / 1.56 / 2.07 in total for light / mid / knee, busiest
worker 0.43 / 0.49 / 0.55, and no run of the 150 saturated. At **three**
workers the ideal spread is 0.46 / 0.52 / 0.69 per worker; with the striping
imbalance measured at four workers (busiest worker 1.07× the ideal) `knee`'s
busiest worker is expected near **0.74** — the bottom of the 0.75–0.88 band
in which a 2-worker, 3-receiver point of the classes experiment saturated run
to run on this guest. So the heavy workload is meant to run close to full
utilisation, and some of its runs may not keep real time: every run is
checked (§11 step 6), a saturated run is reported separately, never pooled,
and the count per cell is in the table. The three mixes share one worker
count so that their names keep their order (a per-workload ceiling would put
`mid` at 2 workers and 0.78 per worker, above `knee`). At three workers the
block-to-worker assignment differs from the four-worker one the figures come
from (block *i* to worker *i* mod 3), so the per-worker demand is an
estimate; the calibration runs of §11 step 3 give the first reading, and if
`knee` saturates in both of them the count is reconsidered before the sweep.

The stimulus is the 60 s cell `data/rt_300_300_102934_QPSK_1_2_s1` (102 934
back-to-back 300-byte QPSK 1/2 frames, 600 002 286 samples). At 5 Msps that
is 120 s of air, at 2.5 Msps 240 s, so a 30 s run reads at most the first
quarter of it; every run of a workload replays the same frames.

## 3. Scheduler configurations

### 3.1 The four

| name | `--policy` | `--fixed-batch` | what the scheduler does |
|---|---|---|---|
| RR unbatched | `rr` | none | round robin in list order; no batch floor or ceiling anywhere, no periods or deadlines declared; the throttle releases whatever is due at each wake-up, up to `chunk` samples |
| RR | `rr` | 1024 | round robin in list order over fixed 1024-sample batches |
| RM | `rm` | 1024 | static priority by declared period, one rank per distinct period (`2da9088`), ties broken by list position |
| EDF | `edf` | 1024 | dynamic: the ready job with the earliest absolute deadline, deadline = release + N/rate |

"RR unbatched" is the configuration the request calls "RR no batch-size
restrictions": `--policy rr` without `--fixed-batch`, which leaves every
block's `max_batch_size` at GR4's default (unbounded), no `min_samples` on any
input, and — because `chain.cpp` attaches `period`/`relative_deadline` only in
fixed-batch mode — no timing attributes at all. Two things follow. The throttle
then runs in its plain mode (§10: it releases min(due, chunk) per call, with
chunk = 4096, the app's default when `--fixed-batch` does not set it), and the
end-to-end metric must be defined without a batch grid (§8). Decided
(2026-09-23): the unbatched run passes **`--chunk 1024`** and **`--catch-up`**.
With the app's default chunk of 4096 the throttle's timer wakes every
4096/rate — 1.6 ms at 2.5 Msps, 819 µs at 5 Msps — and data waits up to a
wake-up in the throttle before the scheduler sees it, so the configuration's
latency would measure the source's granularity rather than the absence of
batching in the pipeline; at 1024 the wake-up period is the batched runs'
batch period and the only difference is the pipeline's batch contract.
`--catch-up` makes a late call release everything that is due, the semantics
the batched runs have through `whole_chunks`, so a guest stall cannot leave a
backlog shape the batched runs cannot have. The arrival-stamp bias stays
bounded by chunk/rate, the same bound as the batched runs.

### 3.2 Scheduler flavour: `Simple` (decided 2026-09-23, replacing `BreadthFirst`)

Every run uses `gr::scheduler::Simple<multiThreaded, …, Policy>`, the flavour
of every earlier sweep and the one `rx_latency4` instantiates. `BreadthFirst`
was considered first and is kept here for the record, because the evaluation
is what led back to `Simple`. What `BreadthFirst` is, from
`core/include/gnuradio-4.0/Scheduler.hpp` (3134–3210):

- It differs from `Simple` **only in `customInit()`**. The run loop, the work
  dispatch, release tracking, house-keeping and every policy live in
  `SchedulerBase`, shared by both. A scheduler flavour contributes one thing:
  the block list and its partition over the workers.
- `Simple` takes the blocks in registration (construction) order.
  `BreadthFirst` flattens the graph and walks it breadth first from every
  source block (a block with no incoming edge), producing the list in BFS
  level order.
- The partition rule is the same in both: block *i* of the list goes to worker
  *i* mod T (`detail::batchBlocks`, 3110–3120). So BFS neighbours — a producer
  and the consumer it feeds — land on different workers, exactly as
  registration-order neighbours do under `Simple`. `BreadthFirst` does not
  give a worker a connected slice of the graph.
- Every policy works with it (same `TPolicy` parameter, all policy logic is
  in the base). How much of the BFS order survives depends on the policy:
  under **RR** the worker executes its BFS slice verbatim — the only case where
  the flavour changes what runs when; under **RM** each worker's list is
  re-sorted by priority and BFS order is only the tie-break (default
  `tie_break = registrationOrder`, i.e. list position); under **EDF** selection
  is dynamic and BFS order decides only which worker owns which block.
- Two limits: it drops any block not reachable from a source (a pure feedback
  cycle) — not our graph; and it compiles only for `singleThreaded` and
  `multiThreaded` execution — the two we use.

So a flavour changes only which blocks share a worker and, under RR, the
sweep order within a worker; it is not a different scheduler. `BreadthFirst`
is not wired into `rx_latency4` (the app hard-codes the `Simple` aliases,
477–480, and writes `"Simple<multiThreaded>"` into every record), and the
decision of 2026-09-23 is to stay with `Simple` for every workload and
configuration, as in the published sweep, rather than add the option. The
`scheduler` field of every run's `latency_summary.json` and `trace_meta.json`
records the flavour.

**Rotation** (decided 2026-09-23: **1**). The published sweep
passed `--rotate 1`: each receiver's construction order shifted by one slot
so that, with `Simple`'s registration-order list striped over the workers,
the heavy blocks of different receivers land on different workers. That
rationale had been set aside for `BreadthFirst`'s BFS order (rotation 0); it
applies again under `Simple`, so the sweep passes **`--rotate 1`** as the
published sweep did, and the worker assignment is the one the load figures of
§2 were measured with (up to the change from four workers to three). The
actual block-to-worker assignment is recorded from the trace (§6). `simple1`
has one receiver, where rotation is moot.

### 3.3 Settings, and which explicit parameters are still needed

The previous sweep's command line, from `scripts/rt-prelim4`, and what the
commits since have done to each item:

| parameter | previous | now | why |
|---|---|---|---|
| `--fixed-batch 1024` | yes | yes (three of four configs) | unchanged; implies `--batch 1024 --chunk 1024 --buffer 16384` |
| `--sched-ratio 4096` | yes, for every policy | **still yes**, for every policy | the re-sync fix (`9126f2a`, `7af5453`) removed the 45 µs graph rebuild, but at ratio 16 EDF still spends 13.6 % of a worker in message phases against 4 % for RR/RM (`results/sweep-x86-8core-m6-2026-09-23/micro/MICRO.md`). The reason is smaller; it stands. Same ratio for every configuration, unbatched RR included |
| `--max-pass-duration` | did not exist | **new, set for every run** (decided 2026-09-23) | `65afb49` (2026-09-22) added a wall-clock bound on a selection pass whose auto rule is a quarter of the shortest declared period on the worker — 250 ns here, because every block declares a 1 µs period on purpose (§9.1). Set it to a quarter of the fastest receiver's batch period, as M6 does: **102 µs** for `simple1`/`simple2` (2.5 Msps), **51 µs** for the mixes (5 Msps); if the workload set changes, the value follows the fastest receiver of each. It reaches EDF alone; RR sweeps unbounded and RM keeps the count bound. Not varied |
| `--rotate 1` | yes | **1** | its rationale is `Simple`'s registration order, back in force (§3.2) |
| `--rm-tiny-periods` | not passed | not passed | RM's default is the true per-receiver periods; unchanged |
| `--catch-up` | not passed | not passed (batched); **passed** (unbatched) | implied by `whole_chunks` in fixed-batch mode; in the unbatched run it is set explicitly (§3.1) |
| `--tiny-period` | default 1 µs | default | the release gate is unchanged since 2026-09-14 (`SchedulingAnalysis.hpp` 332–430: period = minimum separation, no catch-up), so the tiny explicit periods are still what keeps EDF's gate from pacing the pipeline |
| `--deadline-classes`, `--frame-deadline` | not passed | not passed | equal deadlines: one batch period on every block |
| `tie_break` | did not exist | default (`registrationOrder`) | `77fe5c5` added it; the default reproduces the old behaviour |
| `--trace-categories all --trace-buffer R --trace-limit-mb 1024` | yes | mask and ring **revised**, §7 | the capture is analysed and discarded, as before, but the ring should hold the whole analysis window |
| `--timeout-s` | 6 × run_s + 60 | 240 | |
| `--cpus`, `--rt-prio` | did not exist | **`--cpus 4-7 --rt-prio 10`** on every run | added 2026-09-23 (§13): worker *k* pinned to CPU 4+*k*, one per CPU, under `SCHED_FIFO` 10; needs the isolation of §13 to mean anything |

Nothing that was set before has become unnecessary; one new setting
(`--max-pass-duration`) has become necessary. The default values of every
pre-existing setting are unchanged since the last sweep.

## 4. Per-run command lines

Fixed-batch configurations, a mix (`light` shown):

```
build/rx_latency4 --run-dir data/rt_300_300_102934_QPSK_1_2_s1 --out-dir <run> \
    --policy {rr|rm|edf} --fixed-batch 1024 \
    --threads 3 --chains 4 --rates 1250000,1250000,2500000,5000000 --run-s 30 \
    --rotate 1 --sched-ratio 4096 --max-pass-duration 51 --timeout-s 240 \
    --cpus 4-7 --rt-prio 10 \
    --trace-categories <mask> --trace-buffer <R> --trace-limit-mb <M> --trace-out <run>/trace.gr4trace
```

`simple1`/`simple2` use `--chains 1|2 --threads 1|2 --rate 2500000
--max-samples 75000000` (30 s × 2.5 Msps) and `--max-pass-duration 102`.

The unbatched RR run drops `--fixed-batch 1024` and adds `--chunk 1024
--catch-up`; it keeps `--max-pass-duration`, which RR does not read, so that
every run's record carries the same settings.

Every command the driver issues is logged verbatim (`sweep.log`), and every
run's `latency_summary.json` and `trace_meta.json` carry the values actually
applied (`sched_ratio`, `max_pass_duration_us`, `fixed_batch`, `rotate`,
`scheduler`, …) — those, not this document, are the record of a run.

## 5. Where runs and results go

- Runs: `data/rt_300_300_102934_QPSK_1_2_s1/runs4/e2e-x86-8core/<workload>_<config>_rep<k>/`
  (gitignored; the capture is deleted after analysis, §7).
- Report: `results/sweep-x86-8core-e2e/` — `REPORT.md`, the figures, `TABLE.md`,
  `numbers.json`, `index.json`, `sweep.log` (tracked, as the previous sweep's are).
- Machine stamp in the report's setup section: KVM/QEMU guest, "QEMU Virtual
  CPU version 2.5+", 8 vCPUs, no frequency control, kernel as of the run; the
  commit of the tree; the exact command lines.

## 6. What each run records

From `rx_latency4`: `latency.csv` (one row per frame: the arrival stamps of its
first and last sample at the throttle output, the sink's decode stamp, both
latencies, decoded/correct flags), `latency_summary.json` (settings applied,
per-chain frame accounting, the clock-measured latency quantiles, `result`,
`elapsed_s`), `trace_meta.json` (every block's GR4 name against its role, each
throttle's start anchor, GR4's derived scheduling attributes per block —
period, deadline, priority, batch floor and ceiling, each with its origin —,
the ring statistics `recorded` / `lost` / `rings`), and the capture
`trace.gr4trace`.

From the analysis (`trace-batch-rt.py`, extended per §8): `batch_rt.json`,
`batch_rt.csv` (one row per batch), and the new per-frame end-to-end table.

Added for this sweep: the **block-to-worker assignment**, read from the
capture (every record carries its worker id) and written into
`trace_meta.json` or a sibling file, so that the striping of §3.2 is on
record for every run.

## 7. The capture: analysed, then discarded

Decision (2026-09-23): the capture is not kept. After each run the analysis
extracts the end-to-end latency data points (§8) and the run's other tables,
and the driver deletes `trace.gr4trace` and the per-record `trace_*.csv`
exports, exactly as `rt-prelim4` did. What survives per run is the set of
files in §6 minus the capture.

What still has to be decided about the capture is **how much of the run it
holds**, because that is the analysis window. A capture is one ring per
worker, `--trace-buffer` records of 16 bytes each, capped at
`--trace-limit-mb`; a full ring overwrites its oldest records and counts them
as `lost`. With every category live a worker emits 4–9 M records per second
(M6 of 2026-09-23: 8.6 M/s under RR, 4.1 M under EDF, 8.8 M under RM), so the
previous sweep's 1 GB cap held the last 2–4 s of an RR run and 4–11 s of an
EDF run, and its statistics are over those seconds only.

For a per-frame metric over a 30 s run that is a poor window, and it is not
necessary: the bulk of the records — `workPhase` (bit 7), `workProbe` under
`work` (bit 2), `sweep`/`selectEmpty`/`releaseScan` under `schedulerLoop`/
`select` — is the scheduler's idle probing, which no latency analysis reads.
Decided (2026-09-23): trace only what the analysis uses — `workExact` (bit 8:
per-invocation stream positions and counts, the only way to tie a batch or
frame to an invocation), `release` (bit 4) and `deadline` (bit 6) for EDF's
misses, `lifecycle` (bit 0) for the workers' start — mask **`0x151`**, and size
the ring to hold the whole run with `lost` = 0. The record rate under `0x151`
has not been measured; §11 step 3 calibrates it with one 30 s `knee`/EDF and
one `knee`/RR run (`recorded`, `lost` in `trace_meta.json`) and the driver
takes the ring size from that. Since only one capture exists at a time, its
size is not a disk concern; a few GB per run is fine.

⚠ The metric's own trace needs are not final (§8.1, held): if the
implementation that computes it needs a category not in `0x151`, the mask
grows and the calibration is redone. Every run's `trace_meta.json` states the
mask, `recorded` and `lost`, and the report says what window the analysis
covered.

## 8. The metric, the filter, the figures

### 8.1 End-to-end data latency

Definition requested: from the release of the source block's job to the
completion of the sink block's job that carried the same data. Nothing in the
harness computes exactly that today; the two frame-level quantities that exist
are (`scripts/trace-batch-rt.py`, `harness_blocks.hpp`):

- `lat_last_us` (clock-measured, no trace): from the moment a frame's last
  sample became visible at the throttle output, stamped by the
  `ArrivalStamper` — a second reader on the throttle's output — to the
  `LatencySink`'s `CLOCK_MONOTONIC` stamp of the decoded packet, taken inside
  the sink's invocation.
- `frame_batch_rt` (needs the trace for the throttle's anchor): the same sink
  stamp minus the **nominal release** of the batch holding the frame's last
  sample, `throttle_start + (j+1)·N/rate`.

The proposed metric for this sweep, per frame *f*:

> **E2E(f) = end of the sink invocation that delivered f − nominal release of
> the data that completed f.**

- *Release.* In the fixed-batch configurations the data unit the source
  releases is a batch, and batch *j* is released at its nominal arrival
  instant `throttle_start + (j+1)·N/rate` — the instant at which the throttle
  is entitled to publish it (§10). A frame's release is that of the batch
  containing its last sample, exactly as `frame_batch_rt` anchors it. In the
  unbatched configuration there is no batch grid; the data unit is the sample,
  and the release of frame *f* is the nominal arrival of its last sample,
  `throttle_start + (s_last+1)/rate`. ⚠ This gives the unbatched configuration
  up to one batch period (205–819 µs) of head start *by definition*, which is
  a true property of not batching, not an artefact — but the report must say
  it, and the choice must be agreed.
- *Completion.* The end (`start_ns + dur_ns`) of the `latsink` `workExact`
  invocation that consumed frame *f*'s packet. The sink's own clock stamp is
  taken inside that invocation, so `frame_batch_rt` differs from the proposed
  E2E only by the remainder of one sink call — microseconds. Pairing a frame
  with a sink invocation needs the decoded packets' stream positions
  (`workExact` carries the position and count) matched against the sink's
  per-frame arrival order; a frame that was never decoded has no completion
  and is reported as missing, not as a latency.
- ⚠ **Recommendation:** implement E2E as defined (a ~40-line extension of
  `trace-batch-rt.py`: a `frame_e2e.csv` with `chain, seq, release_ns,
  sink_end_ns, e2e_us`), and report `lat_last_us` beside it as the trace-free
  control; if the extension proves awkward, `frame_batch_rt` extended to the
  unbatched case is the fallback, and the report then says the completion
  instant is the sink's stamp rather than its invocation end.

"Source" here is the **throttle**, not the file source. The file source reads
the file as fast as the buffer allows and is unbounded; it is the throttle that
holds the clock, and its nominal arrival grid is the release of data into the
receiver (§10). The report says so.

### 8.2 Analysis window

With the ring sized to hold the run (§7) the window is no longer dictated by
the rings. Discard the first 5 s after the throttle start (warm-up: the first
frame's latency is recorded separately and is always long) and analyse the
remaining 25 s (`--warmup 5 --window 0`). If the ring turns out to hold less,
the window is whatever it retained and the report says how long that was per
run, as before. The trace-free `lat_last_us` control covers the whole run
regardless.

### 8.3 Whole-process stalls

Same rule as the published sweep (`rt-full-figs.py::flag_stalls`, defaults
`--stall-factor 5 --solo-factor 10 --stall-ms 2`), applied to the per-frame
E2E series instead of the per-batch series: a frame is a *whole-process stall*
when its E2E exceeds 5× the median of its cell **and** every other receiver of
the same run has such a frame released within 2 ms; a one-receiver cell, with
no coincidence to test, drops frames above 10× the median. Repeats are pooled
per cell (workload × configuration) before the medians are taken. The filtered
count, the share, and the raw maximum are reported for every cell, the raw
maximum is drawn as a cross in the figures, and a policy effect that hits one
receiver alone is never cut.

### 8.4 Figures and tables

- **Box plots**, one panel per workload, the four configurations side by side
  per receiver: 25th–75th percentile, median, mean, min–max whiskers without
  stalls, raw maximum as a cross; log axis; the receiver's batch period drawn
  as a dotted line for the batched configurations. The layout of
  `rt-scholarly-figs.py`, with four boxes per receiver instead of three.
- **Tables** (decided 2026-09-23): per cell and receiver, the **mean and
  maximum E2E** (without stalls) as requested, plus the median, p99, the raw
  maximum (before the filter, so the cut is visible), the count of frames,
  the count and share filtered as stalls, and the share of frames later than
  the receiver's batch period. Repeat spread (±1 sd of the eight per-repeat
  means) in a second table, so that a difference between configurations can
  be read against the run-to-run noise of this guest: last time it was
  3–9 µs, and a difference smaller than that is not a finding.
  **On the late share:** a frame later than one batch period is *not* to be
  read as a violation of anything that matters in practice. The batch period
  is the deadline the blocks declare to the scheduler, chosen because it is
  the natural pipeline period, not a requirement of the 802.11p application;
  the late share is reported as a sensitivity of each configuration to its
  own period, one column among the others, and the report says so where it
  first appears.
- Saturated runs (any receiver's throttle lag p95 above ten batch periods)
  are listed and excluded from the pooled statistics, and their count per
  cell is in the table.

## 9. How the blocks' scheduling parameters are derived

Two layers: what `chain.cpp` declares on each block, and what GR4's scheduling
analysis derives from the declarations. Both are recorded per run in
`trace_meta.json` (`analysis[]`, one entry per block with each attribute's
origin); the table below is from the `m6_rm_r16` run of 2026-09-23.

### 9.1 What the chain declares (fixed-batch mode, N = 1024)

| block(s) | `period` | `relative_deadline` | batch floor / ceiling | why |
|---|---|---|---|---|
| file source | 0.25 µs | 0.25 µs | none / unbounded | runs ahead of the pipeline; tiny deadline so EDF runs it first; the smallest period so RM ranks it first |
| throttle | 0.5 µs | 0.5 µs | none / unbounded | same; publishes whole N-chunks only (§10) |
| arrival stamper | 0.75 µs | N/rate | 1 / 1024 | a second reader on the throttle; ranked third by RM |
| the nine pre-gate blocks (`mag2 … div`) | 1 µs (RM: N/rate) | N/rate | **1024 / 1024** | exact-N contract: one batch per call, so batch *j* has one invocation at every block |
| the gate `syncshort` | 1 µs (RM: N/rate) | N/rate | 1 / **2048** | no floor and a 2N ceiling: its state machine stops at a detection, so its positions drift off the grid; an exact-N contract cost a full period per batch (`docs/batch-rt-experiments.md` §6) |
| the frame path (`dly320`, `synclong`, `fft`, `eq`, `decode`, `latsink`) | 1 µs (RM: N/rate) | N/rate | the block's own minimum (64 for the OFDM blocks, 48 for the decoder) / 1024 | no fixed floor: a floor would stall each frame's tail |

The reasoning behind the two unusual choices:

- **Every block declares a tiny explicit `period` (1 µs) instead of the
  natural N/rate.** GR4's release gate treats a declared period as a *minimum
  separation between releases* (sporadic model; `SchedulingAnalysis.hpp`
  332–430, unchanged since 2026-09-14): a block admits at most one release per
  period and can never catch up after a hiccup. With period = N/rate, two of
  the throttle's readers under EDF sat a constant fifteen batches behind it.
  With a 1 µs period the gate never binds and releases are data-driven, while
  the **`relative_deadline` of one batch period** carries the timing EDF
  needs: absolute deadline = release + N/rate, which is what the implicit
  deadline would have been.
- **RM gets the true periods on the pipeline blocks** (`rm_true_periods`,
  the default): period N/rate per receiver, so the rate-monotonic rank orders
  receivers by rate (fastest first), with source, throttle and stamper ranked
  above every receiver by their staggered tiny periods. Since `2da9088` blocks
  of equal period form **one rank** (the run above: priority 6 source, 5
  throttle, 4 stamper, 1 for all of a 1.25 Msps receiver's pipeline), and
  within a rank the tie-break is list position.

The deadline factors (`--deadline-classes`, `--frame-deadline`) multiply the
batch period; both are 1 in this sweep, so every pipeline block's deadline is
its receiver's batch period: 819.2 / 409.6 / 204.8 µs at 1.25 / 2.5 / 5 Msps.

### 9.2 What GR4 derives

For each block GR4's analysis records `period`, `relative_deadline`,
`priority`, `batch_floor`, `execution_ceiling`, `nominal_batch` and, for each,
where it came from. In the fixed-batch runs every period and deadline is
`user_set` (the chain's), the priority is `derived_from_rate` (RM's rank from
the period), the floor is the port's `min_samples`, the ceiling the block's
`max_batch_size`, and the nominal batch is the ceiling where one is set and
`assumed_batch` = 4096 for the unbounded source and throttle. The selection
pass budget, when not given, would be a quarter of the shortest period on the
worker — 250 ns from the tiny periods — which is why §3.3 sets it explicitly.

In the **unbatched** configuration none of the timing attributes is declared
(`chain.cpp` attaches them only when N > 0). The source still declares its
`sample_rate`, so GR4 derives a period of nominal-batch/rate for every block
downstream; under RR nothing reads it. The per-block record in
`trace_meta.json` shows exactly what the run had, with origins `derived_*`
instead of `user_set`.

## 10. The clock-driven input

The input is a file replayed through a throttle; the throttle is the clock.

**`FileSourceRaw`** (`include/gr4ieee80211/FileSourceRaw.hpp`): a synchronous
reader with GR3's `file_source` semantics — reads the raw complex64 file
front to back into the output buffer, no header, host byte order, DONE at end
of file. (GR4's own `BasicFileSource` delivered no samples on this tree.) It
is unbounded in batch size, so it fills the edge buffer ahead of the throttle;
`max_items` cuts the replay at 30 s × rate for the simple workloads
(`--max-samples`; the mixes cut by `--run-s`); `pad_to_multiple` = N and
`pad_min_tail` = max(3N, 2560) append zeros after the file so that the last
frame is flushed through the fixed batches. It declares `sample_rate` so GR4's
analysis knows the rates.

**`Throttle`** (`include/gr4ieee80211/basic_blocks.hpp`): GR3's throttle
re-expressed for a scheduler that must not sleep. It uses GR4's `BlockingSync`
mixin: an internal timer thread wakes the scheduler every chunk period, and
`processBulk` releases what is due. Two modes:

- **Fixed-batch (`whole_chunks`, the three batched configurations).** The
  anchor `_start_ns` is `CLOCK_MONOTONIC` at the throttle's *first
  `processBulk` call* (not at `start()`, which precedes the workers by tens of
  milliseconds). On every call: `due = (now − start)·rate − released`, then
  publish k = min(⌊due/N⌋, ⌊available/N⌋) whole chunks. Chunk *j* (samples
  [jN, (j+1)N)) is therefore nominally complete at `start + (j+1)·N/rate` and
  becomes visible downstream at the first call after that; a late call
  publishes several chunks, so lateness is recoverable (catch-up is implied).
  This nominal instant is the batch's *release* for every metric; the actual
  publish time is recorded beside it, and their difference (`throttle_lag`) is
  the saturation criterion (p95 above ten batch periods).
- **Plain (`catch_up` false, the unbatched configuration).** Each call
  releases `syncSamples(available)`: the samples due by `BlockingSync`'s own
  clock (its `system_clock` start time), capped at `chunk_size` per call, so a
  backlog drains only one chunk per pass. With `--catch-up` the whole backlog
  is released in one call. Decided for the unbatched run (§3.1): `--chunk
  1024` and `--catch-up`. One detail to keep in view: the release grid of
  §8.1 uses `_start_ns` (monotonic, first call), the same anchor as the
  batched runs, while the amount released per call follows `BlockingSync`'s
  clock — the two are anchored within microseconds of each other, and the
  arrival-stamp bias is bounded by one chunk, chunk/rate (410 µs at
  2.5 Msps).

**`ArrivalStamper`**: a second reader on the throttle's output that stamps
`CLOCK_MONOTONIC` when the running sample count crosses each frame's first and
last sample (offsets from the cell's manifest) — the clock-measured "source"
end of `lat_last_us`, independent of the trace.

The throttle's start anchor, the trace's monotonic anchor and the stamper all
use `CLOCK_MONOTONIC`, so every instant in the analysis is on one clock.

## 11. Procedure

1. **Build** as in `docs/m6-runbook.md` §3 (Release, g++-15, `-j4`).
2. **Code changes, before anything is run** (each with a smoke test):
   - the block-to-worker assignment written per run (§6);
   - `trace-batch-rt.py`: the per-frame E2E table (§8.1), for batched and
     unbatched runs;
   - a driver `scripts/rt-e2e4` (from `rt-prelim4`: the four configurations,
     `--run-s 30`, `--repeats 8`, the per-workload pass budget, the mask and
     ring of §7, capture deleted after analysis, `--skip-existing`,
     `index.json`, `sweep.log`);
   - a figure script `scripts/rt-e2e-figs.py` (from `rt-full-figs.py` and
     `rt-scholarly-figs.py`: the stall filter on frames, the box plots, the
     tables, `numbers.json`).
3. **Calibrate the capture** (§7): one 30 s `knee`/EDF and one `knee`/RR run
   under the chosen mask, then read `recorded` and `lost`. Size the ring so
   `lost` = 0 with margin. Also confirms the E2E extension on a real capture
   and gives the first look at the three-worker assignment and load.
4. **Dry run** the driver (`--dry-run`: print every command) and read them.
5. **Run** with the machine otherwise idle and the isolation of §13 in
   place (check its step 4 first), under `nohup`, logging to `sweep.log`;
   the repeat index outermost. Expect ~6 h.
6. **Check every run before reading a number** (`index.json`, then each
   `latency_summary.json` / `batch_rt.json` / `trace_meta.json`): `result` ok;
   per-chain `decoded == frames`, `missing_total` 0, `wrong_payload_total` 0;
   `elapsed_s` within a few percent of 30; trace `lost` 0 (or the retained
   span per run, if the ring held less); `saturated` per receiver; the
   `scheduler` field says `Simple<multiThreaded>`; `worker_cpus` and `rt_prio` say
   what §13 asked for. List the failures and the saturated runs in the
   report.
7. **Analyse and report**: `rt-e2e-figs.py` → `results/sweep-x86-8core-e2e/`;
   the report's setup section states the machine, the commit, the mask, what a
   capture holds, the filter's counts, and the saturated runs; then the
   findings.
8. **Commit** the tracked outputs.

## 12. Decisions and open questions

Settled 2026-09-23:

- **160 runs** (4 configurations × 5 workloads × 8 repeats).
- **30 s per run**, not 60.
- **Captures are analysed and discarded**; only the extracted data points and
  the per-run tables are kept.
- **Settings audit** (§3.3) as written: nothing dropped, `--max-pass-duration`
  added, `--sched-ratio 4096` kept for every configuration.

Held by the human:

- **The metric's implementation**: the human will build the trace
  infrastructure that computes the end-to-end latency before the sweep runs;
  §8.1 is the proposed definition, to be reconciled with that implementation.

Settled 2026-09-23, second round:

- **Unbatched RR**: `--chunk 1024` and `--catch-up` (§3.1).
- **Tables**: mean and max plus median, p99, raw max, counts, late share and
  repeat spread; the late share is not a practical violation (§8.4).
- **`--max-pass-duration`**: 102 / 51 µs per workload, following the fastest
  receiver; not varied (§3.3).
- **Capture**: mask `0x151`, ring sized to hold the run, calibrated before
  the sweep (§7); may grow with the metric's needs.

Settled 2026-09-23, third round:

- **Workloads**: the existing five, unchanged; medium = `mid`, heavy = `knee`;
  workers = receivers (§2).
- **Flavour**: `Simple` for every workload and configuration, replacing the
  earlier `BreadthFirst` decision (§3.2).
- **Rotation**: 1, as the published sweep, since `Simple` is back (§3.2).
- **Workers**: 1 / 2 for `simple1` / `simple2`; 3 for the three mixes, the
  ceiling of `knee`'s total demand, close to full utilisation (§2).
- **Worker isolation**: CPUs 4–7 isolated in the guest, workers pinned one
  per CPU under `SCHED_FIFO` 10, everything else on 0–3 (§13).

Open: nothing beyond the held item above.

## 13. Keeping the workers' CPUs to themselves

The aim: the timing of the worker threads disturbed as little as possible by
kernel threads, interrupts and other processes. What the guest can do for
itself is below; what it cannot do is at the end. State of the guest when this
was written (2026-09-23): kernel `7.0.11-rt` with `PREEMPT_RT`, `NO_HZ_FULL`,
`CPU_ISOLATION`, `RCU_NOCB_CPU` and `IRQ_FORCED_THREADING` compiled in; 8
vCPUs; steps 2 and 3 applied and rebooted; **step 1 not yet in effect**
(`/proc/cmdline` had no `isolcpus`, `/sys/devices/system/cpu/isolated` empty).

The layout: workers need at most 3 CPUs (the mixes), so CPUs **4–7** are the
workers' and **0–3** carry everything else — the driver, the throttle timer
threads, the trace export, kernel threads, interrupts.

### Step 1 — kernel command line (one reboot)

```
sudo cp /etc/default/grub /etc/default/grub.bak
sudo sed -i 's/^GRUB_CMDLINE_LINUX_DEFAULT="quiet splash"$/GRUB_CMDLINE_LINUX_DEFAULT="quiet splash isolcpus=domain,managed_irq,4-7 nohz_full=4-7 rcu_nocbs=4-7 irqaffinity=0-3"/' /etc/default/grub
grep GRUB_CMDLINE_LINUX_DEFAULT /etc/default/grub
sudo update-grub
```

`isolcpus=domain` takes 4–7 out of the scheduler's load balancing (nothing
lands there unless pinned); `managed_irq` keeps managed device interrupts off
them; `nohz_full` stops the periodic tick on a CPU running a single task,
which a pinned busy-polling worker is; `rcu_nocbs` moves RCU callbacks off
them; `irqaffinity` puts every interrupt's default mask on 0–3.

### Step 2 — confine systemd and every session to 0–3

```
sudo sed -i 's/^#CPUAffinity=$/CPUAffinity=0-3/' /etc/systemd/system.conf
grep ^CPUAffinity /etc/systemd/system.conf
```

`isolcpus` does not stop a process that pins itself onto 4–7; this keeps every
service and login session, and so every child, off them by inheritance.

### Step 3 — allow real-time priority for the user

```
printf 'gr3 - rtprio 80\ngr3 - memlock unlimited\n' | sudo tee /etc/security/limits.d/90-gr3-rt.conf
```

Takes effect at the next login.

### Step 4 — reboot and verify

```
sudo reboot
cat /proc/cmdline                            # carries the four options of step 1
cat /sys/devices/system/cpu/isolated         # 4-7
cat /sys/devices/system/cpu/nohz_full        # 4-7
cat /proc/irq/default_smp_affinity           # 0f
ps -eLo psr,comm --no-headers | awk '$1>=4' | sort | uniq -c   # only per-CPU kernel threads (ksoftirqd/N, migration/N, rcuc/N, cpuhp/N, kworker/N:*)
ulimit -r                                    # 80
```

If `isolated` is empty the boot line did not apply: `grep isolcpus
/boot/grub/grub.cfg`.

### Step 5 — pin the workers (in the app, done 2026-09-23)

`rx_latency4 --cpus LIST --rt-prio N` (needs `--threads`): worker *k* is
pinned to the *k*-th CPU of LIST, one worker per CPU (the app refuses fewer
CPUs than workers), and the workers run under `SCHED_FIFO` at N. The sweep
uses `--cpus 4-7 --rt-prio 10`: the mixes' three workers land on 4, 5, 6;
`simple1`/`simple2` on 4 and 4–5. Priority 10 is above every `SCHED_OTHER`
task and below the kernel's `irq/*` and `rcuc/*` threads at 50, so a worker
cannot block its CPU's own kernel work. `worker_cpus` and `rt_prio` are
recorded in every run's `latency_summary.json` and `trace_meta.json`.

Found while adding this: GR4's `BasicThreadPool::updateThreadConstraints()`
applied each worker's affinity mask to the *calling* thread (the helper was
called without its `thread` argument and fell back to `pthread_self()`), so
`setAffinityMask()` had never pinned a worker — the main thread ended up
pinned to the last worker's CPU and the workers roamed. Fixed in
`core/include/gnuradio-4.0/thread/thread_pool.hpp` with a regression test in
`core/test/qa_thread_pool.cpp` that observes each worker's affinity from
inside a task. Verified on this guest: a 3-worker run shows
`default_cpu#0/1/2` allowed on 4 / 5 / 6, policy `FF` priority 10, main and
I/O threads `TS` on 0–3.

### Step 6 — check before trusting a number

During a run: `ps -eLo psr,rtprio,policy,comm | awk '$1>=4'` shows the
workers alone on 4–6 at `FF 10`. After it: `awk '/^ *LOC:/{print $6,$7,$8,$9}'
/proc/interrupts` twice, 30 s apart — the tick counts on 4–6 should advance
by hundreds, not tens of thousands. In the run's own record: the stall
filter's count on its frames (0.007 % last time, in 2–3 ms clusters) and the
trace's per-worker lifetime accounting.

### What the guest cannot do

This is a KVM guest. A vCPU is an ordinary thread on the host and is
descheduled like any other; the 2–3 ms whole-process stalls the filter
removes are most likely that. Removing them needs the host's administrator
to pin vCPUs 4–7 one-to-one to physical cores (`libvirt <cputune><vcpupin>`),
isolate those cores on the host the same way as step 1, pin the emulator and
I/O threads elsewhere (`<emulatorpin>`, `<iothreadpin>`), back the guest's
memory with hugepages, and keep other load off those cores. Until then the
stall filter stays, and the report says which stalls it removed.

### To undo

`sudo cp /etc/default/grub.bak /etc/default/grub && sudo update-grub`,
re-comment `CPUAffinity` in `/etc/systemd/system.conf`, remove
`/etc/security/limits.d/90-gr3-rt.conf`, reboot.
