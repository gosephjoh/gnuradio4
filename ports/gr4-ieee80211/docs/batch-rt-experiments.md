# Batch response time under RR, EDF and RM — how to run the experiments

The experiment of decision 0036 (`docs/decisions/0036-gr4-batch-response-time-experiments.md`
in the GR3 project): the same receiver graph, driven at a fixed batch size N,
under GR4's three scheduling policies, traced with the lightweight-tracing
layer, and read back as **batch response times** — from a batch's nominal
arrival to the gate (`sync_short`) having consumed it — beside the **frame
response times** of 0035 (last sample released → decoded packet), which stay
the GR3-comparable backup.

Nothing in GR4's scheduler was changed for this. The policies are the
scheduler's own `RoundRobinPolicy`, `EdfPolicy` and `RateMonotonicPolicy`
(`core/include/gnuradio-4.0/SchedulingPolicy.hpp`), selected as a template
argument by `rx_latency4 --policy`; the trace records are GR4's own. What
this repository adds is the fixed-batch configuration of *our* blocks, the
flags, one exporter and the offline analysis. The two reference documents
are `RT_SCHEDULING_REFERENCE.md` and `LIGHTWEIGHT_TRACING_REFERENCE.md`
(kept outside the tree, at `/mnt/onr/` on the development machine).

## 0. Build

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-15
cmake --build build -j4 --target rx_latency4 gen4 trace_export
```

Tracing is compiled in by default (`GR4WIFI_TRACING=ON` → GR4's
`GR4_ENABLE_TRACING`); with no category live a marker is one predicted
branch, so the same binary serves traced runs and untraced controls.
`-DGR4WIFI_TRACING=OFF` compiles every marker away. `trace_export` links
GR4's reader, so it needs the same tree.

Python 3 with `numpy`, `pandas` and `matplotlib` for the analysis and plots.

## 1. The moving parts

| Piece | Where | Role |
|---|---|---|
| `rx_latency4 --policy --fixed-batch --trace-*` | `apps/rx_latency4.cpp` | one run: builds R receiver chains, runs them under the policy, writes `latency.csv`, `latency_summary.json`, `trace_meta.json`, the capture |
| fixed batching | `src/chain.cpp` | `min_samples = max_samples = N` on every input of the ten blocks before the gate; throttle in `whole_chunks` mode; source pads its tail; source and throttle unbounded and with a tiny explicit `period` |
| `trace_export` | `apps/trace_export.cpp` | `.gr4trace` → CSV tables (uses `gr::trace::load`, so GR4's own format checks apply) |
| `scripts/trace-batch-rt.py` | | one run → `batch_rt.json` + `batch_rt.csv`: batch RT, frame RT, utilization, EDF misses, re-syncs |
| `scripts/rt-sweep4` | | the matrix from a machine profile: probes, plan, sweep, controls; `sweep_index.csv` |
| `scripts/rt-plan4.py` | | probes → predicted utilization grid → the points nearest the targets (`plan.json`) |
| `scripts/rt-plot4.py` | | a sweep → `batch_rt.png`, `frame_rt.png`, `frame_batch_rt.png`, `edf_misses.png`, `summary.csv` |
| `scripts/rt-paper-figs.py` | | the range figures of a sweep as PDF + PNG, and the numbers they show |
| `scripts/rt-class-figs.py` | | the range figures of the deadline-structure sweep (receiver 0 against the rest, per setting) |
| `scripts/rt-microbench4`, `scripts/trace-edf-oracle.py`, `apps/microbench4.cpp` | | the microbenchmarks: isolated block costs, scheduler cost per invocation, blocking bound, EDF selection oracle, trace overhead |
| `experiments/profiles/*.json` | | per-machine parameters: `x86-8core.json`, `pi5.json` |

### What one run does

`--fixed-batch N` (0036 items 4–9):

- `--batch N --chunk N` are implied, `--buffer 16N` unless given;
- every input port of `mag2 avgpow dly16 conj mul avgcor mag div` requires
  exactly N samples per call (port `min_samples = max_samples`, a mutable
  member — no GR4 change), and `MovingAverage.max_iter` is raised to N so a
  batch fits one call; the gate `syncshort` takes any amount up to 2N (§6
  says why);
- the throttle publishes only whole N-sample chunks, k per call, paced by
  `CLOCK_MONOTONIC` from its first call (`Throttle::whole_chunks`); the file
  source declares the rate as `sample_rate` (so GR4's analysis knows the
  rates) and pads the tail so the last frame flushes (see §6);
- **every block gets an explicit tiny `period`** (1 µs; source 0.25, throttle
  0.5, stamper 0.75 µs so RM ranks them first, in that order) **and an
  explicit `relative_deadline` of one batch period** N/rate (source and
  throttle: as short as their period, so EDF runs them first). The derived
  period would also have been N/rate, but GR4's release gate treats a period
  as a minimum separation between releases, which forbids catching up (§6);
- the source and throttle are excluded from the batch ceiling so they can
  run ahead / catch up.

**Deadline structure** (the EDF-benefit experiment; workload settings, no
scheduler change): `--deadline-classes F0,F1,…` gives receiver *k* a relative
deadline of F*k* batch periods on its blocks before the gate — receiver 0 is
the 802.11p control-channel receiver whose frames are safety messages
(0.25 or 0.5), the others service-channel receivers (1); `--frame-deadline F`
gives the frame path after the gate (`dly320`, `sync_long`, FFT, equaliser,
decoder, sink) of every receiver min(class, F) batch periods. Both default
to 1, which is the equal-deadline sweep. Source and throttle keep their tiny
deadlines. RM is unchanged by either (its ranks come from the tiny periods).

`--policy edf|rm` instantiates `Simple<multiThreaded, null::Profiler,
EdfPolicy|RateMonotonicPolicy>`; `--selection heap` (default) is the EDF
selector; `--sched-ratio`, `--max-outstanding-jobs` are the scheduler
settings of 0036. `--threads T` is the worker count: GR4 stripes the blocks
over the workers (block i to worker i mod T), so a policy orders only one
worker's share.

`--trace-categories all --trace-buffer R --trace-out PATH` stages
`trace_buffer_size` then `trace_categories` on the scheduler (applied before
the first record could be emitted) and dumps the capture after
`runAndWait()`, when the workers are parked. `trace_meta.json` beside it
carries every block's GR4 unique name against our role name, each
throttle's start anchor, the derived scheduling attributes GR4 computed
(period, deadline, RM rank, batch floor and ceiling, provenance) and the
ring statistics (`recorded`, `lost`, `rings`).

### What the analysis computes

`trace-batch-rt.py RUN_DIR --warmup 5 --window 20` (0036 items 1–3, 12, 27):

- **batch j** = input samples `[jN, (j+1)N)`; **release** = throttle start +
  (j+1)·N/rate (nominal arrival of its last sample); **done_block** = end of
  the first invocation of that block whose input span covers sample
  (j+1)N−1, from the `workExact` records' stream positions; **batch RT** =
  done_syncshort − release. Also from publish (the throttle's actual chunk
  time), and per block, so the pipeline's build-up is visible.
- **frame RT** = `lat_last_us` of `latency.csv` (works with no trace);
  **frame batch RT** = decode instant − release of the batch holding the
  frame's last sample.
- **utilization**, measured and split three ways per block: **demand** =
  Σ durations of `work()` calls that moved samples / window (the load;
  scales with the rate; what the planner scales and the plots put on x);
  **overhead** = calls the scheduler counted productive that moved nothing
  (a decoder polled between frames); **probe** = the unproductive attempts
  of a spinning worker (`workProbe`). Per worker the sums over its blocks;
  `total_cores_busy` and `max_worker` are demand, `total_occupancy` is what
  the cores actually did. Per block also ns per input sample.
- **EDF**: releases, deadline misses (implicit deadline = period), miss
  ratio, per block; **re-syncs**: how often the house-keeping re-sync ran and
  how many admitted jobs it discarded (`stateSync` records).
- **saturation**: `elapsed / air > 1.05` marks a run whose graph did not
  keep up. Batch response times are then unbounded and the point is useless
  for the plot; the frame metric there measures a full buffer, not latency.
- Statistics per run: n, min, max, mean, sample std, p50/p95/p99, and the
  count over one batch period.

All times are `CLOCK_MONOTONIC` = `steady_clock` nanoseconds; the capture
header records that the two domains matched.

## 2. Running it

All from this directory. `PROFILE` is `experiments/profiles/x86-8core.json`
or `pi5.json`.

```
# 1. probes: one receiver, one worker, RR, every N, traced, at the profile's probe_rate
./scripts/rt-sweep4 $PROFILE --probe
#    -> <cell>/runs4/sweep-<name>/N*_..._probe*/batch_rt.json and plan.json (rt-plan4 ran)

# 2. the sweep: every planned point x policy x repeat, then the untraced controls
./scripts/rt-sweep4 $PROFILE --plan <cell>/runs4/sweep-<name>/plan.json
#    --dry-run prints the commands; --policies edf,rm and --repeats 1 narrow it;
#    --drop-captures deletes each .gr4trace after its analysis (the CSV tables stay)

# 3. the figures
./scripts/rt-plot4.py <cell>/runs4/sweep-<name> --out-dir results/sweep-<name>
./scripts/rt-plot4.py <cell>/runs4/sweep-<name> --x receivers --gr3 <GR3 run dirs...>   # the backup comparison
```

```
# 4. the deadline-structure sweep (profile["classes"]: points x settings x policies x repeats, 30 s runs)
./scripts/rt-sweep4 $PROFILE --classes
./scripts/rt-class-figs.py <cell>/runs4/sweep-<name>-classes --out-dir results/sweep-<name>/paper

# 5. the microbenchmarks (machine otherwise idle, ~12 min): results/sweep-<name>/micro/MICRO.md
./scripts/rt-microbench4 --out-dir results/sweep-<name>/micro
./scripts/trace-edf-oracle.py <run dir with tables>          # EDF selection check on one run

# 6. the paper-style figures of the main sweep (PDF + PNG) and their numbers
./scripts/rt-paper-figs.py <cell>/runs4/sweep-<name> --out-dir results/sweep-<name>/paper --gr3 <GR3 run dir>
```

`--smoke` (with `--cell data/rt_300_300_10000_QPSK_1_2_s1`, a 5.8 s cell
`gen4` makes in four seconds) runs the probes and a one-point, one-repeat
matrix to prove the pipeline in a few minutes.

The cell of the profile (`rt_300_300_102934`, 60 s of air at 10 Msps,
back-to-back 300 B QPSK 1/2 frames, 4.7 GB) is generated by `gen4` on first
use. Every run replays only the first `run_s × rate` samples of it
(`rx_latency4 --max-samples`; frames beyond are dropped from the tables),
so a run lasts `run_s` seconds at any rate — at 2.5 Msps the whole cell
would take four minutes.

Disk: a traced run leaves about 1 GB of capture and several hundred MB of
per-record CSV; the driver deletes both after the analysis unless
`--keep-captures` / `--keep-tables` are given (the smoke keeps nothing
either). `batch_rt.json`, `batch_rt.csv`, the header, the census and the
entities stay; the EDF release and miss tables go too (a 60 s EDF run
releases millions of jobs — hundreds of MB), their summaries are in
`batch_rt.json`. `--rep-start K` resumes a
sweep at repeat K; the index file is appended.

Time: the x86 plan is 43 points × 3 policies × 3 repeats plus 27 controls
at 60 s each, plus about 40 s of dump, export and analysis per traced
run — roughly 14 hours. Run it detached (`nohup … &`) and read
`sweep_index.csv` as it grows; `rt-plot4.py` works on a partial sweep.

### How the points are chosen

The plan is by **utilization** (0036 items 12–13), not by raw knob: from the
probe's per-block busy fractions, `rt-plan4` predicts the busiest worker's
utilization for every (receivers, rate, workers) of the profile — costs
scale with the rate, blocks are dealt to workers as GR4 deals them — and
keeps, per (N, workers), the point nearest each target 0.3 / 0.5 / 0.7 /
0.85 / 0.95, plus the rate sweep. Every run then measures its own
utilization from its own trace; the plot's x axis is the measured value.

A saturated probe (slower than real time) is refused: its busy fractions
understate the demand. Lower `probe_rate` in the profile.

### Reading the plots

`batch_rt.png`: one panel per N, one line per policy, mean with a ±1 σ band
and min–max whiskers, against the measured utilization of the busiest
worker. `frame_rt.png` the same for the 0035 metric; `frame_rt_vs_gr3.png`
(with `--gr3`) adds GR3 runs against the receiver count, since GR3 has no
trace utilization. `edf_misses.png` the EDF deadline-miss ratio.
`summary.csv` has every number. Repeats at a point are pooled.

## 3. Raspberry Pi 5

Two things differ, one of them a compile-time fact:

1. **No SSE2.** The Viterbi decoder in `wifi_codec.hpp` is upstream's SSE2
   version; on aarch64 the build selects upstream's portable
   `viterbi_decoder_generic` (same arithmetic, byte loops), and
   `CMakeLists.txt` drops `-msse2`. The portable decoder is checked on x86
   with `-DGR4WIFI_GENERIC_VITERBI=ON` and the fixture gate
   (`RX=build-generic/rx_latency4 ./scripts/gate4`, 58 cells).
2. **Less of everything**: 4 cores, 8 GB, slower per core. `pi5.json` sets
   3 workers at most, lower rates (1.25–10 Msps, probe at 1.25), 2 M-record
   rings (64 MB per worker) with a 1 GB ceiling. Run the probe, look at its
   `realtime_ratio` and `total_cores_busy`, and let `rt-plan4` choose; if
   every point at some N saturates, lower the rates in the profile. With
   every trace category live and RM's record rate (§5) a 20 s window may
   not fit the ring: the analysis then slides its window to the retained
   span and says so (`window_shifted`).

Toolchain: GR4 needs GCC 15 (or Clang 20), CMake ≥ 3.27; `vir-simd` is
fetched at configure time. Build with `-j1` or `-j2` on 8 GB: the chain
translation unit takes about 2.4 GB.

## 4. Files a run leaves

| File | Written by | Tracked? |
|---|---|---|
| `latency.csv`, `latency_summary.json` | `rx_latency4` | derived, under `data/` (no) |
| `trace_meta.json` | `rx_latency4` | no |
| `trace.gr4trace` | `rx_latency4` (`--trace-out`) | no (`*.gr4trace`), hundreds of MB |
| `trace_header.json`, `trace_entities.csv`, `trace_kinds.csv`, `trace_invocations.csv`, `trace_work.csv`, `trace_releases.csv`, `trace_misses.csv`, `trace_sync.csv`, `trace_probes.csv` | `trace_export` | no |
| `batch_rt.json`, `batch_rt.csv` | `trace-batch-rt.py` | json yes when copied under `results/` |
| `sweep_index.csv`, `plan.json` | `rt-sweep4`, `rt-plan4` | copy under `results/` |
| `*.png`, `summary.csv` | `rt-plot4.py` | yes, under `results/` |

## 5. What the runs showed (development machine, 2026-09-17/18)

**The full sweep** — `results/sweep-x86-8core/` (README with the tables,
figures, `summary.csv`, `plan.json`, `sweep_index.csv`): 40 planned points
× RR / EDF / RM × 3 repeats plus 27 controls, 389 runs, 14.4 h. In short:
where the graph keeps real time, RR and EDF are within tens of µs of each
other (EDF pays 10–40 µs of release bookkeeping at N = 1024 and misses no
pipeline deadline); RM is the outlier under load, starving the throttle's
readers at two workers / three receivers where the other two keep up, and
carrying the longest tails at six workers; two workers barely help because
the heavy blocks of every chain land on the same worker; one receiver at
10 Msps saturates one worker under every policy. Full numbers in the
results README.

The verification point that preceded it:

The verification point, once the corrections of §6 were in: one receiver,
one worker, N = 1024 at 5 Msps (204.8 µs per batch), every trace category
live, window 3 s + 8 s of the 5.8 s-of-air cell replayed at 11.7 s, all
10 000 frames decoded, run time 1.03× air under all three policies:

| Policy | Batch RT mean | std | min | max | over one period | Frame RT mean | Demand |
|---|---|---|---|---|---|---|---|
| RR | 67.9 µs | 30.6 | 48.8 | 369 | 31 of 39 062 | 242 µs | 0.68 |
| EDF | 76.5 µs | 32.4 | 51.7 | 253 | 297 | 243 µs | 0.68 |
| RM | 128.9 µs | 56.7 | 75.7 | 958 | 2 716 | 295 µs | 0.68 |

EDF missed 2 of 623 258 pipeline deadlines; RR's occupancy was 0.89 and
RM's 0.95 against a demand of 0.68 (the rest is probing). `div` was done
27–79 µs after a batch's nominal arrival, the gate 68–129 µs; the throttle
published 5–14 µs after nominal on average.

- **One worker cannot keep 10 Msps**, traced or not, fixed batch or today's
  variable batches: the 5.8 s cell took 1.29× its air time untraced with
  variable batches, 1.44× traced at N = 1024, and still 1.10× with two
  workers and every category live. So on this machine the 10 Msps /
  1–2 worker corner of the original plan is saturated; the profile's rate
  grid starts at 2.5 Msps and the planner picks by utilization.
- Under saturation the batch response time grows without bound (seconds),
  while the frame response time sits flat at the depth of the buffers
  (~6.4 ms at N = 1024, ~24 ms at N = 4096): the stamper reads a throttle
  that is itself late. Only unsaturated points mean anything.
- **An idle worker is the expensive one for the trace.** At 2.5 Msps on
  one worker every category live produced about 7.6 M records/s (178 M in a
  23 s run): a worker with nothing due spins over its blocks, and each
  unproductive attempt leaves `workBegin` + `workExact` records. The ring
  then holds a second or so per 8 M records; the analysis slides its
  window to the retained span and reports `window_covered_s`. The
  profiles therefore ask for 1 GB rings on x86 (512 MB on the Pi), and the
  statistics of a point are over the batches the ring kept — thousands,
  not the full 20 s. This is the price of 0036 item 25; the mask stays.
- Per-call costs are the same at 2.5 and 10 Msps (`sync_short` 29.9 vs
  29.7 µs per 1024-sample call, `sync_long` 29.0 vs 29.0, `eq` 25.1 vs
  25.0), so scaling demand with the rate is sound. One receiver's demand
  at N = 1024 came to about 0.3 cores per 2.5 Msps, i.e. more than one
  core at 10 Msps, which is why one worker cannot keep up there.
- **Striping puts the heavy blocks on one worker.** `Simple<multiThreaded>`
  deals block i of the flattened graph to worker i mod T. A chain is 18
  blocks, so every chain has the same parity pattern, and with two workers
  `sync_short` (index 11), `sync_long` (13) and `eq` (15) of *every*
  receiver land on worker 1: two receivers at 5 Msps under EDF measured
  0.95 cores of demand on worker 1 against 0.16 on worker 0 and fell behind
  (1.12× air) although the total was 1.1 cores for two workers. The
  planner models the same dealing, so it predicts the imbalance; changing
  it would mean choosing a different graph order, a design decision not
  made. With six workers the pattern spreads (18 mod 6 = 0: each worker
  gets the same three roles of every chain).
- **Record rates with every category live**, one receiver at 10 Msps,
  N = 1024: RR 0.97 M/s, EDF 1.23 M/s, **RM 3.41 M/s** — RM's loop restarts
  from the top after every productive call, so most `work()` attempts are
  unproductive and each still leaves `workBegin` + `workExact` records. A
  16 M-record ring (512 MB) held 4.9 s of RM. Size rings from the probe's
  `trace_kinds.csv`, or shorten the window; never the mask (0036 item 25).
- **RM's ranks among equal periods are arbitrary** (but repeatable). Every
  block after the throttle gets the same derived period N/rate, because
  the analysis knows nothing of the gate or of the 80→64→48 rate changes,
  so RM orders the pipeline by a tie-break, not by rate. Source, throttle
  and stamper rank first (tiny period / free-running). Making RM meaningful
  needs explicit per-block `period` values from the true rates — a design
  decision, not made yet.
- The house-keeping re-sync fired every 16 passes (532 per second under
  RR, 765 under EDF here) and **discarded no job** in any single-worker run: under EDF with
  one worker every admitted job runs within its pass.
- EDF at saturation missed 21 % of its implicit deadlines; RR and RM have no
  deadlines to miss.

## 6. Traps already paid for

- **The development machine is a virtual machine.** `/proc/cpuinfo` reports
  "QEMU Virtual CPU version 2.5+", there is no cpufreq interface, 8 vCPUs.
  Every timing figure here carries the host's scheduling noise; a point
  whose busiest worker is above about 0.75 demand can saturate in one run
  and not in the next (the 2-worker, 3-receiver, 2.5 Msps EDF point:
  0.77 demand and 366 µs in the sweep, 0.87 and saturated in the oracle run
  minutes later). Say "QEMU guest" in every setup section.

- **A derived period is a speed limit.** With the derived period N/rate on
  every block (0036 items 7–8 as first written), EDF at 5 Msps on one
  worker fell behind (1.15× air) with the worker idle 38 % of the time:
  `dly16` and `mul` sat a constant fifteen batches behind the throttle
  while `mag2` kept up. GR4's release gate admits a block at most once per
  period (sporadic model, RT reference 4.4), so a block that falls one
  batch behind during the start-up burst can never catch up, and the
  throttle's full output buffer then capped it at one chunk per call.
  Every block now carries a tiny explicit `period` and an explicit
  `relative_deadline` of one batch period; the deadline semantics are the
  same as the implicit ones, the temporal gate is gone. 0036 items 7 and 8
  are corrected accordingly.

- **The gate must not get the exact-N contract.** With `min = max = N` on
  `sync_short`'s inputs too (0036 item 4 as first written), every batch's
  response time came out one full period above the pipeline's cost (478 µs
  at a 410 µs period, with `div` done 30 µs after arrival). `sync_short`
  stops consuming at a detection, so its input positions drift off the
  batch grid; the exact-N call that covers batch j's last sample then
  cannot run until batch j+1 has arrived. The gate now has no floor and a
  2N ceiling, and "sync_short consumed batch j" is still read from stream
  positions. 0036 item 4 is corrected accordingly (source beats doc).

- The **last frame** of a cell is lost under fixed batching at N ≤ 1024
  unless the source pads more than the file's 448-sample flush tail: the
  pipeline flushes in whole batches. `FileSourceRaw.pad_min_tail` (chain.cpp
  asks for max(3N, 2560)) fixed it for N = 512 and 1024 under all three
  policies; the run is not affected otherwise.
- The throttle's nominal-arrival anchor is its **first call**, not
  `start()`: the workers begin tens of milliseconds after `start()`, and
  anchoring there put a start-up transient into every batch.
- `settings().set()` on the scheduler only stages; the app applies
  (`activateContext` + `applyStagedParameters`) before running, buffer size
  first.
- The scheduling analysis is derived in `init()`, i.e. inside
  `runAndWait()`; reading it after `exchange()` gives an empty table.
- `gr::trace::detail::kindName` — the kind names live in `TraceCatapult.hpp`
  under `detail`.
- `pandas`: a column called `flags` must be indexed `df["flags"]`.
