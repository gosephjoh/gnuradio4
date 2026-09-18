# sweep-x86-8core — the 0036 experiment on the development machine

`./scripts/rt-sweep4 experiments/profiles/x86-8core.json --probe` then `--plan`,
2026-09-17 20:19 UTC to 2026-09-18 10:43 UTC (14.4 h), 389 runs: 3 probes,
40 planned points × RR / EDF / RM × 3 repeats, and 27 untraced controls.
Every run replays the first 60 s (`--max-samples 60 s × rate`) of the 60 s cell
`rt_300_300_102934_QPSK_1_2_s1` with every trace category live, 1 GB rings per
worker; the analysis window is the last 20 s the rings retained (see
`docs/batch-rt-experiments.md` §5 on why an idle worker overflows a ring).

Figures: `batch_rt.png` (batch response time, mean with ±1 σ band and min–max
whiskers, against the measured demand of the busiest worker, one panel per N;
saturated points left out and counted in the footer),
`frame_rt.png` and `frame_batch_rt.png` (the 0035 frame metric, two anchors),
`edf_misses.png` (EDF pipeline deadline-miss ratio); `by-receivers/` the same
against the receiver count, with the GR3 60 s run as the fourth series in
`frame_rt_vs_gr3.png`. `summary.csv` has every pooled number, `plan.json` the
points and their predicted demand, `sweep_index.csv` every run.

## Batch response time (µs), repeats pooled

Rows are the planned points; "saturated" = the graph did not keep real time
there (batch response times grow without bound), "no batch completed" = the
throttle stalled so long that no batch reached the gate inside the window
(the same regime). Demand is measured, not predicted.

| N | workers | rx | Msps | demand of busiest worker | RR mean / std / max | EDF mean / std / max | RM mean / std / max | EDF pipeline misses |
|---|---|---|---|---|---|---|---|---|
| 1024 | 1 | 1 | 2.5 | 0.34 | 66 / 29 / 230 | 74 / 32 / 371 | 101 / 33 / 500 | 0.0000 |
| 1024 | 1 | 1 | 5 | 0.67 | 67 / 29 / 397 | 75 / 32 / 455 | 117 / 49 / 1348 | 0.0000 |
| 1024 | 1 | 1 | 10 | 0.88 | saturated | saturated | no batch completed | 0.0018 |
| 1024 | 1 | 1 | 20 | 0.88 | no batch completed | no batch completed | no batch completed | 0.0922 |
| 1024 | 1 | 3 | 2.5 | 0.87 | saturated | saturated | no batch completed | 0.0000 |
| 1024 | 2 | 1 | 2.5 | 0.24 | 63 / 8 / 605 | 82 / 11 / 469 | 66 / 8 / 431 | 0.0000 |
| 1024 | 2 | 1 | 5 | 0.50 | 68 / 72 / 3139 | 88 / 50 / 2342 | 66 / 6 / 268 | 0.0001 |
| 1024 | 2 | 3 | 2.5 | 0.75 | 225 / 64 / 634 | 366 / 87 / 1109 | saturated | 0.0000 |
| 1024 | 2 | 4 | 2.5 | 0.94 | saturated | saturated | no batch completed | 0.0000 |
| 1024 | 6 | 2 | 10 | 0.76 | 193 / 92 / 1228 | 598 / 2465 / 31671 | 548 / 1864 / 30641 | 0.0020 |
| 1024 | 6 | 3 | 2.5 | 0.29 | 119 / 82 / 3454 | 177 / 64 / 2494 | 95 / 42 / 2834 | 0.0000 |
| 1024 | 6 | 3 | 5 | 0.57 | 179 / 83 / 3427 | 233 / 92 / 3331 | 173 / 115 / 5806 | 0.0001 |
| 2048 | 1 | 1 | 2.5 | 0.33 | 144 / 68 / 1047 | 150 / 67 / 756 | 181 / 72 / 935 | 0.0000 |
| 2048 | 1 | 1 | 5 | 0.64 | 143 / 72 / 1229 | 149 / 66 / 492 | 178 / 69 / 621 | 0.0000 |
| 2048 | 1 | 1 | 10 | 0.92 | saturated | saturated | no batch completed | 0.0000 |
| 2048 | 1 | 1 | 20 | 0.92 | saturated | no batch completed | no batch completed | 0.0619 |
| 2048 | 1 | 2 | 2.5 | 0.65 | 254 / 146 / 705 | 292 / 154 / 992 | 492 / 200 / 2776 | 0.0000 |
| 2048 | 1 | 3 | 2.5 | 0.91 | saturated | saturated | no batch completed | 0.0000 |
| 2048 | 2 | 1 | 2.5 | 0.24 | 109 / 10 / 519 | 132 / 13 / 928 | 111 / 12 / 679 | 0.0000 |
| 2048 | 2 | 2 | 2.5 | 0.47 | 219 / 94 / 992 | 219 / 67 / 1848 | 223 / 100 / 1087 | 0.0000 |
| 2048 | 2 | 2 | 5 | 0.96 | saturated | saturated | saturated | 0.0000 |
| 2048 | 2 | 3 | 2.5 | 0.70 | 305 / 114 / 1248 | 401 / 186 / 3265 | 486 / 1242 / 34699 | 0.0000 |
| 2048 | 6 | 2 | 10 | 0.73 | 376 / 427 / 5343 | 327 / 336 / 8156 | 454 / 1002 / 13306 | 0.0003 |
| 2048 | 6 | 3 | 2.5 | 0.28 | 214 / 96 / 3049 | 255 / 88 / 3246 | 176 / 65 / 1469 | 0.0000 |
| 2048 | 6 | 3 | 5 | 0.56 | 240 / 105 / 2282 | 321 / 86 / 3573 | 211 / 80 / 3276 | 0.0000 |
| 2048 | 6 | 3 | 10 | 0.97 | no batch completed | saturated | saturated | 0.0039 |
| 2048 | 6 | 4 | 5 | 0.74 | 452 / 130 / 3578 | 562 / 642 / 9018 | 355 / 462 / 20442 | 0.0004 |
| 4096 | 1 | 1 | 2.5 | 0.32 | 337 / 120 / 1220 | 341 / 115 / 751 | 374 / 120 / 1753 | 0.0000 |
| 4096 | 1 | 1 | 5 | 0.64 | 331 / 122 / 1436 | 340 / 115 / 730 | 374 / 119 / 984 | 0.0000 |
| 4096 | 1 | 1 | 10 | 0.94 | saturated | saturated | no batch completed | 0.0000 |
| 4096 | 1 | 1 | 20 | 0.94 | saturated | saturated | no batch completed | 0.0904 |
| 4096 | 1 | 3 | 2.5 | 0.93 | saturated | saturated | saturated | 0.0000 |
| 4096 | 2 | 1 | 2.5 | 0.24 | 208 / 35 / 1872 | 227 / 21 / 1486 | 210 / 23 / 1242 | 0.0000 |
| 4096 | 2 | 1 | 5 | 0.48 | 203 / 15 / 1007 | 226 / 17 / 1045 | 205 / 15 / 976 | 0.0000 |
| 4096 | 2 | 1 | 10 | 0.97 | saturated | saturated | saturated | 0.0000 |
| 4096 | 2 | 3 | 2.5 | 0.70 | 540 / 179 / 2127 | 629 / 253 / 2996 | 699 / 307 / 2523 | 0.0000 |
| 4096 | 6 | 2 | 10 | 0.74 | 634 / 887 / 14805 | 549 / 237 / 3862 | 506 / 260 / 8423 | 0.0002 |
| 4096 | 6 | 3 | 2.5 | 0.28 | 381 / 110 / 2343 | 450 / 170 / 6232 | 363 / 138 / 3234 | 0.0000 |
| 4096 | 6 | 3 | 5 | 0.56 | 437 / 137 / 2892 | 472 / 133 / 1828 | 381 / 210 / 9265 | 0.0000 |
| 4096 | 6 | 3 | 10 | 0.97 | no batch completed | saturated | no batch completed | 0.0013 |

## Later experiments

- `prelim/REPORT.md` — the ten-minute preliminary matrix (2026-09-18): which
  policy for which workload — RR for the simple one, EDF where receivers differ
  and a choice must be made, RM for the fastest receiver.
- `multirate/REPORT.md` — four workers, four receivers of different rates
  (2026-09-18): where EDF wins once the house-keeping ratio is raised, with
  the M6 pass-cost microbenchmark that explains every EDF loss in the tables
  below (all measured at ratio 16).
- `paper/RESULTS.md` — the paper-style write-up (microbenchmarks, the
  deadline-structure experiment, §3b the multi-rate summary).

## What it says

- Where the graph keeps up, **RR and EDF sit within tens of µs of each
  other**, EDF paying its release bookkeeping (10–40 µs at N = 1024, less in
  proportion at larger N), and **EDF's pipeline deadline misses stay at zero**
  until the point saturates.
- **RM is the odd one out under load.** Its ranks among the equal-period
  blocks are a tie-break, so with two workers and three receivers it starved
  the throttle's readers and saturated where RR and EDF did not; with six
  workers its tails are the longest.
- **Two workers barely help this graph**: GR4 deals the heavy blocks of every
  chain to the same worker (18 blocks per chain, odd indices), so the busiest
  worker carries nearly the whole load (see `docs/batch-rt-experiments.md` §5).
- The floor of the batch response time is the pipeline's own cost per batch
  (≈ 50 µs at N = 1024, 90 at 2048, 180 at 4096); the mean at low load sits
  just above it, and at six workers the cross-worker hops add 50–150 µs.
- One receiver at 10 Msps saturates one worker whatever the policy; the
  20 Msps rate-sweep points are there to show the unbounded regime.

## Untraced controls (0036 item 26)

Same binary, `trace_categories = 0`, at the top predicted-demand point per N;
frame response time only (no capture). Repeats pooled. The planner's top
point per N is the 20 Msps single-worker one, which is saturated, so these
controls compare the two binaries in the overload regime only (tracing adds
5–13 % to a frame time that is itself a full buffer). The unsaturated
comparison is the smoke's: 303 µs untraced against 310 µs traced at
2.5 Msps (`results/batch-rt-smoke`). A control at an unsaturated point
needs the profile's `controls` rule changed; not done.

| N | policy | workers | rx | Msps | untraced frame RT mean | traced frame RT mean | saturated |
|---|---|---|---|---|---|---|---|
| 1024 | edf | 1 | 1 | 20 | 2042 µs | 2305 µs | yes |
| 1024 | rm | 1 | 1 | 20 | 4877 µs | 6015 µs | yes |
| 1024 | rr | 1 | 1 | 20 | 6079 µs | 6474 µs | yes |
| 2048 | edf | 1 | 1 | 20 | 3893 µs | 4372 µs | yes |
| 2048 | rm | 1 | 1 | 20 | 9064 µs | 10326 µs | yes |
| 2048 | rr | 1 | 1 | 20 | 12048 µs | 12497 µs | yes |
| 4096 | edf | 1 | 1 | 20 | 7968 µs | 8596 µs | yes |
| 4096 | rm | 1 | 1 | 20 | 17414 µs | 18883 µs | yes |
| 4096 | rr | 1 | 1 | 20 | 24192 µs | 24958 µs | yes |
