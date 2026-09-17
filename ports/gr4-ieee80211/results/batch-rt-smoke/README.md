# batch-rt-smoke — the pipeline proven, not the experiment

`./scripts/rt-sweep4 experiments/profiles/x86-8core.json --smoke --cell data/rt_300_300_10000_QPSK_1_2_s1`
on 2026-09-17: three probes (one receiver, one worker, RR, N = 1024 / 2048 /
4096 at 2.5 Msps, every trace category live), the plan `rt-plan4` drew from
them (`plan.json`, 43 points), one point under RR, EDF and RM, and one
untraced control — on the 5.8 s cell, so each figure has a single x per
policy. It shows the tooling works end to end; the experiment of 0036 is the
60 s cell and the full plan (`docs/batch-rt-experiments.md` §2).

Numbers (`summary.csv`), N = 1024, 2.5 Msps, one worker, demand 0.34 cores:

| Policy | Batch RT mean | std | max | Frame RT mean |
|---|---|---|---|---|
| RR | 67.2 µs | 30.3 | 351 | 310 µs |
| EDF | 74.1 µs | 31.7 | 464 | 311 µs |
| RM | 101.3 µs | 34.4 | 467 | 354 µs |
| RR, untraced control | — | — | — | 303 µs |

Files: `batch_rt.png`, `frame_rt.png`, `frame_batch_rt.png`,
`edf_misses.png`, `summary.csv`, `plan.json`, `sweep_index.csv`.
