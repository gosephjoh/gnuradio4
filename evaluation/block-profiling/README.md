# Block cost-model experiments (July–August 2026, Raspberry Pi 5)

Standalone measurements of GNU Radio 4 per-block execution cost, kept here so they can be
shared and re-run. They are **not** wired into the GR4 build: each program was built against an
installed GR4 with `CMakeLists.standalone.txt` (which also lists targets not included here).
Plots regenerate from `data/` with `plot_linear_regression.py`; only the two summary plots are
committed.

## 1. Per-block linear regression — `gr4_all_blocks_profile.cpp`

Twelve library blocks (AddConst / MultiplyConst / SubtractConst / DivideConst float, AddConst /
MultiplyConst complex, Rotator, fir_filter, iir_filter, Decimator, SavitzkyGolayFilter, Delay),
each in a 3-block graph `CountingSource → block → NullSink` under `scheduler::Simple`.

- x = **total samples processed per run** (2–20 million), y = wall-clock time of `runAndWait()`.
- Data: `data/gr4_all_blocks_profiling.csv`. Fit: `plot_linear_regression.py`
  → `plots/all_blocks_regression_comparison.png`, `plots/summary_throughput_bar_chart.png`.
- Fitted slope = marginal cost per sample, 4.7 ns (MultiplyConst) to 46 ns (fir_filter); R² ≥ 0.99.
- **Caveat:** the intercept (2.5–9 ms) is the fixed cost of a whole *run* — graph construction,
  scheduler start/stop, first touch — not the per-`work()` invocation overhead. This experiment
  measures marginal cost per block; it does not by itself measure the per-call term. Batch size was
  not controlled (default 65 536-sample edge buffers).

## 2. Batch-size sweep — `gr4_complex_scheduler_benchmark.cpp`

Random DAGs of 17–24 FIR blocks, edge buffer size (= batch) swept over
256 / 1 024 / 8 192 / 32 768 / 131 072, 50 runs per point, five stock schedulers.
Data: `data/gr4_batching_sweep_benchmark.csv` (also `*_200runs.csv` from the same program).

- Single-threaded throughput is nearly flat, 5.3–5.7 Msps: −7 % at 256, a slight cache-driven
  decline at 131 072. For a whole graph, batch size barely matters above ~1 000 samples.

## 3. Ticks micro-benchmark — `add_const_micro_ticks.cpp`

`gr4-packet-modem`'s `Add<complex<float>>` with its `pc_work_time_total()` performance counter,
reporting **ticks per item** (November 2025). Single block, single point; no saved results.

## 4. Precursor — `gr4_profile.cpp`

MultiplyConst only, total samples swept; `data/gr4_threshold_performance.csv`.

## 5. The `c(k) = o + k·p` measurement (19 August 2026) — files not recovered

The result that "per-invocation overhead dominates for cheap blocks" came from a separate run:
a 3-block graph under `Simple`, chunk size swept, a synthetic block at 1 / 8 / 64 / 256 flops per
sample. Findings: `o ≈ 1 µs` per graph round (~330 ns per block, within ±6 % across a 256× range of
per-sample work); `p` ≈ 5–6 ns/sample at 1 flop to ~400 ns/sample at 256 flops; floor `o/p` ≈
170–210 samples for a trivial block ("roughly 200"), i.e. ~20× smaller than the GR3 add_const
figure in He & Ward (GRCon 2025, I ≈ 11 081 cycles, Δ ≈ 2.8 cycles/sample). Reproduced twice
that day with independent block implementations. The source and CSV lived in a session scratch
directory that has since been cleared; the numbers above are from the project record. A
reproducible `bm_WorkQuantum` in `core/benchmarks/` is the intended replacement.
