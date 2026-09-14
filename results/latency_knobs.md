# Why GR4's tail was worse, measured (2026-09-14)

Cell rt_300_300_205867 (300 B, QPSK 1/2, 205 867 packets = 60 s of air at 20 Msps), one receiver, --rate 20e6.
GR3 row: the user's run (runs/rate20000000_chains1). GR4 rows: scratch runs of the same binary with the knobs shown;
CPU column from a `ps -T` snapshot 20 s into the run (per-thread %CPU).

| configuration | elapsed s | p50 us | p95 us | p99 us | max us | CPU |
|---|---|---|---|---|---|---|
| GR3 rx_latency (thread-per-block, 8192-item buffers) | 60.0 | 977 | 1582 | 2119 | 6704 | 4.2 cores (Phase 10 load table) |
| GR4 default: 8 workers, 65536-item buffers | 60.09 | 1253 | 8540 | 10581 | 34704 | 8 workers at 91-98 % |
| GR4 --buffer 8192 | 65.27 (fell behind) | 837 | 2327 | 4053 | 16274 | 8 workers at 92-97 % |
| GR4 --buffer 16384 | 60.07 | 1538 | 3508 | 5562 | 36164 | 8 workers at 91-96 % |
| GR4 --threads 6 | 60.07 | 686 | 7055 | 7355 | 11465 | 6 workers at 99-100 % |
| GR4 --threads 6 --buffer 8192 | 60.05 | 624 | 920 | 1070 | 5755 | 6 workers at 99-100 % |

Throttle diagnostics (latency_summary.json counts): with the GR4 defaults the longest gap between two
throttle work calls was 18.6 ms and the deepest backlog 650k samples (32 ms of air); with 6 workers 3.3 ms.

Reading: GR4's multi-threaded scheduler on this tree busy-polls -- every worker in the pool runs at
~100 % whatever the load -- and a block's work call processes everything in its input buffer (65 536
items = 3.3 ms of air at 20 Msps). With one worker per hardware thread the throttle's timer thread, the
main thread and the OS have to preempt a spinning worker, and a block's turn on its worker comes after
its list-mates' multi-millisecond calls: the throttle is starved for up to 18.6 ms, a backlog builds and
drains slowly, and the slow frames come in long runs (mean 23, max 757 consecutive frames above p95;
GR3: mean 2.4). Two cores left free stop the starvation; GR3-sized buffers (8 192 items, what GR3's
32 KB double-mapped buffers hold) restore fine-grained interleaving. Both together beat GR3 on every
percentile. Smaller buffers alone let the tail collapse but the throttle then cannot sustain 20 Msps
(its call rate is bound to the scheduler's pass rate, ~6.5k calls/s x 4 096 samples).

## The same knobs at 10 Msps (cell rt_300_300_102934, 60 s of air)

| configuration | elapsed s | p50 us | p95 us | p99 us | max us |
|---|---|---|---|---|---|
| GR3, 1 receiver (results/phase10 reference run) | 60.01 | 475 | 1138 | 2105 | 5513 |
| GR3, 2 receivers (load calibration c2_r10) | 60.00 | 748 | 2344 | 3767 | 9258 |
| GR4 default, 1 receiver | 60.07 | 134 | 4086 | 6865 | 17707 |
| GR4 --threads 6 --buffer 8192, 1 receiver | 60.09 | 209 | 356 | 764 | 6256 |
| GR4 --threads 6 --buffer 8192, 2 receivers (chain 0 / 1) | 60.09 | 412 / 407 | 787 / 799 | 1196 / 1192 | 5340 / 5234 |

GR4's six workers read 99-100 % CPU each in every tuned run (busy-polling), one or two receivers
alike; GR3 used 2.3 cores for one receiver and 4.7 for two (results/phase10/load in the GR3 project).
