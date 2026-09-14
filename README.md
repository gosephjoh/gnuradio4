# gr4-ieee80211 — the GR4 port of the Phase 10 response-time graph

The GNU Radio 4 twin of `~/gr3-ieee802-11-project`'s `rx_latency`: the
gr-ieee802-11 receiver, block for block, replaying the **same sample files**
the GR3 harness generates, behind the same throttle, with the same arrival
and decode stamps and the same output files. Response times and CPU load are
then compared packet for packet between the two runtimes.

- GR4: `~/gnuradio4`, fork `gosephjoh/gnuradio4`, branch `modular-scheduling`,
  commit `29320de`, consumed as a CMake subdirectory (nothing of its tests,
  examples, benchmarks or plugins is built).
- Upstream receiver: `~/gr-ieee802-11` at `ad0598e`, ported to C++23 GR4
  blocks under `include/gr4ieee80211/` (GPL-3.0-or-later, as upstream).
- `docs/port-notes.md`: the block mapping, every departure from upstream,
  the two GR4 stock blocks that did not work here, and the arithmetic
  parity statement.

## Build

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-15
cmake --build build -j1 --target rx_latency4      # ~6 min the first time; one TU needs 2.4 GB
```

## Quick start — generate samples, run, read the response times

All commands run from this folder (`cd ~/gr4-ieee80211`). The GR3 project
must be built too (`~/gr3-ieee802-11-project/build/`): it generates the
samples, and its `compare` tool is the correctness judge.

### 1. Generate the samples

One command takes the same levers as GR3's `rt-run` and makes (or reuses)
the sample file, so GR3 and GR4 replay identical bytes:

```
./scripts/rt-run4 MIN MAX RATE CHAINS [--packets N] [--mcs NAME] [--seed S] [--chunk N] [--deadline-ms F] [--run]

./scripts/rt-run4 8 1500 10e6 1 --packets 1000       # payloads 8..1500 B, 1000 packets, real time, one receiver
./scripts/rt-run4 300 300 10e6 2 --packets 102934    # one payload size, 60 s of air, two receivers
```

- `MIN MAX` payload bytes, drawn uniformly per packet; `MIN == MAX` is one size (8..1500).
- `RATE` throttle in samples/s: `10e6` = 802.11p real time, `20e6` twice that, `0` unthrottled.
- `CHAINS` concurrent receivers on the same file in one process.
- The cell lands in `~/gr3-ieee802-11-project/data/phase10/rt_<MIN>_<MAX>_<N>_<MCS>_s<seed>/`
  (4.8 GB and ~65 s per 60-second cell, once; reused afterwards). Without `--run`
  the script only generates and **prints the commands** below, so you can start
  `htop -t` in another terminal first.

To generate by hand instead (what `rt-run4` calls):

```
( cd ~/gr3-ieee802-11-project && ./scripts/rt-gen 300 300 102934 --out data/phase10/rt_300_300_102934_QPSK_1_2_s1 )
```

### 2. Execute

Either add `--run` to the `rt-run4` line, or run the receiver line it printed:

```
./build/rx_latency4 --run-dir ~/gr3-ieee802-11-project/data/phase10/rt_300_300_102934_QPSK_1_2_s1 \
    --input ~/gr3-ieee802-11-project/data/phase10/rt_300_300_102934_QPSK_1_2_s1/rx_stimulus.cf32 \
    --out-dir ~/gr3-ieee802-11-project/data/phase10/rt_300_300_102934_QPSK_1_2_s1/runs4/rate10000000_chains2 \
    --rate 10000000 --chains 2 --chunk 4096 --deadline-ms 100
```

While it runs, `htop -t` shows the GR4 scheduler's worker threads
`default_cpu#1..8` (one per hardware thread on this box), the IO-pool threads
`default_io#k` that run the throttles' timers, and the main thread. GR4 is
not thread-per-block, so there is no thread per receiver block as with GR3;
the per-block picture comes from the stage counters in `latency_summary.json`.
The run ends by itself at the end of the file; a per-chain line is printed:

```
rx_latency4: chain 0 -- 102934/102934 decoded, 0 missing, 0 dup, 0 unpairable, 0 wrong payload;
             last->decode us: first 1131.0 p50 1188.8 p95 3886.2 p99 5121.1 max 6257.2; 0 over 100 ms; ...
```

### 3. Look at the response times

Outputs are in `<cell>/runs4/rate<R>_chains<C>/`, beside GR3's `runs/` for the
same configuration:

```
OUT=~/gr3-ieee802-11-project/data/phase10/rt_300_300_102934_QPSK_1_2_s1/runs4/rate10000000_chains2

# the summary: per chain decoded/frames, p50/p95/p99/max of last-sample-to-decode, deadline misses
python3 -c "import json; s=json.load(open('$OUT/latency_summary.json')); [print('chain %d: %d/%d decoded (%s wrong payload), last->decode us p50 %.0f p95 %.0f p99 %.0f max %.0f, %d over %g ms' % (c['chain'], c['decoded'], c['frames'], c['wrong_payload'], c['p50_us'], c['p95_us'], c['p99_us'], c['max_us'], c['deadline_misses'], s['latency']['deadline_ms'])) for c in s['per_chain']]"

# every packet: chain,seq,length,t_first_ns,t_last_ns,t_decode_ns,lat_first_us,lat_last_us,decoded,correct
head -5 $OUT/latency.csv

# GR3 vs GR4 on the same cell, packet for packet (run GR3's ./scripts/rt-run for the same levers first)
python3 scripts/compare-runs.py ${OUT/runs4/runs}/latency.csv $OUT/latency.csv
```

`lat_last_us` is the headline number: the frame's last sample released by
the throttle to the decoded packet, in microseconds. `lat_first_us` adds the
frame's air time. `correct` is 1 when the decoded payload matched
`payloads.bin`. `latency_summary.json` also carries per-stage counts
(`sync_short_detections`, `sync_long_frames`, `signal_ok`, `crc_failed`).

### Packet counts for 60 seconds, and the load configurations GR3 measured

The replay lasts `total_samples / RATE`; these counts give 60 s at 10 Msps
(double them for 20 Msps). Same cells as the GR3 README, so the comparison
is on identical bytes.

| payload | `--packets` for 60 s at 10 Msps |
|---|---|
| 8..1500 | 50509 |
| 8..8 | 314301 |
| 100..100 | 192988 |
| 300..300 | 102934 |
| 1500..1500 | 27486 |

```
./scripts/rt-run4 300 300 10e6 1 --packets 102934 --run    # GR3: 2.3 cores busy
./scripts/rt-run4 300 300 10e6 2 --packets 102934 --run    # GR3: 4.7 cores
./scripts/rt-run4 300 300 10e6 3 --packets 102934 --run    # GR3: 5.9 cores, still real time
./scripts/rt-run4 300 300 10e6 4 --packets 102934 --run    # GR3: 6.5 cores, throttle 14 % behind
./scripts/rt-run4 300 300 20e6 1 --packets 205867 --run    # GR3: 4.2 cores at twice real time
```

The GR3 numbers are `~/gr3-ieee802-11-project/results/phase10/load/README.md`;
GR4's are not measured yet — run the lines above and read the CPU bars.

The binary's own options:

```
./build/rx_latency4 --run-dir DIR [--input F] [--out-dir D] [--rate SPS] [--chunk N]
                    [--chains N] [--deadline-ms F] [--timeout-s S] [--record] [--no-check]
```

`--record` adds `rx_pdus.bin`, `rx_log.csv`, `rx_symbols.cf32`, `rx_symbols.csv`
in the GR3 formats for `compare`; `--no-check` skips the payload check. The GR3
run directory is never written to.

## Verification

```
./scripts/gate4                       # every golden cell through GR3's compare --rx-only
python3 scripts/compare-runs.py GR3/latency.csv GR4/latency.csv
```

`gate4` replays each of the 58 golden fixture cells of the GR3 project and
has the GR3 project's own `build/compare --rx-only` judge the port: the
equalised-symbols plane within the fixture's tolerance, the decoded PDUs
byte for byte, and a frame GR3 loses that the port decodes correctly
reported as GR3's defect. Result on 2026-09-14: **58 of 58 cells PASS** (53 cells decode 200/200, the three rate-3/4 cells with 4-byte payloads decode 193/200 exactly as GR3 does, the 4 200-frame cell 4200/4200, the edge cell 8/8), zero wrong payloads. `results/gate4.{log,csv}`.

On the Phase 10 smoke cell (200 random-length packets, QPSK 1/2) the port
decodes 200 of 200 with every payload byte-correct; GR3 decodes 199, losing
frame 192 to its decoder's memory gamble (GR3 gotchas #38).

## Layout

```
include/gr4ieee80211/   wifi_codec.hpp (params, tables, Viterbi, descrambler, CRC),
                        basic_blocks.hpp (Throttle, SampleDelay, MagSquared, Conjugate,
                        Multiply2, Divide2, MovingAverage), FileSourceRaw.hpp, SyncShort.hpp,
                        SyncLong.hpp, Fft64.hpp, FrameEqualizer.hpp, DecodeMac.hpp,
                        harness_blocks.hpp (ArrivalStamper, LatencySink, PduRecorder,
                        SymbolsRecorder), chain.hpp
src/chain.cpp           one receiver chain wired into a gr::Graph (the heavy TU)
apps/rx_latency4.cpp    the app; apps/probe.cpp the block-by-block diagnostic; apps/hello.cpp
scripts/                rt-run4, gate4, compare-runs.py
docs/port-notes.md      what changed and why
results/                gate4.log, gate4.csv
```
