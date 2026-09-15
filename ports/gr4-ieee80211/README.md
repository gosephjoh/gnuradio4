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
cmake --build build -j1 --target rx_latency4 gen4    # ~6 min the first time; one TU needs 2.4 GB
```

## Quick start — generate samples, run, read the response times

All commands run from this folder (`cd ~/gr4-ieee80211`). Nothing here needs
the GR3 project: the sample generator (`gen4`) is standalone, and the only
thing that does need GR3 is the optional fixture gate (`scripts/gate4`).

### 0. Moving it to another machine

The port is also a directory of the GR4 fork itself: branch
`ieee80211-rx-latency` of `github.com/gosephjoh/gnuradio4`, at
`ports/gr4-ieee80211/`, based on `modular-scheduling` (`29320de`). Cloning
that branch gives both the GR4 tree and the port in one checkout, and the
port's CMake finds the tree two levels up:

```
git clone -b ieee80211-rx-latency https://github.com/gosephjoh/gnuradio4.git
cd gnuradio4/ports/gr4-ieee80211
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-15
cmake --build build -j1 --target rx_latency4 gen4
```

Standalone, copy two directories instead: this repo and the GR4 tree it
builds against (`~/gnuradio4`; set `-DGNURADIO4_DIR=/path` if it is
elsewhere).

The branch carries this repository as a `git subtree`. To move commits
between the two (from a checkout of the fork):

```
git subtree pull --prefix=ports/gr4-ieee80211 <path-or-url-of-this-repo> master   # standalone -> branch
git subtree push --prefix=ports/gr4-ieee80211 <path-or-url-of-this-repo> master   # branch -> standalone
```
Needs GCC 15 (or Clang 20), CMake ≥ 3.27, Python 3 with numpy for the check
scripts, about 2.5 GB of RAM for the one heavy translation unit, and network
access at configure time for GR4's own dependency (`vir-simd`). The JSON
library is vendored under `third_party/`.

### 1. Generate the samples

One command takes the same levers as GR3's `rt-run` and makes (or reuses)
the sample file with this repo's own generator:

```
./scripts/rt-run4 MIN MAX RATE CHAINS [--packets N] [--mcs NAME] [--seed S] [--chunk N] [--deadline-ms F]
                                      [--threads N] [--buffer N] [--gr3-cell DIR] [--run]

./scripts/rt-run4 8 1500 10e6 1 --packets 1000       # payloads 8..1500 B, 1000 packets, real time, one receiver
./scripts/rt-run4 300 300 10e6 2 --packets 102934    # one payload size, 60 s of air, two receivers
```

- `MIN MAX` payload bytes, drawn uniformly per packet; `MIN == MAX` is one size (8..1500).
- `RATE` throttle in samples/s: `10e6` = 802.11p real time, `20e6` twice that, `0` unthrottled.
- `CHAINS` concurrent receivers on the same file in one process.
- `--threads` / `--buffer` are the two scheduler knobs `results/latency_knobs.md`
  measured: GR4's stock defaults (a busy-polling worker per hardware thread,
  65 536-item buffers) gave a p95 five times GR3's; `rt-run4` defaults to
  hardware threads − 2 and 8 192-item buffers, which beat GR3 on every
  percentile. `--threads 0 --buffer 0` restores GR4's defaults.
- The cell lands in `data/rt_<MIN>_<MAX>_<N>_<MCS>_s<seed>/` here (4.8 GB and
  under a minute per 60-second cell, once; reused afterwards). Without `--run`
  the script only generates and **prints the commands** below, so you can start
  `htop -t` in another terminal first.
- `gen4` reproduces the GR3 project's generator: same `payloads.bin` and
  manifest byte for byte, samples equal to float rounding (`max|Δ|` 6e-7
  against GR3's files, `scripts/check-gen4.py`), same noise generator. Two
  machines running the same `rt-run4` line replay the same bytes. On a box
  that also has the GR3 project, `--gr3-cell DIR` replays a GR3-generated cell
  instead, for a comparison on literally identical files.

To generate by hand instead (what `rt-run4` calls):

```
./build/gen4 --out data/rt_300_300_102934_QPSK_1_2_s1 --frames 102934 --payload-range 300,300 --mcs QPSK_1_2 --seed 1
```

### 2. Execute

Either add `--run` to the `rt-run4` line, or run the receiver line it printed:

```
./build/rx_latency4 --run-dir data/rt_300_300_102934_QPSK_1_2_s1 \
    --input data/rt_300_300_102934_QPSK_1_2_s1/rx_stimulus.cf32 \
    --out-dir data/rt_300_300_102934_QPSK_1_2_s1/runs4/rate10000000_chains2 \
    --rate 10000000 --chains 2 --chunk 4096 --deadline-ms 100
```

While it runs, `htop -t` shows the GR4 scheduler's worker threads
`default_cpu#k` (six by default here, one per hardware thread with
`--threads 0`), the IO-pool threads `default_io#k` that run the throttles'
timers, and the main thread. **Every worker runs at ~100 % whatever the
load**: this GR4 tree's multi-threaded scheduler busy-polls, so GR4's CPU
figure is the pool size, not the work. GR4 is not thread-per-block, so there
is no thread per receiver block as with GR3; the per-block picture comes from
the stage counters in `latency_summary.json`.
The run ends by itself at the end of the file; a per-chain line is printed:

```
rx_latency4: chain 0 -- 102934/102934 decoded, 0 missing, 0 dup, 0 unpairable, 0 wrong payload;
             last->decode us: first 1131.0 p50 1188.8 p95 3886.2 p99 5121.1 max 6257.2; 0 over 100 ms; ...
```

### 3. Look at the response times

Outputs are in `<cell>/runs4/rate<R>_chains<C>/`:

```
OUT=data/rt_300_300_102934_QPSK_1_2_s1/runs4/rate10000000_chains2

# the summary as a table, one row per receiver; several run directories (GR3 and GR4
# alike) compare side by side; --md for Markdown, --csv, --wide for the knobs and stage counts
./scripts/rt-table $OUT
./scripts/rt-table --md <GR3 run dir> $OUT

# every packet: chain,seq,length,t_first_ns,t_last_ns,t_decode_ns,lat_first_us,lat_last_us,decoded,correct
head -5 $OUT/latency.csv

# GR3 vs GR4 for the same levers, packet for packet (the GR3 latency.csv comes from ./scripts/rt-run there)
python3 scripts/compare-runs.py <GR3 latency.csv> $OUT/latency.csv
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
                    [--threads N] [--buffer N] [--batch N] [--catch-up] [--single]
```

The binary's own defaults are GR4's (`--threads 0` = all hardware threads,
`--buffer 0` = 65 536 items); `rt-run4` passes the tuned values. `--batch`,
`--catch-up` and `--single` are the other knobs the investigation tried
(`docs/port-notes.md`).

`--record` adds `rx_pdus.bin`, `rx_log.csv`, `rx_symbols.cf32`, `rx_symbols.csv`
in the GR3 formats for `compare`; `--no-check` skips the payload check. The GR3
run directory is never written to.

## Verification

```
./scripts/gate4                       # every golden cell through GR3's compare --rx-only
python3 scripts/compare-runs.py GR3/latency.csv GR4/latency.csv
```

`gate4` needs the GR3 project on the same box (its fixture and its `compare`
tool); it is the correctness proof, not a runtime dependency. It replays each of the 58 golden fixture cells of the GR3 project and
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
include/gr4ieee80211/wifi_tx.hpp, wifi_tables.h   the standalone transmitter (gen4)
apps/rx_latency4.cpp    the app; apps/gen4.cpp the generator; apps/probe.cpp the diagnostic
scripts/                rt-run4, rt-table, gate4, compare-runs.py, check-gen4.py
third_party/nlohmann    vendored JSON header (MIT)
docs/port-notes.md      what changed and why
results/                gate4.log, gate4.csv
```
