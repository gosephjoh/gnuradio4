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

## Run — the same levers as GR3's `rt-run`

```
./scripts/rt-run4 MIN MAX RATE CHAINS [--packets N] [--mcs NAME] [--seed S] [--chunk N] [--deadline-ms F] [--run]
./scripts/rt-run4 8 1500 10e6 1 --packets 1000     # random sizes, real time, one receiver
./scripts/rt-run4 300 300 0 4 --packets 102934     # one size, unthrottled, four receivers
```

It reuses the cell the GR3 project generated for the same MIN/MAX/packets/
MCS/seed (`~/gr3-ieee802-11-project/data/phase10/rt_<MIN>_<MAX>_<N>_<MCS>_s<seed>/`,
or generates it there with GR3's `rt-gen` — both runtimes must replay the
identical bytes), creates `<cell>/runs4/rate<R>_chains<C>/` beside GR3's
`runs/`, and prints: `htop -t` for a second terminal, the `rx_latency4`
line, a one-liner that reads `latency_summary.json`, and the GR3-vs-GR4
packet-for-packet comparison. `--run` executes the receiver.

The binary itself:

```
./build/rx_latency4 --run-dir DIR [--input F] [--out-dir D] [--rate SPS] [--chunk N]
                    [--chains N] [--deadline-ms F] [--timeout-s S] [--record] [--no-check]
```

Outputs (`--out-dir`): `latency.csv` — GR3's columns plus `correct` (the
decoded payload matched `payloads.bin`) — and `latency_summary.json` — GR3's
fields plus `wrong_payload`, the per-stage `counts`, `runtime`, `scheduler`,
`hw_threads`. `--record` adds `rx_pdus.bin`, `rx_log.csv`, `rx_symbols.cf32`,
`rx_symbols.csv` in the GR3 formats for `compare`. The GR3 run directory is
never written to.

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
