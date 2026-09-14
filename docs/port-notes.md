# Port notes — what was ported, what changed, and how it is verified

The GR3 reference is `~/gr3-ieee802-11-project` (Phase 10, `rx_latency`,
decision 0035). Upstream is `~/gr-ieee802-11` at `ad0598e` (`maint-3.10`).
GR4 is `~/gnuradio4`, fork `gosephjoh/gnuradio4`, branch `modular-scheduling`,
commit `29320de`, built with GCC 15.2 / CMake 4.2 on this box.

## Block-for-block mapping (decision: mirror GR3's topology)

| GR3 block (wifi_phy_rx.cc) | GR4 block here | origin |
|---|---|---|
| `blocks::file_source` | `FileSourceRaw<cf>` | own; GR4's `BasicFileSource` delivered no sample on this tree (see departures) |
| `blocks::throttle` | `Throttle<cf>` | own, on GR4's `BlockingSync` mixin |
| `complex_to_mag_squared` | `MagSquared` | own (`re*re + im*im`) |
| `moving_average_ff(64)` / `_cc(48)` | `MovingAverage<float>` / `<cf>` | own, GR3's re-sum-per-call arithmetic |
| `delay(16)` / `delay(320)` | `SampleDelay<cf>` | own |
| `conjugate_cc` | `Conjugate` | own |
| `multiply_cc` | `Multiply2<cf>` | own; GR4's `Multiply<T>` with `in#k` ports never ran here |
| `complex_to_mag` | `gr::blocks::type::converter::Abs<cf>` | **GR4 stock** |
| `divide_ff` | `Divide2<float>` | own; as `Multiply2` |
| `ieee802_11::sync_short` | `SyncShort` | ported |
| `ieee802_11::sync_long` | `SyncLong` | ported |
| `stream_to_vector(64)` + `fft_vcc(64, shift)` | `Fft64` | own, on `gr::algorithm::FFT` |
| `ieee802_11::frame_equalizer` (LS) | `FrameEqualizer` | ported, LS only |
| `ieee802_11::decode_mac` | `DecodeMac` | ported |
| harness `arrival_stamper` / `latency_sink` | `ArrivalStamper` / `LatencySink` | ported |
| harness `pdu_recorder` (RX_PDU, SYMBOLS) | `PduRecorder` / `SymbolsRecorder` | ported, same file formats |

Wiring is `src/chain.cpp`; it is the connection map of
`docs/findings/cpp-build-notes.md` in the GR3 project, line for line.

## Departures from upstream, all listed

1. **Viterbi decoder input zero-filled past the frame** (decision 0035
   question 4). Upstream reads `16 × ntraceback` symbols past the
   depunctured frame from whatever its member array holds (GR3 gotchas #28,
   #36, #38). Zero is a fresh process's content, which is what the golden
   fixture was captured with, so on fixed-length cells the port reproduces
   GR3's bytes exactly. On a random-length run GR3's array holds earlier
   longer frames' bits and it loses frames the port decodes (smoke cell:
   frame 192; the port decodes it byte-correctly — recorded as GR3's defect,
   gotchas #38).
2. **`POLARITY[(sym − 2) % 127]` is guarded for sym < 2.** Upstream reads
   out of bounds there (gotchas #4); the value only reaches `er`, which is
   discarded for those symbols. Output identical (port-requirements 1.3).
3. **Tags that upstream attaches to the *next* output item from inside a
   call** (sync_short's SEARCH→COPY `wifi_start`, frame_equalizer's frame
   tag) are held and published on that item when it is produced. Same
   absolute index.
4. **SyncLong's two unreachable branches:** upstream throws `"wtf"` on a
   frame tag arriving mid-SYNC; the port logs and resets. A completed RESET
   falls straight into SYNC in the same call instead of waiting to be
   rescheduled. Fewer than 64 samples before a frame tag while searching
   (impossible with MIN_GAP) skips to the tag instead of stalling.
5. **Tag keys:** GR3 propagates any tag; GR4's default forwarding only
   passes `gr:`-prefixed keys, so every block between a tag producer and its
   consumer here (Fft64, FrameEqualizer, DecodeMac) forwards or publishes
   its tags explicitly. `wifi_start` and the frame tag keep upstream's names.
6. **Decoded frames travel as `gr::Packet<uint8_t>` on an asynchronous
   stream port**, not as a PMT message: GR4 messages carry key-value maps,
   not sample payloads (decision 0035 question 7).
7. **`csi` is not attached** to the frame tag; nothing downstream reads it.

## A GR4 rule GR3 does not have: a call that publishes nothing loses its tags

`Block::finaliseIO` sets `tagsPublished = 0` on every output span when the
call published no samples. Upstream's `sync_short` tags the *next* output
item and returns without producing it (SEARCH→COPY, and the in-COPY
re-trigger); under GR3 the tag survives, under GR4 it is discarded whenever
the trigger lands on the first sample of a call. The symptom was one frame
lost at `sync_long` in roughly one two-chain real-time run of three, never
unthrottled, never on a fixture cell — found with the per-call trace
(`GR4_SYNCLONG_TRACE=<path>` writes one CSV per sync_long / Fft64 /
FrameEqualizer instance) showing a tag published by sync_short that
sync_long never received. Both `wifi_start` tags of `SyncShort` are now held
and published on the first sample of the next COPY call; the index is the
same. Twelve two-chain real-time runs of the 1000-frame random-length cell
after the fix: 24 000 of 24 000 frames. The same rule is why
`FrameEqualizer` holds its frame tag until the first data symbol is written.

## GR4 stock blocks that did not work on this tree

Measured with `apps/probe.cpp`, kept as the diagnostic:

- `gr::blocks::fileio::BasicFileSource<std::complex<float>>` → `CountingSink`:
  0 samples after 10 s under both the single- and the multi-threaded
  `scheduler::Simple`; every block `STOPPED` after `requestStop()` (probe
  mode `fsrc`). Replaced by `FileSourceRaw` (synchronous `fread`, GR3's
  semantics).
- `gr::blocks::math::Multiply<std::complex<float>>` with `n_inputs = 2`
  connected through the runtime `connect(src, "out", mult, "in#0")` API:
  0 samples (probe mode `m4`). Replaced by `Multiply2` / `Divide2` with
  static ports.

Not investigated further: the port needs the GR3 semantics, not those
blocks.

## Arithmetic parity, where it is exact and where it is not

- Exact: hard decisions, deinterleaver, descrambler, CRC-32, Viterbi (8-bit
  wrapping metrics, signed survivor compare, minimum subtraction every 8
  bits, fixed-delay traceback), SIGNAL parse, all integer.
- To float rounding: the front end (GR3 uses VOLK kernels, this uses scalar
  `std::complex`), the moving averages (re-summed per call; the scheduler's
  chunking decides where — the ULP mechanism of port-requirements 1.2), the
  64-point FFT (FFTW vs `gr::algorithm::FFT`, both single precision), the
  equaliser's `exp`/`arg`. The symbols plane is therefore toleranced
  (`max|Δ| ≤ 1e-5`, NMSE ≤ −80 dB) and the decoded planes are bit-exact,
  which is exactly what `compare --rx-only` asserts.

## The standalone generator, `gen4`

`apps/gen4.cpp` + `include/gr4ieee80211/wifi_tx.hpp` reproduce the GR3
project's `gen-input` + `tx_capture` with no GNU Radio of either generation:
GR3's payload rules (`mt19937_64` bodies, u32 sequence numbers, a second
generator for `--payload-range`), `mac.cc`'s header and CRC, the mapper
pipeline of `lib/utils.cc`, the SIGNAL field, the constellations, the
carrier allocator with the generated tables (`wifi_tables.h`, copied from
the GR3 project), an unnormalised radix-2 inverse FFT with GR3's
`1/sqrt(52)` window and input-half swap, the cyclic prefixer with its
rolloff-2 flanks, `packet_pad2`, and GR3's own noise generator
(`gr::random`: xoroshiro128+ seeded through splitmix64 and a jump,
`std::uniform_real_distribution<float>`, Marsaglia-polar `gasdev`, the
complex source's `ampl / sqrt(2)` and right-to-left argument evaluation).
Checked against GR3-generated cells with `scripts/check-gen4.py`:
`payloads.bin` and `manifest.json`'s `frames[]` identical; `tx_samples.cf32`
and `rx_stimulus.cf32` `max|Δ|` 6e-7 (NMSE −141 dB, a third of the samples
bit-identical) on the 300 B/200-frame cell and the 8–1500 B/1000-frame cell.
Two facts that cost a retry each: `xoroshiro128p_seed` increments `state[0]`
inside `splitmix64_next` before it becomes the seed word, and the complex
noise source scales by `1/sqrt(2)`.

## Why GR4's response-time tail was worse, and the two knobs that fix it

The user's 20 Msps run: GR3 p50 977 / p95 1582 / p99 2119 / max 6704 µs;
GR4 p50 657 / p95 6791 / p99 9927 / max 34985 µs. The slow GR4 frames came
in long runs (mean 23 consecutive frames above p95, max 757; GR3: mean 2.4)
with latency decreasing ~70 µs per frame along a run: a backlog draining.
Measured causes (`results/latency_knobs.md`, per-thread `ps -T` and the
throttle's gap/backlog counters in `latency_summary.json`):

1. **Every worker of this tree's multi-threaded scheduler busy-polls.** One
   receiver at 20 Msps shows 8 workers at 91–98 % CPU; at 10 Msps too. With a
   worker per hardware thread, the throttle's timer thread (IO pool), the
   main thread and the OS preempt spinners, and a block's turn on its worker
   waits behind its list-mates: the longest gap between two throttle calls
   was 18.6 ms, the deepest backlog 650k samples.
2. **A work call takes the whole buffer** — 65 536 items, 3.3 ms of air at
   20 Msps — so one slow block's turn delays every block sharing its worker
   by milliseconds. GR3 offers at most 4 096 items per call.

`--threads 6` (two hardware threads left free) cuts the worst throttle gap
to 3.3 ms; `--buffer 8192` (GR3's size) restores fine interleaving; both
together give p50 624 / p95 920 / p99 1070 / max 5755 µs at 20 Msps and
p50 209 / p95 356 / p99 764 µs at 10 Msps — ahead of GR3 on every percentile
while holding real time. `--buffer 8192` alone collapses the tail but the
throttle then falls behind (its call rate is bound to the scheduler pass
rate); `--batch 4096` (`max_batch_size`) alone changes nothing; `--single`
cannot keep up at 20 Msps. `rt-run4` therefore defaults to
`hardware threads − 2` and 8 192; the binary keeps GR4's defaults.

Consequence for the CPU comparison: GR4's "cores busy" equals its worker
count whatever the load (six workers at 100 % for one receiver *and* for
two), where GR3 measured 2.3 and 4.7 cores. Compare GR4 pool sizes, not
utilisation, against GR3's core counts.

A defect of this port found on the way: the experiment binary read the
stamps after the scheduler — which owns the graph and the blocks — had gone
out of scope; two frames per run showed an impossible arrival stamp of 0
and the throttle counters read garbage. Fixed (the scheduler now outlives
the reporting); every number above is from the fixed binary.

## Diagnostics kept in the code

Per-stage item and tag counters are in every own block and reported under
`counts` in `latency_summary.json` (`sync_short_detections`,
`sync_long_frames`, `fft_tags_in/out`, `eq_tags_in`, `signal_ok/bad`,
`frames_started/decoded`, `crc_failed`, `sl_*`). `GR4_SYNCLONG_TRACE=<path>`
enables the per-call traces. `apps/probe.cpp` builds sub-graphs
(`fsrc`, `raw`, `stamp`, `m1..m5`, `full`, `full10`; third argument
`single` for the single-threaded scheduler) and prints every block's
lifecycle state and the stage counters.

## Verification

- `scripts/gate4`: every golden cell of the fixture replayed through
  `rx_latency4 --record --rate 0`, judged by the GR3 project's
  `build/compare --rx-only` (symbols within tolerance, PDUs byte for byte,
  `GR3-DEFECT` for a GR3-lost frame the port decodes correctly). Results
  in `results/gate4.{log,csv}`.
- `scripts/compare-runs.py`: a GR3 `latency.csv` against a GR4 one on the
  same cell — decoded sets, GR3-only / GR4-only frames, payload correctness
  of the GR4-only ones, latency percentiles side by side.
- `rx_latency4` itself checks every decoded payload against `payloads.bin`
  (`correct` column, `wrong_payload` count) unless `--no-check`.

## Build facts

- One translation unit instantiates the whole chain (`src/chain.cpp`):
  about 4 minutes and 2.4 GB of RSS with GCC 15 at `-O3`. Build with `-j1`
  on this 3.3 GB box. The app TU is light.
- `libgnuradio-core.a`'s plugin loader needs `libgnuradio-blocklib-core.so`
  even with the registry off; link it with `-Wl,--no-as-needed`.
- GR4 options set for the subdirectory: tests, examples, benchmarks, block
  registry, plugins, mold, ccache, HTTP all off; warnings not errors.
