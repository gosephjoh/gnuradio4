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
