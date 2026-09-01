#include <gnuradio-4.0/BlockingSync.hpp>
#include <gnuradio-4.0/EarliestDeadlineFirst.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/algorithm/filter/FilterTool.hpp>
#include <gnuradio-4.0/basic/ConverterBlocks.hpp>
#include <gnuradio-4.0/math/ExpressionBlocks.hpp>
#include <gnuradio-4.0/math/Math.hpp>
#include <gnuradio-4.0/filter/time_domain_filter.hpp>
#include <gnuradio-4.0/fourier/fft.hpp>
#include <gnuradio-4.0/math/Rotator.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstdint>
#include <format>
#include <functional>
#include <numbers>
#include <print>
#include <random>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

// A suite of classic GNU Radio applications -- NBFM/WBFM/AM/SSB receivers, an FFT spectrum analyser, a QPSK
// modem link, a multi-channel NBFM monitor and an audio effects chain -- each fed by a wall-clock paced source
// at its native rate in 10 ms chunks, with a sink that must receive every chunk within one chunk period and
// that verifies the application actually works (tone recovered, spectrum peak, EVM). Optionally the
// application shares its worker with a saturating bulk pipeline. Every built-in scheduler and the two
// release-aware policies are run on identical graphs.

namespace {

using SteadyClock = std::chrono::steady_clock;
using cf          = std::complex<float>;
constexpr float   kTwoPi = 2.f * std::numbers::pi_v<float>;

std::atomic<std::int64_t> gAnchorNs{0}; // first source's start instant; sinks measure against it

[[nodiscard]] std::int64_t nowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now().time_since_epoch()).count(); }

// the chunk clock: unit n (sample or dataset) is nominally available at the end of its chunk
struct ChunkClock {
    double        unitsPerSecond = 1.0;
    std::uint64_t chunk          = 1UZ;
    std::uint64_t nUnits         = 0UZ;

    void note(std::int64_t arrivalNs, std::vector<std::int64_t>& latencies) {
        nUnits++;
        if (nUnits % chunk == 0UZ) {
            const double       nsPerChunk = 1e9 * static_cast<double>(chunk) / unitsPerSecond;
            const std::int64_t boundary   = gAnchorNs.load() + static_cast<std::int64_t>(static_cast<double>(nUnits / chunk) * nsPerChunk);
            latencies.push_back(arrivalNs - boundary);
        }
    }
};

template<typename TSignal>
struct PacedSource : gr::Block<PacedSource<TSignal>>, gr::BlockingSync<PacedSource<TSignal>, SteadyClock> {
    gr::PortOut<typename TSignal::value_type> out;

    gr::Annotated<float, "sample_rate">        sample_rate         = 48'000.f;
    gr::Annotated<gr::Size_t, "chunk_size">    chunk_size          = 480U;
    gr::Annotated<bool, "use_internal_thread"> use_internal_thread = true;

    GR_MAKE_REFLECTABLE(PacedSource, out, sample_rate, chunk_size, use_internal_thread);

    TSignal       signal;
    std::uint64_t _nChunksProduced = 0UZ;

    void start() {
        this->blockingSyncStart();
        std::int64_t expected = 0;
        gAnchorNs.compare_exchange_strong(expected, std::chrono::duration_cast<std::chrono::nanoseconds>(this->blockingSyncStartTime().time_since_epoch()).count());
    }
    void stop() { this->blockingSyncStop(); }

    gr::work::Status processBulk(gr::OutputSpanLike auto& outSpan) {
        const double        elapsedSeconds = std::chrono::duration<double>(SteadyClock::now() - this->blockingSyncStartTime()).count();
        const auto          chunk          = static_cast<std::uint64_t>(chunk_size.value);
        const auto          owedChunks     = static_cast<std::uint64_t>(elapsedSeconds * static_cast<double>(sample_rate)) / chunk;
        const std::uint64_t nChunks        = std::min(owedChunks - std::min(owedChunks, _nChunksProduced), static_cast<std::uint64_t>(outSpan.size()) / chunk);
        if (nChunks == 0UZ) {
            outSpan.publish(0UZ);
            return gr::work::Status::INSUFFICIENT_INPUT_ITEMS;
        }
        const std::size_t n = nChunks * chunk;
        for (std::size_t index = 0UZ; index < n; ++index) {
            outSpan[index] = signal.next(sample_rate);
        }
        outSpan.publish(n);
        _nChunksProduced += nChunks;
        return gr::work::Status::OK;
    }
};

struct FmToneSignal { // FM-modulated 1 kHz tone
    using value_type = cf;
    float maxDev = 5'000.f, toneHz = 1'000.f, tonePhase = 0.f, carrierPhase = 0.f;
    [[nodiscard]] cf next(float fs) {
        tonePhase    = std::fmod(tonePhase + kTwoPi * toneHz / fs, kTwoPi);
        carrierPhase = std::fmod(carrierPhase + kTwoPi * maxDev / fs * std::sin(tonePhase), kTwoPi);
        return std::polar(1.f, carrierPhase);
    }
};

struct AmToneSignal { // 50% AM of a 1 kHz tone on a 10 kHz intermediate frequency
    using value_type = cf;
    float ifHz = 10'000.f, toneHz = 1'000.f, tonePhase = 0.f, ifPhase = 0.f;
    [[nodiscard]] cf next(float fs) {
        tonePhase = std::fmod(tonePhase + kTwoPi * toneHz / fs, kTwoPi);
        ifPhase   = std::fmod(ifPhase + kTwoPi * ifHz / fs, kTwoPi);
        return std::polar(1.f + 0.5f * std::sin(tonePhase), ifPhase);
    }
};

struct SsbToneSignal { // upper-sideband 1 kHz tone on a 12 kHz intermediate frequency
    using value_type = cf;
    float ifHz = 12'000.f, toneHz = 1'000.f, phase = 0.f;
    [[nodiscard]] cf next(float fs) {
        phase = std::fmod(phase + kTwoPi * (ifHz + toneHz) / fs, kTwoPi);
        return std::polar(1.f, phase);
    }
};

struct ComplexToneSignal { // pure tone at +100 kHz for the spectrum analyser
    using value_type = cf;
    float toneHz = 100'000.f, phase = 0.f;
    [[nodiscard]] cf next(float fs) {
        phase = std::fmod(phase + kTwoPi * toneHz / fs, kTwoPi);
        return std::polar(1.f, phase);
    }
};

struct QpskSymbolSignal { // random QPSK symbols
    using value_type = cf;
    std::uint32_t lfsr = 0xACE1u;
    [[nodiscard]] cf next(float /*fs*/) {
        const auto bit = [this] { const std::uint32_t b = ((lfsr >> 0) ^ (lfsr >> 2) ^ (lfsr >> 3) ^ (lfsr >> 5)) & 1u; lfsr = (lfsr >> 1) | (b << 15); return b; };
        const float i = bit() ? 1.f : -1.f, q = bit() ? 1.f : -1.f;
        return cf{i, q} * std::numbers::sqrt2_v<float> / 2.f;
    }
};

struct AudioToneSignal { // 440 Hz sine
    using value_type = float;
    float phase = 0.f;
    [[nodiscard]] float next(float fs) {
        phase = std::fmod(phase + kTwoPi * 440.f / fs, kTwoPi);
        return 0.5f * std::sin(phase);
    }
};

struct ComplexFir : gr::Block<ComplexFir> {
    gr::PortIn<cf>  in;
    gr::PortOut<cf> out;
    GR_MAKE_REFLECTABLE(ComplexFir, in, out);
    std::vector<float> taps{1.f};
    std::vector<cf>    _history;
    std::size_t        _head = 0UZ;
    [[nodiscard]] cf processOne(cf x) noexcept {
        if (_history.size() != taps.size()) {
            _history.assign(taps.size(), cf{0.f, 0.f});
        }
        _history[_head]   = x;
        cf                y{0.f, 0.f};
        const std::size_t n = taps.size();
        for (std::size_t i = 0UZ; i < n; ++i) {
            y += _history[(_head + n - i) % n] * taps[i];
        }
        _head = (_head + 1UZ) % n;
        return y;
    }
};

struct RealFir : gr::Block<RealFir> {
    gr::PortIn<float>  in;
    gr::PortOut<float> out;
    GR_MAKE_REFLECTABLE(RealFir, in, out);
    std::vector<float> taps{1.f};
    std::vector<float> _history;
    std::size_t        _head = 0UZ;
    [[nodiscard]] float processOne(float x) noexcept {
        if (_history.size() != taps.size()) {
            _history.assign(taps.size(), 0.f);
        }
        _history[_head]   = x;
        float             y = 0.f;
        const std::size_t n = taps.size();
        for (std::size_t i = 0UZ; i < n; ++i) {
            y += _history[(_head + n - i) % n] * taps[i];
        }
        _head = (_head + 1UZ) % n;
        return y;
    }
};

struct Squelch : gr::Block<Squelch> {
    gr::PortIn<cf>  in;
    gr::PortOut<cf> out;
    gr::Annotated<float, "threshold_db"> threshold_db = -50.f;
    GR_MAKE_REFLECTABLE(Squelch, in, out, threshold_db);
    float _envelope = 0.f;
    [[nodiscard]] cf processOne(cf x) noexcept {
        _envelope = 0.001f * std::norm(x) + 0.999f * _envelope;
        return 10.f * std::log10(_envelope + 1e-20f) >= threshold_db ? x : cf{0.f, 0.f};
    }
};

struct QuadratureDemod : gr::Block<QuadratureDemod> {
    gr::PortIn<cf>     in;
    gr::PortOut<float> out;
    gr::Annotated<float, "gain"> gain = 1.f;
    GR_MAKE_REFLECTABLE(QuadratureDemod, in, out, gain);
    cf _previous{1.f, 0.f};
    [[nodiscard]] float processOne(cf x) noexcept {
        const float y = gain * std::arg(x * std::conj(_previous));
        _previous     = x;
        return y;
    }
};

struct FmDeemphasis : gr::Block<FmDeemphasis> {
    gr::PortIn<float>  in;
    gr::PortOut<float> out;
    gr::Annotated<float, "tau">         tau         = 75e-6f;
    gr::Annotated<float, "sample_rate"> sample_rate = 192'000.f;
    GR_MAKE_REFLECTABLE(FmDeemphasis, in, out, tau, sample_rate);
    float _state = 0.f;
    [[nodiscard]] float processOne(float x) noexcept {
        const float dt = 1.f / sample_rate;
        _state         = (dt / (tau + dt)) * x + (tau / (tau + dt)) * _state;
        return _state;
    }
};

struct Volume : gr::Block<Volume> {
    gr::PortIn<float>  in;
    gr::PortOut<float> out;
    gr::Annotated<float, "gain"> gain = 0.5f;
    GR_MAKE_REFLECTABLE(Volume, in, out, gain);
    [[nodiscard]] constexpr float processOne(float x) const noexcept { return gain * x; }
};

struct Magnitude : gr::Block<Magnitude> {
    gr::PortIn<cf>     in;
    gr::PortOut<float> out;
    GR_MAKE_REFLECTABLE(Magnitude, in, out);
    [[nodiscard]] float processOne(cf x) const noexcept { return std::abs(x); }
};

struct RealPart : gr::Block<RealPart> {
    gr::PortIn<cf>     in;
    gr::PortOut<float> out;
    GR_MAKE_REFLECTABLE(RealPart, in, out);
    [[nodiscard]] constexpr float processOne(cf x) const noexcept { return x.real(); }
};

struct DcBlocker : gr::Block<DcBlocker> {
    gr::PortIn<float>  in;
    gr::PortOut<float> out;
    GR_MAKE_REFLECTABLE(DcBlocker, in, out);
    float _previousIn = 0.f, _previousOut = 0.f;
    [[nodiscard]] float processOne(float x) noexcept {
        const float y = x - _previousIn + 0.995f * _previousOut;
        _previousIn   = x;
        _previousOut  = y;
        return y;
    }
};

struct Biquad : gr::Block<Biquad> { // direct form I, coefficients set after construction
    gr::PortIn<float>  in;
    gr::PortOut<float> out;
    GR_MAKE_REFLECTABLE(Biquad, in, out);
    float b0 = 1.f, b1 = 0.f, b2 = 0.f, a1 = 0.f, a2 = 0.f;
    float _x1 = 0.f, _x2 = 0.f, _y1 = 0.f, _y2 = 0.f;
    [[nodiscard]] float processOne(float x) noexcept {
        const float y = b0 * x + b1 * _x1 + b2 * _x2 - a1 * _y1 - a2 * _y2;
        _x2 = _x1; _x1 = x; _y2 = _y1; _y1 = y;
        return y;
    }
    void lowPass(float fc, float fs, float q) {
        const float w = kTwoPi * fc / fs, alpha = std::sin(w) / (2.f * q), c = std::cos(w), a0 = 1.f + alpha;
        b0 = (1.f - c) / 2.f / a0; b1 = (1.f - c) / a0; b2 = b0; a1 = -2.f * c / a0; a2 = (1.f - alpha) / a0;
    }
};

struct SoftClip : gr::Block<SoftClip> {
    gr::PortIn<float>  in;
    gr::PortOut<float> out;
    GR_MAKE_REFLECTABLE(SoftClip, in, out);
    [[nodiscard]] float processOne(float x) const noexcept { return std::tanh(1.5f * x); }
};

struct RrcInterpolator : gr::Block<RrcInterpolator, gr::Resampling<1UZ, 4UZ>> { // 4 samples per symbol
    gr::PortIn<cf>  in;
    gr::PortOut<cf> out;
    GR_MAKE_REFLECTABLE(RrcInterpolator, in, out);
    std::vector<float> taps{1.f}; // length must be a multiple of 4
    std::vector<cf>    _symbols;
    std::size_t        _head = 0UZ;
    [[nodiscard]] gr::work::Status processBulk(std::span<const cf> input, std::span<cf> output) noexcept {
        const std::size_t nPhases = taps.size() / 4UZ;
        if (_symbols.size() != nPhases) {
            _symbols.assign(nPhases, cf{0.f, 0.f});
        }
        for (std::size_t s = 0UZ; s < input.size(); ++s) {
            _symbols[_head] = input[s];
            for (std::size_t phase = 0UZ; phase < 4UZ; ++phase) {
                cf y{0.f, 0.f};
                for (std::size_t m = 0UZ; m < nPhases; ++m) {
                    y += _symbols[(_head + nPhases - m) % nPhases] * taps[4UZ * m + phase];
                }
                output[4UZ * s + phase] = y;
            }
            _head = (_head + 1UZ) % nPhases;
        }
        return gr::work::Status::OK;
    }
};

struct AwgnChannel : gr::Block<AwgnChannel> {
    gr::PortIn<cf>  in;
    gr::PortOut<cf> out;
    gr::Annotated<float, "noise_sigma"> noise_sigma = 0.05f;
    GR_MAKE_REFLECTABLE(AwgnChannel, in, out, noise_sigma);
    std::mt19937                    _rng{42u};
    std::normal_distribution<float> _gauss{0.f, 1.f};
    [[nodiscard]] cf processOne(cf x) noexcept { return x + noise_sigma.value * cf{_gauss(_rng), _gauss(_rng)}; }
};

struct AudioSink : gr::Block<AudioSink> {
    gr::PortIn<float> in;
    gr::Annotated<float, "sample_rate">     sample_rate = 48'000.f;
    gr::Annotated<gr::Size_t, "chunk_size"> chunk_size  = 480U;
    GR_MAKE_REFLECTABLE(AudioSink, in, sample_rate, chunk_size);
    ChunkClock                clock;
    std::vector<std::int64_t> latencies;
    double                    sumSquares = 0.0;
    std::uint64_t             nZeroCrossings = 0UZ;
    float                     _previous = 0.f;
    void start() { clock = ChunkClock{.unitsPerSecond = static_cast<double>(sample_rate), .chunk = static_cast<std::uint64_t>(chunk_size.value)}; }
    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan) {
        const std::int64_t arrival = nowNs();
        for (const float sample : inSpan) {
            sumSquares += static_cast<double>(sample) * static_cast<double>(sample);
            nZeroCrossings += ((sample >= 0.f) != (_previous >= 0.f)) ? 1UZ : 0UZ;
            _previous = sample;
            clock.note(arrival, latencies);
        }
        return gr::work::Status::OK;
    }
    [[nodiscard]] std::string quality() const {
        const double seconds = static_cast<double>(clock.nUnits) / static_cast<double>(sample_rate);
        return std::format("tone {:.0f} Hz rms {:.3f}", clock.nUnits > 0UZ ? static_cast<double>(nZeroCrossings) / 2.0 / seconds : 0.0, clock.nUnits > 0UZ ? std::sqrt(sumSquares / static_cast<double>(clock.nUnits)) : 0.0);
    }
};

struct EvmSink : gr::Block<EvmSink> {
    gr::PortIn<cf> in;
    gr::Annotated<float, "sample_rate">     sample_rate = 62'500.f;
    gr::Annotated<gr::Size_t, "chunk_size"> chunk_size  = 625U;
    GR_MAKE_REFLECTABLE(EvmSink, in, sample_rate, chunk_size);
    ChunkClock                clock;
    std::vector<std::int64_t> latencies;
    double                    errorPower = 0.0, signalPower = 0.0;
    void start() { clock = ChunkClock{.unitsPerSecond = static_cast<double>(sample_rate), .chunk = static_cast<std::uint64_t>(chunk_size.value)}; }
    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan) {
        const std::int64_t arrival = nowNs();
        for (const cf r : inSpan) {
            const float magnitude = std::abs(r);
            const cf    ideal     = cf{r.real() >= 0.f ? 1.f : -1.f, r.imag() >= 0.f ? 1.f : -1.f} * (magnitude * std::numbers::sqrt2_v<float> / 2.f);
            errorPower += static_cast<double>(std::norm(r - ideal));
            signalPower += static_cast<double>(std::norm(ideal));
            clock.note(arrival, latencies);
        }
        return gr::work::Status::OK;
    }
    [[nodiscard]] std::string quality() const { return std::format("EVM {:.1f}%", signalPower > 0.0 ? 100.0 * std::sqrt(errorPower / signalPower) : 0.0); }
};

struct SpectrumSink : gr::Block<SpectrumSink> {
    gr::PortIn<gr::DataSet<float>> in;
    gr::Annotated<float, "frames_per_second"> frames_per_second = 100.f;
    gr::Annotated<gr::Size_t, "chunk_size">   chunk_size        = 1U;
    GR_MAKE_REFLECTABLE(SpectrumSink, in, frames_per_second, chunk_size);
    ChunkClock                clock;
    std::vector<std::int64_t> latencies;
    std::size_t               peakBin = 0UZ, nFrames = 0UZ;
    void start() { clock = ChunkClock{.unitsPerSecond = static_cast<double>(frames_per_second), .chunk = static_cast<std::uint64_t>(chunk_size.value)}; }
    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan) {
        const std::int64_t arrival = nowNs();
        for (const gr::DataSet<float>& frame : inSpan) {
            if (!frame.signal_values.empty() && !frame.signal_names.empty()) {
                // the block packs magnitude, phase, real and imaginary spectra back to back; the magnitude comes first
                const auto magnitude = std::span<const float>(frame.signal_values).first(frame.signal_values.size() / frame.signal_names.size());
                peakBin              = static_cast<std::size_t>(std::ranges::distance(magnitude.begin(), std::ranges::max_element(magnitude)));
            }
            nFrames++;
            clock.note(arrival, latencies);
        }
        return gr::work::Status::OK;
    }
    [[nodiscard]] std::string quality() const { return std::format("peak bin {} of {} frames", peakBin, nFrames); }
};

struct BusyWork : gr::Block<BusyWork> {
    gr::PortIn<float>  in;
    gr::PortOut<float> out;
    gr::Annotated<gr::Size_t, "ops_per_sample"> ops_per_sample = 32U;
    GR_MAKE_REFLECTABLE(BusyWork, in, out, ops_per_sample);
    [[nodiscard]] float processOne(float sample) const noexcept {
        float value = sample;
        for (gr::Size_t op = 0U; op < ops_per_sample; op++) {
            value = value * 1.0000001f + 1e-9f;
        }
        return value;
    }
};

[[nodiscard]] std::vector<float> lowPassTaps(double cutoffHz, double fs) {
    return gr::filter::fir::designFilter<float>(gr::filter::Type::LOWPASS, {.fLow = cutoffHz, .attenuationDb = 60.0, .fs = fs}).b;
}

[[nodiscard]] std::vector<float> rrcTaps(std::size_t nTaps, float alpha, float sps) { // unit energy
    std::vector<float> taps(nTaps);
    const float        centre = static_cast<float>(nTaps - 1UZ) / 2.f;
    for (std::size_t i = 0UZ; i < nTaps; ++i) {
        const float t = (static_cast<float>(i) - centre) / sps;
        float       h;
        if (std::abs(t) < 1e-6f) {
            h = 1.f - alpha + 4.f * alpha / std::numbers::pi_v<float>;
        } else if (std::abs(std::abs(4.f * alpha * t) - 1.f) < 1e-4f) {
            h = alpha / std::numbers::sqrt2_v<float> * ((1.f + 2.f / std::numbers::pi_v<float>) * std::sin(std::numbers::pi_v<float> / (4.f * alpha)) + (1.f - 2.f / std::numbers::pi_v<float>) * std::cos(std::numbers::pi_v<float> / (4.f * alpha)));
        } else {
            const float pt = std::numbers::pi_v<float> * t;
            h              = (std::sin(pt * (1.f - alpha)) + 4.f * alpha * t * std::cos(pt * (1.f + alpha))) / (pt * (1.f - (4.f * alpha * t) * (4.f * alpha * t)));
        }
        taps[i] = h;
    }
    const float energy = std::sqrt(std::inner_product(taps.begin(), taps.end(), taps.begin(), 0.f));
    for (float& tap : taps) {
        tap /= energy;
    }
    return taps;
}

constexpr std::uint64_t kPeriodNs = 10'000'000; // every application runs on 10 ms chunks

void annotateUrgent(auto& block, std::uint64_t batch) {
    using namespace gr::scheduler;
    block.meta_information.value[std::string(kBatchSizeKey)] = batch;
    block.meta_information.value[std::string(kPeriodKey)]    = kPeriodNs;
    block.meta_information.value[std::string(kDeadlineKey)]  = kPeriodNs;
    block.meta_information.value[std::string(kPriorityKey)]  = std::int64_t{0};
}

struct Readout {
    std::vector<std::function<const std::vector<std::int64_t>&()>> latencies;
    std::function<std::string()>                                   quality;
};

template<typename A, typename B>
void link(gr::Graph& flow, A& a, B& b, gr::Size_t buffer) {
    if (!flow.connect<"out", "in">(a, b, {.minBufferSize = buffer}).has_value()) {
        throw std::runtime_error("failed to connect");
    }
}

// --- the applications -------------------------------------------------------------------------------------

Readout buildNbfm(gr::Graph& flow, std::string_view suffix = "") {
    constexpr float      fs = 192'000.f;
    constexpr gr::Size_t chunk = 1920U;
    auto& source  = flow.emplaceBlock<PacedSource<FmToneSignal>>({{"name", std::format("fmSource{}", suffix)}, {"sample_rate", fs}, {"chunk_size", chunk}});
    auto& chanFir = flow.emplaceBlock<ComplexFir>({{"name", std::format("channelFir{}", suffix)}});
    auto& squelch = flow.emplaceBlock<Squelch>({{"name", std::format("squelch{}", suffix)}});
    auto& demod   = flow.emplaceBlock<QuadratureDemod>({{"name", std::format("quadDemod{}", suffix)}, {"gain", fs / (kTwoPi * 5'000.f)}});
    auto& deemph  = flow.emplaceBlock<FmDeemphasis>({{"name", std::format("deemphasis{}", suffix)}, {"sample_rate", fs}});
    auto& audioFir = flow.emplaceBlock<RealFir>({{"name", std::format("audioFir{}", suffix)}});
    auto& volume  = flow.emplaceBlock<Volume>({{"name", std::format("volume{}", suffix)}});
    auto& sink    = flow.emplaceBlock<AudioSink>({{"name", std::format("audioSink{}", suffix)}, {"sample_rate", fs}, {"chunk_size", chunk}});
    chanFir.taps  = lowPassTaps(6'000.0, fs);
    audioFir.taps = lowPassTaps(2'700.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(chanFir, chunk); annotateUrgent(squelch, chunk); annotateUrgent(demod, chunk);
    annotateUrgent(deemph, chunk); annotateUrgent(audioFir, chunk); annotateUrgent(volume, chunk); annotateUrgent(sink, chunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, chanFir, buffer); link(flow, chanFir, squelch, buffer); link(flow, squelch, demod, buffer); link(flow, demod, deemph, buffer);
    link(flow, deemph, audioFir, buffer); link(flow, audioFir, volume, buffer); link(flow, volume, sink, buffer);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildWbfm(gr::Graph& flow) { // broadcast FM: 240 kS/s quadrature, audio decimated by 5 to 48 kS/s
    constexpr float      fs = 240'000.f;
    constexpr gr::Size_t chunk = 2400U, audioChunk = 480U;
    auto& source  = flow.emplaceBlock<PacedSource<FmToneSignal>>({{"name", "fmSource"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    source.signal.maxDev = 75'000.f;
    auto& chanFir = flow.emplaceBlock<ComplexFir>({{"name", "channelFir"}});
    auto& demod   = flow.emplaceBlock<QuadratureDemod>({{"name", "quadDemod"}, {"gain", fs / (kTwoPi * 75'000.f)}});
    auto& deemph  = flow.emplaceBlock<FmDeemphasis>({{"name", "deemphasis"}, {"sample_rate", fs}});
    auto& audioFir = flow.emplaceBlock<RealFir>({{"name", "audioFir"}});
    auto& decim   = flow.emplaceBlock<gr::filter::Decimator<float>>({{"name", "audioDecimator"}, {"decim", 5U}});
    auto& volume  = flow.emplaceBlock<Volume>({{"name", "volume"}});
    auto& sink    = flow.emplaceBlock<AudioSink>({{"name", "audioSink"}, {"sample_rate", 48'000.f}, {"chunk_size", audioChunk}});
    chanFir.taps  = lowPassTaps(100'000.0, fs);
    audioFir.taps = lowPassTaps(15'000.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(chanFir, chunk); annotateUrgent(demod, chunk); annotateUrgent(deemph, chunk);
    annotateUrgent(audioFir, chunk); annotateUrgent(decim, chunk); annotateUrgent(volume, audioChunk); annotateUrgent(sink, audioChunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, chanFir, buffer); link(flow, chanFir, demod, buffer); link(flow, demod, deemph, buffer); link(flow, deemph, audioFir, buffer);
    link(flow, audioFir, decim, buffer); link(flow, decim, volume, audioChunk * 16U); link(flow, volume, sink, audioChunk * 16U);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildAm(gr::Graph& flow) { // envelope detector: 96 kS/s IF, decimated by 2 to 48 kS/s audio
    constexpr float      fs = 96'000.f;
    constexpr gr::Size_t chunk = 960U, audioChunk = 480U;
    auto& source   = flow.emplaceBlock<PacedSource<AmToneSignal>>({{"name", "amSource"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    auto& chanFir  = flow.emplaceBlock<ComplexFir>({{"name", "channelFir"}});
    auto& envelope = flow.emplaceBlock<Magnitude>({{"name", "envelope"}});
    auto& dc       = flow.emplaceBlock<DcBlocker>({{"name", "dcBlocker"}});
    auto& audioFir = flow.emplaceBlock<RealFir>({{"name", "audioFir"}});
    auto& decim    = flow.emplaceBlock<gr::filter::Decimator<float>>({{"name", "audioDecimator"}, {"decim", 2U}});
    auto& volume   = flow.emplaceBlock<Volume>({{"name", "volume"}});
    auto& sink     = flow.emplaceBlock<AudioSink>({{"name", "audioSink"}, {"sample_rate", 48'000.f}, {"chunk_size", audioChunk}});
    chanFir.taps   = lowPassTaps(15'000.0, fs);
    audioFir.taps  = lowPassTaps(4'000.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(chanFir, chunk); annotateUrgent(envelope, chunk); annotateUrgent(dc, chunk);
    annotateUrgent(audioFir, chunk); annotateUrgent(decim, chunk); annotateUrgent(volume, audioChunk); annotateUrgent(sink, audioChunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, chanFir, buffer); link(flow, chanFir, envelope, buffer); link(flow, envelope, dc, buffer); link(flow, dc, audioFir, buffer);
    link(flow, audioFir, decim, buffer); link(flow, decim, volume, audioChunk * 16U); link(flow, volume, sink, audioChunk * 16U);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildSsb(gr::Graph& flow) { // filter-method USB receiver on a 12 kHz IF at 48 kS/s
    constexpr float      fs = 48'000.f;
    constexpr gr::Size_t chunk = 480U;
    auto& source  = flow.emplaceBlock<PacedSource<SsbToneSignal>>({{"name", "ssbSource"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    auto& rotator = flow.emplaceBlock<gr::blocks::math::Rotator<cf>>({{"name", "bfo"}, {"sample_rate", fs}, {"frequency_shift", -12'000.f}});
    auto& sbFir   = flow.emplaceBlock<ComplexFir>({{"name", "sidebandFir"}});
    auto& real    = flow.emplaceBlock<RealPart>({{"name", "productDetector"}});
    auto& audioFir = flow.emplaceBlock<RealFir>({{"name", "audioFir"}});
    auto& volume  = flow.emplaceBlock<Volume>({{"name", "volume"}});
    auto& sink    = flow.emplaceBlock<AudioSink>({{"name", "audioSink"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    sbFir.taps    = lowPassTaps(3'000.0, fs);
    audioFir.taps = lowPassTaps(3'000.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(rotator, chunk); annotateUrgent(sbFir, chunk); annotateUrgent(real, chunk);
    annotateUrgent(audioFir, chunk); annotateUrgent(volume, chunk); annotateUrgent(sink, chunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, rotator, buffer); link(flow, rotator, sbFir, buffer); link(flow, sbFir, real, buffer); link(flow, real, audioFir, buffer);
    link(flow, audioFir, volume, buffer); link(flow, volume, sink, buffer);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildSpectrum(gr::Graph& flow) { // 1.024 MS/s spectrum analyser: 1024-point FFT, 10 frames per chunk
    constexpr float      fs = 1'024'000.f;
    constexpr gr::Size_t chunk = 10'240U, frames = 10U;
    auto& source = flow.emplaceBlock<PacedSource<ComplexToneSignal>>({{"name", "iqSource"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    auto& fft    = flow.emplaceBlock<gr::blocks::fft::FFT<cf>>({{"name", "fft"}, {"fftSize", 1024U}, {"sample_rate", fs}, {"outputInDb", false}});
    auto& sink   = flow.emplaceBlock<SpectrumSink>({{"name", "spectrumSink"}, {"frames_per_second", fs / 1024.f}, {"chunk_size", frames}});
    annotateUrgent(source, chunk); annotateUrgent(fft, chunk); annotateUrgent(sink, frames);
    link(flow, source, fft, chunk * 16U);
    if (!flow.connect<"out", "in">(fft, sink, {.minBufferSize = 64U}).has_value()) {
        throw std::runtime_error("failed to connect fft sink");
    }
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildQpsk(gr::Graph& flow) { // 62.5 ksym/s QPSK link: RRC pulse shaping, AWGN channel, matched filter, symbol decimation
    constexpr float      symbolRate = 62'500.f;
    constexpr gr::Size_t chunk = 625U, samples = 2'500U;
    auto& source  = flow.emplaceBlock<PacedSource<QpskSymbolSignal>>({{"name", "symbolSource"}, {"sample_rate", symbolRate}, {"chunk_size", chunk}});
    auto& shaper  = flow.emplaceBlock<RrcInterpolator>({{"name", "rrcShaper"}});
    auto& channel = flow.emplaceBlock<AwgnChannel>({{"name", "awgnChannel"}, {"noise_sigma", 0.05f}});
    auto& matched = flow.emplaceBlock<ComplexFir>({{"name", "matchedFilter"}});
    auto& decim   = flow.emplaceBlock<gr::filter::Decimator<cf>>({{"name", "symbolSampler"}, {"decim", 4U}});
    auto& sink    = flow.emplaceBlock<EvmSink>({{"name", "evmSink"}, {"sample_rate", symbolRate}, {"chunk_size", chunk}});
    // group delays: 44-tap shaper 21.5 samples + 46-tap matched filter 22.5 samples = 44 = 11 symbols exactly, so the
    // decimator's phase-0 output lands on the symbol instants
    shaper.taps  = rrcTaps(44UZ, 0.35f, 4.f);
    matched.taps = rrcTaps(46UZ, 0.35f, 4.f);
    annotateUrgent(source, chunk); annotateUrgent(shaper, chunk); annotateUrgent(channel, samples); annotateUrgent(matched, samples); annotateUrgent(decim, samples); annotateUrgent(sink, chunk);
    link(flow, source, shaper, chunk * 16U); link(flow, shaper, channel, samples * 16U); link(flow, channel, matched, samples * 16U); link(flow, matched, decim, samples * 16U); link(flow, decim, sink, chunk * 16U);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildScanner(gr::Graph& flow) { // three NBFM channels monitored at once
    Readout all;
    for (int channel = 0; channel < 3; ++channel) {
        Readout one = buildNbfm(flow, std::format("_{}", channel));
        all.latencies.push_back(one.latencies.front());
        if (channel == 0) {
            all.quality = one.quality;
        }
    }
    return all;
}

Readout buildAudioEq(gr::Graph& flow) { // 48 kS/s audio: six biquad stages and a soft clipper
    constexpr float      fs = 48'000.f;
    constexpr gr::Size_t chunk = 480U;
    auto& source = flow.emplaceBlock<PacedSource<AudioToneSignal>>({{"name", "audioIn"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    std::vector<Biquad*> stages;
    for (int stage = 0; stage < 6; ++stage) {
        Biquad& biquad = flow.emplaceBlock<Biquad>({{"name", std::format("eq{}", stage)}});
        biquad.lowPass(8'000.f + 1'000.f * static_cast<float>(stage), fs, 0.707f);
        stages.push_back(std::addressof(biquad));
    }
    auto& clip = flow.emplaceBlock<SoftClip>({{"name", "softClip"}});
    auto& sink = flow.emplaceBlock<AudioSink>({{"name", "audioOut"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    annotateUrgent(source, chunk); annotateUrgent(clip, chunk); annotateUrgent(sink, chunk);
    for (Biquad* stage : stages) {
        annotateUrgent(*stage, chunk);
    }
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, *stages.front(), buffer);
    for (std::size_t index = 1UZ; index < stages.size(); ++index) {
        link(flow, *stages[index - 1UZ], *stages[index], buffer);
    }
    link(flow, *stages.back(), clip, buffer); link(flow, clip, sink, buffer);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

gr::testing::AtomicCountingSink<float>* buildBulk(gr::Graph& flow, std::size_t depth, gr::Size_t ops, std::size_t batch) {
    using namespace gr::scheduler;
    auto& bulkSource = flow.emplaceBlock<gr::testing::CountingSource<float>>({{"name", "bulkSource"}});
    std::vector<BusyWork*> stages;
    for (std::size_t index = 0UZ; index < depth; ++index) {
        stages.push_back(std::addressof(flow.emplaceBlock<BusyWork>({{"name", std::format("busy{}", index)}, {"ops_per_sample", ops}})));
    }
    auto& bulkSink = flow.emplaceBlock<gr::testing::AtomicCountingSink<float>>({{"name", "bulkSink"}});
    const auto annotate = [batch](auto& block) {
        block.meta_information.value[std::string(kBatchSizeKey)] = static_cast<std::uint64_t>(batch);
        block.meta_information.value[std::string(kDeadlineKey)]  = std::uint64_t{250'000'000};
        block.meta_information.value[std::string(kPriorityKey)]  = std::int64_t{10};
    };
    annotate(bulkSource); annotate(bulkSink);
    for (BusyWork* stage : stages) {
        annotate(*stage);
    }
    const auto buffer = static_cast<gr::Size_t>(2UZ * batch);
    link(flow, bulkSource, *stages.front(), buffer);
    for (std::size_t index = 1UZ; index < stages.size(); ++index) {
        link(flow, *stages[index - 1UZ], *stages[index], buffer);
    }
    link(flow, *stages.back(), bulkSink, buffer);
    return std::addressof(bulkSink);
}


// --- the same applications with built-in blocks wherever one exists ------------------------------------------
// The paced source, the measuring sinks, and the stages with no built-in counterpart (complex FIR, quadrature
// demodulator, squelch, RRC shaper, channel model) stay as above; everything else is the library block.

using BuiltinFir  = gr::filter::fir_filter<float>;
using BuiltinIir  = gr::filter::iir_filter<float, gr::filter::IIRForm::DF_II>;
using BuiltinGain = gr::blocks::math::MultiplyConst<float>;

[[nodiscard]] gr::Tensor<float> tensorOf(const std::vector<float>& values) { return gr::Tensor<float>(gr::data_from, values); }

// first-order de-emphasis y = bt*x + at*y[n-1] as H(z) = bt / (1 - at*z^-1)
[[nodiscard]] std::pair<gr::Tensor<float>, gr::Tensor<float>> deemphasisCoefficients(float tau, float fs) {
    const float dt = 1.f / fs;
    return {tensorOf({dt / (tau + dt)}), tensorOf({1.f, -(tau / (tau + dt))})};
}

Readout buildNbfmBuiltin(gr::Graph& flow, std::string_view suffix = "") {
    constexpr float      fs = 192'000.f;
    constexpr gr::Size_t chunk = 1920U;
    const auto [deemphB, deemphA] = deemphasisCoefficients(75e-6f, fs);
    auto& source   = flow.emplaceBlock<PacedSource<FmToneSignal>>({{"name", std::format("fmSource{}", suffix)}, {"sample_rate", fs}, {"chunk_size", chunk}});
    auto& chanFir  = flow.emplaceBlock<ComplexFir>({{"name", std::format("channelFir{}", suffix)}});
    auto& squelch  = flow.emplaceBlock<Squelch>({{"name", std::format("squelch{}", suffix)}});
    auto& demod    = flow.emplaceBlock<QuadratureDemod>({{"name", std::format("quadDemod{}", suffix)}, {"gain", fs / (kTwoPi * 5'000.f)}});
    auto& deemph   = flow.emplaceBlock<BuiltinIir>({{"name", std::format("deemphasis{}", suffix)}, {"b", deemphB}, {"a", deemphA}});
    auto& audioFir = flow.emplaceBlock<BuiltinFir>({{"name", std::format("audioFir{}", suffix)}, {"b", tensorOf(lowPassTaps(2'700.0, fs))}});
    auto& volume   = flow.emplaceBlock<BuiltinGain>({{"name", std::format("volume{}", suffix)}, {"value", 0.5f}});
    auto& sink     = flow.emplaceBlock<AudioSink>({{"name", std::format("audioSink{}", suffix)}, {"sample_rate", fs}, {"chunk_size", chunk}});
    chanFir.taps   = lowPassTaps(6'000.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(chanFir, chunk); annotateUrgent(squelch, chunk); annotateUrgent(demod, chunk);
    annotateUrgent(deemph, chunk); annotateUrgent(audioFir, chunk); annotateUrgent(volume, chunk); annotateUrgent(sink, chunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, chanFir, buffer); link(flow, chanFir, squelch, buffer); link(flow, squelch, demod, buffer); link(flow, demod, deemph, buffer);
    link(flow, deemph, audioFir, buffer); link(flow, audioFir, volume, buffer); link(flow, volume, sink, buffer);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildWbfmBuiltin(gr::Graph& flow) {
    constexpr float      fs = 240'000.f;
    constexpr gr::Size_t chunk = 2400U, audioChunk = 480U;
    const auto [deemphB, deemphA] = deemphasisCoefficients(75e-6f, fs);
    auto& source   = flow.emplaceBlock<PacedSource<FmToneSignal>>({{"name", "fmSource"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    source.signal.maxDev = 75'000.f;
    auto& chanFir  = flow.emplaceBlock<ComplexFir>({{"name", "channelFir"}});
    auto& demod    = flow.emplaceBlock<QuadratureDemod>({{"name", "quadDemod"}, {"gain", fs / (kTwoPi * 75'000.f)}});
    auto& deemph   = flow.emplaceBlock<BuiltinIir>({{"name", "deemphasis"}, {"b", deemphB}, {"a", deemphA}});
    auto& audioFir = flow.emplaceBlock<BuiltinFir>({{"name", "audioFir"}, {"b", tensorOf(lowPassTaps(15'000.0, fs))}});
    auto& decim    = flow.emplaceBlock<gr::filter::Decimator<float>>({{"name", "audioDecimator"}, {"decim", 5U}});
    auto& volume   = flow.emplaceBlock<BuiltinGain>({{"name", "volume"}, {"value", 0.5f}});
    auto& sink     = flow.emplaceBlock<AudioSink>({{"name", "audioSink"}, {"sample_rate", 48'000.f}, {"chunk_size", audioChunk}});
    chanFir.taps   = lowPassTaps(100'000.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(chanFir, chunk); annotateUrgent(demod, chunk); annotateUrgent(deemph, chunk);
    annotateUrgent(audioFir, chunk); annotateUrgent(decim, chunk); annotateUrgent(volume, audioChunk); annotateUrgent(sink, audioChunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, chanFir, buffer); link(flow, chanFir, demod, buffer); link(flow, demod, deemph, buffer); link(flow, deemph, audioFir, buffer);
    link(flow, audioFir, decim, buffer); link(flow, decim, volume, audioChunk * 16U); link(flow, volume, sink, audioChunk * 16U);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildAmBuiltin(gr::Graph& flow) {
    constexpr float      fs = 96'000.f;
    constexpr gr::Size_t chunk = 960U, audioChunk = 480U;
    auto& source   = flow.emplaceBlock<PacedSource<AmToneSignal>>({{"name", "amSource"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    auto& chanFir  = flow.emplaceBlock<ComplexFir>({{"name", "channelFir"}});
    auto& envelope = flow.emplaceBlock<gr::blocks::type::converter::Abs<cf>>({{"name", "envelope"}});
    auto& dc       = flow.emplaceBlock<BuiltinIir>({{"name", "dcBlocker"}, {"b", tensorOf({1.f, -1.f})}, {"a", tensorOf({1.f, -0.995f})}});
    auto& audioFir = flow.emplaceBlock<BuiltinFir>({{"name", "audioFir"}, {"b", tensorOf(lowPassTaps(4'000.0, fs))}});
    auto& decim    = flow.emplaceBlock<gr::filter::Decimator<float>>({{"name", "audioDecimator"}, {"decim", 2U}});
    auto& volume   = flow.emplaceBlock<BuiltinGain>({{"name", "volume"}, {"value", 0.5f}});
    auto& sink     = flow.emplaceBlock<AudioSink>({{"name", "audioSink"}, {"sample_rate", 48'000.f}, {"chunk_size", audioChunk}});
    chanFir.taps   = lowPassTaps(15'000.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(chanFir, chunk); annotateUrgent(envelope, chunk); annotateUrgent(dc, chunk);
    annotateUrgent(audioFir, chunk); annotateUrgent(decim, chunk); annotateUrgent(volume, audioChunk); annotateUrgent(sink, audioChunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, chanFir, buffer);
    if (!flow.connect<"out", "in">(chanFir, envelope, {.minBufferSize = buffer}).has_value() || !flow.connect<"abs", "in">(envelope, dc, {.minBufferSize = buffer}).has_value()) {
        throw std::runtime_error("failed to connect envelope detector");
    }
    link(flow, dc, audioFir, buffer); link(flow, audioFir, decim, buffer); link(flow, decim, volume, audioChunk * 16U); link(flow, volume, sink, audioChunk * 16U);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildSsbBuiltin(gr::Graph& flow) {
    constexpr float      fs = 48'000.f;
    constexpr gr::Size_t chunk = 480U;
    auto& source   = flow.emplaceBlock<PacedSource<SsbToneSignal>>({{"name", "ssbSource"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    auto& rotator  = flow.emplaceBlock<gr::blocks::math::Rotator<cf>>({{"name", "bfo"}, {"sample_rate", fs}, {"frequency_shift", -12'000.f}});
    auto& sbFir    = flow.emplaceBlock<ComplexFir>({{"name", "sidebandFir"}});
    auto& real     = flow.emplaceBlock<gr::blocks::type::converter::Real<cf>>({{"name", "productDetector"}});
    auto& audioFir = flow.emplaceBlock<BuiltinFir>({{"name", "audioFir"}, {"b", tensorOf(lowPassTaps(3'000.0, fs))}});
    auto& volume   = flow.emplaceBlock<BuiltinGain>({{"name", "volume"}, {"value", 0.5f}});
    auto& sink     = flow.emplaceBlock<AudioSink>({{"name", "audioSink"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    sbFir.taps     = lowPassTaps(3'000.0, fs);
    annotateUrgent(source, chunk); annotateUrgent(rotator, chunk); annotateUrgent(sbFir, chunk); annotateUrgent(real, chunk);
    annotateUrgent(audioFir, chunk); annotateUrgent(volume, chunk); annotateUrgent(sink, chunk);
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, rotator, buffer); link(flow, rotator, sbFir, buffer);
    if (!flow.connect<"out", "in">(sbFir, real, {.minBufferSize = buffer}).has_value() || !flow.connect<"real", "in">(real, audioFir, {.minBufferSize = buffer}).has_value()) {
        throw std::runtime_error("failed to connect product detector");
    }
    link(flow, audioFir, volume, buffer); link(flow, volume, sink, buffer);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

Readout buildScannerBuiltin(gr::Graph& flow) {
    Readout all;
    for (int channel = 0; channel < 3; ++channel) {
        Readout one = buildNbfmBuiltin(flow, std::format("_{}", channel));
        all.latencies.push_back(one.latencies.front());
        if (channel == 0) {
            all.quality = one.quality;
        }
    }
    return all;
}

Readout buildAudioEqBuiltin(gr::Graph& flow) {
    constexpr float      fs = 48'000.f;
    constexpr gr::Size_t chunk = 480U;
    auto& source = flow.emplaceBlock<PacedSource<AudioToneSignal>>({{"name", "audioIn"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    std::vector<BuiltinIir*> stages;
    for (int stage = 0; stage < 6; ++stage) {
        Biquad design;
        design.lowPass(8'000.f + 1'000.f * static_cast<float>(stage), fs, 0.707f);
        stages.push_back(std::addressof(flow.emplaceBlock<BuiltinIir>({{"name", std::format("eq{}", stage)}, {"b", tensorOf({design.b0, design.b1, design.b2})}, {"a", tensorOf({1.f, design.a1, design.a2})}})));
    }
    auto& clip = flow.emplaceBlock<gr::blocks::math::ExpressionSISO<float>>({{"name", "softClip"}, {"expr_string", std::string("tanh(1.5 * x)")}});
    auto& sink = flow.emplaceBlock<AudioSink>({{"name", "audioOut"}, {"sample_rate", fs}, {"chunk_size", chunk}});
    annotateUrgent(source, chunk); annotateUrgent(clip, chunk); annotateUrgent(sink, chunk);
    for (BuiltinIir* stage : stages) {
        annotateUrgent(*stage, chunk);
    }
    const gr::Size_t buffer = chunk * 16U;
    link(flow, source, *stages.front(), buffer);
    for (std::size_t index = 1UZ; index < stages.size(); ++index) {
        link(flow, *stages[index - 1UZ], *stages[index], buffer);
    }
    link(flow, *stages.back(), clip, buffer); link(flow, clip, sink, buffer);
    return Readout{.latencies = {[&sink]() -> const std::vector<std::int64_t>& { return sink.latencies; }}, .quality = [&sink] { return sink.quality(); }};
}

struct Application {
    std::string_view                      name;
    std::function<Readout(gr::Graph&)>    build;
};

const std::vector<Application> kApplications{
    {"nbfm_rx", [](gr::Graph& g) { return buildNbfm(g); }},
    {"wbfm_rx", buildWbfm},
    {"am_rx", buildAm},
    {"ssb_rx", buildSsb},
    {"fft_analyzer", buildSpectrum},
    {"qpsk_link", buildQpsk},
    {"nbfm_scanner3", buildScanner},
    {"audio_eq", buildAudioEq},
    {"nbfm_rx_builtin", [](gr::Graph& g) { return buildNbfmBuiltin(g); }},
    {"wbfm_rx_builtin", buildWbfmBuiltin},
    {"am_rx_builtin", buildAmBuiltin},
    {"ssb_rx_builtin", buildSsbBuiltin},
    {"nbfm_scanner3_builtin", buildScannerBuiltin},
    {"audio_eq_builtin", buildAudioEqBuiltin},
};

template<typename TScheduler>
void runOnce(std::string_view policy, const Application& app, std::size_t depth, double seconds) {
    constexpr std::size_t kBulkBatch = 65'536UZ;
    gAnchorNs.store(0);
    gr::Graph flow;
    Readout   readout  = app.build(flow);
    auto*     bulkSink = depth > 0UZ ? buildBulk(flow, depth, 32U, kBulkBatch) : nullptr;

    TScheduler scheduler({{"max_work_items", kBulkBatch}});
    if (auto exchanged = scheduler.exchange(std::move(flow)); !exchanged.has_value()) {
        throw std::runtime_error(std::format("scheduler exchange failed: {}", exchanged.error()));
    }
    std::jthread stopper([&scheduler, seconds] {
        while (scheduler.state() != gr::lifecycle::State::RUNNING) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
        scheduler.requestStop();
    });
    const auto start = SteadyClock::now();
    if (auto result = scheduler.runAndWait(); !result.has_value()) {
        throw std::runtime_error(std::format("scheduler run failed: {}", result.error()));
    }
    const double elapsed = std::chrono::duration<double>(SteadyClock::now() - start).count();

    std::vector<std::int64_t> lat;
    for (const auto& source : readout.latencies) {
        const std::vector<std::int64_t>& one = source();
        const auto warmup = std::min(one.size(), 20UZ);
        lat.insert(lat.end(), one.begin() + static_cast<std::ptrdiff_t>(warmup), one.end());
    }
    // a chunk that never reached the sink by the end of the run is as missed as a late one; each source owes
    // 100 chunks per second, of which the first 20 are the discarded warm-up and the last one is still in
    // flight when the run is stopped
    const std::size_t expected = readout.latencies.size() * (static_cast<std::size_t>(seconds * 100.0) - 21UZ);
    const auto nLate      = static_cast<std::size_t>(std::ranges::count_if(lat, [](std::int64_t l) { return l > static_cast<std::int64_t>(kPeriodNs); }));
    const auto nMissed    = nLate + (expected > lat.size() ? expected - lat.size() : 0UZ);
    std::ranges::sort(lat);
    const auto pct = [&lat](double f) { return lat.empty() ? 0.0 : static_cast<double>(lat[static_cast<std::size_t>(f * static_cast<double>(lat.size() - 1UZ))]) / 1e3; };
    const double bulkMsps = bulkSink != nullptr ? static_cast<double>(bulkSink->loadCount()) / elapsed / 1e6 : 0.0;
    std::println("{},{},{},{},{},{:.2f},{:.1f},{:.1f},{:.1f},{:.3f},\"{}\"", app.name, policy, depth, lat.size(), nMissed, expected > 0UZ ? 100.0 * static_cast<double>(std::min(nMissed, expected)) / static_cast<double>(expected) : 0.0, pct(0.5), pct(0.99),
        lat.empty() ? 0.0 : static_cast<double>(lat.back()) / 1e3, bulkMsps, readout.quality());
}

[[nodiscard]] std::size_t readArgument(std::string_view text, std::string_view name) {
    std::size_t parsed = 0UZ;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument(std::format("{} must be a non-negative integer", name));
    }
    return parsed;
}

} // namespace

int main(int argc, char** argv) {
    const double seconds = argc > 1 ? static_cast<double>(readArgument(argv[1], "seconds")) : 3.0;
    std::vector<std::size_t> depths{0UZ, 8UZ};
    if (argc > 2) {
        depths.clear();
        for (const auto part : std::views::split(std::string_view(argv[2]), ',')) {
            depths.push_back(readArgument(std::string_view(part.begin(), part.end()), "depth"));
        }
    }
    const std::string_view only = argc > 3 ? argv[3] : "";

    std::println("app,policy,depth,delivered,missed,miss_pct,p50_us,p99_us,max_us,bulk_msps,quality");
    for (const Application& app : kApplications) {
        if (!only.empty() && app.name != only) {
            continue;
        }
        for (const std::size_t depth : depths) {
            runOnce<gr::scheduler::Simple<>>("Simple", app, depth, seconds);
            runOnce<gr::scheduler::BreadthFirst<>>("BreadthFirst", app, depth, seconds);
            runOnce<gr::scheduler::DepthFirst<>>("DepthFirst", app, depth, seconds);
            runOnce<gr::scheduler::EarliestDeadlineFirst<>>("EDF", app, depth, seconds);
            runOnce<gr::scheduler::FixedPriority<>>("FixedPriority", app, depth, seconds);
        }
    }
}
