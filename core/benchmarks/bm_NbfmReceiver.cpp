#include <gnuradio-4.0/BlockingSync.hpp>
#include <gnuradio-4.0/EarliestDeadlineFirst.hpp>
#include <gnuradio-4.0/Profiler.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/algorithm/filter/FilterTool.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstdint>
#include <format>
#include <numbers>
#include <print>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

// A narrowband FM receiver -- channel FIR, squelch, quadrature demodulator, de-emphasis, audio FIR, volume --
// fed by a wall-clock paced IQ source at the quadrature rate, the way a driver delivers it, with an audio sink
// that must receive every chunk within one chunk period. Optionally the receiver shares its worker with a
// saturating bulk pipeline. Reports per-chunk audio tardiness and misses under every policy, and checks that
// the demodulated tone is actually recovered.

namespace {

using SteadyClock = std::chrono::steady_clock;

std::atomic<std::int64_t> gAnchorNs{0}; // source start instant on the steady clock, shared with the sink

[[nodiscard]] std::int64_t nowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now().time_since_epoch()).count(); }

struct FmSource : gr::Block<FmSource>, gr::BlockingSync<FmSource, SteadyClock> {
    gr::PortOut<std::complex<float>> out;

    gr::Annotated<float, "sample_rate">        sample_rate         = 192'000.f;
    gr::Annotated<gr::Size_t, "chunk_size">    chunk_size          = 1920U;
    gr::Annotated<bool, "use_internal_thread"> use_internal_thread = true;
    gr::Annotated<float, "tone_hz">            tone_hz             = 1'000.f;
    gr::Annotated<float, "max_dev">            max_dev             = 5'000.f;

    GR_MAKE_REFLECTABLE(FmSource, out, sample_rate, chunk_size, use_internal_thread, tone_hz, max_dev);

    std::uint64_t _nChunksProduced = 0UZ;
    float         _carrierPhase    = 0.f;
    float         _tonePhase       = 0.f;

    void start() {
        this->blockingSyncStart();
        gAnchorNs.store(std::chrono::duration_cast<std::chrono::nanoseconds>(this->blockingSyncStartTime().time_since_epoch()).count());
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
        constexpr float twoPi         = 2.f * std::numbers::pi_v<float>;
        const float     toneStep      = twoPi * tone_hz / sample_rate;
        const float     deviationStep = twoPi * max_dev / sample_rate;
        const std::size_t n           = nChunks * chunk;
        for (std::size_t index = 0UZ; index < n; ++index) {
            _tonePhase = std::fmod(_tonePhase + toneStep, twoPi);
            _carrierPhase = std::fmod(_carrierPhase + deviationStep * std::sin(_tonePhase), twoPi);
            outSpan[index] = std::polar(1.f, _carrierPhase);
        }
        outSpan.publish(n);
        _nChunksProduced += nChunks;
        return gr::work::Status::OK;
    }
};

struct ComplexFir : gr::Block<ComplexFir> {
    gr::PortIn<std::complex<float>>  in;
    gr::PortOut<std::complex<float>> out;

    GR_MAKE_REFLECTABLE(ComplexFir, in, out);

    std::vector<float>               taps{1.f};
    std::vector<std::complex<float>> _history;
    std::size_t                      _head = 0UZ;

    [[nodiscard]] std::complex<float> processOne(std::complex<float> x) noexcept {
        if (_history.size() != taps.size()) {
            _history.assign(taps.size(), {0.f, 0.f});
        }
        _history[_head] = x;
        std::complex<float> y{0.f, 0.f};
        const std::size_t   n = taps.size();
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
        _history[_head]     = x;
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
    gr::PortIn<std::complex<float>>  in;
    gr::PortOut<std::complex<float>> out;

    gr::Annotated<float, "threshold_db"> threshold_db = -50.f;
    gr::Annotated<float, "alpha">        alpha        = 0.001f;

    GR_MAKE_REFLECTABLE(Squelch, in, out, threshold_db, alpha);

    float _envelope = 0.f;

    [[nodiscard]] std::complex<float> processOne(std::complex<float> x) noexcept {
        _envelope = alpha * std::norm(x) + (1.f - alpha) * _envelope;
        return 10.f * std::log10(_envelope + 1e-20f) >= threshold_db ? x : std::complex<float>{0.f, 0.f};
    }
};

struct QuadratureDemod : gr::Block<QuadratureDemod> {
    gr::PortIn<std::complex<float>> in;
    gr::PortOut<float>              out;

    gr::Annotated<float, "gain"> gain = 1.f;

    GR_MAKE_REFLECTABLE(QuadratureDemod, in, out, gain);

    std::complex<float> _previous{1.f, 0.f};

    [[nodiscard]] float processOne(std::complex<float> x) noexcept {
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

// audio sink with a per-chunk delivery deadline: sample n is nominally available at the end of its chunk
struct AudioSink : gr::Block<AudioSink> {
    gr::PortIn<float> in;

    gr::Annotated<float, "sample_rate">     sample_rate = 192'000.f;
    gr::Annotated<gr::Size_t, "chunk_size"> chunk_size  = 1920U;

    GR_MAKE_REFLECTABLE(AudioSink, in, sample_rate, chunk_size);

    std::vector<std::int64_t> chunkLatencyNs; // per chunk: arrival of its last sample minus its nominal boundary
    double                    sumSquares = 0.0;
    std::uint64_t             nSamples = 0UZ;
    std::uint64_t             nZeroCrossings = 0UZ;
    float                     _previous = 0.f;

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan) {
        const std::int64_t arrival   = nowNs();
        const auto         chunk     = static_cast<std::uint64_t>(chunk_size.value);
        const double       nsPerChunk = 1e9 * static_cast<double>(chunk) / static_cast<double>(sample_rate);
        for (const float sample : inSpan) {
            sumSquares += static_cast<double>(sample) * static_cast<double>(sample);
            if ((sample >= 0.f) != (_previous >= 0.f)) {
                nZeroCrossings++;
            }
            _previous = sample;
            nSamples++;
            if (nSamples % chunk == 0UZ) {
                const std::int64_t boundary = gAnchorNs.load() + static_cast<std::int64_t>(static_cast<double>(nSamples / chunk) * nsPerChunk);
                chunkLatencyNs.push_back(arrival - boundary);
            }
        }
        return gr::work::Status::OK;
    }
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

struct Config {
    double      seconds      = 3.0;
    std::size_t depth        = 0UZ;
    gr::Size_t  opsPerSample = 32U;
    float       quadRate     = 192'000.f;
    gr::Size_t  chunk        = 1920U; // 10 ms
    float       maxDev       = 5'000.f;
    std::size_t bulkBatch    = 65'536UZ;
};

struct Built {
    gr::Graph                               flow;
    AudioSink*                              audio      = nullptr;
    gr::testing::AtomicCountingSink<float>* bulkSink   = nullptr;
    std::size_t                             channelTaps = 0UZ;
    std::size_t                             audioTaps   = 0UZ;
};

[[nodiscard]] Built makeGraph(const Config& config) {
    using namespace gr::scheduler;
    Built built;

    const auto channel = gr::filter::fir::designFilter<float>(gr::filter::Type::LOWPASS, {.fLow = 6'000.0, .attenuationDb = 60.0, .fs = static_cast<double>(config.quadRate)});
    const auto audio   = gr::filter::fir::designFilter<float>(gr::filter::Type::LOWPASS, {.fLow = 2'700.0, .attenuationDb = 60.0, .fs = static_cast<double>(config.quadRate)});
    built.channelTaps  = channel.b.size();
    built.audioTaps    = audio.b.size();

    auto& source   = built.flow.emplaceBlock<FmSource>({{"name", "fmSource"}, {"sample_rate", config.quadRate}, {"chunk_size", config.chunk}, {"max_dev", config.maxDev}});
    auto& chanFir  = built.flow.emplaceBlock<ComplexFir>({{"name", "channelFir"}});
    auto& squelch  = built.flow.emplaceBlock<Squelch>({{"name", "squelch"}});
    auto& demod    = built.flow.emplaceBlock<QuadratureDemod>({{"name", "quadDemod"}, {"gain", config.quadRate / (2.f * std::numbers::pi_v<float> * config.maxDev)}});
    auto& deemph   = built.flow.emplaceBlock<FmDeemphasis>({{"name", "deemphasis"}, {"sample_rate", config.quadRate}});
    auto& audioFir = built.flow.emplaceBlock<RealFir>({{"name", "audioFir"}});
    auto& volume   = built.flow.emplaceBlock<Volume>({{"name", "volume"}});
    auto& sink     = built.flow.emplaceBlock<AudioSink>({{"name", "audioSink"}, {"sample_rate", config.quadRate}, {"chunk_size", config.chunk}});
    chanFir.taps   = channel.b;
    audioFir.taps  = audio.b;
    built.audio    = std::addressof(sink);
    sink.chunkLatencyNs.reserve(static_cast<std::size_t>(config.seconds * 200.0));

    const auto chunkPeriodNs = static_cast<std::uint64_t>(1e9 * static_cast<double>(config.chunk) / static_cast<double>(config.quadRate));
    const auto annotateUrgent = [&](auto& block) {
        block.meta_information.value[std::string(kBatchSizeKey)] = static_cast<std::uint64_t>(config.chunk);
        block.meta_information.value[std::string(kPeriodKey)]    = chunkPeriodNs;
        block.meta_information.value[std::string(kDeadlineKey)]  = chunkPeriodNs;
        block.meta_information.value[std::string(kPriorityKey)]  = std::int64_t{0};
    };
    annotateUrgent(source); annotateUrgent(chanFir); annotateUrgent(squelch); annotateUrgent(demod);
    annotateUrgent(deemph); annotateUrgent(audioFir); annotateUrgent(volume); annotateUrgent(sink);

    const gr::Size_t buffer = config.chunk * 16U;
    const auto link = [&built, buffer](auto& a, auto& b) {
        if (!built.flow.connect<"out", "in">(a, b, {.minBufferSize = buffer}).has_value()) {
            throw std::runtime_error("failed to connect receiver chain");
        }
    };
    link(source, chanFir); link(chanFir, squelch); link(squelch, demod); link(demod, deemph); link(deemph, audioFir); link(audioFir, volume); link(volume, sink);

    if (config.depth > 0UZ) {
        auto& bulkSource = built.flow.emplaceBlock<gr::testing::CountingSource<float>>({{"name", "bulkSource"}});
        std::vector<BusyWork*> stages;
        for (std::size_t index = 0UZ; index < config.depth; ++index) {
            stages.push_back(std::addressof(built.flow.emplaceBlock<BusyWork>({{"name", std::format("busy{}", index)}, {"ops_per_sample", config.opsPerSample}})));
        }
        auto& bulkSink = built.flow.emplaceBlock<gr::testing::AtomicCountingSink<float>>({{"name", "bulkSink"}});
        built.bulkSink = std::addressof(bulkSink);
        const auto annotateBulk = [&config](auto& block) {
            block.meta_information.value[std::string(kBatchSizeKey)] = static_cast<std::uint64_t>(config.bulkBatch);
            block.meta_information.value[std::string(kDeadlineKey)]  = std::uint64_t{250'000'000};
            block.meta_information.value[std::string(kPriorityKey)]  = std::int64_t{10};
        };
        annotateBulk(bulkSource);
        for (BusyWork* stage : stages) {
            annotateBulk(*stage);
        }
        annotateBulk(bulkSink);
        const auto bulkBuffer = static_cast<gr::Size_t>(2UZ * config.bulkBatch);
        const auto linkBulk = [&built, bulkBuffer](auto& a, auto& b) {
            if (!built.flow.connect<"out", "in">(a, b, {.minBufferSize = bulkBuffer}).has_value()) {
                throw std::runtime_error("failed to connect bulk chain");
            }
        };
        linkBulk(bulkSource, *stages.front());
        for (std::size_t index = 1UZ; index < stages.size(); ++index) {
            linkBulk(*stages[index - 1UZ], *stages[index]);
        }
        linkBulk(*stages.back(), bulkSink);
    }
    return built;
}

template<typename TScheduler>
void runOnce(std::string_view policy, const Config& config) {
    Built      built = makeGraph(config);
    TScheduler scheduler({{"max_work_items", config.depth > 0UZ ? config.bulkBatch : static_cast<std::size_t>(config.chunk)}});
    if (auto exchanged = scheduler.exchange(std::move(built.flow)); !exchanged.has_value()) {
        throw std::runtime_error(std::format("scheduler exchange failed: {}", exchanged.error()));
    }
    std::jthread stopper([&scheduler, &config] {
        while (scheduler.state() != gr::lifecycle::State::RUNNING) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::duration<double>(config.seconds));
        scheduler.requestStop();
    });
    const auto start = SteadyClock::now();
    if (auto result = scheduler.runAndWait(); !result.has_value()) {
        throw std::runtime_error(std::format("scheduler run failed: {}", result.error()));
    }
    const double seconds = std::chrono::duration<double>(SteadyClock::now() - start).count();

    const AudioSink& audio = *built.audio;
    std::vector<std::int64_t> lat(audio.chunkLatencyNs);
    const auto warmup = std::min(lat.size(), 20UZ); // first 200 ms: buffers filling, filters settling
    lat.erase(lat.begin(), lat.begin() + static_cast<std::ptrdiff_t>(warmup));
    const auto deadlineNs = static_cast<std::int64_t>(1e9 * static_cast<double>(config.chunk) / static_cast<double>(config.quadRate));
    const auto nMissed    = static_cast<std::size_t>(std::ranges::count_if(lat, [deadlineNs](std::int64_t l) { return l > deadlineNs; }));
    std::ranges::sort(lat);
    const auto pct = [&lat](double f) { return lat.empty() ? 0.0 : static_cast<double>(lat[static_cast<std::size_t>(f * static_cast<double>(lat.size() - 1UZ))]) / 1e3; };
    const double rms     = audio.nSamples > 0UZ ? std::sqrt(audio.sumSquares / static_cast<double>(audio.nSamples)) : 0.0;
    const double toneHz  = audio.nSamples > 0UZ ? static_cast<double>(audio.nZeroCrossings) / 2.0 / (static_cast<double>(audio.nSamples) / static_cast<double>(config.quadRate)) : 0.0;
    const double bulkMsps = built.bulkSink != nullptr ? static_cast<double>(built.bulkSink->loadCount()) / seconds / 1e6 : 0.0;

    std::uint64_t withdrawn = 0UZ, sweeps = 0UZ;
    if constexpr (requires(const TScheduler& s) { s.statistics(); }) {
        const auto statistics = scheduler.statistics();
        withdrawn = statistics.nWithdrawn;
        sweeps    = statistics.nSweeps;
    }
    std::println("{},{},{},{},{:.2f},{:.1f},{:.1f},{:.1f},{:.3f},{:.0f},{:.3f},{},{}", policy, config.depth, lat.size(), nMissed, lat.empty() ? 0.0 : 100.0 * static_cast<double>(nMissed) / static_cast<double>(lat.size()), pct(0.5), pct(0.99),
        lat.empty() ? 0.0 : static_cast<double>(lat.back()) / 1e3, rms, toneHz, bulkMsps, withdrawn, sweeps);
}

[[nodiscard]] std::size_t readArgument(std::string_view text, std::string_view name) {
    std::size_t parsed = 0UZ;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument(std::format("{} must be a non-negative integer", name));
    }
    return parsed;
}

// trace mode: run EDF once with the file profiler so every release and dispatch decision -- with the absolute
// deadline and the eligible alternatives -- lands in a Chrome-trace JSON file for auditing
void traceEdf(const Config& config, std::string_view file) {
    using Traced = gr::scheduler::EarliestDeadlineFirst<gr::scheduler::ExecutionPolicy::singleThreaded, SteadyClock, gr::profiling::Profiler>;
    Built  built = makeGraph(config);
    {
        Traced scheduler({{"max_work_items", config.depth > 0UZ ? config.bulkBatch : static_cast<std::size_t>(config.chunk)}});
        if (auto exchanged = scheduler.exchange(std::move(built.flow), gr::profiling::Options{.output_file = std::string(file), .output_mode = gr::profiling::OutputMode::File}); !exchanged.has_value()) {
            throw std::runtime_error(std::format("scheduler exchange failed: {}", exchanged.error()));
        }
        std::jthread stopper([&scheduler, &config] {
            while (scheduler.state() != gr::lifecycle::State::RUNNING) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            std::this_thread::sleep_for(std::chrono::duration<double>(config.seconds));
            scheduler.requestStop();
        });
        if (auto result = scheduler.runAndWait(); !result.has_value()) {
            throw std::runtime_error(std::format("scheduler run failed: {}", result.error()));
        }
    } // the profiler flushes its file when the scheduler is destroyed
    std::println("# trace written to {} ({} audio chunks observed)", file, built.audio->chunkLatencyNs.size());
}

} // namespace

int main(int argc, char** argv) {
    if (argc > 1 && std::string_view(argv[1]) == "trace") {
        Config config;
        config.seconds = argc > 2 ? static_cast<double>(readArgument(argv[2], "seconds")) : 1.0;
        config.depth   = argc > 3 ? readArgument(argv[3], "depth") : 8UZ;
        traceEdf(config, argc > 4 ? argv[4] : "edf.trace.json");
        return 0;
    }
    Config config;
    config.seconds = argc > 1 ? static_cast<double>(readArgument(argv[1], "seconds")) : 3.0;
    std::vector<std::size_t> depths{0UZ, 4UZ, 8UZ, 16UZ};
    if (argc > 2) {
        depths.clear();
        for (const auto part : std::views::split(std::string_view(argv[2]), ',')) {
            depths.push_back(readArgument(std::string_view(part.begin(), part.end()), "depth"));
        }
    }
    config.opsPerSample = static_cast<gr::Size_t>(argc > 3 ? readArgument(argv[3], "ops_per_sample") : 32UZ);

    const Built probe = makeGraph(config);
    std::println("# NBFM receiver: quad rate {} Hz, chunk {} samples ({:.1f} ms period = deadline), channel FIR {} taps, audio FIR {} taps, max deviation {} Hz, tone 1000 Hz; bulk ops={}", config.quadRate, config.chunk,
        1e3 * static_cast<double>(config.chunk) / static_cast<double>(config.quadRate), probe.channelTaps, probe.audioTaps, config.maxDev, config.opsPerSample);
    std::println("policy,depth,chunks,missed,miss_pct,p50_us,p99_us,max_us,audio_rms,tone_hz,bulk_msps,withdrawn,sweeps");

    for (const std::size_t depth : depths) {
        config.depth = depth;
        runOnce<gr::scheduler::Simple<>>("RoundRobin", config);
        runOnce<gr::scheduler::EarliestDeadlineFirst<>>("EDF", config);
        runOnce<gr::scheduler::FixedPriority<>>("FixedPriority", config);
    }
}
