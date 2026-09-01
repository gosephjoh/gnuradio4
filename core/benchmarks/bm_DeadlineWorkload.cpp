#include <gnuradio-4.0/BlockingSync.hpp>
#include <gnuradio-4.0/EarliestDeadlineFirst.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>

#include "EdfSchedulerPrototype.hpp"

#include <gnuradio-4.0/testing/NullSources.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <format>
#include <limits>
#include <print>
#include <ranges>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

// Measures what a deadline-aware policy *buys* rather than what it costs: a wall-clock paced urgent chain
// (audio-like) shares one worker with a saturating bulk pipeline. Each urgent sample carries its nominal
// availability instant, so the sink observes true end-to-end tardiness -- including the source being
// dispatched late -- identically under every policy. Round-robin makes the urgent chain wait for a full sweep
// of bulk batches; a deadline policy waits at most one batch (the non-preemptive blocking term).

namespace {

using SteadyClock = std::chrono::steady_clock;

[[nodiscard]] std::int64_t nowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now().time_since_epoch()).count(); }

// paced source producing whole chunks at their wall-clock boundaries, the way an ADC driver hands over
// filled buffers. Every sample carries its chunk's boundary instant (ns on the steady clock): that is the
// earliest moment the chunk can exist as a unit, so arrival minus that stamp is pure scheduling tardiness.
// A backlog is produced in one call, so measured tardiness is not capped by a one-chunk-per-dispatch limit.
struct NominalTimeSource : gr::Block<NominalTimeSource>, gr::BlockingSync<NominalTimeSource, SteadyClock> {
    gr::PortOut<std::int64_t> out;

    gr::Annotated<float, "sample_rate">              sample_rate         = 48'000.f;
    gr::Annotated<gr::Size_t, "chunk_size">          chunk_size          = 480U;
    gr::Annotated<bool, "use_internal_thread">       use_internal_thread = true;

    GR_MAKE_REFLECTABLE(NominalTimeSource, out, sample_rate, chunk_size, use_internal_thread);

    std::uint64_t             _nChunksProduced = 0UZ;
    std::vector<std::int64_t> productionLagNs; // per chunk: production instant minus nominal boundary

    void start() { this->blockingSyncStart(); }
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

        const std::int64_t anchorNs      = std::chrono::duration_cast<std::chrono::nanoseconds>(this->blockingSyncStartTime().time_since_epoch()).count();
        const double       nsPerChunk    = 1e9 * static_cast<double>(chunk) / static_cast<double>(sample_rate);
        const std::int64_t producedAt    = nowNs();
        for (std::uint64_t chunkIndex = 0UZ; chunkIndex < nChunks; ++chunkIndex) {
            const std::int64_t boundaryNs = anchorNs + static_cast<std::int64_t>(static_cast<double>(_nChunksProduced + chunkIndex + 1UZ) * nsPerChunk);
            productionLagNs.push_back(producedAt - boundaryNs);
            for (std::uint64_t sample = 0UZ; sample < chunk; ++sample) {
                outSpan[chunkIndex * chunk + sample] = boundaryNs;
            }
        }
        outSpan.publish(nChunks * chunk);
        _nChunksProduced += nChunks;
        return gr::work::Status::OK;
    }
};

struct LatencySink : gr::Block<LatencySink> {
    gr::PortIn<std::int64_t> in;

    GR_MAKE_REFLECTABLE(LatencySink, in);

    std::vector<std::int64_t> latenciesNs;

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan) {
        const std::int64_t arrival = nowNs();
        for (const std::int64_t nominal : inSpan) {
            latenciesNs.push_back(arrival - nominal);
        }
        return gr::work::Status::OK;
    }
};

// tunable per-sample compute cost; the loop-carried dependency keeps the work proportional to ops_per_sample
struct BusyWork : gr::Block<BusyWork> {
    gr::PortIn<float>  in;
    gr::PortOut<float> out;

    gr::Annotated<gr::Size_t, "ops_per_sample"> ops_per_sample = 16U;

    GR_MAKE_REFLECTABLE(BusyWork, in, out, ops_per_sample);

    [[nodiscard]] float processOne(float sample) const noexcept {
        float value = sample;
        for (gr::Size_t op = 0U; op < ops_per_sample; op++) {
            value = value * 1.0000001f + 1e-9f;
        }
        return value;
    }
};

struct WorkloadConfig {
    double        seconds      = 3.0;
    std::size_t   depth        = 8UZ;
    gr::Size_t    opsPerSample = 16U;
    std::size_t   bulkBatch    = 65'536UZ;
    float         urgentRate   = 48'000.f;
    gr::Size_t    urgentChunk  = 480U;
    std::int64_t  deadlineNs   = 10'000'000;
};

struct BuiltGraph {
    gr::Graph                               flow;
    NominalTimeSource*                      urgentSource = nullptr;
    LatencySink*                            urgentSink   = nullptr;
    gr::testing::AtomicCountingSink<float>* bulkSink     = nullptr;
};

[[nodiscard]] BuiltGraph makeGraph(const WorkloadConfig& config) {
    using namespace gr::scheduler;
    BuiltGraph built;

    auto& urgentSource = built.flow.emplaceBlock<NominalTimeSource>({{"name", "urgentSource"}, {"sample_rate", config.urgentRate}, {"chunk_size", config.urgentChunk}});
    auto& urgentSink   = built.flow.emplaceBlock<LatencySink>({{"name", "urgentSink"}});
    built.urgentSource = std::addressof(urgentSource);
    built.urgentSink   = std::addressof(urgentSink);
    urgentSink.latenciesNs.reserve(static_cast<std::size_t>(static_cast<double>(config.urgentRate) * config.seconds * 1.25));

    const auto chunkPeriodNs = static_cast<std::uint64_t>(1e9 * static_cast<double>(config.urgentChunk) / static_cast<double>(config.urgentRate));
    for (auto* annotated : std::initializer_list<gr::property_map*>{std::addressof(urgentSource.meta_information.value), std::addressof(urgentSink.meta_information.value)}) {
        (*annotated)[std::string(kBatchSizeKey)] = static_cast<std::uint64_t>(config.urgentChunk);
        (*annotated)[std::string(kPeriodKey)]    = chunkPeriodNs;
        (*annotated)[std::string(kDeadlineKey)]  = static_cast<std::uint64_t>(config.deadlineNs);
        (*annotated)[std::string(kPriorityKey)]  = std::int64_t{0};
    }

    const std::size_t urgentBuffer = std::max(8'192UZ, static_cast<std::size_t>(config.urgentChunk) * 16UZ);
    if (!built.flow.connect<"out", "in">(urgentSource, urgentSink, {.minBufferSize = static_cast<gr::Size_t>(urgentBuffer)}).has_value()) {
        throw std::runtime_error("failed to connect urgent chain");
    }

    auto& bulkSource = built.flow.emplaceBlock<gr::testing::CountingSource<float>>({{"name", "bulkSource"}});
    std::vector<BusyWork*> stages;
    stages.reserve(config.depth);
    for (std::size_t index = 0UZ; index < config.depth; ++index) {
        stages.push_back(std::addressof(built.flow.emplaceBlock<BusyWork>({{"name", std::format("busy{}", index)}, {"ops_per_sample", config.opsPerSample}})));
    }
    auto& bulkSink = built.flow.emplaceBlock<gr::testing::AtomicCountingSink<float>>({{"name", "bulkSink"}});
    built.bulkSink = std::addressof(bulkSink);

    auto annotateBulk = [&config](auto& block) {
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
    if (!built.flow.connect<"out", "in">(bulkSource, *stages.front(), {.minBufferSize = bulkBuffer}).has_value()) {
        throw std::runtime_error("failed to connect bulk source");
    }
    for (std::size_t index = 1UZ; index < stages.size(); ++index) {
        if (!built.flow.connect<"out", "in">(*stages[index - 1UZ], *stages[index], {.minBufferSize = bulkBuffer}).has_value()) {
            throw std::runtime_error("failed to connect bulk stage");
        }
    }
    if (!built.flow.connect<"out", "in">(*stages.back(), bulkSink, {.minBufferSize = bulkBuffer}).has_value()) {
        throw std::runtime_error("failed to connect bulk sink");
    }
    return built;
}

struct RunOutcome {
    std::vector<std::int64_t> latenciesNs;
    std::uint64_t             bulkSamples     = 0UZ;
    double                    seconds         = 0.0;
    std::uint64_t             schedSelections = 0UZ;
    std::uint64_t             schedSweeps     = 0UZ;
    std::uint64_t             schedMisses     = 0UZ;
    std::uint64_t             schedCancelled  = 0UZ;
};

template<typename TScheduler>
[[nodiscard]] RunOutcome runOnce(const WorkloadConfig& config) {
    BuiltGraph built = makeGraph(config);
    TScheduler scheduler({{"max_work_items", config.bulkBatch}});
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
    const auto stop = SteadyClock::now();

    RunOutcome outcome{
        .latenciesNs = std::move(built.urgentSink->latenciesNs),
        .bulkSamples = built.bulkSink->loadCount(),
        .seconds     = std::chrono::duration<double>(stop - start).count(),
    };
    if (std::vector<std::int64_t> lag = std::move(built.urgentSource->productionLagNs); !lag.empty()) {
        std::ranges::sort(lag);
        std::println("#   source production lag: p50={:.1f}us p99={:.1f}us max={:.1f}us over {} chunks", static_cast<double>(lag[lag.size() / 2UZ]) / 1e3, static_cast<double>(lag[(lag.size() * 99UZ) / 100UZ]) / 1e3,
            static_cast<double>(lag.back()) / 1e3, lag.size());
    }
    if constexpr (requires(const TScheduler& instance) { instance.statistics(); }) {
        const auto statistics   = scheduler.statistics();
        outcome.schedSelections = statistics.nSelections;
        outcome.schedSweeps     = statistics.nSweeps;
        outcome.schedMisses     = statistics.nMisses;
        outcome.schedCancelled  = statistics.nCancelled;
        for (const std::string_view name : {"urgentSource", "urgentSink"}) {
            if (const auto* task = scheduler.findTask(name); task != nullptr) {
                std::println("#   {}: dispatches={} released={} completed={} cancelled={} missed={} maxResponse_us={:.1f} batch={} period_us={:.1f} effDeadline_us={:.1f}", name, task->nDispatches, task->jobs.nReleased,
                    task->jobs.nCompleted, task->jobs.nCancelled, task->jobs.nMissed, static_cast<double>(task->jobs.maxResponseTime.count()) / 1e3, task->parameters.batchSize,
                    static_cast<double>(task->parameters.period.count()) / 1e3, static_cast<double>(task->effectiveDeadline.count()) / 1e3);
            }
        }
    }
    return outcome;
}

void printResult(std::string_view policy, const WorkloadConfig& config, RunOutcome outcome) {
    // the start-up transient -- buffers filling, first sweep -- is not steady-state behaviour
    const auto warmup = std::min(outcome.latenciesNs.size(), static_cast<std::size_t>(config.urgentRate) / 2UZ);
    std::vector<std::int64_t> steady(outcome.latenciesNs.begin() + static_cast<std::ptrdiff_t>(warmup), outcome.latenciesNs.end());
    if (steady.empty()) {
        std::println("{},{},0,,,,,,,,,", policy, config.depth);
        return;
    }
    std::ranges::sort(steady);

    const auto percentile = [&steady](double fraction) { return static_cast<double>(steady[static_cast<std::size_t>(fraction * static_cast<double>(steady.size() - 1UZ))]) / 1e3; };
    const auto nMissed    = static_cast<std::size_t>(std::ranges::distance(std::ranges::upper_bound(steady, config.deadlineNs), steady.end()));
    const double missPct  = 100.0 * static_cast<double>(nMissed) / static_cast<double>(steady.size());
    const double bulkMsps = static_cast<double>(outcome.bulkSamples) / outcome.seconds / 1e6;

    std::println("{},{},{},{:.1f},{:.1f},{:.1f},{:.3f},{:.3f},{},{},{},{}", policy, config.depth, steady.size(), percentile(0.5), percentile(0.99), static_cast<double>(steady.back()) / 1e3, missPct, bulkMsps,
        outcome.schedSelections, outcome.schedSweeps, outcome.schedMisses, outcome.schedCancelled);
}

[[nodiscard]] std::size_t readArgument(char* value, std::string_view name) {
    std::size_t            parsed = 0UZ;
    const std::string_view text(value);
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (error != std::errc{} || end != text.data() + text.size() || parsed == 0UZ) {
        throw std::invalid_argument(std::format("{} must be a positive integer", name));
    }
    return parsed;
}

[[nodiscard]] std::vector<std::size_t> readList(std::string_view list) {
    std::vector<std::size_t> values;
    for (const auto part : std::views::split(list, ',')) {
        const std::string_view text(part.begin(), part.end());
        std::size_t            parsed = 0UZ;
        const auto [end, error]       = std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (error != std::errc{} || end != text.data() + text.size() || parsed == 0UZ) {
            throw std::invalid_argument("list entries must be positive integers");
        }
        values.push_back(parsed);
    }
    return values;
}

} // namespace

int main(int argc, char** argv) {
    WorkloadConfig config;
    config.seconds                        = argc > 1 ? static_cast<double>(readArgument(argv[1], "seconds")) : 3.0;
    const std::vector<std::size_t> depths = argc > 2 ? readList(argv[2]) : std::vector<std::size_t>{1UZ, 2UZ, 4UZ, 8UZ, 16UZ};
    config.opsPerSample                   = static_cast<gr::Size_t>(argc > 3 ? readArgument(argv[3], "ops_per_sample") : 16UZ);
    config.bulkBatch                      = argc > 4 ? readArgument(argv[4], "bulk_batch") : 65'536UZ;
    config.deadlineNs                     = argc > 5 ? static_cast<std::int64_t>(readArgument(argv[5], "deadline_ns")) : 10'000'000;

    std::println("# seconds={}, ops_per_sample={}, bulk_batch={}, urgent_rate={}, urgent_chunk={}, deadline_ns={}", config.seconds, config.opsPerSample, config.bulkBatch, config.urgentRate, config.urgentChunk, config.deadlineNs);
    std::println("policy,depth,urgent_n,p50_us,p99_us,max_us,miss_pct,bulk_msps,selections,sweeps,sched_misses,sched_cancelled");

    using RoundRobin     = gr::scheduler::Simple<>;
    using ReleaseAwareEDF = gr::scheduler::EarliestDeadlineFirst<>;
    using FixedPriority  = gr::scheduler::FixedPriority<>;
    using StaticDepth    = gr::scheduler::EDF<>; // hand-built prototype: depth-ordered static sweep

    for (const std::size_t depth : depths) {
        config.depth = depth;
        printResult("RoundRobin", config, runOnce<RoundRobin>(config));
        printResult("StaticDepth", config, runOnce<StaticDepth>(config));
        printResult("EDF", config, runOnce<ReleaseAwareEDF>(config));
        printResult("FixedPriority", config, runOnce<FixedPriority>(config));
    }
}
