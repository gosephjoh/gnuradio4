#include <gnuradio-4.0/EarliestDeadlineFirst.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cmath>
#include <cstdint>
#include <format>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <print>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct RunResult {
    double        seconds      = 0.0;
    std::uint64_t dispatches   = 0UZ;
    std::uint64_t selections   = 0UZ;
    std::uint64_t sweeps       = 0UZ;
    std::uint64_t probes       = 0UZ;
};

struct GraphAndSink {
    gr::Graph                                graph;
    gr::testing::AtomicCountingSink<float>* sink = nullptr;
};

[[nodiscard]] GraphAndSink makeGraph(gr::Size_t nSamples, std::size_t depth, std::size_t batchSize) {
    GraphAndSink result;
    auto& source = result.graph.emplaceBlock<gr::testing::CountingSource<float>>({{"name", "source"}, {"n_samples_max", nSamples}});

    std::vector<gr::testing::Copy<float>*> copies;
    copies.reserve(depth);
    for (std::size_t index = 0UZ; index < depth; ++index) {
        copies.push_back(std::addressof(result.graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::format("copy{}", index)}})));
    }

    auto& sink = result.graph.emplaceBlock<gr::testing::AtomicCountingSink<float>>({{"name", "sink"}, {"n_samples_max", nSamples}});
    result.sink = std::addressof(sink);

    constexpr std::size_t kBufferSize = 65'536UZ;
    if (!result.graph.connect<"out", "in">(source, *copies.front(), {.minBufferSize = kBufferSize}).has_value()) {
        throw std::runtime_error("failed to connect benchmark source");
    }
    for (std::size_t index = 1UZ; index < copies.size(); ++index) {
        if (!result.graph.connect<"out", "in">(*copies[index - 1UZ], *copies[index], {.minBufferSize = kBufferSize}).has_value()) {
            throw std::runtime_error("failed to connect benchmark copy block");
        }
    }
    if (!result.graph.connect<"out", "in">(*copies.back(), sink, {.minBufferSize = kBufferSize}).has_value()) {
        throw std::runtime_error("failed to connect benchmark sink");
    }

    for (const std::shared_ptr<gr::BlockModel>& block : result.graph.blocks()) {
        block->metaInformation()[std::string(gr::scheduler::kBatchSizeKey)] = static_cast<std::uint64_t>(batchSize);
        block->metaInformation()[std::string(gr::scheduler::kPeriodKey)]    = std::uint64_t{10'000'000UZ};
        block->metaInformation()[std::string(gr::scheduler::kPriorityKey)]  = std::int64_t{0};
    }
    return result;
}

template<typename TScheduler>
[[nodiscard]] RunResult runOnce(gr::Size_t nSamples, std::size_t depth, std::size_t batchSize) {
    GraphAndSink graph = makeGraph(nSamples, depth, batchSize);
    // EDF_HEAP=<-1|0|1> selects the deadline schedulers' selection structure (automatic, linear scan, heap)
    const char*        heapEnv   = std::getenv("EDF_HEAP");
    const std::int64_t heapMode  = heapEnv == nullptr ? std::int64_t{-1} : static_cast<std::int64_t>(std::atoi(heapEnv));
    TScheduler         scheduler({{"max_work_items", batchSize}, {"sched_settings", gr::property_map{{"heap_selection", heapMode}}}});
    if (auto exchanged = scheduler.exchange(std::move(graph.graph)); !exchanged.has_value()) {
        throw std::runtime_error(std::format("scheduler exchange failed: {}", exchanged.error()));
    }

    const auto start = Clock::now();
    if (auto result = scheduler.runAndWait(); !result.has_value()) {
        throw std::runtime_error(std::format("scheduler run failed: {}", result.error()));
    }
    const auto stop = Clock::now();

    if (graph.sink->loadCount() != nSamples) {
        throw std::runtime_error(std::format("sink received {} of {} samples", graph.sink->loadCount(), nSamples));
    }

    RunResult measured{.seconds = std::chrono::duration<double>(stop - start).count()};
    if constexpr (requires(const TScheduler& instance) { instance.statistics(); }) {
        const auto statistics = scheduler.statistics();
        measured.dispatches   = statistics.nDispatches;
        measured.selections   = statistics.nSelections;
        measured.sweeps       = statistics.nSweeps;
        measured.probes       = statistics.nProbes;
    }
    return measured;
}

[[nodiscard]] std::size_t readArgument(char* value, std::string_view name) {
    std::size_t parsed = 0UZ;
    const std::string_view text(value);
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (error != std::errc{} || end != text.data() + text.size() || parsed == 0UZ) {
        throw std::invalid_argument(std::format("{} must be a positive integer", name));
    }
    return parsed;
}

struct Summary {
    double median = 0.0;
    double mean   = 0.0;
    double stddev = 0.0;
    double minimum = 0.0;
    double maximum = 0.0;
};

[[nodiscard]] Summary summarise(std::vector<double> values) {
    std::ranges::sort(values);
    const double mean = std::ranges::fold_left(values, 0.0, std::plus<>{}) / static_cast<double>(values.size());
    const double squaredError = std::ranges::fold_left(values, 0.0, [mean](double total, double value) { return total + (value - mean) * (value - mean); });
    return Summary{
        .median  = values[values.size() / 2UZ],
        .mean    = mean,
        .stddev  = std::sqrt(squaredError / static_cast<double>(values.size())),
        .minimum = values.front(),
        .maximum = values.back(),
    };
}

void printResult(std::string_view policy, std::size_t batchSize, gr::Size_t nSamples, const std::vector<RunResult>& runs) {
    std::vector<double> seconds;
    seconds.reserve(runs.size());
    std::ranges::transform(runs, std::back_inserter(seconds), &RunResult::seconds);
    const Summary summary = summarise(std::move(seconds));
    const double  throughput = static_cast<double>(nSamples) / summary.median;

    std::println("{},{},{:.9f},{:.9f},{:.9f},{:.9f},{:.9f},{:.3f},{},{},{},{}", policy, batchSize, summary.median, summary.mean, summary.stddev, summary.minimum, summary.maximum, throughput,
        runs.back().dispatches, runs.back().selections, runs.back().sweeps, runs.back().probes);
}

} // namespace

int main(int argc, char** argv) {
    const auto nSamples  = static_cast<gr::Size_t>(argc > 1 ? readArgument(argv[1], "samples") : 1'048'576UZ);
    const auto iterations = argc > 2 ? readArgument(argv[2], "iterations") : 9UZ;
    const auto depth      = argc > 3 ? readArgument(argv[3], "depth") : 8UZ;
    if (depth == 0UZ) {
        throw std::invalid_argument("depth must be positive");
    }

    // optional 4th argument: comma-separated batch sizes, e.g. 16,32,64,128 -- defaults to the original three
    std::vector<std::size_t> batchSizes{64UZ, 1'024UZ, 16'384UZ};
    if (argc > 4) {
        batchSizes.clear();
        const std::string_view list(argv[4]);
        for (const auto part : std::views::split(list, ',')) {
            const std::string_view text(part.begin(), part.end());
            std::size_t            parsed = 0UZ;
            const auto [end, error]       = std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (error != std::errc{} || end != text.data() + text.size() || parsed == 0UZ) {
                throw std::invalid_argument("batch sizes must be positive integers");
            }
            batchSizes.push_back(parsed);
        }
    }
    std::println("# samples={}, iterations={}, depth={}", nSamples, iterations, depth);
    std::println("policy,batch,median_s,mean_s,stddev_s,min_s,max_s,samples_per_s,dispatches,selections,sweeps,probes");

    using RoundRobin   = gr::scheduler::Simple<>;
    using EDF          = gr::scheduler::EarliestDeadlineFirst<>;
    using FixedPriority = gr::scheduler::FixedPriority<>;

    for (const std::size_t batchSize : batchSizes) {
        std::ignore = runOnce<RoundRobin>(nSamples, depth, batchSize);
        std::ignore = runOnce<EDF>(nSamples, depth, batchSize);
        std::ignore = runOnce<FixedPriority>(nSamples, depth, batchSize);

        std::vector<RunResult> roundRobin;
        std::vector<RunResult> edf;
        std::vector<RunResult> fixedPriority;
        roundRobin.reserve(iterations);
        edf.reserve(iterations);
        fixedPriority.reserve(iterations);

        for (std::size_t iteration = 0UZ; iteration < iterations; ++iteration) {
            switch (iteration % 3UZ) {
            case 0UZ:
                roundRobin.push_back(runOnce<RoundRobin>(nSamples, depth, batchSize));
                edf.push_back(runOnce<EDF>(nSamples, depth, batchSize));
                fixedPriority.push_back(runOnce<FixedPriority>(nSamples, depth, batchSize));
                break;
            case 1UZ:
                edf.push_back(runOnce<EDF>(nSamples, depth, batchSize));
                fixedPriority.push_back(runOnce<FixedPriority>(nSamples, depth, batchSize));
                roundRobin.push_back(runOnce<RoundRobin>(nSamples, depth, batchSize));
                break;
            default:
                fixedPriority.push_back(runOnce<FixedPriority>(nSamples, depth, batchSize));
                roundRobin.push_back(runOnce<RoundRobin>(nSamples, depth, batchSize));
                edf.push_back(runOnce<EDF>(nSamples, depth, batchSize));
                break;
            }
        }

        printResult("RoundRobin", batchSize, nSamples, roundRobin);
        printResult("EDF", batchSize, nSamples, edf);
        printResult("FixedPriority", batchSize, nSamples, fixedPriority);
    }
}
