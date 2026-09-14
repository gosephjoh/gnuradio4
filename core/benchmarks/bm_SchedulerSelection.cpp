#include <algorithm>
#include <chrono>
#include <cstdio>
#include <format>
#include <memory>
#include <print>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Profiler.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace gr::scheduler;

namespace {

/// The default 65536-sample buffers, not the scheduler, limit how wide this graph can go: at 256
/// chains the stream and tag buffers alone exhaust memory. Selection cost is the subject, so the
/// buffers are shrunk to the smallest that keeps data flowing.
inline constexpr std::size_t kBufferSize = 1024UZ;

inline constexpr std::size_t kWarmupSteps  = 20UZ;
inline constexpr std::size_t kMeasureSteps = 200UZ;

/// Selection cost is what is being measured, so the graph is **wide, not deep**: a chain has one
/// block eligible at a time, and a heap can only beat a scan when many blocks hold released jobs at
/// once. Deadlines differ per chain so the ordering is non-trivial.
void buildWideGraph(gr::Graph& graph, std::size_t chains) {
    for (std::size_t c = 0UZ; c < chains; ++c) {
        const float deadline = 0.001f * static_cast<float>(c + 1UZ);

        auto& src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::format("src{}", c)}, {"relative_deadline", deadline}});
        auto& mid  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::format("mid{}", c)}, {"relative_deadline", deadline}});
        auto& sink = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::format("sink{}", c)}, {"relative_deadline", deadline}});

        const gr::EdgeParameters edge{.minBufferSize = kBufferSize};
        std::ignore = graph.connect<"out", "in">(src, mid, edge);
        std::ignore = graph.connect<"out", "in">(mid, sink, edge);
    }
}

using BenchScheduler = gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy>;

std::unique_ptr<BenchScheduler> makeRunning(std::size_t chains, SelectionStrategy strategy) {
    gr::Graph graph;
    buildWideGraph(graph, chains);

    auto sched                = std::make_unique<BenchScheduler>();
    sched->selection_strategy = strategy;
    std::ignore               = sched->exchange(std::move(graph));
    std::ignore               = sched->changeStateTo(gr::lifecycle::State::INITIALISED);
    std::ignore               = sched->changeStateTo(gr::lifecycle::State::RUNNING);
    return sched;
}

/// Median of the per-step durations: a mean would be dominated by the occasional house-keeping pass.
double medianStepNanos(std::size_t chains, SelectionStrategy strategy) {
    auto sched = makeRunning(chains, strategy);

    for (std::size_t i = 0UZ; i < kWarmupSteps; ++i) {
        std::ignore = sched->step();
    }

    std::vector<double> samples;
    samples.reserve(kMeasureSteps);
    for (std::size_t i = 0UZ; i < kMeasureSteps; ++i) {
        const auto start   = std::chrono::steady_clock::now();
        const auto result  = sched->step();
        const auto elapsed = std::chrono::steady_clock::now() - start;
        samples.push_back(static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()));
        if (result.status == gr::work::Status::ERROR) {
            std::print(stderr, "step reported ERROR at {} chains\n", chains);
            break;
        }
    }

    std::ignore = sched->changeStateTo(gr::lifecycle::State::STOPPED);

    std::ranges::sort(samples);
    return samples.empty() ? 0.0 : samples[samples.size() / 2UZ];
}

} // namespace

/// N.B. each sample is a whole `step()`, so the absolute figures include `work()` and job release,
/// not selection alone. The comparison between the two rows at a given width is what isolates
/// selection: everything else about the pass is identical.
///
/// Written as a plain timing loop rather than through `benchmark.hpp`: that harness prints from a
/// static destructor, which threw on this workload before any result reached the terminal.
int main() {
    std::print("{:>8} {:>8} {:>14} {:>14} {:>10}\n", "chains", "blocks", "linearScan/ns", "readyHeap/ns", "heap/scan");
    for (const std::size_t chains : {4UZ, 16UZ, 64UZ, 256UZ}) {
        const double scan = medianStepNanos(chains, SelectionStrategy::linearScan);
        const double heap = medianStepNanos(chains, SelectionStrategy::readyHeap);
        std::print("{:>8} {:>8} {:>14.0f} {:>14.0f} {:>10.2f}\n", chains, 3UZ * chains, scan, heap, scan > 0.0 ? heap / scan : 0.0);
        std::fflush(stdout);
    }
    return 0;
}
