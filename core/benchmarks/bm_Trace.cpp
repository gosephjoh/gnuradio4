#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <print>
#include <string>
#include <tuple>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/Trace.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace gr::trace;

namespace {

inline constexpr std::size_t kIterations = 1'000'000UZ;
inline constexpr std::size_t kRepeats    = 9UZ;

/**
 * Nanoseconds per operation, as the **minimum** over `kRepeats` runs of `kIterations` calls.
 *
 * Minimum rather than mean: it is the estimate least polluted by preemption, migration and page
 * faults. The clock is read twice per *repeat*, not per call, so its own cost is amortised away and
 * does not enter the figure.
 */
template<typename TBody>
[[nodiscard]] double nanosPerOp(TBody&& body) {
    double best = std::numeric_limits<double>::max();
    for (std::size_t repeat = 0UZ; repeat < kRepeats; ++repeat) {
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t i = 0UZ; i < kIterations; ++i) {
            body(static_cast<std::uint32_t>(i));
        }
        const auto elapsed = std::chrono::steady_clock::now() - start;
        const auto nanos   = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
        best               = std::min(best, nanos / static_cast<double>(kIterations));
    }
    return best;
}

/// Keeps a value the optimiser cannot reason about, so a loop body whose result is unused is not
/// deleted outright. Cheaper than a full memory clobber, which would inhibit the very inlining the
/// measurement is supposed to include.
template<typename T>
void doNotOptimise(T& value) {
    asm volatile("" : "+r,m"(value) : : "memory");
}

struct RunResult {
    double        nanos    = 0.0;
    std::uint64_t recorded = 0UL;
    std::uint64_t lost     = 0UL;
};

/**
 * Wall time to push a fixed quantity of data through a three-block chain.
 *
 * Deliberately **not** a per-step average. A step's cost is bimodal -- most passes find a full or an
 * empty buffer and return almost immediately, a few move a large batch -- so a median step reports
 * the cheap case, a mean step is dominated by the rare expensive one, and a percentage built on
 * either says more about the shape of that distribution than about tracing. Total time for a fixed
 * amount of work has no such freedom: it is what a user waits for.
 *
 * The ring is sized so the run cannot wrap, since a wrapped ring would fold record eviction into the
 * figure; `lost` is reported so that assumption is checked rather than trusted.
 */
/// As above, but on a release-tracking policy, which is the only kind that compiles the release,
/// selection and deadline markers at all. Two independent chains so the selector faces a real choice:
/// a linear chain never contends, and a selection cost measured where nothing is ever chosen between
/// would understate it.
[[nodiscard]] RunResult timeFixedWorkEdf(std::uint32_t liveCategories, gr::Size_t sampleCount, gr::Size_t batchCeiling) {
    gr::Graph  graph;
    const auto branch = [&graph](const std::string& suffix, gr::Size_t n) {
        auto& source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", "src" + suffix}, {"n_samples_max", n}, {"relative_deadline", 0.010f}});
        auto& copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", "mid" + suffix}, {"relative_deadline", 0.010f}});
        auto& sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", "snk" + suffix}, {"relative_deadline", 0.010f}});
        std::ignore  = graph.connect<"out", "in">(source, copy);
        std::ignore  = graph.connect<"out", "in">(copy, sink);
    };
    branch("A", sampleCount);
    branch("B", sampleCount);

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
    std::ignore = scheduler.exchange(std::move(graph));
    if (batchCeiling > 0U) {
        std::ignore = scheduler.settings().set({{"max_work_items", batchCeiling}});
        std::ignore = scheduler.settings().activateContext();
        std::ignore = scheduler.settings().applyStagedParameters();
    }
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::INITIALISED);
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::RUNNING);

    reset();
    std::ignore = setRingCapacity(1UZ << 22U);
    setCategories(liveCategories);

    const auto start = std::chrono::steady_clock::now();
    for (std::size_t guard = 0UZ; guard < 1'000'000UZ; ++guard) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            break;
        }
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;

    const RingStats stats = ringStats();
    const RunResult result{.nanos = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()), .recorded = stats.recorded, .lost = stats.lost};

    setCategories(0U);
    reset();
    std::ignore = setRingCapacity(65536UZ);
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    return result;
}

[[nodiscard]] RunResult timeFixedWork(std::uint32_t liveCategories, gr::Size_t sampleCount) {
    gr::Graph graph;
    auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", sampleCount}});
    auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
    auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
    std::ignore      = graph.connect<"out", "in">(source, copy);
    std::ignore      = graph.connect<"out", "in">(copy, sink);

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep> scheduler;
    std::ignore = scheduler.exchange(std::move(graph));
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::INITIALISED);
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::RUNNING);

    reset();
    std::ignore = setRingCapacity(1UZ << 21U);
    setCategories(liveCategories);

    const auto start = std::chrono::steady_clock::now();
    for (std::size_t guard = 0UZ; guard < 1'000'000UZ; ++guard) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            break;
        }
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;

    const RingStats stats = ringStats();
    const RunResult result{.nanos = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()), .recorded = stats.recorded, .lost = stats.lost};

    setCategories(0U);
    reset();
    std::ignore = setRingCapacity(65536UZ);
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    return result;
}

} // namespace

/**
 * Two measurements, answering different questions.
 *
 * The first table is a marker in a hot loop: warm cache, perfect prediction, nothing else touching
 * the store buffer. That is the best case, and the right figure for comparing the paths against one
 * another -- disabled versus enabled, two clock reads versus none.
 *
 * The second is a graph doing a fixed amount of work. That is the figure to quote when anyone asks
 * what tracing costs, and the only one of the two that accounts for the cache pressure the records
 * themselves create.
 */
int main() {
    std::ignore = setRingCapacity(65536UZ);

    setCategories(0U);
    const double disabled = nanosPerOp([](std::uint32_t i) { emit(Event{.payload0 = i, .kind = Kind::workEnd}); });

    // The cost *when inlined*, which the constructor forces at every call site. This row documents the
    // target; it cannot guard it, because inlining is decided per call site and this small loop would
    // be inlined either way. The `qa_TraceScopeInlined` check guards it, on the test binary's symbols.
    const double scopeDisabled = nanosPerOp([](std::uint32_t i) {
        Scope scope{Event{.payload0 = i, .kind = Kind::workEnd}};
        (void)scope;
    });

    setCategories(categoryMask(Category::deadline));
    const double otherCategory = nanosPerOp([](std::uint32_t i) { emit(Event{.payload0 = i, .kind = Kind::workEnd}); });

    setCategories(categoryMask(Category::work));
    const double enabled = nanosPerOp([](std::uint32_t i) { emit(Event{.payload0 = i, .kind = Kind::workEnd}); });

    const double clockRead = nanosPerOp([](std::uint32_t) {
        std::uint64_t instant = now();
        doNotOptimise(instant);
    });

    const double scopePaired = nanosPerOp([](std::uint32_t i) {
        Scope scope{Event{.payload0 = i, .kind = Kind::workEnd}};
        (void)scope;
    });

    const double scopeChained = nanosPerOp([](std::uint32_t i) {
        Scope scope{Event{.startNs = 1UL, .kind = Kind::workEnd, .workerId = static_cast<std::uint8_t>(i)}};
        scope.finish(2UL);
    });

    // Read twice per worker thread for the whole of its life, never on a marker path -- so this is
    // reported for honesty about what T4h added, not because it is on any hot path. On Linux it is a
    // real syscall rather than a vDSO read, which is exactly why it is not on one.
    std::uint64_t cpuSink       = 0UL;
    const double  threadCpuRead = nanosPerOp([&cpuSink](std::uint32_t) { cpuSink += gr::trace::threadCpuNow().value_or(0UL); });

    setCategories(0U);
    reset();

    std::print("{:<44} {:>10}\n", "path", "ns/op");
    std::print("{:<44} {:>10.2f}\n", "emit(), tracing compiled in, mask 0", disabled);
    std::print("{:<44} {:>10.2f}\n", "Scope, tracing compiled in, mask 0", scopeDisabled);
    std::print("{:<44} {:>10.2f}\n", "emit(), another category live", otherCategory);
    std::print("{:<44} {:>10.2f}\n", "emit(), category live (check + ring store)", enabled);
    std::print("{:<44} {:>10.2f}\n", "now(), one clock read", clockRead);
    std::print("{:<44} {:>10.2f}\n", "Scope, two clock reads", scopePaired);
    std::print("{:<44} {:>10.2f}\n", "Scope, both instants supplied", scopeChained);
    std::print("{:<44} {:>10.2f}\n", "threadCpuNow(), 2x per worker lifetime", threadCpuRead);
    if (cpuSink == 0UL) {
        std::print("(the thread-CPU clock returned nothing on this platform)\n");
    }

    if constexpr (!kEnabled) {
        std::print("\nTracing is COMPILED OUT, so every emit path above is 0.00 by construction:\n");
        std::print("that is the zero-cost-when-compiled-out requirement, not a failed measurement.\n");
        std::print("Only now() is real here. Rebuild with -DGR4_ENABLE_TRACING=ON for the rest.\n");
    }

    constexpr gr::Size_t kSamples = 1U << 22U;

    const RunResult off  = timeFixedWork(0U, kSamples);
    const RunResult work = timeFixedWork(categoryMask(Category::work), kSamples);
    const RunResult all  = timeFixedWork(kAllCategories, kSamples);

    std::print("\n{} samples through ConstantSource -> Copy -> NullSink\n", static_cast<std::uint32_t>(kSamples));
    std::print("{:<36} {:>10} {:>11} {:>12}\n", "configuration", "ms", "vs mask 0", "records");
    std::print("{:<36} {:>10.2f} {:>11} {:>12}\n", "tracing compiled in, mask 0", off.nanos / 1e6, "--", off.recorded);
    std::print("{:<36} {:>10.2f} {:>10.1f}% {:>12}\n", "work markers live", work.nanos / 1e6, 100.0 * (work.nanos - off.nanos) / off.nanos, work.recorded);
    std::print("{:<36} {:>10.2f} {:>10.1f}% {:>12}\n", "every category live", all.nanos / 1e6, 100.0 * (all.nanos - off.nanos) / off.nanos, all.recorded);
    if (off.lost + work.lost + all.lost > 0UL) {
        std::print("WARNING: records were lost, so a ring wrapped and eviction is inside these figures\n");
    }
    if constexpr (!kEnabled) {
        std::print("\n(compiled out: all three rows are the same build and differ only by noise)\n");
    }

    // The T2 gate. `release` alone must sit in the noise floor: it is the configuration a validation
    // capture starts from, and the decision to record *unproductive* detection scans rests on it
    // being free. `select` is expected to be measurable and is allowed to be, provided the figure is
    // stated rather than discovered later.
    constexpr gr::Size_t kEdfSamples = 1U << 19U;

    // **Minimum of several runs, not one run.** A single timing of each configuration is what made an
    // earlier reading of this table report +5.8 % for `release` alone -- a figure no arithmetic
    // supports, since that configuration writes 113 records into a 2 ms run. Repeated, the same
    // configuration spans -1.2 % to +5.8 % while the *baseline* spans 2.03 to 2.16 ms, so the outlier
    // was the measurement, not the markers. This is the T1 section 8G.3 trap in a new table, and the
    // fix is the same one: stop letting a single sample speak.
    const auto bestOf = [](std::uint32_t mask, gr::Size_t batchCeiling) {
        RunResult best{.nanos = std::numeric_limits<double>::max(), .recorded = 0UL, .lost = 0UL};
        for (std::size_t repeat = 0UZ; repeat < 7UZ; ++repeat) {
            const RunResult run = timeFixedWorkEdf(mask, kEdfSamples, batchCeiling);
            if (run.nanos < best.nanos) {
                best = run;
            }
        }
        return best;
    };

    // Two regimes, because one of them cannot answer the question. With an unbounded batch the whole
    // run emits about a hundred records, and a hundred records cannot account for a percent of a
    // two-millisecond run whichever way the figure falls -- a delta there is measurement noise being
    // read as a result. The bounded batch emits thousands, which is where a per-marker cost becomes
    // resolvable, and is also the regime the RT and batching thrusts target.
    //
    // The ns/record column exists so an implausible figure is obvious immediately: a marker costs a
    // few nanoseconds, so anything in the hundreds is the measurement talking, not the markers.
    const auto gate = [&](const char* title, gr::Size_t batchCeiling) {
        const RunResult baseline   = bestOf(0U, batchCeiling);
        const RunResult release    = bestOf(categoryMask(Category::release), batchCeiling);
        const RunResult deadline   = bestOf(categoryMask(Category::release, Category::deadline), batchCeiling);
        const RunResult everything = bestOf(categoryMask(Category::release, Category::select, Category::deadline), batchCeiling);
        // T4: the block-side markers. `work` alone is the scheduler-boundary baseline they sit
        // inside, so the difference between these two rows is `workExact`'s own cost and nothing
        // else. `workPhases` is expected to be the expensive one -- four scopes against one -- and
        // the point of printing it is that the figure is stated rather than discovered later.
        const RunResult boundary = bestOf(categoryMask(Category::work), batchCeiling);
        const RunResult exact    = bestOf(categoryMask(Category::work, Category::workExact), batchCeiling);
        const RunResult phases   = bestOf(categoryMask(Category::work, Category::workExact, Category::workPhases), batchCeiling);

        std::print("\n{}\n", title);
        std::print("{:<32} {:>9} {:>10} {:>10} {:>11}\n", "configuration", "ms", "vs mask 0", "records", "ns/record");
        std::print("{:<32} {:>9.2f} {:>10} {:>10} {:>11}\n", "tracing compiled in, mask 0", baseline.nanos / 1e6, "--", baseline.recorded, "--");
        const auto row = [&](const char* label, const RunResult& run) {
            const double delta = run.nanos - baseline.nanos;
            std::print("{:<32} {:>9.2f} {:>9.1f}% {:>10} {:>11.1f}\n", label, run.nanos / 1e6, 100.0 * delta / baseline.nanos, run.recorded, run.recorded > 0UL ? delta / static_cast<double>(run.recorded) : 0.0);
        };
        row("release", release);
        row("release | deadline", deadline);
        row("release | select | deadline", everything);
        row("work", boundary);
        row("work | workExact", exact);
        row("work | workExact | workPhases", phases);
        if (baseline.lost + release.lost + deadline.lost + everything.lost + boundary.lost + exact.lost + phases.lost > 0UL) {
            std::print("WARNING: records were lost, so a ring wrapped and eviction is inside these figures\n");
        }
    };

    gate("EDF, unbounded batch -- too few markers to resolve; a delta here is noise", 0U);
    gate("EDF, 512-sample batches -- the T2 gate proper (best of 7 per row)", 512U);

    std::fflush(stdout);
    return 0;
}
