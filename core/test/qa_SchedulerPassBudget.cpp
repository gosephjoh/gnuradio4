#include <boost/ut.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
#include <map>
#include <string>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

#include <gnuradio-4.0/Trace.hpp>

using namespace boost::ut;
using namespace gr::scheduler;

/**
 * @brief The wall-clock budget that bounds one selection pass.
 *
 * A block with no same-worker producer -- a source, or anything fed across a worker boundary -- is
 * released only by the per-sweep backstop scan, which runs once at the top of a pass. The pass was
 * bounded by a *count* of selections, so the delay before that block is noticed was bounded in
 * selections rather than in time, and one selection is anywhere from a microsecond to a millisecond.
 *
 * The derivation is pure and is tested exactly. The loop behaviour is timing-dependent and is
 * therefore asserted as **invariants rather than values**, in the manner the trace suites use: none
 * of the assertions below depends on how long anything took, only on relations that must hold however
 * long it took.
 */
namespace {

/// `SchedState` carrying a period, which is the only field `passBudget` reads.
SchedState withPeriod(double periodSeconds) {
    SchedState state{};
    state.periodSeconds = periodSeconds;
    return state;
}

constexpr std::uint64_t kAuto = 0ULL; /// `requestedMicros` sentinel: derive from the periods

/// A source feeding `depth` copies, every block declaring `period`. The blocks are deliberately
/// cheap: the tests here are about when the pass *ends*, not about what it costs.
gr::Graph chainGraph(std::size_t depth, float period, gr::Size_t samples) {
    gr::Graph graph;
    auto&     src = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", samples}, {"period", period}, {"relative_deadline", period}});

    gr::testing::Copy<float>* previous = nullptr;
    for (std::size_t i = 0UZ; i < depth; ++i) {
        auto& copy = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::format("mid{}", i)}, {"period", period}, {"relative_deadline", period}});
        if (previous == nullptr) {
            std::ignore = graph.connect<"out", "in">(src, copy);
        } else {
            std::ignore = graph.connect<"out", "in">(*previous, copy);
        }
        previous = std::addressof(copy);
    }
    auto& sink  = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::string("sink")}, {"period", period}, {"relative_deadline", period}});
    std::ignore = graph.connect<"out", "in">(*previous, sink);
    return graph;
}

using StepScheduler = Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy>;

/// Steps until the graph finishes or the deadline passes, returning what the sink received. The
/// deadline is a safety net, never the thing that ends a healthy run.
struct RunOutcome {
    bool       finished  = false;
    gr::Size_t delivered = 0U;
};

RunOutcome runToCompletion(StepScheduler& scheduler, const gr::testing::CountingSink<float>* sink, std::chrono::seconds limit = std::chrono::seconds{10}) {
    RunOutcome outcome;
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            outcome.finished = true;
            break;
        }
    }
    outcome.delivered = sink == nullptr ? 0U : sink->count.value;
    return outcome;
}

} // namespace

const boost::ut::suite<"pass budget derivation"> derivationTests = [] {
    "the budget is a quarter of the shortest declared period"_test = [] {
        const std::vector<SchedState> states{withPeriod(0.029), withPeriod(0.003), withPeriod(0.011)};
        expect(eq(passBudget(states, kAuto).count(), std::int64_t{750'000})) << "3 ms / 4 = 750 us; taking the max or the mean would give 7.25 ms or 3.58 ms";
    };

    "an unset period is excluded rather than treated as the fastest"_test = [] {
        // The trap this guards: `0.0` compares less than every real period, so a `min` that does not
        // filter would return zero, which `passBudget` documents as meaning *inactive*, and the budget would
        // silently switch off on exactly the graphs that declare timing for some of their blocks.
        const std::vector<SchedState> states{withPeriod(0.0), withPeriod(0.008), withPeriod(0.0)};
        expect(eq(passBudget(states, kAuto).count(), std::int64_t{2'000'000})) << "the 8 ms period must decide the budget, not the two unset ones";
    };

    "no declared period at all yields zero, meaning inactive"_test = [] {
        const std::vector<SchedState> states{withPeriod(0.0), withPeriod(0.0)};
        expect(eq(passBudget(states, kAuto).count(), std::int64_t{0})) << "a graph that declares no timing must keep the count-bounded behaviour exactly";
    };

    "an empty state list yields zero"_test = [] { expect(eq(passBudget(std::span<const SchedState>{}, kAuto).count(), std::int64_t{0})); };

    "an explicit request overrides the derivation"_test = [] {
        const std::vector<SchedState> states{withPeriod(0.003)};
        expect(eq(passBudget(states, 250ULL).count(), std::int64_t{250'000})) << "the setting must win over the derived 750 us";
        expect(eq(passBudget(std::span<const SchedState>{}, 250ULL).count(), std::int64_t{250'000})) << "and must not require any block to declare a period";
    };

    "the derivation is monotone in the shortest period"_test = [] {
        // A property rather than a point: halving the fastest period must halve the budget, which
        // catches a derivation that clamps, rounds to a constant, or ignores its input entirely.
        for (const double period : {0.001, 0.002, 0.004, 0.008}) {
            const std::vector<SchedState> states{withPeriod(period)};
            expect(eq(passBudget(states, kAuto).count(), static_cast<std::int64_t>(period * 0.25 * 1e9))) << std::format("period {} s", period);
        }
    };
};

const boost::ut::suite<"pass budget behaviour"> behaviourTests = [] {
    "an inactive budget leaves the run identical"_test = [] {
        // No block declares a period, so the budget is zero and the loop must behave exactly as it
        // did before the change -- including delivering every sample.
        constexpr gr::Size_t kSamples = 4096U;
        gr::Graph            graph;
        auto&                src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", kSamples}});
        auto&                mid  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&                sink = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::string("sink")}});
        std::ignore               = graph.connect<"out", "in">(src, mid);
        std::ignore               = graph.connect<"out", "in">(mid, sink);
        const auto* sinkPtr       = std::addressof(sink);

        StepScheduler scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        const RunOutcome outcome = runToCompletion(scheduler, sinkPtr);
        std::ignore              = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(outcome.finished) << "an unpaced graph must still run to completion";
        expect(eq(outcome.delivered, kSamples)) << "and deliver every sample";
    };

    "a budget shorter than one invocation still makes progress"_test = [] {
        // The degenerate case. With a 1 us budget every pass expires immediately, so the floor --
        // at least one selection per pass -- is the only thing between this and a worker that spends
        // its whole life on backstop scans and never runs a block. Removing the `selections == 0`
        // guard makes this test hang rather than fail, which is why the run is deadline-bounded.
        constexpr gr::Size_t kSamples = 2048U;
        gr::Graph            graph    = chainGraph(2UZ, 0.0005f, kSamples);
        const auto*          sinkPtr  = static_cast<const gr::testing::CountingSink<float>*>(graph.blocks().back()->raw());

        StepScheduler scheduler{{{"max_pass_duration_us", gr::Size_t{1U}}}};
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        const RunOutcome outcome = runToCompletion(scheduler, sinkPtr);
        std::ignore              = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(outcome.finished) << "a 1 us budget must not stall the graph";
        expect(eq(outcome.delivered, kSamples)) << "and must not lose samples";
    };

    "a sub-microsecond derived budget cannot stall the loop"_test = [] {
        // The one-selection floor is load-bearing only here, and mutation testing is how that was
        // established: removing it while the smallest *settable* budget is 1 us changes nothing,
        // because the first check happens ~25 ns after the timestamp is taken and so always permits
        // one selection. The *derived* budget has no such granularity -- a 40 ns declared period
        // gives 10 ns, below the cost of reading the clock -- and without the floor every pass would
        // then end before running anything, leaving the graph to spin on backstop scans until the
        // deadline below.
        constexpr gr::Size_t kSamples = 2048U;
        gr::Graph            graph    = chainGraph(2UZ, 4e-8f, kSamples); // 40 ns period -> 10 ns budget
        const auto*          sinkPtr  = static_cast<const gr::testing::CountingSink<float>*>(graph.blocks().back()->raw());

        StepScheduler scheduler; // auto-derived, so the budget is far below one clock read
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        const RunOutcome outcome = runToCompletion(scheduler, sinkPtr, std::chrono::seconds{5});
        std::ignore              = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(outcome.finished) << "a budget below one clock read must still let each pass run a block";
        expect(eq(outcome.delivered, kSamples)) << "and must not lose samples";
    };

    "a budget-terminated pass does not report the graph finished"_test = [] {
        // The most serious failure available here: if the budget break fell through to the DONE
        // determination, a loaded graph would terminate early and silently deliver less than it was
        // given. Asserted through the outcome rather than by inspecting a flag -- an early DONE shows
        // up as a short delivery, which is the symptom a user would actually see.
        constexpr gr::Size_t kSamples = 8192U;
        gr::Graph            graph    = chainGraph(6UZ, 0.0005f, kSamples);
        const auto*          sinkPtr  = static_cast<const gr::testing::CountingSink<float>*>(graph.blocks().back()->raw());

        StepScheduler scheduler{{{"max_pass_duration_us", gr::Size_t{1U}}}};
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        const RunOutcome outcome = runToCompletion(scheduler, sinkPtr);
        std::ignore              = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(outcome.finished) << "the graph must still reach DONE eventually";
        expect(eq(outcome.delivered, kSamples)) << "every sample must arrive -- a short count means a pass reported DONE while work remained";
    };

    "the budget does not change what a graph delivers"_test = [] {
        // Across a range of budgets from far below one invocation to far above any pass, the sample
        // count is invariant. Only *when* work happens may change; *what* happens may not.
        constexpr gr::Size_t kSamples = 4096U;
        for (const gr::Size_t budgetUs : {gr::Size_t{1U}, gr::Size_t{50U}, gr::Size_t{5000U}, gr::Size_t{0U}}) {
            gr::Graph   graph   = chainGraph(4UZ, 0.001f, kSamples);
            const auto* sinkPtr = static_cast<const gr::testing::CountingSink<float>*>(graph.blocks().back()->raw());

            StepScheduler scheduler{{{"max_pass_duration_us", budgetUs}}};
            expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
            expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
            expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
            const RunOutcome outcome = runToCompletion(scheduler, sinkPtr);
            std::ignore              = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

            expect(outcome.finished) << std::format("budget {} us: the graph must finish", budgetUs);
            expect(eq(outcome.delivered, kSamples)) << std::format("budget {} us: every sample must arrive", budgetUs);
        }
    };

    "the heap selector honours the budget too"_test = [] {
        // The two selectors are separate loops and a guard added to one is easy to omit from the
        // other. Same assertion, `readyHeap` selected.
        constexpr gr::Size_t kSamples = 4096U;
        gr::Graph            graph    = chainGraph(4UZ, 0.001f, kSamples);
        const auto*          sinkPtr  = static_cast<const gr::testing::CountingSink<float>*>(graph.blocks().back()->raw());

        StepScheduler scheduler{{{"max_pass_duration_us", gr::Size_t{1U}}}};
        scheduler.selection_strategy = SelectionStrategy::readyHeap; // an enum the property map cannot carry
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        const RunOutcome outcome = runToCompletion(scheduler, sinkPtr);
        std::ignore              = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(outcome.finished) << "the heap loop must not stall under a 1 us budget";
        expect(eq(outcome.delivered, kSamples));
    };

    "a non-release-tracking policy is unaffected"_test = [] {
        // `passBudget` sits inside `if constexpr (needsReleaseTracking(...))`, so for round robin the
        // budget does not exist. Setting it must therefore change nothing at all.
        constexpr gr::Size_t kSamples = 4096U;
        gr::Graph            graph    = chainGraph(4UZ, 0.001f, kSamples);
        const auto*          sinkPtr  = static_cast<const gr::testing::CountingSink<float>*>(graph.blocks().back()->raw());

        Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, RoundRobinPolicy> scheduler{{{"max_pass_duration_us", gr::Size_t{1U}}}};
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

        RunOutcome outcome;
        const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds{10};
        while (std::chrono::steady_clock::now() < limit) {
            if (scheduler.step().status == gr::work::Status::DONE) {
                outcome.finished = true;
                break;
            }
        }
        outcome.delivered = sinkPtr->count.value;
        std::ignore       = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(outcome.finished) << "round robin must be untouched by a setting it does not read";
        expect(eq(outcome.delivered, kSamples));
    };
};

/// Worst deviation, in ms, between a source's successive releases and its declared period.
///
/// This is the quantity the whole change exists to control: the source has no producer, so only the
/// backstop can release it, and the backstop runs once per pass. A pass that outlasts the period
/// makes the source late by however long it overran.
struct PacingError {
    double      worstMs  = 0.0;
    std::size_t releases = 0UZ;
};

/// Three chains at 3/11/29 ms, each carrying blocks expensive enough that draining a pass takes
/// longer than the fastest period. That is what the existing `deepChain` pacing test lacks, and why
/// it passes on the unfixed code.
PacingError measurePacing(gr::Size_t budgetUs) {
    // 12 batches of 512, so there are eleven inter-release gaps to judge the pacing by. Fewer than
    // that and a single late release dominates the statistic.
    constexpr gr::Size_t kSamples = 6144U;
    constexpr float      kLoad    = 300e3f; // S/s: ~1.7 ms for a 512-sample batch

    gr::Graph                           graph;
    gr::testing::ConstantSource<float>* fastSource = nullptr;
    for (const float period : {0.003f, 0.011f, 0.029f}) {
        auto& src = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::format("src{}", period)}, {"n_samples_max", kSamples}, {"period", period}, {"relative_deadline", period}, {"max_batch_size", gr::Size_t{512U}}});
        if (fastSource == nullptr) {
            fastSource = std::addressof(src);
        }
        gr::testing::SimCompute<float>* previous = nullptr;
        for (std::size_t stage = 0UZ; stage < 3UZ; ++stage) {
            auto& load = graph.emplaceBlock<gr::testing::SimCompute<float>>({{"name", std::format("load{}.{}", period, stage)}, {"target_throughput", kLoad}, {"complexity_order", 1.0f}, {"busy_wait", true}, {"period", period}, {"relative_deadline", period}, {"max_batch_size", gr::Size_t{512U}}});
            if (previous == nullptr) {
                expect(graph.connect<"out", "in">(src, load).has_value() >> fatal) << std::format("src->load for period {}", period);
            } else {
                expect(graph.connect<"out", "in">(*previous, load).has_value() >> fatal) << std::format("load->load for period {}", period);
            }
            previous = std::addressof(load);
        }
        auto& sink = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::format("sink{}", period)}, {"period", period}, {"relative_deadline", period}, {"max_batch_size", gr::Size_t{512U}}});
        expect(graph.connect<"out", "in">(*previous, sink).has_value() >> fatal) << std::format("load->sink for period {}", period);
    }
    const void* fastKey = static_cast<const void*>(fastSource);

    // `Category::release` carries `releaseScan` as well as `jobRelease`, and a scan is emitted once
    // per sweep -- tens of thousands over a run of this length. At the default 65536-record ring the
    // early releases are overwritten before they can be read, and a pacing statistic computed over a
    // window that silently moved is simply wrong. Sized up here, and the loss count is asserted
    // below rather than assumed.
    std::ignore = gr::trace::setRingCapacity(1UZ << 20UZ);
    gr::trace::reset();
    gr::trace::setCategories(gr::trace::categoryMask(gr::trace::Category::release));

    StepScheduler scheduler{{{"max_pass_duration_us", budgetUs}}};
    expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{20};
    while (std::chrono::steady_clock::now() < deadline) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            break;
        }
    }
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

    std::ignore = fastKey;

    // The fast source is identified by *name*, not by a guessed id: `reset()` drops the identity
    // table but not the id counter, so ids carry an offset that depends on what ran before in this
    // process. Registration order still holds, so the wanted block is the lowest-numbered
    // `ConstantSource` -- the 3 ms chain is emplaced first.
    struct Entities {
        std::map<gr::trace::EntityId, std::string> names;
    } entities;
    std::ignore = gr::trace::forEachEntity(
        [](gr::trace::EntityId id, const gr::trace::EntityDescription& description, void* user) noexcept { //
            static_cast<Entities*>(user)->names.emplace(id, std::string(description.uniqueName));
        },
        &entities);

    gr::trace::EntityId fastSourceId = 0U;
    for (const auto& [id, name] : entities.names) {
        if (name.contains("ConstantSource")) {
            fastSourceId = id;
            break; // the map is ordered, so this is the lowest id and therefore the first emplaced
        }
    }
    expect(fastSourceId != 0U >> fatal) << "no ConstantSource was interned -- the graph is not what this test assumes";

    std::vector<std::uint64_t> releases;
    struct Collect {
        std::vector<std::uint64_t>* out;
        gr::trace::EntityId         id;
    };
    Collect collect{.out = std::addressof(releases), .id = fastSourceId};
    std::ignore = gr::trace::forEachEvent(
        [](const gr::trace::Event& event, void* user) noexcept {
            auto& c = *static_cast<Collect*>(user);
            if (event.kind == gr::trace::Kind::jobRelease && event.entity == c.id) {
                c.out->push_back(event.startNs);
            }
        },
        &collect);

    const gr::trace::RingStats stats = gr::trace::ringStats();
    gr::trace::setCategories(0U);
    expect(eq(stats.lost, 0UZ) >> fatal) << std::format("the capture lost {} records, so any pacing figure from it is understated", stats.lost);

    std::ranges::sort(releases);
    PacingError error{.releases = releases.size()};
    for (std::size_t i = 1UZ; i < releases.size(); ++i) {
        const double gapMs = static_cast<double>(releases[i] - releases[i - 1UZ]) / 1e6;
        error.worstMs      = std::max(error.worstMs, std::abs(gapMs - 3.0));
    }
    return error;
}

const boost::ut::suite<"pass budget efficacy"> efficacyTests = [] {
    if constexpr (!gr::trace::kEnabled) {
        "release pacing needs the trace layer to observe"_test = [] { expect(true); };
        return;
    }

    "the budget keeps a source paced where a long pass would not"_test = [] {
        // The efficacy test, and the only one here that fails on the unfixed code. A 10 s budget is
        // effectively no budget, and reproduces the behaviour this change replaces; the auto-derived
        // 750 us is the fix. Both run the same graph.
        const PacingError unbounded = measurePacing(gr::Size_t{10'000'000U});
        const PacingError bounded   = measurePacing(gr::Size_t{0U});

        std::println("  3 ms source pacing: no budget -> worst gap error {:.2f} ms over {} releases; auto budget -> {:.2f} ms over {} releases", //
            unbounded.worstMs, unbounded.releases, bounded.worstMs, bounded.releases);

        expect(gt(bounded.releases, 8UZ) >> fatal) << "too few releases to say anything about pacing";
        expect(eq(bounded.releases, unbounded.releases)) << "the two runs must move the same data, or they are not comparable";

        // Asserted as a ratio, not against an absolute bound. The fix cannot reach the 3 ms period on
        // this graph and is not claimed to: the longest single invocation is ~1.7 ms and `work()` is
        // non-preemptive, so a large fraction of a period is unreachable by any budget. What the
        // budget must do is *substantially* reduce the drift, and a factor of two is well inside the
        // ~5x measured while leaving room for a loaded machine.
        expect(lt(bounded.worstMs, unbounded.worstMs * 0.5)) << std::format("the budget must at least halve the pacing error: {:.2f} ms against {:.2f} ms", bounded.worstMs, unbounded.worstMs);
    };
};

/// Which bound, if either, ended each pass -- the question `flag::kBoundWasTime` exists to answer.
struct BoundRecords {
    std::size_t viaTime  = 0UZ;
    std::size_t viaCount = 0UZ;
    std::size_t budgetUs = 0UZ; /// `payload0` of the last time-bounded record
};

/// Runs a 4-deep chain under EDF and reports how its passes ended. `period` of zero declares none,
/// which leaves the budget inactive so that the count bound is the only one that can fire.
BoundRecords boundsHit(float period, gr::Size_t budgetUs, gr::Size_t selectionsPerPass) {
    constexpr gr::Size_t kSamples = 4096U;

    gr::Graph                       graph;
    auto&                           src      = graph.emplaceBlock<gr::testing::ConstantSource<float>>(period > 0.f //
                                                                                                          ? gr::property_map{{"name", std::string("src")}, {"n_samples_max", kSamples}, {"period", period}, {"relative_deadline", period}, {"max_batch_size", gr::Size_t{256U}}}
                                                                                                          : gr::property_map{{"name", std::string("src")}, {"n_samples_max", kSamples}, {"max_batch_size", gr::Size_t{256U}}});
    gr::testing::SimCompute<float>* previous = nullptr;
    for (std::size_t stage = 0UZ; stage < 4UZ; ++stage) {
        gr::property_map settings{{"name", std::format("load{}", stage)}, {"target_throughput", 400e3f}, {"complexity_order", 1.0f}, {"busy_wait", true}, {"max_batch_size", gr::Size_t{256U}}};
        if (period > 0.f) {
            settings["period"]            = period;
            settings["relative_deadline"] = period;
        }
        auto& load = graph.emplaceBlock<gr::testing::SimCompute<float>>(settings);
        if (previous == nullptr) {
            expect(graph.connect<"out", "in">(src, load).has_value() >> fatal);
        } else {
            expect(graph.connect<"out", "in">(*previous, load).has_value() >> fatal);
        }
        previous = std::addressof(load);
    }
    auto& sink = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::string("sink")}, {"max_batch_size", gr::Size_t{256U}}});
    expect(graph.connect<"out", "in">(*previous, sink).has_value() >> fatal);

    std::ignore = gr::trace::setRingCapacity(1UZ << 20UZ);
    gr::trace::reset();
    gr::trace::setCategories(gr::trace::categoryMask(gr::trace::Category::select));

    StepScheduler scheduler{{{"max_pass_duration_us", budgetUs}, {"max_selections_per_pass", selectionsPerPass}}};
    expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{20};
    while (std::chrono::steady_clock::now() < deadline) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            break;
        }
    }
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

    BoundRecords records;
    std::ignore = gr::trace::forEachEvent(
        [](const gr::trace::Event& event, void* user) noexcept {
            if (event.kind != gr::trace::Kind::selectionBoundHit) {
                return;
            }
            auto& r = *static_cast<BoundRecords*>(user);
            if ((event.flags & gr::trace::flag::kBoundWasTime) != 0U) {
                ++r.viaTime;
                r.budgetUs = event.payload0;
            } else {
                ++r.viaCount;
            }
        },
        &records);
    const gr::trace::RingStats stats = gr::trace::ringStats();
    gr::trace::setCategories(0U);
    expect(eq(stats.lost, 0UZ) >> fatal) << "the capture lost records, so the census understates";
    return records;
}

const boost::ut::suite<"pass budget observability"> observabilityTests = [] {
    if constexpr (!gr::trace::kEnabled) {
        "which bound fired is only observable through the trace"_test = [] { expect(true); };
        return;
    }

    "a time-bounded pass is recorded as such, carrying the budget in microseconds"_test = [] {
        // The two bounds have different remedies -- a longer budget against a larger selection count
        // -- so a record that cannot say which one fired diagnoses neither.
        const BoundRecords records = boundsHit(0.002f, gr::Size_t{200U}, gr::Size_t{0U});

        expect(gt(records.viaTime, 0UZ)) << "a 200 us budget against ~640 us invocations must end passes on time";
        expect(eq(records.viaCount, 0UZ)) << "the selection count was left at auto and cannot have been the binding bound";
        expect(eq(records.budgetUs, std::size_t{200U})) << "payload0 must carry the budget in microseconds, not a selection count";
    };

    "a count-bounded pass is recorded without the time flag"_test = [] {
        // The other half: with no period declared the budget is inactive, so only the count can fire.
        // Without this the flag could be set unconditionally and the previous test would still pass.
        const BoundRecords records = boundsHit(0.0f, gr::Size_t{0U}, gr::Size_t{2U});

        expect(gt(records.viaCount, 0UZ)) << "a two-selection cap on a six-block graph must bind";
        expect(eq(records.viaTime, 0UZ)) << "no period is declared, so no time budget exists to fire";
    };
};

const boost::ut::suite<"pass budget policy scoping"> scopingTests = [] {
    "the task-level fixed-priority loop is untouched by the setting"_test = [] {
        // `FixedPriority` and `RateMonotonic` run the other selection loop, which has no backstop and
        // therefore no latency to bound. The budget must not reach it -- tested rather than assumed,
        // because both loops sit in the same function and a guard added to one is easy to paste into
        // the other.
        constexpr gr::Size_t kSamples = 4096U;
        gr::Graph            graph    = chainGraph(4UZ, 0.001f, kSamples);
        const auto*          sinkPtr  = static_cast<const gr::testing::CountingSink<float>*>(graph.blocks().back()->raw());

        Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, RateMonotonicPolicy> scheduler{{{"max_pass_duration_us", gr::Size_t{1U}}}};
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

        bool       finished = false;
        const auto limit    = std::chrono::steady_clock::now() + std::chrono::seconds{10};
        while (std::chrono::steady_clock::now() < limit) {
            if (scheduler.step().status == gr::work::Status::DONE) {
                finished = true;
                break;
            }
        }
        const gr::Size_t delivered = sinkPtr->count.value;
        std::ignore                = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(finished) << "rate monotonic must be unaffected by a setting its loop does not read";
        expect(eq(delivered, kSamples));
    };
};

int main() { /* boost.ut runs the suites */ }
