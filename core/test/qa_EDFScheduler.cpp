#include <boost/ut.hpp>

#include <gnuradio-4.0/EarliestDeadlineFirst.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

namespace {

struct DispatchLog {
    std::atomic<std::size_t> nCalls{0UZ};
    std::mutex               runnerMutex;
    std::set<std::size_t>    runnerIDs;

    void record(std::size_t runnerID) {
        nCalls.fetch_add(1UZ, std::memory_order_relaxed);
        std::lock_guard guard(runnerMutex);
        runnerIDs.insert(runnerID);
    }

    [[nodiscard]] std::size_t nDistinctRunners() {
        std::lock_guard guard(runnerMutex);
        return runnerIDs.size();
    }
};

// exercises the dispatchOnce() seam: reversing the block list still produces a correct run, which proves the
// override drives execution rather than the base's round-robin.
template<gr::scheduler::ExecutionPolicy execution>
struct ReverseOrderScheduler : gr::scheduler::SchedulerBase<ReverseOrderScheduler<execution>, execution> {
    using base_t = gr::scheduler::SchedulerBase<ReverseOrderScheduler<execution>, execution>;
    using base_t::base_t;

    std::shared_ptr<DispatchLog> dispatchLog = std::make_shared<DispatchLog>();

    void customInit() {
        const gr::Graph                                    flatGraph = gr::graph::flatten(*this->_graph);
        const std::vector<std::shared_ptr<gr::BlockModel>> blockList(flatGraph.blocks().begin(), flatGraph.blocks().end());
        const std::size_t                                  nBatches = (execution == gr::scheduler::ExecutionPolicy::multiThreaded) //
                                                                          ? std::max(1UZ, std::min(static_cast<std::size_t>(this->_pool->maxThreads()), blockList.size()))
                                                                          : 1UZ;

        std::lock_guard lock(this->_executionOrderMutex);
        std::lock_guard guard(this->_adoptionBlocksMutex);
        this->_adoptionBlocks.clear();
        this->_adoptionBlocks.resize(nBatches);
        *this->_executionOrder = gr::scheduler::detail::batchBlocks(blockList, nBatches);
    }

    void customReset() { customInit(); }

    gr::work::Result dispatchOnce(std::size_t runnerID, const std::vector<std::shared_ptr<gr::BlockModel>>& blocks) {
        dispatchLog->record(runnerID);
        const std::vector<std::shared_ptr<gr::BlockModel>> reversed(blocks.rbegin(), blocks.rend());
        return this->traverseBlockListOnce(reversed);
    }
};

// Deterministic stand-in for steady_clock: every observation advances virtual time by a fixed quantum, so a
// run's deadline accounting depends only on the sequence of dispatches and never on wall-clock timing. This
// keeps the deadline tests free of sleeps, per the project's no-timing-based-tests rule.
struct TickClock {
    using duration   = std::chrono::nanoseconds;
    using rep        = duration::rep;
    using period     = duration::period;
    using time_point = std::chrono::time_point<TickClock, duration>;

    static constexpr bool is_steady   = true;
    static constexpr rep  kTickNanos  = 1'000;
    static inline std::atomic<rep> elapsed{0};

    static time_point now() noexcept { return time_point{duration{elapsed.fetch_add(kTickNanos, std::memory_order_relaxed)}}; }
    static void       reset() noexcept { elapsed.store(0, std::memory_order_relaxed); }
};

struct TraceCounters {
    std::atomic<std::uint64_t> releases{0UZ};
    std::atomic<std::uint64_t> completions{0UZ};
    std::atomic<std::uint64_t> misses{0UZ};
    std::atomic<std::uint64_t> dispatches{0UZ};
};

struct RecordingEvent {
    void finish() noexcept {}
};

struct RecordingStepEvent {
    void step() noexcept {}
    void finish() noexcept {}
};

struct RecordingHandler {
    TraceCounters* counters = nullptr;

    void instantEvent(std::string_view name, std::string_view = {}, std::initializer_list<gr::profiling::arg_value> = {}) noexcept {
        if (name == "scheduler.job.release") {
            counters->releases.fetch_add(1UZ, std::memory_order_relaxed);
        } else if (name == "scheduler.job.complete") {
            counters->completions.fetch_add(1UZ, std::memory_order_relaxed);
        } else if (name == "scheduler.job.deadline_miss") {
            counters->misses.fetch_add(1UZ, std::memory_order_relaxed);
        }
    }

    void counterEvent(std::string_view, std::string_view = {}, std::initializer_list<gr::profiling::arg_value> = {}) const noexcept {}

    [[nodiscard]] RecordingEvent startCompleteEvent(std::string_view name, std::string_view = {}, std::initializer_list<gr::profiling::arg_value> = {}) noexcept {
        if (name == "scheduler.job.dispatch" || name == "scheduler.job.fallback") {
            counters->dispatches.fetch_add(1UZ, std::memory_order_relaxed);
        }
        return {};
    }

    [[nodiscard]] RecordingStepEvent startAsyncEvent(std::string_view, std::string_view = {}, std::initializer_list<gr::profiling::arg_value> = {}) const noexcept { return {}; }
};

struct RecordingProfiler {
    TraceCounters    counters;
    RecordingHandler handler{std::addressof(counters)};

    explicit RecordingProfiler(const gr::profiling::Options& = {}) {}
    void              reset() noexcept {}
    RecordingHandler* forThisThread() noexcept { return std::addressof(handler); }
};

static_assert(gr::profiling::ProfilerLike<RecordingProfiler>);

template<typename T>
struct Adder : gr::Block<Adder<T>> {
    gr::PortIn<T>  addend0;
    gr::PortIn<T>  addend1;
    gr::PortOut<T> sum;

    GR_MAKE_REFLECTABLE(Adder, addend0, addend1, sum);

    [[nodiscard]] constexpr T processOne(T a, T b) const noexcept { return a + b; }
};

struct FeedbackGraph {
    gr::Graph                           flow;
    gr::testing::CountingSource<float>* source = nullptr;
    Adder<float>*                       adder  = nullptr;
    gr::testing::Copy<float>*           loopBack = nullptr;
};

// src -> adder -> sink, with adder -> loopBack -> adder closing a 1:1 rate-consistent cycle. GR4 primes the
// back-edge at connect time, so the loop runs; the deadline transform must treat the primed edge as carrying
// no intra-iteration precedence.
FeedbackGraph makeFeedbackGraph(gr::Size_t nSamples) {
    using namespace gr::testing;
    FeedbackGraph built;

    auto& source   = built.flow.emplaceBlock<CountingSource<float>>({{"name", "src"}, {"n_samples_max", nSamples}});
    auto& adder    = built.flow.emplaceBlock<Adder<float>>({{"name", "adder"}});
    auto& loopBack = built.flow.emplaceBlock<Copy<float>>({{"name", "loopBack"}});
    auto& sink     = built.flow.emplaceBlock<AtomicCountingSink<float>>({{"name", "sink"}});

    boost::ut::expect(built.flow.connect<"out", "addend0">(source, adder).has_value());
    boost::ut::expect(built.flow.connect<"sum", "in">(adder, sink).has_value());
    boost::ut::expect(built.flow.connect<"sum", "in">(adder, loopBack).has_value());
    boost::ut::expect(built.flow.connect<"out", "addend1">(loopBack, adder).has_value());

    built.source   = &source;
    built.adder    = &adder;
    built.loopBack = &loopBack;
    return built;
}

struct LinearGraph {
    gr::Graph                                     flow;
    gr::testing::CountingSource<float>*           source = nullptr;
    gr::testing::Copy<float>*                     copyA  = nullptr;
    gr::testing::AtomicCountingSink<float>*       sink   = nullptr;
};

// blocks live behind shared_ptr inside the Graph, so these observers stay valid after the Graph is moved
// into a scheduler.
LinearGraph makeLinearGraph(gr::Size_t nSamples) {
    using namespace gr::testing;
    LinearGraph built;

    auto& source = built.flow.emplaceBlock<CountingSource<float>>({{"name", "src"}, {"n_samples_max", nSamples}});
    auto& copyA  = built.flow.emplaceBlock<Copy<float>>({{"name", "copyA"}});
    auto& copyB  = built.flow.emplaceBlock<Copy<float>>({{"name", "copyB"}});
    auto& sink   = built.flow.emplaceBlock<AtomicCountingSink<float>>({{"name", "sink"}});

    boost::ut::expect(built.flow.connect<"out", "in">(source, copyA).has_value());
    boost::ut::expect(built.flow.connect<"out", "in">(copyA, copyB).has_value());
    boost::ut::expect(built.flow.connect<"out", "in">(copyB, sink).has_value());

    built.source = &source;
    built.copyA  = &copyA;
    built.sink   = &sink;
    return built;
}

gr::Graph makeLinearFlow(gr::Size_t nSamples) { return std::move(makeLinearGraph(nSamples).flow); }

// independent source->sink chains: unlike a linear pipeline these genuinely compete for workers, so the
// partitioner has a real decision to make.
gr::Graph makeParallelChains(std::size_t nChains, gr::Size_t nSamples, std::uint64_t urgentDeadlineNs) {
    using namespace gr::testing;
    gr::Graph flow;

    for (std::size_t chain = 0UZ; chain < nChains; ++chain) {
        auto& source = flow.emplaceBlock<CountingSource<float>>({{"name", std::format("src{}", chain)}, {"n_samples_max", nSamples}});
        auto& sink   = flow.emplaceBlock<AtomicCountingSink<float>>({{"name", std::format("sink{}", chain)}});

        // every other chain is urgent, so a deadline-blind partitioner can co-locate all the urgent work
        if (chain % 2UZ == 0UZ && urgentDeadlineNs > 0UZ) {
            source.meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = urgentDeadlineNs;
            sink.meta_information.value[std::string(gr::scheduler::kDeadlineKey)]   = urgentDeadlineNs;
        }

        boost::ut::expect(flow.connect<"out", "in">(source, sink).has_value());
    }

    return flow;
}

} // namespace

const boost::ut::suite<"DispatchSeam"> dispatchSeamTests = [] {
    using namespace boost::ut;
    using gr::scheduler::ExecutionPolicy;

    "the default dispatchOnce preserves the historic round-robin"_test = [] {
        gr::scheduler::Simple<> sched;
        expect(sched.exchange(makeLinearFlow(1000U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());
    };

    "an override drives execution on the single-threaded policy"_test = [] {
        ReverseOrderScheduler<ExecutionPolicy::singleThreaded> sched;
        auto                                                   log = sched.dispatchLog;

        expect(sched.exchange(makeLinearFlow(1000U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        expect(log->nCalls.load() > 0UZ) << "dispatchOnce() was never reached — the seam is dead";
        expect(log->nDistinctRunners() == 1UZ) << "single-threaded must drive exactly one runner";
    };

    "an override is invoked per worker on the multi-threaded policy"_test = [] {
        ReverseOrderScheduler<ExecutionPolicy::multiThreaded> sched;
        auto                                                  log = sched.dispatchLog;

        expect(sched.exchange(makeLinearFlow(1000U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        expect(log->nCalls.load() > 0UZ) << "dispatchOnce() was never reached — the seam is dead";
        expect(log->nDistinctRunners() > 1UZ) << "multi-threaded must dispatch through more than one runner, "
                                                 "otherwise per-worker EDF state has no basis";
    };
};

const boost::ut::suite<"EarliestDeadlineFirst"> edfTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    "runs a graph to completion on the single-threaded policy"_test = [] {
        constexpr gr::Size_t nSamples = 2000U;

        LinearGraph                                       built = makeLinearGraph(nSamples);
        auto*                                             sink  = built.sink;
        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;

        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        expect(sched.totalDispatches() > 0UZ) << "EDF never selected a block";
        expect(that % sink->count.value == nSamples) << "sink did not receive every sample";
    };

    "runs a graph to completion on the multi-threaded policy"_test = [] {
        constexpr gr::Size_t nSamples = 2000U;

        LinearGraph                                          built = makeLinearGraph(nSamples);
        auto*                                                sink  = built.sink;
        EarliestDeadlineFirst<ExecutionPolicy::multiThreaded> sched;

        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        expect(sched.tasksPerRunner.size() > 1UZ) << "expected a partitioned task table, one entry per worker";
        expect(sched.totalDispatches() > 0UZ) << "EDF never selected a block";
        expect(that % sink->count.value == nSamples) << "sink did not receive every sample";
    };

    "per-block deadlines are read from block metadata"_test = [] {
        constexpr std::uint64_t tightDeadlineNs = 250'000U;

        LinearGraph built = makeLinearGraph(500U);
        built.copyA->meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = tightDeadlineNs;

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* annotated = sched.findTask("copyA");
        expect(annotated != nullptr) << "annotated block missing from the task table" << fatal;
        expect(that % annotated->relativeDeadline.count() == static_cast<std::int64_t>(tightDeadlineNs));

        const auto* plain = sched.findTask("copyB");
        expect(plain != nullptr) << fatal;
        expect(that % plain->relativeDeadline.count() == sched.defaultDeadline.count()) << "unannotated block should take the default";
    };

    "the default deadline is configurable through sched_settings"_test = [] {
        constexpr std::uint64_t defaultNs = 3'000'000U;

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        gr::property_map                                       edfSettings;
        edfSettings[std::string(gr::scheduler::kDefaultDeadlineKey)] = defaultNs;
        expect(sched.settings().set({{"sched_settings", edfSettings}}).empty());
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();

        expect(sched.exchange(makeLinearFlow(500U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        expect(that % sched.defaultDeadline.count() == static_cast<std::int64_t>(defaultNs));
    };
};

const boost::ut::suite<"EDFDeadlineAccounting"> edfDeadlineTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    auto withDeadlines = [](LinearGraph& built, std::uint64_t deadlineNs) {
        const std::string key(gr::scheduler::kDeadlineKey);
        built.source->meta_information.value[key] = deadlineNs;
        built.copyA->meta_information.value[key]  = deadlineNs;
        built.sink->meta_information.value[key]   = deadlineNs;
    };

    "deadlines that cannot be missed produce no misses"_test = [&] {
        constexpr std::uint64_t hourNs = 3'600'000'000'000ULL;

        LinearGraph built = makeLinearGraph(2000U);
        withDeadlines(built, hourNs);

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto stats = sched.statistics();
        expect(stats.nDispatches > 0UZ) << "no dispatches to account for";
        expect(that % stats.nMisses == 0UZ) << "generous deadlines must never be reported as missed";
        expect(that % stats.maxLateness.count() == 0) << "lateness must stay zero when nothing is late";
    };

    "deadlines that cannot be met are reported as missed"_test = [&] {
        LinearGraph built = makeLinearGraph(2000U);
        withDeadlines(built, 1U); // 1 ns: every re-release is already overdue

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto stats = sched.statistics();
        expect(stats.nDispatches > 0UZ) << fatal;
        expect(stats.nMisses > 0UZ) << "impossible deadlines must be reported as missed";
        expect(stats.maxLateness.count() > 0) << "a missed deadline implies positive lateness";
    };

    "the tightest deadline never loses a selection tie"_test = [] {
        LinearGraph built = makeLinearGraph(4000U);
        built.copyA->meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = std::uint64_t{1'000};

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        // an unbounded work quantum lets each block drain the whole graph in a handful of calls, leaving the
        // scheduler almost nothing to decide; bounding it is what gives EDF something to govern
        expect(sched.settings().set({{"max_work_items", std::size_t{64}}}).empty());
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();

        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* tight = sched.findTask("copyA");
        const auto* loose = sched.findTask("copyB");
        expect(tight != nullptr && loose != nullptr) << fatal;

        const auto stats = sched.statistics();
        std::println("copyA (1 us deadline): {} dispatches | copyB (default): {} dispatches", tight->nDispatches, loose->nDispatches);
        std::println("EDF governance: {} selections vs {} fallback sweeps ({:.1f}% EDF-governed)", stats.nSelections, stats.nSweeps, //
            100.0 * static_cast<double>(stats.nSelections) / static_cast<double>(std::max<std::uint64_t>(1U, stats.nSelections + stats.nSweeps)));
        expect(tight->nDispatches > 0UZ) << "the tight-deadline block was never selected";
        expect(tight->nDispatches >= loose->nDispatches) << "a tighter deadline must never be scheduled less often than a looser one";

        // The bounded fallback sweep is round-robin, not EDF. An optimisation that quietly shifts dispatch
        // into the sweep leaves every other assertion here green while dismantling the discipline under test,
        // so the governed share is asserted rather than merely reported.
        const double governedShare = static_cast<double>(stats.nSelections) / static_cast<double>(std::max<std::uint64_t>(1U, stats.nSelections + stats.nSweeps));
        expect(governedShare > 0.9) << "EDF must govern dispatch; the round-robin fallback sweep has taken over";
    };

    "an injected clock makes deadline accounting reproducible"_test = [] {
        auto runOnce = [] {
            TickClock::reset();
            LinearGraph built = makeLinearGraph(1500U);
            built.copyA->meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = std::uint64_t{5'000};

            EarliestDeadlineFirst<ExecutionPolicy::singleThreaded, TickClock> sched;
            expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
            expect(sched.runAndWait().has_value());
            return sched.statistics();
        };

        const auto first  = runOnce();
        const auto second = runOnce();

        expect(first.nDispatches > 0UZ) << "virtual-clock run performed no work" << fatal;
        expect(that % first.nDispatches == second.nDispatches) << "dispatch count must be reproducible under a virtual clock";
        expect(that % first.nMisses == second.nMisses) << "miss count must be reproducible under a virtual clock";
    };
};

const boost::ut::suite<"EDFPrecedence"> edfPrecedenceTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    "an urgent sink tightens every upstream block"_test = [] {
        constexpr std::uint64_t urgentNs = 2'000U;

        LinearGraph built = makeLinearGraph(500U);
        built.sink->meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = urgentNs;

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* source = sched.findTask("src");
        const auto* sink   = sched.findTask("sink");
        expect(source != nullptr && sink != nullptr) << fatal;

        // the source keeps its own declared deadline, but is *scheduled* as though it were as urgent as the
        // sink it feeds -- that is the whole point of the precedence transform
        expect(that % source->relativeDeadline.count() == sched.defaultDeadline.count()) << "declared deadline must be left intact";
        expect(source->effectiveDeadline <= sink->relativeDeadline) << "upstream block did not inherit downstream urgency";
        expect(source->effectiveDeadline < source->relativeDeadline) << "effective deadline should have been tightened";
    };

    "worst-case execution times widen the upstream margin"_test = [] {
        constexpr std::uint64_t urgentNs = 10'000U;
        constexpr std::uint64_t wcetNs   = 3'000U;

        LinearGraph built = makeLinearGraph(500U);
        built.sink->meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = urgentNs;
        built.sink->meta_information.value[std::string(gr::scheduler::kWcetKey)]     = wcetNs;

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* upstream = sched.findTask("copyB"); // immediate predecessor of the sink
        expect(upstream != nullptr) << fatal;

        // d*(copyB) = min( d(copyB), d*(sink) - C(sink) ) = 10 us - 3 us
        expect(that % upstream->effectiveDeadline.count() == static_cast<std::int64_t>(urgentNs - wcetNs)) //
            << "predecessor must finish a full WCET before its successor's deadline";
    };

    "a primed feedback edge does not ratchet deadlines down"_test = [] {
        using namespace std::chrono_literals;
        constexpr std::uint64_t sinkDeadlineNs = 8'000'000U; // 8 ms
        constexpr std::uint64_t wcetNs         = 1'000'000U; // 1 ms

        FeedbackGraph built = makeFeedbackGraph(200U);
        built.adder->meta_information.value[std::string(gr::scheduler::kWcetKey)]    = wcetNs;
        built.loopBack->meta_information.value[std::string(gr::scheduler::kWcetKey)] = wcetNs;

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        for (const auto& block : built.flow.blocks()) {
            if (block->name() == "sink") {
                block->metaInformation()[std::string(gr::scheduler::kDeadlineKey)] = sinkDeadlineNs;
            }
        }

        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value()) << "primed feedback graph must still run";

        const auto* adder    = sched.findTask("adder");
        const auto* source   = sched.findTask("src");
        const auto* loopBack = sched.findTask("loopBack");
        expect(adder != nullptr && source != nullptr && loopBack != nullptr) << fatal;

        // On the DAG left after dropping the back-edge: d*(adder) = min(default, 8 ms - 0) = 8 ms, and
        // d*(src) = d*(adder) - C(adder) = 7 ms. Treating the back-edge as a real constraint would subtract
        // C once per relaxation round and drive both below these values.
        expect(that % adder->effectiveDeadline.count() == std::chrono::nanoseconds(8ms).count()) << "adder deadline was ratcheted by the feedback edge";
        expect(that % source->effectiveDeadline.count() == std::chrono::nanoseconds(7ms).count()) << "source deadline was ratcheted by the feedback edge";
        expect(loopBack->effectiveDeadline > EarliestDeadlineFirst<ExecutionPolicy::singleThreaded>::kMinimumDeadline) << "loop member collapsed to the floor";
        expect(that % sched.nInfeasibleDeadlines == 0UZ) << "no edge should be reported infeasible here";
    };

    "the precedence transform can be switched off"_test = [] {
        LinearGraph built = makeLinearGraph(500U);
        built.sink->meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = std::uint64_t{2'000};

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        gr::property_map                                       edfSettings;
        edfSettings[std::string(gr::scheduler::kPrecedenceKey)] = false;
        expect(sched.settings().set({{"sched_settings", edfSettings}}).empty());
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();

        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* source = sched.findTask("src");
        expect(source != nullptr) << fatal;
        expect(that % source->effectiveDeadline.count() == source->relativeDeadline.count()) << "disabled transform must leave deadlines alone";
    };
};

const boost::ut::suite<"EDFPartitioning"> edfPartitioningTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    constexpr std::size_t nChains = 4UZ;
    constexpr std::size_t nBlocks = nChains * 2UZ;

    auto assignedBlocks = [](const auto& sched) {
        std::vector<const gr::BlockModel*> seen;
        for (const auto& tasks : sched.tasksPerRunner) {
            for (const auto& task : tasks) {
                seen.push_back(task.block.get());
            }
        }
        std::ranges::sort(seen);
        return seen;
    };

    "every block is assigned to exactly one worker"_test = [&] {
        EarliestDeadlineFirst<ExecutionPolicy::multiThreaded> sched;
        expect(sched.exchange(makeParallelChains(nChains, 400U, 5'000U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        auto seen = assignedBlocks(sched);
        expect(that % seen.size() == nBlocks) << "partition lost or duplicated blocks";
        expect(std::ranges::adjacent_find(seen) == seen.end()) << "a block was assigned to more than one worker";
    };

    "the urgency partitioner balances workers"_test = [&] {
        EarliestDeadlineFirst<ExecutionPolicy::multiThreaded> sched;
        expect(sched.exchange(makeParallelChains(nChains, 400U, 5'000U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        std::vector<std::size_t> sizes;
        for (const auto& tasks : sched.tasksPerRunner) {
            sizes.push_back(tasks.size());
        }
        const auto [smallest, largest] = std::ranges::minmax(sizes);
        std::println("partition sizes across {} workers: {}", sizes.size(), sizes);
        expect(largest - smallest <= 1UZ) << "greedy least-loaded assignment should leave workers within one block of each other";
    };

    "stride partitioning remains available and correct"_test = [&] {
        EarliestDeadlineFirst<ExecutionPolicy::multiThreaded> sched;
        gr::property_map                                      edfSettings;
        edfSettings[std::string(gr::scheduler::kPartitioningKey)] = false;
        expect(sched.settings().set({{"sched_settings", edfSettings}}).empty());
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();

        expect(sched.exchange(makeParallelChains(nChains, 400U, 5'000U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        expect(not sched.deadlinePartitioning) << "setting should have switched the partitioner off";
        auto seen = assignedBlocks(sched);
        expect(that % seen.size() == nBlocks) << "stride partition lost or duplicated blocks";
        expect(std::ranges::adjacent_find(seen) == seen.end()) << "stride partition assigned a block twice";
    };
};

const boost::ut::suite<"EDFBufferPressure"> edfBufferPressureTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    auto withBufferPressure = [](auto& sched, bool enabled) {
        gr::property_map edfSettings;
        edfSettings[std::string(gr::scheduler::kBufferPressureDeadlineKey)] = enabled;
        expect(sched.settings().set({{"sched_settings", edfSettings}}).empty());
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();
    };

    "buffer-derived urgency is off unless asked for"_test = [] {
        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(not sched.bufferPressureDeadlines) << "buffer pressure changes EDF ordering, so it must be opt-in";
    };

    "a graph runs correctly with buffer-derived urgency"_test = [&] {
        constexpr gr::Size_t nSamples = 2000U;

        LinearGraph                                           built = makeLinearGraph(nSamples);
        auto*                                                 sink  = built.sink;
        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        withBufferPressure(sched, true);

        expect(sched.settings().set({{"max_work_items", std::size_t{64}}}).empty());
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();

        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        expect(sched.bufferPressureDeadlines) << "setting did not take effect";
        expect(that % sink->count.value == nSamples) << "buffer-derived urgency must not change what the graph computes";

        const auto stats = sched.statistics();
        std::println("buffer-pressure run: {} selections vs {} sweeps", stats.nSelections, stats.nSweeps);
        expect(stats.nSelections > stats.nSweeps) << "EDF selection should still govern the run";
    };

    "backlog tightens a block's deadline"_test = [&] {
        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        withBufferPressure(sched, true);
        expect(sched.exchange(makeLinearFlow(500U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* sinkTask = sched.findTask("sink");
        expect(sinkTask != nullptr) << fatal;

        // a drained sink keeps its base deadline; the same sink holding a backlog is scaled down
        const auto base    = sinkTask->effectiveDeadline;
        const auto drained = sched.pressureAdjusted(*sinkTask->block, base);
        expect(drained <= base) << "pressure adjustment must never loosen a deadline";
        expect(drained > std::chrono::nanoseconds::zero()) << "adjusted deadline must stay positive";
    };
};

const boost::ut::suite<"JobTracking"> jobTrackingTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    "a deadline stays anchored to release until the fixed batch completes"_test = [] {
        using State = gr::scheduler::JobState<TickClock>;
        using TimePoint = TickClock::time_point;
        using namespace std::chrono_literals;

        State state;
        const TimePoint release{1'000ns};
        state.observeReleases(16UZ, 8UZ, release);

        expect(that % state.nReleased == 2UZ);
        expect(that % state.nPendingJobs() == 2UZ);
        expect(that % state.earliestDeadline(100ns)->time_since_epoch().count() == 1'100);

        state.observeCompletion(4UZ, 8UZ, 100ns, TimePoint{1'050ns});
        expect(that % state.nCompleted == 0UZ) << "a partial batch must not complete a job";
        expect(that % state.earliestDeadline(100ns)->time_since_epoch().count() == 1'100) << "dispatch must not push the deadline forward";

        state.observeCompletion(4UZ, 8UZ, 100ns, TimePoint{1'080ns});
        expect(that % state.nCompleted == 1UZ);
        state.observeCompletion(8UZ, 8UZ, 100ns, TimePoint{1'200ns});
        expect(that % state.nCompleted == 2UZ);
        expect(that % state.nMissed == 1UZ);
        expect(that % state.maxLateness.count() == 100);
    };

    "batch bounds and period form the implicit deadline"_test = [] {
        constexpr gr::Size_t nSamples = 128U;
        constexpr std::uint64_t periodNs = 8'000UZ;

        LinearGraph built = makeLinearGraph(nSamples);
        auto& meta = built.copyA->meta_information.value;
        meta[std::string(gr::scheduler::kMinBatchSizeKey)] = std::uint64_t{8UZ};
        meta[std::string(gr::scheduler::kMaxBatchSizeKey)] = std::uint64_t{16UZ};
        meta[std::string(gr::scheduler::kBatchSizeKey)]    = std::uint64_t{64UZ};
        meta[std::string(gr::scheduler::kPeriodKey)]       = periodNs;

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* task = sched.findTask("copyA");
        expect(task != nullptr) << fatal;
        expect(that % task->parameters.minBatchSize == 8UZ);
        expect(that % task->parameters.maxBatchSize == 16UZ);
        expect(that % task->parameters.batchSize == 16UZ) << "requested batch must be clamped to the block maximum";
        expect(that % task->relativeDeadline.count() == static_cast<std::int64_t>(periodNs)) << "implicit deadline must equal the batch period";
        expect(that % task->jobs.nCompleted == static_cast<std::uint64_t>(nSamples) / 16UZ);
        expect(that % task->jobs.nPendingJobs() == 0UZ);
    };

    "a source rate propagates to downstream batch periods"_test = [] {
        LinearGraph built = makeLinearGraph(64U);
        built.source->meta_information.value[std::string(gr::scheduler::kBatchSizeKey)] = std::uint64_t{8UZ};
        built.source->meta_information.value[std::string(gr::scheduler::kPeriodKey)]    = std::uint64_t{8'000UZ};
        built.copyA->meta_information.value[std::string(gr::scheduler::kBatchSizeKey)]  = std::uint64_t{16UZ};

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* downstream = sched.findTask("copyA");
        expect(downstream != nullptr) << fatal;
        expect(downstream->parameters.periodFromSampleRate) << "downstream rate was not inferred from the source";
        expect(that % downstream->parameters.period.count() == 16'000) << "doubling the batch at the same rate must double the period";
        expect(that % downstream->relativeDeadline.count() == 16'000) << "the propagated period must supply the implicit deadline";
    };
};

const boost::ut::suite<"FixedPriorityScheduling"> fixedPriorityTests = [] {
    using namespace boost::ut;
    using gr::scheduler::ExecutionPolicy;
    using gr::scheduler::FixedPriority;

    "the most urgent released job dispatches first"_test = [] {
        gr::Graph flow = makeParallelChains(2UZ, 64U, 0UZ);
        for (const auto& block : flow.blocks()) {
            if (block->name() == "src0") {
                block->metaInformation()[std::string(gr::scheduler::kPriorityKey)] = std::int64_t{1};
                block->metaInformation()[std::string(gr::scheduler::kBatchSizeKey)] = std::uint64_t{8UZ};
            } else if (block->name() == "src1") {
                block->metaInformation()[std::string(gr::scheduler::kPriorityKey)] = std::int64_t{10};
                block->metaInformation()[std::string(gr::scheduler::kBatchSizeKey)] = std::uint64_t{8UZ};
            }
        }

        FixedPriority<ExecutionPolicy::singleThreaded> sched;
        expect(sched.schedulingPolicyName() == "FixedPriority");
        expect(sched.exchange(std::move(flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const auto* high = sched.findTask("src0");
        const auto* low  = sched.findTask("src1");
        expect(high != nullptr && low != nullptr) << fatal;
        expect(that % high->fixedPriority == std::int64_t{1});
        expect(that % low->fixedPriority == std::int64_t{10});
        expect(high->firstDispatchOrder < low->firstDispatchOrder) << "smaller numeric priority must dispatch first";
        expect(high->jobs.nCompleted > 0UZ && low->jobs.nCompleted > 0UZ) << "priority changes order, not graph completion";
    };
};

const boost::ut::suite<"JobTracing"> jobTracingTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    "release dispatch completion and miss events reach the tracing interface"_test = [] {
        LinearGraph built = makeLinearGraph(32U);
        built.copyA->meta_information.value[std::string(gr::scheduler::kDeadlineKey)] = std::uint64_t{1UZ};

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded, TickClock, RecordingProfiler> sched;
        TickClock::reset();
        expect(sched.exchange(std::move(built.flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());

        const TraceCounters& trace = sched.profiler().counters;
        expect(trace.releases.load(std::memory_order_relaxed) > 0UZ);
        expect(trace.dispatches.load(std::memory_order_relaxed) > 0UZ);
        expect(trace.completions.load(std::memory_order_relaxed) > 0UZ);
        expect(trace.misses.load(std::memory_order_relaxed) > 0UZ);
    };
};

// Deliberately no test flips real worker threads to SCHED_FIFO: a runaway real-time thread on a 4-core board
// can starve the machine, and an unattainable priority inside GR4's setThreadSchedulingParameter() is a
// [[noreturn]] fatal. What is tested is that EDF declines rather than aborts, which is the property that
// matters for anything running unattended.
const boost::ut::suite<"EDFRealTimePriority"> edfRealTimeTests = [] {
    using namespace boost::ut;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    auto withRealTimePriority = [](auto& sched, std::int64_t priority) {
        gr::property_map edfSettings;
        edfSettings[std::string(gr::scheduler::kRealTimePriorityKey)] = priority;
        expect(sched.settings().set({{"sched_settings", edfSettings}}).empty());
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();
    };

    "worker threads stay on the default policy unless asked"_test = [] {
        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(that % sched.realTimePriority == 0) << "real-time scheduling must never be the default";
    };

    "an unattainable priority is declined, not fatal"_test = [&] {
        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        withRealTimePriority(sched, 10'000); // far above any SCHED_FIFO ceiling

        expect(sched.exchange(makeLinearFlow(500U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value()) << "an impossible priority must not stop the graph running";
        expect(that % sched.realTimePriority == 0) << "out-of-range priority should have been refused";
    };

    "a negative priority is treated as disabled"_test = [&] {
        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        withRealTimePriority(sched, -5);

        expect(sched.exchange(makeLinearFlow(500U)).has_value()) << fatal;
        expect(sched.runAndWait().has_value());
        expect(that % sched.realTimePriority == 0);
    };
};

namespace {

// stands in for a wall-clock-paced source (ClockSource, hardware drivers): work() finds output room but no
// data due yet, so it publishes nothing. The refusal count plays the role of elapsed time, keeping the test
// deterministic, and each refusal records how far a peer chain had progressed by then.
template<typename T>
struct RefusingSource : gr::Block<RefusingSource<T>> {
    gr::PortOut<T> out;

    gr::Annotated<gr::Size_t, "n_samples_max">                                                                 n_samples_max = 64U;
    gr::Annotated<gr::Size_t, "n_refusals", gr::Doc<"work() calls answered with no samples before producing">> n_refusals    = 4U;

    GR_MAKE_REFLECTABLE(RefusingSource, out, n_samples_max, n_refusals);

    std::function<gr::Size_t()>  observePeer;
    std::vector<gr::Size_t>      peerCountAtRefusal;
    gr::Size_t                   _nRefused  = 0U;
    gr::Size_t                   _nProduced = 0U;

    gr::work::Status processBulk(gr::OutputSpanLike auto& outSpan) {
        if (_nRefused < n_refusals) {
            _nRefused++;
            if (observePeer) {
                peerCountAtRefusal.push_back(observePeer());
            }
            outSpan.publish(0UZ);
            return gr::work::Status::INSUFFICIENT_INPUT_ITEMS;
        }
        if (_nProduced >= n_samples_max) {
            outSpan.publish(0UZ);
            return gr::work::Status::DONE;
        }
        const std::size_t nProducible = std::min(static_cast<std::size_t>(n_samples_max - _nProduced), outSpan.size());
        outSpan.publish(nProducible);
        _nProduced += static_cast<gr::Size_t>(nProducible);
        return gr::work::Status::OK;
    }
};

} // namespace

const boost::ut::suite<"EDFPacedSources"> edfPacedSourceTests = [] {
    using namespace boost::ut;
    using namespace gr::testing;
    using gr::scheduler::EarliestDeadlineFirst;
    using gr::scheduler::ExecutionPolicy;

    "a source that is not yet due neither spins nor starves its peers"_test = [] {
        constexpr gr::Size_t kPacedSamples = 64U;
        constexpr gr::Size_t kRefusals     = 4U;
        constexpr gr::Size_t kBulkSamples  = 20'000U;

        gr::Graph flow;
        auto&     paced     = flow.emplaceBlock<RefusingSource<float>>({{"name", "paced"}, {"n_samples_max", kPacedSamples}, {"n_refusals", kRefusals}});
        auto&     pacedSink = flow.emplaceBlock<CountingSink<float>>({{"name", "pacedSink"}, {"n_samples_max", kPacedSamples}});
        auto&     bulkSrc   = flow.emplaceBlock<CountingSource<float>>({{"name", "bulkSrc"}, {"n_samples_max", kBulkSamples}});
        auto&     bulkSink  = flow.emplaceBlock<AtomicCountingSink<float>>({{"name", "bulkSink"}, {"n_samples_max", kBulkSamples}});

        // the paced chain is the most urgent in the graph -- exactly the setting in which a premature release
        // would win every selection and spin the dispatch loop dry
        paced.meta_information.value[std::string(gr::scheduler::kDeadlineKey)]      = std::uint64_t{1'000'000};
        pacedSink.meta_information.value[std::string(gr::scheduler::kDeadlineKey)]  = std::uint64_t{1'000'000};
        bulkSrc.meta_information.value[std::string(gr::scheduler::kBatchSizeKey)]   = std::uint64_t{256};
        bulkSink.meta_information.value[std::string(gr::scheduler::kBatchSizeKey)]  = std::uint64_t{256};

        expect(flow.connect<"out", "in">(paced, pacedSink).has_value());
        expect(flow.connect<"out", "in">(bulkSrc, bulkSink).has_value());

        // the bulk source is the first bulk block to run after a parked refusal, so its counter is the
        // earliest observable proof that the rest of the graph kept moving
        paced.observePeer = [source = &bulkSrc] { return source->count.value; };

        EarliestDeadlineFirst<ExecutionPolicy::singleThreaded> sched;
        expect(sched.exchange(std::move(flow)).has_value()) << fatal;
        expect(sched.runAndWait().has_value()) << fatal;

        expect(that % paced._nProduced == kPacedSamples);
        expect(that % pacedSink.count.value == kPacedSamples);
        expect(that % bulkSink.loadCount() == kBulkSamples);
        expect(that % paced._nRefused == kRefusals);

        expect(that % paced.peerCountAtRefusal.size() == static_cast<std::size_t>(kRefusals)) << fatal;
        expect(that % paced.peerCountAtRefusal.back() > 0U) << "the bulk chain must progress between refusals; a frozen peer count means the parked source still monopolised the dispatch loop";

        const auto statistics = sched.statistics();
        expect(that % statistics.nWithdrawn >= static_cast<std::uint64_t>(kRefusals)) << "every premature release must be withdrawn, not left pending";
    };
};

int main() { /* tests are statically executed */ }
