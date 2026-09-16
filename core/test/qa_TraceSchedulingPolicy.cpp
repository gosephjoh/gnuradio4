#include <boost/ut.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <map>
#include <optional>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/Trace.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::scheduler;

using Clock = std::chrono::steady_clock;

/**
 * Trace coverage for the release, selection and deadline markers -- the policy-dependent half of the
 * layer, kept apart from `qa_TraceScheduler.cpp` because it exercises code that only some policies
 * compile at all.
 *
 * Most scenarios drive `releaseIfEligible()` directly rather than through a running graph. That is
 * not a shortcut: the markers being tested fire inside a free function whose inputs are a block, a
 * state and an instant, so calling it directly makes the release deterministic and the assertion
 * exact, where a scheduler would make both depend on thread timing.
 */
namespace {

/// Owns the ring storage a `SchedState` spans into, so release can be exercised without a scheduler.
/// Fixed size at construction: the span must not be invalidated.
struct StateFixture {
    std::vector<gr::scheduler::Job> storage;
    SchedState                      state{};

    explicit StateFixture(std::size_t capacity) : storage(capacity) { state.jobs.storage = std::span<gr::scheduler::Job>{storage}; }
};

void activate(gr::BlockModel& block) {
    expect(block.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
    expect(block.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
}

/// `ConstantSource -> Copy -> NullSink`, connected, every buffer empty.
struct Chain {
    gr::Graph       graph;
    gr::BlockModel* mid    = nullptr;
    gr::BlockModel* source = nullptr;

    Chain() {
        auto& src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto& copy = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto& sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, copy).has_value() >> fatal);
        expect(graph.connect<"out", "in">(copy, sink).has_value() >> fatal);
        expect(graph.connectPendingEdges() >> fatal);
        source = graph.blocks()[0].get();
        mid    = graph.blocks()[1].get();
    }

    /// Only valid before activation: priming a RUNNING block is refused by design.
    void prime(std::size_t n) { expect(mid->primeInputPort(0UZ, n).has_value() >> fatal); }
};

/// `src -> copy -> sink` driven until the source has stopped and published its end-of-stream tag,
/// leaving the copy holding `samples` samples with EOS pending behind them.
struct EndedChain {
    gr::Graph       graph;
    gr::BlockModel* src = nullptr;
    gr::BlockModel* mid = nullptr;

    explicit EndedChain(gr::Size_t samples) {
        auto& source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", samples}});
        auto& copy   = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto& sink   = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(source, copy).has_value() >> fatal);
        expect(graph.connect<"out", "in">(copy, sink).has_value() >> fatal);
        expect(graph.connectPendingEdges() >> fatal);

        src = graph.blocks()[0].get();
        mid = graph.blocks()[1].get();
        activate(*src);
        activate(*mid);

        for (std::size_t i = 0UZ; i < 8UZ && src->state() == gr::lifecycle::State::RUNNING; ++i) {
            std::ignore = src->work(2UZ * static_cast<std::size_t>(samples));
        }
        expect(src->state() != gr::lifecycle::State::RUNNING) << fatal << "the source must have stopped";
        expect(mid->inputStreamEnded()) << fatal << "and its end-of-stream tag must have reached the consumer";
    }
};

void collectingConsumer(const gr::trace::Event& event, void* user) noexcept { static_cast<std::vector<gr::trace::Event>*>(user)->push_back(event); }

[[nodiscard]] std::vector<gr::trace::Event> collect() {
    std::vector<gr::trace::Event> events;
    std::ignore = gr::trace::forEachEvent(collectingConsumer, &events);
    return events;
}

[[nodiscard]] std::vector<gr::trace::Event> ofKind(gr::trace::Kind kind) {
    std::vector<gr::trace::Event> matching;
    for (const gr::trace::Event& event : collect()) {
        if (event.kind == kind) {
            matching.push_back(event);
        }
    }
    return matching;
}

/// Writes the capture to `$GR4_TRACE_ARTEFACT_DIR/<name>.gr4trace` when that variable is set.
///
/// Unset -- every ordinary run, and CI -- and this is a no-op, so the suite stays hermetic. Capturing
/// a timeline to look at by hand is an explicit opt-in, which keeps a test that asserts behaviour
/// from also being a test that depends on a writable directory.
void exportTimeline([[maybe_unused]] std::string_view name) {
    if constexpr (gr::trace::kEnabled) {
        const char* directory = std::getenv("GR4_TRACE_ARTEFACT_DIR");
        if (directory == nullptr) {
            return;
        }
        std::ignore = gr::trace::dump(std::format("{}/{}.gr4trace", directory, name));
    }
}

/// Drives an `externalStep` EDF scheduler over `src -> mid -> snk` to completion, with an optional
/// settings override applied before it starts. Returns nothing: the assertion material is the trace.
template<typename TPolicy, typename TConfigure>
void runChain(gr::Size_t samples, TConfigure&& configure) {
    gr::Graph graph;
    auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", samples}});
    auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
    auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
    std::ignore      = graph.connect<"out", "in">(source, copy);
    std::ignore      = graph.connect<"out", "in">(copy, sink);

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, TPolicy> scheduler;
    expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
    configure(scheduler);
    expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
    for (std::size_t pass = 0UZ; pass < 64UZ; ++pass) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            break;
        }
    }
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
}

template<typename TConfigure>
void runEdfChain(gr::Size_t samples, TConfigure&& configure) {
    runChain<gr::scheduler::EdfPolicy>(samples, std::forward<TConfigure>(configure));
}

/// The post-release queue depth, unpacked from the flag byte's high six bits.
[[nodiscard]] std::uint32_t packedDepthOf(const gr::trace::Event& event) noexcept { return static_cast<std::uint32_t>(event.flags >> 2U); }

} // namespace

const boost::ut::suite<"TraceSchedulingPolicy"> policyMarkerTests = [] {
    using namespace gr::trace;

    if constexpr (!kEnabled) {
        "a compiled-out build emits no release markers"_test = [] {
            Chain chain;
            chain.prime(64UZ);
            activate(*chain.mid);

            StateFixture fixture{4UZ};
            setCategories(kAllCategories);
            releaseIfEligible(*chain.mid, fixture.state, Clock::now());

            expect(!fixture.state.jobs.empty()) << "the release itself must still happen";
            expect(eq(ringStats().rings, 0UZ)) << "GR4_ENABLE_TRACING is off; no ring may be allocated";
            setCategories(0U);
        };
        return;
    }

    "a release emits one record carrying its batch, deadline and worker"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        reset();
        setCategories(categoryMask(Category::release));

        StateFixture fixture{4UZ};
        fixture.state.workerId                = 3U;
        fixture.state.relativeDeadlineSeconds = 0.25;

        const Clock::time_point releaseInstant = Clock::now();
        releaseIfEligible(*chain.mid, fixture.state, releaseInstant);
        expect(!fixture.state.jobs.empty() >> fatal) << "the scenario needs an actual release to observe";

        const std::vector<Event> records = ofKind(Kind::jobRelease);
        expect(eq(records.size(), 1UZ) >> fatal) << "one release, one record";
        expect(eq(records.front().payload0, static_cast<std::uint32_t>(fixture.state.jobs.front().batch))) << "the record's batch must be the job's batch";
        expect(eq(records.front().payload1, 250'000'000U)) << "a 0.25 s relative deadline, in nanoseconds";
        expect(eq(std::uint32_t{records.front().workerId}, 3U)) << "release happens in a free function; without SchedState::workerId every release reads as worker 0";
        expect(eq(records.front().entity, fixture.state.entityId));

        // `startNs` *is* the release instant, in the marker clock's domain. A report subtracts a
        // completion from it directly, so a conversion that disagreed would corrupt every response
        // time without ever looking wrong.
        const std::uint64_t expectedNs = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(releaseInstant.time_since_epoch()).count());
        expect(eq(records.front().startNs, expectedNs)) << "the release instant must be recorded, not a second clock read";

        setCategories(0U);
        reset();
    };

    "an unset deadline is distinguishable from a zero one"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        reset();
        setCategories(categoryMask(Category::release));

        StateFixture fixture{4UZ}; // no period, no relative deadline
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());

        const std::vector<Event> records = ofKind(Kind::jobRelease);
        expect(eq(records.size(), 1UZ) >> fatal);
        expect(eq(records.front().payload1, kUnsetDeadline)) << "a job that sorts last must not be reported as one due immediately";

        setCategories(0U);
        reset();
    };

    "a deadline too large to convert is reported as saturated, not as a plausible number"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        reset();
        setCategories(categoryMask(Category::release));

        // Past `kMaxRepresentableDeadlineSeconds` the seconds-to-ticks conversion leaves the integer
        // range, which is undefined behaviour rather than wrap-around -- so the guard has to run
        // before it, and this scenario must not depend on whatever the conversion produced.
        StateFixture fixture{4UZ};
        fixture.state.relativeDeadlineSeconds = 1.0e30;
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());

        const std::vector<Event> records = ofKind(Kind::jobRelease);
        expect(eq(records.size(), 1UZ) >> fatal);
        expect(eq(records.front().payload1, kSaturated)) << "an unrepresentable deadline must be flagged, never laundered into a tardiness figure";
        expect(neq(records.front().payload1, kUnsetDeadline)) << "and it is not the same thing as having no deadline at all";

        setCategories(0U);
        reset();
    };

    "the detection path is recorded, so the backstop and the successor walk can be told apart"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        reset();
        setCategories(categoryMask(Category::release));

        StateFixture backstop{4UZ};
        backstop.state.batchCeiling = 1UZ;
        releaseIfEligible(*chain.mid, backstop.state, Clock::now(), 0U);
        releaseIfEligible(*chain.mid, backstop.state, Clock::now(), flag::kViaSuccessorWalk);

        const std::vector<Event> records = ofKind(Kind::jobRelease);
        expect(eq(records.size(), 2UZ) >> fatal);
        expect(eq(records[0].flags & flag::kViaSuccessorWalk, std::uint8_t{0U})) << "clear means the per-sweep backstop";
        expect(eq(records[1].flags & flag::kViaSuccessorWalk, flag::kViaSuccessorWalk)) << "set means the event-driven walk";

        setCategories(0U);
        reset();
    };

    "a real EDF run exercises both detection paths, and labels each correctly"_test = [] {
        // The scenario above proves `releaseIfEligible()` propagates the flag it is handed. It cannot
        // prove the *scheduler* hands it the right one -- a mutation swapping the successor-walk call
        // site to the backstop value passes it untouched. Only a real run closes that, because only a
        // real run has both call sites in it.
        reset();
        setCategories(categoryMask(Category::release));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{8192U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        // externalStep so the run is driven by this thread and nothing depends on scheduling timing.
        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 64UZ; ++pass) {
            if (scheduler.step().status == gr::work::Status::DONE) {
                break;
            }
        }

        std::size_t                   viaBackstop = 0UZ;
        std::size_t                   viaWalk     = 0UZ;
        std::set<gr::trace::EntityId> walkedEntities;
        for (const Event& event : ofKind(Kind::jobRelease)) {
            if ((event.flags & flag::kViaSuccessorWalk) != 0U) {
                ++viaWalk;
                walkedEntities.insert(event.entity);
            } else {
                ++viaBackstop;
            }
        }

        expect(gt(viaBackstop, 0UZ)) << "a source has no producer to trigger it, so the backstop must release it";
        expect(gt(viaWalk, 0UZ)) << "a consumer fed within the worker must be released by the event-driven walk -- if this is zero, either the walk is dead or its call site lost the flag";
        expect(gt(walkedEntities.size(), 0UZ)) << "and the walk's releases must name the blocks it woke";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        setCategories(0U);
        reset();
    };

    "a drain release says the batch floor was waived"_test = [] {
        // End of stream waives the floor, so the last partial batch runs instead of waiting for data
        // that is never coming. Without the flag that release is indistinguishable from an ordinary
        // one, and a report would read a short final batch as the block having been starved.
        constexpr gr::Size_t kLeftover = 8U;
        EndedChain           chain{kLeftover};

        reset();
        setCategories(categoryMask(Category::release));

        StateFixture fixture{4UZ};
        fixture.state.batchFloor = 64UZ; // far above what is left, so the gate is shut for ever

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(!fixture.state.jobs.empty() >> fatal) << "an ended stream must release what is left";

        const std::vector<Event> records = ofKind(Kind::jobRelease);
        expect(eq(records.size(), 1UZ) >> fatal);
        expect(eq(records.front().flags & flag::kEosWaived, flag::kEosWaived)) << "the waiver must be recorded, or a drain job reads as an ordinary starved one";
        expect(eq(records.front().payload0, static_cast<std::uint32_t>(kLeftover))) << "and the record carries the remainder it drained";

        setCategories(0U);
        reset();
    };

    "an ordinary release does not claim the waiver"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        reset();
        setCategories(categoryMask(Category::release));

        StateFixture fixture{4UZ};
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());

        const std::vector<Event> records = ofKind(Kind::jobRelease);
        expect(eq(records.size(), 1UZ) >> fatal);
        expect(eq(records.front().flags & flag::kEosWaived, std::uint8_t{0U})) << "a flag set unconditionally would be as useless as one never set";

        setCategories(0U);
        reset();
    };

    "both detection passes are costed, including the ones that find nothing"_test = [] {
        // The cost side of the ledger T2a's flag opened. "Did event-driven detection earn its keep"
        // is a ratio -- releases won against scanning done -- and it is only answerable if the
        // *unproductive* scans are recorded too. A layer that recorded only the productive ones would
        // make both paths look free and answer the question wrong in the flattering direction.
        reset();
        setCategories(categoryMask(Category::release));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{8192U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 64UZ; ++pass) {
            if (scheduler.step().status == gr::work::Status::DONE) {
                break;
            }
        }

        std::size_t backstops = 0UZ, walks = 0UZ, barrenWalks = 0UZ, walkReleases = 0UZ;
        bool        backstopNamedABlock = false, everyWalkNamedItsProducer = true;
        for (const Event& event : ofKind(Kind::releaseScan)) {
            if ((event.flags & flag::kViaSuccessorWalk) != 0U) {
                ++walks;
                walkReleases += event.payload1;
                barrenWalks += event.payload1 == 0U ? 1UZ : 0UZ;
                everyWalkNamedItsProducer = everyWalkNamedItsProducer && event.entity != kNoEntity;
            } else {
                ++backstops;
                backstopNamedABlock = backstopNamedABlock || event.entity != kNoEntity;
            }
        }

        expect(gt(backstops, 0UZ) >> fatal) << "the per-sweep backstop must be costed once per sweep";
        expect(gt(walks, 0UZ) >> fatal) << "and each successor walk costed where it happened";
        expect(gt(barrenWalks, 0UZ)) << "a walk that releases nothing must still be recorded -- otherwise the cost of event-driven detection is invisible and it looks free";
        expect(lt(walkReleases, walks)) << "if every walk released something, this graph is not exercising the unproductive path the ratio depends on";
        expect(everyWalkNamedItsProducer) << "a walk is attributable to the block whose output triggered it";
        expect(!backstopNamedABlock) << "the backstop scans every block, so it belongs to no single one";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        setCategories(0U);
        reset();
    };

    "a scan counts what it scanned, and what that produced"_test = [] {
        reset();
        setCategories(categoryMask(Category::release));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{2048U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 32UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        const std::vector<Event> scans    = ofKind(Kind::releaseScan);
        const std::size_t        releases = ofKind(Kind::jobRelease).size();
        expect(gt(scans.size(), 0UZ) >> fatal);

        std::size_t countedReleases = 0UZ;
        for (const Event& scan : scans) {
            expect(le(scan.payload1, scan.payload0)) << "a scan cannot release more blocks than it looked at";
            countedReleases += scan.payload1;
            if ((scan.flags & flag::kViaSuccessorWalk) == 0U) {
                expect(eq(scan.payload0, 3U)) << "the backstop scans every block on the worker, and this graph has three";
            }
        }
        expect(eq(countedReleases, releases)) << "every release must be attributed to exactly one scan -- a mismatch means a release happened on a path nothing is costing";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        setCategories(0U);
        reset();
    };

    "a job-backed execution is always preceded by the release that admitted it"_test = [] {
        // The ordering the whole release category rests on: under a job-driven policy a block runs
        // *because* a job was released for it, so every execution must have a release behind it. If
        // one did not, the block ran on a job nothing recorded, and every response time computed from
        // this capture would be attributed to the wrong release.
        //
        // This is also the scenario the documentation figure is rendered from, which is why it runs
        // with work, release and scheduler-loop markers together rather than release alone.
        reset();
        setCategories(categoryMask(Category::work, Category::release, Category::select, Category::schedulerLoop));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{4096U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);

        // A bounded batch, so the run is many modest invocations rather than two enormous ones. That
        // is both the regime the batching and RT thrusts target and the one where a release-to-
        // execution distance is a meaningful number rather than an artefact of a single huge call.
        expect(scheduler.settings().set({{"max_work_items", gr::Size_t{512U}}}).empty() >> fatal);
        std::ignore = scheduler.settings().activateContext();
        std::ignore = scheduler.settings().applyStagedParameters();

        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 24UZ; ++pass) {
            if (scheduler.step().status == gr::work::Status::DONE) {
                break;
            }
        }

        // Records come back oldest-first per ring, and this run is single-threaded, so one pass in
        // order is enough to check that a release precedes the execution it paid for.
        std::map<EntityId, std::size_t> outstanding;
        std::size_t                     jobBackedRuns = 0UZ;
        std::size_t                     unbacked      = 0UZ;
        for (const Event& event : collect()) {
            if (event.kind == Kind::jobRelease) {
                ++outstanding[event.entity];
            } else if (event.kind == Kind::workEnd && (event.flags & flag::kJobBacked) != 0U) {
                ++jobBackedRuns;
                if (outstanding[event.entity] == 0UZ) {
                    ++unbacked;
                } else {
                    --outstanding[event.entity];
                }
            }
        }

        expect(gt(jobBackedRuns, 0UZ) >> fatal) << "an EDF run must execute job-backed work, or this asserts nothing";
        expect(eq(unbacked, 0UZ)) << "a job-backed execution with no release before it means the trace cannot attribute response times";
        expect(eq(ofKind(Kind::select).size(), jobBackedRuns)) << "one selection authorises one job-backed call, so the counts must agree exactly";

        exportTimeline("t2-release-to-execution");

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        setCategories(0U);
        reset();
    };

    "a selection names the block that runs, and the ready set it came from"_test = [] {
        reset();
        setCategories(categoryMask(Category::work, Category::select));

        runEdfChain(4096U, [](auto& scheduler) {
            expect(scheduler.settings().set({{"max_work_items", gr::Size_t{512U}}}).empty() >> fatal);
            std::ignore = scheduler.settings().activateContext();
            std::ignore = scheduler.settings().applyStagedParameters();
        });

        // A selection authorises exactly one call, so the record must sit immediately before the
        // workBegin it authorised and must name the same block. Anything else means the oracle would
        // line a decision up against the wrong execution.
        // The ordinal counts selections *within a pass* and restarts at zero on the next one, since
        // the bound it is measured against is per pass. So the invariant is not that it rises
        // globally but that it advances by one or restarts -- a skipped value would mean a selection
        // happened that nothing recorded.
        std::size_t                     selections = 0UZ, matched = 0UZ;
        std::optional<gr::trace::Event> pendingSelect;
        std::uint32_t                   previousOrdinal   = 0U;
        bool                            ordinalContiguous = true;
        for (const Event& event : collect()) {
            if (event.kind == Kind::select) {
                ordinalContiguous = ordinalContiguous && (selections == 0UZ ? event.payload1 == 0U : (event.payload1 == previousOrdinal + 1U || event.payload1 == 0U));
                previousOrdinal   = event.payload1;
                ++selections;
                expect(ge(event.payload0, 1U)) << "a selection was made, so at least one block was ready";
                pendingSelect = event;
            } else if (event.kind == Kind::workBegin && pendingSelect.has_value()) {
                matched += pendingSelect->entity == event.entity ? 1UZ : 0UZ;
                pendingSelect.reset();
            }
        }

        expect(gt(selections, 0UZ) >> fatal) << "a job-driven run must record its selections";
        expect(eq(matched, selections)) << "every selection must be followed by the execution it authorised, on the same block";
        expect(ordinalContiguous) << "ordinals must run 0,1,2... within a pass and restart on the next -- a skipped value means a selection nothing recorded";

        // Contiguity alone is satisfied by an ordinal hard-coded to zero, since a restart is legal
        // anywhere. Requiring that some pass made a second selection is what gives it teeth.
        expect(gt(previousOrdinal + (selections > 0UZ ? 1U : 0U), 1U) or std::ranges::any_of(collect(), [](const Event& e) { return e.kind == Kind::select && e.payload1 > 0U; })) //
            << "no pass ever recorded a second selection, so the ordinal is not being counted at all";

        setCategories(0U);
        reset();
    };

    "the two selection strategies are distinguishable in the trace"_test = [] {
        // A report that mixed a heap run with a scan run would be comparing two different algorithms
        // as though they were one. The strategies must be told apart from the records alone.
        const auto pathFlagsSeenWith = [](gr::scheduler::SelectionStrategy strategy) {
            reset();
            setCategories(categoryMask(Category::select));
            runEdfChain(4096U, [strategy](auto& scheduler) {
                scheduler.selection_strategy = strategy; // the reflected form wants an enum *string*; the field is the direct route
                expect(scheduler.settings().set({{"max_work_items", gr::Size_t{512U}}}).empty() >> fatal);
                std::ignore = scheduler.settings().activateContext();
                std::ignore = scheduler.settings().applyStagedParameters();
            });
            std::size_t viaHeap = 0UZ, viaScan = 0UZ;
            for (const Event& event : ofKind(Kind::select)) {
                (((event.flags & flag::kViaHeap) != 0U) ? viaHeap : viaScan)++;
            }
            setCategories(0U);
            return std::pair{viaHeap, viaScan};
        };

        const auto [heapHeap, heapScan] = pathFlagsSeenWith(gr::scheduler::SelectionStrategy::readyHeap);
        expect(gt(heapHeap, 0UZ)) << "a readyHeap run must record selections made through the heap";
        expect(eq(heapScan, 0UZ)) << "and none through the scan";

        const auto [scanHeap, scanScan] = pathFlagsSeenWith(gr::scheduler::SelectionStrategy::linearScan);
        expect(gt(scanScan, 0UZ)) << "a linearScan run must record selections made by scanning";
        expect(eq(scanHeap, 0UZ)) << "and none through the heap";

        reset();
    };

    "a pass with nothing released records that it found nothing"_test = [] {
        // M3 section 20.7 item 6: a worker whose blocks are all waiting loops on readiness probes,
        // and round robin spins when idle too, so parity was argued rather than measured. This is the
        // measurement. A finished graph is the cleanest way to reach the state deterministically.
        reset();
        setCategories(categoryMask(Category::select));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{512U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

        for (std::size_t pass = 0UZ; pass < 16UZ; ++pass) {
            if (scheduler.step().status == gr::work::Status::DONE) {
                break;
            }
        }
        const std::size_t beforeIdling = ofKind(Kind::selectEmpty).size();
        for (std::size_t pass = 0UZ; pass < 4UZ; ++pass) {
            std::ignore = scheduler.step(); // every block finished: nothing can be selected
        }

        const std::vector<Event> empties = ofKind(Kind::selectEmpty);
        expect(gt(empties.size(), beforeIdling) >> fatal) << "a pass over an exhausted graph selects nothing, and must say so";
        expect(eq(empties.back().payload0, 3U)) << "the record carries how many blocks were looked at";
        expect(eq(empties.back().payload1, 0U)) << "and that no selection was made in the pass that found nothing";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        setCategories(0U);
        reset();
    };

    "the selection category is independent of the work category"_test = [] {
        // select doubles the per-invocation record count, which is why it is its own category and off
        // by default. That separation is only real if enabling one does not drag in the other.
        reset();
        setCategories(categoryMask(Category::select));
        runEdfChain(2048U, [](auto&) {});

        expect(gt(ofKind(Kind::select).size(), 0UZ) >> fatal) << "selections must be recorded when their category is live";
        expect(eq(ofKind(Kind::workBegin).size(), 0UZ)) << "and the work category, being off, must contribute nothing";
        expect(eq(ofKind(Kind::jobRelease).size(), 0UZ)) << "nor the release category";

        setCategories(0U);
        reset();
    };

    "hitting the per-pass selection bound is recorded, with the bound that bound"_test = [] {
        // RT section 8.3 lists the default multiplier of four as an unprofiled default. Whether it
        // ever binds could not be asked of a running graph before this record existed.
        reset();
        setCategories(categoryMask(Category::select));

        constexpr gr::Size_t kTinyBound = 2U;
        runEdfChain(8192U, [kTinyBound](auto& scheduler) {
            expect(scheduler.settings().set({{"max_selections_per_pass", kTinyBound}, {"max_work_items", gr::Size_t{256U}}}).empty() >> fatal);
            std::ignore = scheduler.settings().activateContext();
            std::ignore = scheduler.settings().applyStagedParameters();
        });

        const std::vector<Event> hits = ofKind(Kind::selectionBoundHit);
        expect(gt(hits.size(), 0UZ) >> fatal) << "a bound of two against a graph with more work than that must bind";
        expect(eq(hits.front().payload0, static_cast<std::uint32_t>(kTinyBound))) << "the record carries the bound that bound, not the default";
        expect(eq(hits.front().payload1, 3U)) << "and how many blocks the pass was choosing among";

        setCategories(0U);
        reset();
    };

    "the heap selection loop is bounded too, and says so"_test = [] {
        // The bound exists on all three selection loops, and a mutation deleting it from the heap
        // path survived every scenario that drove the default linear scan. Strategy is a setting, so
        // a marker tested through only one of its values is a marker half tested.
        reset();
        setCategories(categoryMask(Category::select));

        constexpr gr::Size_t kTinyBound = 2U;
        runEdfChain(8192U, [kTinyBound](auto& scheduler) {
            scheduler.selection_strategy = gr::scheduler::SelectionStrategy::readyHeap;
            expect(scheduler.settings().set({{"max_selections_per_pass", kTinyBound}, {"max_work_items", gr::Size_t{256U}}}).empty() >> fatal);
            std::ignore = scheduler.settings().activateContext();
            std::ignore = scheduler.settings().applyStagedParameters();
        });

        const auto viaHeap = std::ranges::count_if(collect(), [](const Event& e) { return e.kind == Kind::select && (e.flags & flag::kViaHeap) != 0U; });
        expect(gt(viaHeap, 0) >> fatal) << "the heap path must be the one running, or this tests the scan again";

        const std::vector<Event> hits = ofKind(Kind::selectionBoundHit);
        expect(gt(hits.size(), 0UZ) >> fatal) << "the heap loop stops at the same bound and must record it";
        expect(eq(hits.front().payload0, static_cast<std::uint32_t>(kTinyBound)));

        setCategories(0U);
        reset();
    };

    "a generous bound does not bind, so the marker is not merely always-on"_test = [] {
        reset();
        setCategories(categoryMask(Category::select));

        runEdfChain(2048U, [](auto& scheduler) {
            expect(scheduler.settings().set({{"max_selections_per_pass", gr::Size_t{4096U}}}).empty() >> fatal);
            std::ignore = scheduler.settings().activateContext();
            std::ignore = scheduler.settings().applyStagedParameters();
        });

        expect(eq(ofKind(Kind::selectionBoundHit).size(), 0UZ)) << "a bound nothing reaches must produce no records, or the marker says nothing when it fires";
        expect(gt(ofKind(Kind::select).size(), 0UZ)) << "and the run must still have selected, so this is not vacuous";

        setCategories(0U);
        reset();
    };

    "the bound is recorded under fixed priority too, where no selection records accompany it"_test = [] {
        // The distinctive claim of the design note: this marker is gated on selecting by priority,
        // not on release tracking, because the fixed-priority loop's strict restart makes hitting the
        // bound *more* likely, not less. A reader seeing bound hits with no `select` beside them is
        // looking at a fixed-priority run, and the report has to say so rather than call it a gap.
        reset();
        setCategories(categoryMask(Category::select));

        runChain<gr::scheduler::FixedPriorityPolicy>(8192U, [](auto& scheduler) {
            expect(scheduler.settings().set({{"max_selections_per_pass", gr::Size_t{2U}}, {"max_work_items", gr::Size_t{256U}}}).empty() >> fatal);
            std::ignore = scheduler.settings().activateContext();
            std::ignore = scheduler.settings().applyStagedParameters();
        });

        expect(gt(ofKind(Kind::selectionBoundHit).size(), 0UZ) >> fatal) << "the fixed-priority loop is bounded too, and its restart makes the bound easier to reach";
        expect(eq(ofKind(Kind::select).size(), 0UZ)) << "that loop makes no job-driven selections, so a bound hit stands alone";
        expect(eq(ofKind(Kind::jobRelease).size(), 0UZ)) << "and fixed priority has no release tracking at all";

        setCategories(0U);
        reset();
    };

    "a sized ready-heap never silently reverts to the linear scan"_test = [] {
        // RT defect B3: requesting the ready heap reverts to the scan when the scratch is too small,
        // and says nothing. The marker exists to make that loud. Measured here rather than assumed:
        // on this path it **never fires**, because `buildReleaseStorage` sizes the scratch to the
        // block count on every rebuild, and the pool worker rebuilds on the same house-keeping pass
        // that adoption grows the list on. See the design note for what that means for B3.
        //
        // The assertion is therefore "must not fire", which is a real claim in both directions: a
        // marker that fired spuriously would be a false alarm about a defect, and that is what this
        // catches. Nothing here can catch a marker that never fires, because no reachable state
        // makes it fire -- stated rather than papered over.
        reset();
        setCategories(categoryMask(Category::select));

        runEdfChain(8192U, [](auto& scheduler) {
            scheduler.selection_strategy = gr::scheduler::SelectionStrategy::readyHeap;
            expect(scheduler.settings().set({{"max_work_items", gr::Size_t{256U}}}).empty() >> fatal);
            std::ignore = scheduler.settings().activateContext();
            std::ignore = scheduler.settings().applyStagedParameters();
        });

        const auto heapSelections = std::ranges::count_if(collect(), [](const Event& e) { return e.kind == Kind::select && (e.flags & flag::kViaHeap) != 0U; });
        expect(gt(heapSelections, 0) >> fatal) << "the heap must actually have been used, or asserting that it did not fall back proves nothing";
        expect(eq(ofKind(Kind::heapFallback).size(), 0UZ)) << "the scratch is sized to the block count on every rebuild, so a request for the heap must be honoured";

        setCategories(0U);
        reset();
    };

    "a full queue records the drop, and does not record a release"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        reset();
        setCategories(categoryMask(Category::release));

        constexpr std::size_t kCapacity = 2UZ;
        StateFixture          fixture{kCapacity};
        fixture.state.batchCeiling = 1UZ;

        for (std::size_t attempt = 0UZ; attempt < kCapacity + 2UZ; ++attempt) {
            releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        }

        const std::vector<Event> released = ofKind(Kind::jobRelease);
        const std::vector<Event> dropped  = ofKind(Kind::jobReleaseDropped);
        expect(eq(released.size(), kCapacity)) << "a full ring admits nothing further";
        expect(eq(dropped.size(), 2UZ) >> fatal) << "and every refusal is recorded -- this is the only place the max_outstanding_jobs clamp becomes visible";
        expect(eq(dropped.front().payload1, static_cast<std::uint32_t>(kCapacity))) << "the drop carries the true capacity, which is what disambiguates a saturated packed depth";
        expect(eq(dropped.back().payload2, 2U)) << "and the running total, so backlog is readable without differencing";

        setCategories(0U);
        reset();
    };

    "the queue depth saturates rather than wrapping to empty"_test = [] {
        Chain chain;
        chain.prime(256UZ);
        activate(*chain.mid);

        reset();
        setCategories(categoryMask(Category::release));

        // Six bits hold 0..63 while `max_outstanding_jobs` defaults to 64, so the one value that
        // does not fit is a completely full queue. Masking would report it as empty.
        constexpr std::size_t kCapacity = 70UZ;
        StateFixture          fixture{kCapacity};
        fixture.state.batchCeiling = 1UZ;

        for (std::size_t attempt = 0UZ; attempt < 66UZ; ++attempt) {
            releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        }

        const std::vector<Event> records = ofKind(Kind::jobRelease);
        expect(ge(records.size(), 65UZ) >> fatal) << "the scenario needs to drive the depth past the packed width";
        expect(eq(packedDepthOf(records[0]), 1U)) << "the first release leaves one job outstanding";
        expect(eq(packedDepthOf(records[62]), 63U)) << "the last depth the field can express";
        expect(eq(packedDepthOf(records[63]), 63U)) << "and past it the value saturates";
        expect(eq(packedDepthOf(records.back()), 63U)) << "a wrapped depth would read as 0 and report a full queue as an empty one";

        setCategories(0U);
        reset();
    };

    "round robin emits no release markers at all, with every category live"_test = [] {
        // The neutrality gate. A marker that escaped its `if constexpr` is still gated on the runtime
        // mask -- and the mask is fully open here -- so it would emit, and this fails. A symbol-level
        // check cannot do this: `Kind::jobRelease` is an inlined enumerator and never appears in an
        // object file whether the marker compiled or not.
        reset();
        setCategories(kAllCategories);

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{4096U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded> scheduler; // RoundRobinPolicy by default
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.runAndWait().has_value() >> fatal);

        expect(gt(collect().size(), 0UZ) >> fatal) << "the run must have traced something, or this asserts nothing";
        expect(eq(ofKind(Kind::jobRelease).size(), 0UZ)) << "round robin has no release tracking to record";
        expect(eq(ofKind(Kind::jobReleaseDropped).size(), 0UZ));
        expect(eq(ofKind(Kind::releaseScan).size(), 0UZ)) << "and no detection pass to pay for";

        setCategories(0U);
        reset();
    };
};

int main() { /* tests are statically registered as suites */ }
