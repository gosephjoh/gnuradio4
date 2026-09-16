#include <boost/ut.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <set>
#include <span>
#include <string>
#include <tuple>
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

        setCategories(0U);
        reset();
    };
};

int main() { /* tests are statically registered as suites */ }
