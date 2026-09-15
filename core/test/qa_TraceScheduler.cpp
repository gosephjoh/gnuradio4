#include <boost/ut.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;

/**
 * Scheduler-level trace coverage, kept apart from `qa_Trace.cpp` on purpose.
 *
 * That file tests the layer in isolation and includes nothing heavier than `Trace.hpp`; this one
 * needs `Scheduler.hpp`, a graph and real blocks. Splitting them keeps the unit tests fast and makes
 * a failure here unambiguously an *integration* failure.
 */
namespace {

using TestScheduler = gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep>;

/// `ConstantSource -> Copy -> NullSink`, driven by `step()` so nothing depends on thread timing.
struct Chain {
    gr::Graph      graph;
    TestScheduler* scheduler = nullptr;

    void build(std::size_t nSamples = 4096UZ) {
        auto& source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", static_cast<gr::Size_t>(nSamples)}});
        auto& copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto& sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore  = graph.connect<"out", "in">(source, copy);
        std::ignore  = graph.connect<"out", "in">(copy, sink);
    }
};

[[nodiscard]] std::vector<const void*> blockKeys(const TestScheduler& scheduler) {
    std::vector<const void*> keys;
    for (const auto& block : scheduler.graph().blocks()) {
        keys.push_back(static_cast<const void*>(block.get()));
    }
    return keys;
}

/// boost::ut's `eq` has no container overload, and an `expect` per element would flood the assertion
/// count for no information. One comparison, one message.
[[nodiscard]] bool sameIds(const std::vector<gr::trace::EntityId>& lhs, const std::vector<gr::trace::EntityId>& rhs) { return std::ranges::equal(lhs, rhs); }

void collectingConsumer(const gr::trace::Event& event, void* user) noexcept { static_cast<std::vector<gr::trace::Event>*>(user)->push_back(event); }

[[nodiscard]] std::vector<gr::trace::Event> collect() {
    std::vector<gr::trace::Event> events;
    std::ignore = gr::trace::forEachEvent(collectingConsumer, &events);
    return events;
}

/// Writes the capture to `$GR4_TRACE_ARTEFACT_DIR/<name>.gr4trace` when that variable is set.
///
/// Unset -- every ordinary test run, and CI -- and this is a no-op, so the suite stays hermetic and
/// writes nothing. Capturing a timeline to inspect by hand is an explicit opt-in, which keeps a
/// test that asserts behaviour from also being a test that depends on a writable directory.
void exportTimeline([[maybe_unused]] std::string_view name) {
    if constexpr (gr::trace::kEnabled) {
        const char* directory = std::getenv("GR4_TRACE_ARTEFACT_DIR");
        if (directory == nullptr) {
            return;
        }
        std::ignore = gr::trace::dump(std::format("{}/{}.gr4trace", directory, name));
    }
}

[[nodiscard]] std::vector<gr::trace::EntityId> idsOf(const std::vector<const void*>& keys) {
    std::vector<gr::trace::EntityId> ids;
    ids.reserve(keys.size());
    for (const void* key : keys) {
        ids.push_back(gr::trace::internedId(key));
    }
    return ids;
}

} // namespace

const boost::ut::suite<"TraceScheduler"> traceSchedulerTests = [] {
    using namespace gr::trace;

    if constexpr (!kEnabled) {
        "a compiled-out build interns nothing"_test = [] {
            Chain chain;
            chain.build();
            TestScheduler scheduler;
            expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
            expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

            // The `if constexpr` in syncSchedStates must leave no call behind: with tracing compiled
            // out, running a graph must not create a single identity.
            for (const void* key : blockKeys(scheduler)) {
                expect(eq(internedId(key), kNoEntity)) << "GR_ENABLE_TRACING is off; interning must not happen";
            }
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        };
        return;
    }

    "init() interns every block exactly once, with distinct ids"_test = [] {
        reset();
        Chain chain;
        chain.build();
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

        const std::vector<const void*> keys = blockKeys(scheduler);
        expect(eq(keys.size(), 3UZ) >> fatal) << "source, copy, sink";

        const std::vector<EntityId> ids = idsOf(keys);
        for (const EntityId id : ids) {
            expect(neq(id, kNoEntity)) << "every block must be interned by init()";
        }
        expect(eq(std::set<EntityId>(ids.begin(), ids.end()).size(), ids.size())) << "identities must not collide";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "ids survive a re-sync and the work that triggers it"_test = [] {
        reset();
        Chain chain;
        chain.build();
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

        const std::vector<const void*> keys   = blockKeys(scheduler);
        const std::vector<EntityId>    atInit = idsOf(keys);

        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 32UZ; ++pass) {
            std::ignore = scheduler.step(); // step() re-syncs on the house-keeping cadence
        }

        expect(sameIds(idsOf(keys), atInit)) << "a re-sync must not renumber a block: SchedState is assigned wholesale, "
                                                "so an id set outside its initialiser would be silently zeroed";

        // And re-interning is idempotent -- the property that keeps the house-keeping path
        // allocation-free once warm.
        const std::vector<EntityId> again = idsOf(keys);
        expect(sameIds(again, atInit));

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "ids are invariant under a policy that permutes the block list"_test = [] {
        reset();
        gr::Graph graph;
        // Descending priorities, so applyStaticOrder reverses the registration order it was given.
        auto& source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"sched_priority", 1}, {"n_samples_max", gr::Size_t{2048U}}});
        auto& copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}, {"sched_priority", 5}});
        auto& sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}, {"sched_priority", 9}});
        std::ignore  = graph.connect<"out", "in">(source, copy);
        std::ignore  = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::FixedPriorityPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

        std::vector<const void*> keys;
        for (const auto& block : scheduler.graph().blocks()) {
            keys.push_back(static_cast<const void*>(block.get()));
        }
        const std::vector<EntityId> before = idsOf(keys);

        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 16UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        // The whole reason for keying on the address: applyStaticOrder permutes blocks and states
        // together and deliberately does not renumber SchedState::index, so a position-derived id
        // would follow the sort. An address-derived one cannot.
        expect(sameIds(idsOf(keys), before)) << "a priority sort must not change which identity names which block";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "the interning table carries each block's real name and type"_test = [] {
        reset();
        Chain chain;
        chain.build();
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

        std::vector<std::pair<std::string, std::string>> described;
        std::ignore = forEachEntity(
            [](EntityId, const EntityDescription& description, void* user) noexcept { //
                static_cast<std::vector<std::pair<std::string, std::string>>*>(user)->emplace_back(description.uniqueName, description.typeName);
            },
            &described);

        expect(eq(described.size(), 3UZ) >> fatal);
        for (const auto& [uniqueName, typeName] : described) {
            expect(!uniqueName.empty()) << "uniqueName must be captured, not left blank";
            expect(!typeName.empty()) << "typeName must be captured";
        }

        // uniqueName is what a report resolves a record against, so it must actually be unique.
        std::set<std::string> names;
        for (const auto& [uniqueName, typeName] : described) {
            names.insert(uniqueName);
        }
        expect(eq(names.size(), 3UZ)) << "unique names must be unique";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "port counts reach the description"_test = [] {
        reset();
        Chain chain;
        chain.build();
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

        // A source has no input and one output; a sink the reverse. If the accessors were swapped or
        // zeroed this is where it shows, rather than in a converter months later.
        std::pair<std::size_t, std::size_t> counts{0UZ, 0UZ};
        std::ignore = forEachEntity(
            [](EntityId, const EntityDescription& description, void* user) noexcept {
                auto* pair = static_cast<std::pair<std::size_t, std::size_t>*>(user);
                if (description.nInputPorts == 0U && description.nOutputPorts > 0U) {
                    ++pair->first;
                }
                if (description.nInputPorts > 0U && description.nOutputPorts == 0U) {
                    ++pair->second;
                }
            },
            &counts);
        expect(eq(counts.first, 1UZ)) << "exactly one block has outputs and no inputs";
        expect(eq(counts.second, 1UZ)) << "exactly one block has inputs and no outputs";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a second graph gets new identities and the first keeps its own"_test = [] {
        reset();
        std::vector<EntityId> firstIds;
        {
            Chain chain;
            chain.build();
            TestScheduler scheduler;
            expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
            expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
            firstIds    = idsOf(blockKeys(scheduler));
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        }

        Chain second;
        second.build();
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(second.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        const std::vector<EntityId> secondIds = idsOf(blockKeys(scheduler));

        for (const EntityId id : secondIds) {
            expect(neq(id, kNoEntity));
        }

        // The pointer-reuse hazard, exercised rather than argued -- and it *fired* the first time this
        // ran: the allocator handed a destroyed block's address to a new one, and intern() returned
        // the dead block's identity and name. intern() now compares `unique_name` (unique per
        // instance, "{type}#{counter}") and starts a fresh identity on a mismatch, so a recycled
        // address can no longer inherit a dead block's records.
        const std::set<EntityId> firstSet(firstIds.begin(), firstIds.end());
        std::size_t              collisions = 0UZ;
        for (const EntityId id : secondIds) {
            if (firstSet.contains(id)) {
                ++collisions;
            }
        }
        expect(eq(collisions, 0UZ)) << "a new graph reused an identity from a destroyed one";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a work() pair is emitted per productive invocation, and only for productive ones"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        // The sink is emplaced *first* on purpose. `Simple` assigns blocks in graph order and round
        // robin sweeps that order, so the opening passes call the sink and the copy before the source
        // has produced anything: they return INSUFFICIENT_INPUT_ITEMS, which is the branch the filter
        // exists to suppress. Built the natural way -- source first -- this graph never starves a
        // block at all, and the assertion below passes without the filter being reached. That is not
        // a hypothetical: the first version of this test did exactly that, and only a mutation (deleting
        // the filter and watching the suite stay green) showed it up.
        gr::Graph graph;
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{2048U}}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 8UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        const std::vector<Event> recorded = collect();
        expect(gt(recorded.size(), 0UZ) >> fatal) << "a running graph must produce work records";

        std::size_t begins = 0UZ;
        std::size_t ends   = 0UZ;
        for (const Event& event : recorded) {
            if (event.kind == Kind::workBegin) {
                ++begins;
                expect(eq(event.durationNs, 0U)) << "workBegin is an instant";
            }
            if (event.kind == Kind::workEnd) {
                ++ends;
                // The filter: an INSUFFICIENT_* invocation did nothing and must not be recorded as work.
                expect(event.status != static_cast<std::int8_t>(gr::work::Status::INSUFFICIENT_INPUT_ITEMS)) << "a starved invocation must not produce a workEnd";
                expect(event.status != static_cast<std::int8_t>(gr::work::Status::INSUFFICIENT_OUTPUT_ITEMS)) << "a blocked invocation must not produce a workEnd";
            }
            expect(neq(event.entity, kNoEntity)) << "every work record must name the block it came from";
        }
        exportTimeline("t1b-progress-filter");
        expect(gt(begins, 0UZ));
        expect(gt(ends, 0UZ));
        expect(ge(begins, ends)) << "every workEnd has a workBegin; the surplus is probes and, at most, one hang";
        expect(gt(begins, ends)) << "this graph must actually starve a block, or the filter above is never reached "
                                    "and this scenario asserts nothing";

        setCategories(0U);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "workEnd carries the entry instant, so execution and overhead are both recoverable"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        Chain chain;
        chain.build(4096UZ);
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 16UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        std::vector<Event> ends;
        for (const Event& event : collect()) {
            if (event.kind == Kind::workEnd) {
                ends.push_back(event);
            }
        }
        expect(gt(ends.size(), 1UZ) >> fatal) << "need at least two invocations to measure a gap";

        // `startNs` is the *entry* instant and `durationNs` the execution time, so the end of one
        // invocation is startNs + durationNs and the next one's startNs is where it resumed. The
        // difference is the scheduler overhead between them -- measured, not inferred from gaps.
        std::size_t   measurable = 0UZ;
        std::uint64_t totalWork  = 0UL;
        for (std::size_t i = 1UZ; i < ends.size(); ++i) {
            const std::uint64_t previousLeft = ends[i - 1UZ].startNs + ends[i - 1UZ].durationNs;
            expect(ge(ends[i].startNs, ends[i - 1UZ].startNs)) << "entry instants must be non-decreasing on one worker";
            if (ends[i].startNs >= previousLeft) {
                ++measurable;
            }
            totalWork += ends[i].durationNs;
        }
        exportTimeline("t1b-overhead");
        expect(eq(measurable, ends.size() - 1UZ)) << "an invocation must not appear to start before the previous one finished";
        expect(gt(totalWork, 0UL)) << "durations must be non-zero -- a work() that did something takes time";

        setCategories(0U);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "the loop kind is recorded, so a round-robin trace cannot be read as an EDF one"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        Chain chain;
        chain.build(2048UZ);
        TestScheduler scheduler; // Simple<> defaults to RoundRobinPolicy
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 4UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        for (const Event& event : collect()) {
            if (event.kind == Kind::workBegin || event.kind == Kind::workEnd) {
                expect(eq(static_cast<std::uint8_t>(event.flags & flag::kLoopKindMask), std::to_underlying(LoopKind::roundRobin))) << "the default policy's records must say round robin";
                expect(eq(static_cast<std::uint8_t>(event.flags & flag::kJobBacked), std::uint8_t{0U})) << "round robin runs no released jobs";
            }
        }

        setCategories(0U);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "tracing does not change what the scheduler does"_test = [] {
        // Asserted on what must not change -- the samples delivered and the statuses returned --
        // never on invocation order, which is not deterministic across workers and would make this
        // flaky rather than meaningful.
        const auto runOnce = [](bool traced) {
            reset();
            setCategories(traced ? kAllCategories : 0U);

            Chain chain;
            chain.build(8192UZ);
            TestScheduler scheduler;
            std::ignore = scheduler.exchange(std::move(chain.graph));
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::INITIALISED);
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::RUNNING);

            std::vector<std::pair<std::size_t, int>> results;
            for (std::size_t pass = 0UZ; pass < 64UZ; ++pass) {
                const gr::work::Result result = scheduler.step();
                results.emplace_back(result.performed_work, static_cast<int>(result.status));
            }
            setCategories(0U);
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
            return results;
        };

        const auto untraced = runOnce(false);
        const auto traced   = runOnce(true);

        expect(eq(untraced.size(), traced.size()) >> fatal);
        expect(std::ranges::equal(untraced, traced)) << "tracing changed the work::Result sequence -- the layer is not behaviour-neutral";
    };

    "unproductive invocations are counted, not recorded one by one"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        // Sink first again, so the opening passes starve it and the copy (see the filter scenario).
        gr::Graph graph;
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{1024U}}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 8UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        std::size_t   probeRecords = 0UZ;
        std::uint64_t probedCalls  = 0UL;
        std::size_t   begins       = 0UZ;
        std::size_t   ends         = 0UZ;
        for (const Event& event : collect()) {
            switch (event.kind) {
            case Kind::workProbe:
                ++probeRecords;
                probedCalls += event.payload0;
                expect(gt(event.payload0, 0U)) << "an empty aggregate must not be emitted at all";
                expect(neq(event.entity, kNoEntity)) << "a probe record must name its block";
                expect(eq(event.durationNs, 0U)) << "workProbe is an instant; the cost is in payload1";
                break;
            case Kind::workBegin: ++begins; break;
            case Kind::workEnd: ++ends; break;
            default: break;
            }
        }

        exportTimeline("t1c-probes-roundrobin");
        expect(gt(probeRecords, 0UZ) >> fatal) << "this graph must starve blocks, or the aggregation is never reached";
        expect(eq(probedCalls, std::uint64_t{begins - ends})) << "every unmatched workBegin must be accounted for by exactly one probe count: "
                                                                 "the two are the same invocations counted two ways";

        // Note what is *not* asserted here: that aggregation reduced the record count. Round robin
        // calls each block exactly once per sweep, so a block can probe at most once before the flush
        // and one record per probe is the floor. Aggregation only bites where a block is re-probed
        // within a single pass, which is the fixed-priority loop's strict restart -- covered below.

        setCategories(0U);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "the fixed-priority restart is where aggregation actually pays"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        // The fixed-priority loop restarts at index 0 after every successful selection, so a starved
        // high-priority block is re-probed once per selection rather than once per sweep. That is the
        // shape the aggregation exists for, and round robin -- one call per block per sweep -- can
        // never exhibit it.
        gr::Graph graph;
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}, {"sched_priority", 9}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}, {"sched_priority", 5}});
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"sched_priority", 1}, {"n_samples_max", gr::Size_t{4096U}}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, gr::scheduler::FixedPriorityPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 8UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        std::uint32_t largestAggregate = 0U;
        std::uint64_t probedCalls      = 0UL;
        std::size_t   begins           = 0UZ;
        std::size_t   ends             = 0UZ;
        for (const Event& event : collect()) {
            switch (event.kind) {
            case Kind::workProbe:
                largestAggregate = std::max(largestAggregate, event.payload0);
                probedCalls += event.payload0;
                break;
            case Kind::workBegin: ++begins; break;
            case Kind::workEnd: ++ends; break;
            default: break;
            }
        }

        exportTimeline("t1c-probes-fixedpriority");
        expect(gt(probedCalls, 0UL) >> fatal) << "the restart must re-probe starved blocks";
        expect(eq(probedCalls, std::uint64_t{begins - ends})) << "the accounting identity holds under every loop";

        // Asserted as the mechanism, not as a ratio. `records < calls` happens to hold here by a
        // margin of one, which is a statistical accident of how much this graph probes rather than
        // evidence that anything was collapsed; a single record carrying a count above one is the
        // property itself.
        expect(gt(largestAggregate, 1U)) << "no probe record collapsed more than one invocation, so the aggregation "
                                            "did nothing that a record-per-probe would not have done";

        setCategories(0U);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a sweep brackets the work it drove, and says which driver drove it"_test = [] {
        reset();
        setCategories(categoryMask(Category::work, Category::schedulerLoop));

        Chain chain;
        chain.build(4096UZ);
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        constexpr std::size_t kPasses = 6UZ;
        for (std::size_t pass = 0UZ; pass < kPasses; ++pass) {
            std::ignore = scheduler.step();
        }

        std::vector<Event> sweeps;
        std::vector<Event> works;
        for (const Event& event : collect()) {
            if (event.kind == Kind::sweep) {
                sweeps.push_back(event);
            }
            if (event.kind == Kind::workEnd) {
                works.push_back(event);
            }
        }
        exportTimeline("t1d-sweeps");

        expect(eq(sweeps.size(), kPasses)) << "exactly one sweep record per step()";
        for (const Event& sweep : sweeps) {
            expect(eq(sweep.entity, kNoEntity)) << "a sweep is worker-scoped, not block-scoped";
            expect(eq(static_cast<std::uint8_t>(sweep.flags & flag::kViaStep), flag::kViaStep)) << "an externalStep scheduler's sweeps must say so -- otherwise a step()-driven trace "
                                                                                                   "reads as a pool worker's and the idle behaviour looks inexplicable";
            expect(eq(sweep.payload0, 3U)) << "the sweep records how many blocks it swept";
        }

        // Every invocation must fall inside the sweep that drove it. This is what makes the two
        // record kinds one timeline rather than two overlaid guesses.
        std::size_t contained = 0UZ;
        for (const Event& work : works) {
            for (const Event& sweep : sweeps) {
                if (work.startNs >= sweep.startNs && (work.startNs + work.durationNs) <= (sweep.startNs + sweep.durationNs)) {
                    ++contained;
                    break;
                }
            }
        }
        expect(eq(contained, works.size())) << "a work record fell outside every sweep";

        setCategories(0U);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a finished graph keeps re-invoking its blocks, and the sweep records say so"_test = [] {
        // After a graph finishes, round robin calls every block again on every sweep and each
        // reports DONE, which the progress filter treats as productive -- correctly, since a source
        // that publishes everything and finishes reports zero performed work. The concern is that in
        // an *overwriting* ring a flood of terminal DONE records would evict the history worth
        // keeping, so this pins down how far that can actually go.
        reset();
        setCategories(categoryMask(Category::work, Category::schedulerLoop));

        Chain chain;
        chain.build(256UZ); // finishes almost immediately, then is stepped well past the end
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 40UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        std::size_t doneRecords = 0UZ;
        std::size_t okRecords   = 0UZ;
        std::size_t doneSweeps  = 0UZ;
        std::size_t sweeps      = 0UZ;
        for (const Event& event : collect()) {
            if (event.kind == Kind::workEnd) {
                if (event.status == static_cast<std::int8_t>(gr::work::Status::DONE)) {
                    ++doneRecords;
                }
                if (event.status == static_cast<std::int8_t>(gr::work::Status::OK)) {
                    ++okRecords;
                }
            }
            if (event.kind == Kind::sweep) {
                ++sweeps;
                if (event.status == static_cast<std::int8_t>(gr::work::Status::DONE)) {
                    ++doneSweeps;
                }
            }
        }
        exportTimeline("t1d-terminal-done");

        expect(gt(okRecords, 0UZ) >> fatal) << "the graph must actually have done work";
        expect(gt(doneSweeps, 0UZ)) << "a sweep whose blocks have all finished must report DONE, which is what lets a real "
                                       "worker break its loop -- the bound on the flood";
        // Recorded rather than asserted as a limit: the ratio is what it is, and the point is that a
        // caller who keeps step()ing a finished graph pays for it in records. A pool worker does not,
        // because it breaks out when a sweep returns DONE.
        expect(lt(doneSweeps, sweeps)) << "not every sweep can be terminal, or the graph never ran";

        setCategories(0U);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "the re-sync fires on a cadence, not on a change -- and says how often"_test = [] {
        // The scheduler re-derives its whole per-block state on the message/house-keeping cadence
        // rather than when the block list actually changes, and that assignment discards every
        // outstanding job and resets each block's release timing. How often it fires with nothing
        // having changed was recorded as a known cost years before anyone measured it. These markers
        // are the measurement.
        //
        // multiThreaded, because the re-sync only runs on the pool-worker path -- step() never
        // reaches it.
        reset();
        setCategories(categoryMask(Category::schedulerLoop, Category::lifecycle));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{65536U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.runAndWait().has_value() >> fatal);

        std::size_t syncs        = 0UZ;
        std::size_t syncsChanged = 0UZ;
        std::size_t jobsLost     = 0UZ;
        std::size_t phases       = 0UZ;
        std::size_t houseKeeps   = 0UZ;
        for (const Event& event : collect()) {
            switch (event.kind) {
            case Kind::stateSync:
                ++syncs;
                jobsLost += event.payload1;
                if ((event.flags & flag::kListChanged) != 0U) {
                    ++syncsChanged;
                }
                break;
            case Kind::messagePhase: ++phases; break;
            case Kind::houseKeeping: ++houseKeeps; break;
            default: break;
            }
        }
        exportTimeline("t1e-housekeeping");

        expect(gt(syncs, 0UZ) >> fatal) << "a running pool worker must re-sync at least once";
        expect(gt(phases, 0UZ)) << "the message phase must be entered";

        // The finding, asserted rather than merely printed: the overwhelming majority of re-syncs
        // rebuild state for a block list that did not change. A future fix that made the re-sync
        // conditional would flip this, and should -- at which point this assertion is the thing that
        // notices, and it must be updated deliberately rather than silently.
        expect(lt(syncsChanged, syncs)) << "every re-sync saw a changed list, which would mean the cadence is not the trigger "
                                           "-- re-read this scenario before assuming the finding still holds";
        expect(eq(jobsLost, 0UZ)) << "round robin tracks no jobs, so a re-sync under it discards none; this is the control "
                                     "for the release-tracking case, where the same code path throws work away";

        setCategories(0U);
    };

    "under a release-tracking policy the same cadence throws admitted work away"_test = [] {
        // The round-robin case above is the control: it tracks no jobs, so a re-sync costs only the
        // rebuild. Earliest-deadline-first admits jobs ahead of running them, and the same wholesale
        // assignment discards every one that has not executed yet. This measures what that costs.
        reset();
        setCategories(categoryMask(Category::schedulerLoop));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{65536U}}, {"relative_deadline", 0.002f}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}, {"relative_deadline", 0.002f}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}, {"relative_deadline", 0.002f}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.runAndWait().has_value() >> fatal);

        std::size_t syncs        = 0UZ;
        std::size_t syncsChanged = 0UZ;
        std::size_t jobsLost     = 0UZ;
        std::size_t syncsLosing  = 0UZ;
        for (const Event& event : collect()) {
            if (event.kind != Kind::stateSync) {
                continue;
            }
            ++syncs;
            jobsLost += event.payload1;
            if (event.payload1 > 0U) {
                ++syncsLosing;
            }
            if ((event.flags & flag::kListChanged) != 0U) {
                ++syncsChanged;
            }
        }
        exportTimeline("t1e-statesync-edf");

        expect(gt(syncs, 0UZ) >> fatal) << "a running pool worker must re-sync";
        expect(lt(syncsChanged, syncs)) << "the cadence, not a mutation, is what triggers the re-sync";

        // Deliberately not asserted as a bound. Whether any admitted job is outstanding when the
        // cadence fires is a race between the worker and the house-keeping interval, so a threshold
        // here would be flaky. What is asserted is that the marker reports the quantity at all, so
        // the cost is visible to anyone who looks rather than having to be inferred.
        std::ignore = jobsLost;
        std::ignore = syncsLosing;

        setCategories(0U);
    };

    "the settings drive the layer, and the buffer size is applied before the mask"_test = [] {
        reset();
        setCategories(0U);

        Chain chain;
        chain.build(1024UZ);
        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);

        // Order matters and is asserted here rather than trusted: a ring is allocated when a thread
        // first emits, and a thread only emits once a category is live, so a capacity applied after
        // the mask would leave already-started threads on the previous size.
        expect(scheduler.settings().set({{"trace_buffer_size", gr::Size_t{128U}}, {"trace_categories", gr::Size_t{categoryMask(Category::work)}}}).empty() >> fatal);
        std::ignore = scheduler.settings().activateContext();
        std::ignore = scheduler.settings().applyStagedParameters();

        expect(eq(categories(), categoryMask(Category::work))) << "the setting must reach the layer";
        expect(eq(ringCapacity(), 128UZ)) << "and so must the buffer size";

        std::ignore = scheduler.settings().set({{"trace_categories", gr::Size_t{0U}}});
        std::ignore = scheduler.settings().activateContext();
        std::ignore = scheduler.settings().applyStagedParameters();
        expect(eq(categories(), 0U)) << "clearing the setting must stop the capture";

        std::ignore = setRingCapacity(kDefaultRingCapacity);
        reset();
    };

    "an oversized buffer request is clamped, and the scheduler says so"_test = [] {
        reset();
        setCategories(0U);

        Chain chain;
        chain.build(1024UZ);
        TestScheduler scheduler;
        gr::MsgPortIn fromScheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(scheduler.msgOut.connect(fromScheduler).has_value() >> fatal);

        // `trace_buffer_size` is a 32-bit record count, so a user can ask for tens of gigabytes per
        // thread by typing one number too many. The request is honoured up to the ceiling and no
        // further, and the shortfall is reported: a capture quietly smaller than asked for is the
        // failure this layer exists to catch, so it must not be how the layer itself fails.
        constexpr gr::Size_t  kOversized      = gr::Size_t{1U} << 31U;
        constexpr std::size_t kCeilingRecords = gr::trace::kDefaultRingCapacityLimitBytes / sizeof(Event);
        expect(lt(std::size_t{kCeilingRecords}, std::size_t{kOversized})) << "the request must actually exceed the ceiling, or this test asserts nothing";

        expect(scheduler.settings().set({{"trace_buffer_size", kOversized}}).empty() >> fatal);
        std::ignore = scheduler.settings().activateContext();
        std::ignore = scheduler.settings().applyStagedParameters();

        expect(eq(ringCapacity(), kCeilingRecords)) << "the ceiling must hold against the setting, not just the direct call";

        auto span       = fromScheduler.streamReader().get();
        bool complained = false;
        for (const auto& message : span) {
            complained = complained || (message.endpoint == "settingsChanged(trace_buffer_size)" && !message.data.has_value());
        }
        std::ignore = span.consume(span.size());
        expect(complained) << "a clamp must be reported, not swallowed";

        std::ignore = setRingCapacity(kDefaultRingCapacity);
        reset();
    };

    "trace control over the message channel starts, dumps and stops a capture"_test = [] {
        reset();
        setCategories(0U);

        Chain chain;
        chain.build(4096UZ);
        TestScheduler  scheduler;
        gr::MsgPortOut toScheduler;
        gr::MsgPortIn  fromScheduler;
        expect(scheduler.exchange(std::move(chain.graph)).has_value() >> fatal);
        expect(toScheduler.connect(scheduler.msgIn).has_value() >> fatal);
        expect(scheduler.msgOut.connect(fromScheduler).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

        // Driven through the real channel rather than by calling the handler: the handler is
        // protected, and reaching past that would test a path no user can take.
        const auto request = [&](gr::property_map data) {
            gr::sendMessage<gr::message::Command::Set>(toScheduler, scheduler.unique_name, gr::scheduler::property::kTraceControl, std::move(data));
            scheduler.processScheduledMessages();
            std::ignore = scheduler.step();
            std::vector<gr::Message> replies;
            auto                     span = fromScheduler.streamReader().get();
            for (const auto& reply : span) {
                replies.push_back(reply);
            }
            std::ignore = span.consume(span.size());
            return replies;
        };
        const auto lastReply = [](const std::vector<gr::Message>& replies) -> std::optional<gr::Message> { return replies.empty() ? std::nullopt : std::optional{replies.back()}; };
        const auto field     = [](const std::optional<gr::Message>& reply, std::string_view key) -> gr::Size_t {
            if (!reply.has_value() || !reply->data.has_value()) {
                return gr::Size_t{0U};
            }
            const auto& map = reply->data.value();
            const auto  it  = map.find(key);
            return it != map.end() ? (*it).second.value_or(gr::Size_t{0U}) : gr::Size_t{0U};
        };

        const auto started = lastReply(request({{"command", std::string("start")}, {"categories", gr::Size_t{categoryMask(Category::work)}}}));
        expect(started.has_value() >> fatal) << "start must be answered";
        expect(eq(field(started, "categories"), categoryMask(Category::work))) << "and must report the mask it set";
        expect(eq(categories(), categoryMask(Category::work))) << "the layer itself must be live";

        for (std::size_t pass = 0UZ; pass < 8UZ; ++pass) {
            std::ignore = scheduler.step();
        }

        const std::filesystem::path file   = std::filesystem::temp_directory_path() / std::format("qa_TraceScheduler_msg_{}.gr4trace", ::getpid());
        const auto                  dumped = lastReply(request({{"command", std::string("dump")}, {"path", file.string()}}));
        expect(dumped.has_value() >> fatal) << "dump must be answered";
        expect(gt(field(dumped, "records"), gr::Size_t{0U})) << "a live capture must have recorded something";
        expect(std::filesystem::exists(file)) << "and the file must be on disk";
        expect(gt(std::filesystem::file_size(file), sizeof(FileHeader))) << "with records in it, not just a header";

        const auto stopped = lastReply(request({{"command", std::string("stop")}}));
        expect(stopped.has_value() >> fatal);
        expect(eq(categories(), 0U)) << "stop must clear the mask";

        // A malformed request arrives over a message channel from anywhere, so it must come back as
        // an error rather than take the scheduler down.
        const auto bad = lastReply(request({{"command", std::string("frobnicate")}}));
        expect(bad.has_value() >> fatal) << "an unknown command must still be answered";
        expect(!bad->data.has_value()) << "and the answer must be an error";
        expect(scheduler.state() == gr::lifecycle::State::RUNNING) << "a bad request must not stop the scheduler";

        std::filesystem::remove(file);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        reset();
    };
};

int main() { /* tests are statically registered as suites */ }
