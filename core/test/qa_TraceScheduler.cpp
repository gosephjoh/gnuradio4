#include <boost/ut.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
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
};

int main() { /* tests are statically registered as suites */ }
