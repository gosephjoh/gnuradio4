#include <boost/ut.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <optional>
#include <set>
#include <string>
#include <thread>
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

    "a thread's first invocation does not absorb the construction of its ring"_test = [] {
        // `workBegin` is stamped with the invocation's entry instant and *then* appended, and `workEnd`
        // measures from that same instant. If `workBegin` is the thread's first record, the ring --
        // about a millisecond to build -- was built inside the span `workEnd` reports, so the thread's
        // first invocation reported the recorder's setup as execution time, straight into the report's
        // worst case. Only the `work` category is live, which is what makes it the thread's first
        // record; a fresh thread, because only a thread that has never recorded builds a ring.
        reset();
        setCategories(categoryMask(Category::work));

        // Results are carried out of the thread and asserted here: a fatal assertion throws, and an
        // exception escaping a `std::thread` terminates the whole suite rather than failing one test.
        std::optional<std::uint64_t> createdNs;
        bool                         started = false;
        std::thread                  fresh([&createdNs, &started] {
            gr::Graph graph;
            auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{2048U}}});
            auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
            std::ignore      = graph.connect<"out", "in">(source, sink);

            TestScheduler scheduler;
            started = scheduler.exchange(std::move(graph)).has_value() && scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() && scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value();
            for (std::size_t pass = 0UZ; started && pass < 4UZ; ++pass) {
                std::ignore = scheduler.step();
            }
            createdNs   = gr::trace::detail::threadRingCreatedNs();
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        });
        fresh.join();

        const std::vector<Event> recorded = collect();
        expect(started >> fatal) << "the scheduler must start on the fresh thread";
        expect(createdNs.has_value() >> fatal) << "the thread must have recorded, or this scenario tests nothing";
        expect(gt(std::ranges::count(recorded, Kind::workBegin, &Event::kind), 0) >> fatal);
        for (const Event& event : recorded) {
            expect(le(*createdNs, event.startNs)) << "a record on this thread started before its ring existed, so its span contains the construction (kind " << static_cast<int>(std::to_underlying(event.kind)) << ")";
        }

        setCategories(0U);
        reset();
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
        expect(gt(doneRecords, okRecords)) << "stepped well past its end, a finished graph must be re-invoked more often than it "
                                              "did work -- the flood of terminal records this scenario exists to show";
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
        expect(gt(houseKeeps, 0UZ)) << "buffer house-keeping rides the message phase under the default policy";
        expect(le(houseKeeps, phases)) << "house-keeping runs inside a message phase, so it cannot happen more often";

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

    "under a release-tracking policy the re-sync reports the admitted work it discards"_test = [] {
        // The round-robin case above is the control: it tracks no jobs, so a re-sync costs only the
        // rebuild. Earliest-deadline-first admits jobs ahead of running them. Those are carried across
        // a re-sync in which nothing changed, and dropped only when the list or the topology did; the
        // marker's payload counts what was actually dropped, not what was merely outstanding.
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

        // Deliberately not asserted as a bound. A drop needs a job outstanding at the moment the list or
        // the topology changes, which is a race between the worker and the mutation, and a topology
        // change is not flagged on the record -- so a threshold here would be flaky. The carry itself
        // is pinned deterministically in `qa_SchedulingJobs`; this only keeps the quantity visible.
        std::ignore = jobsLost;
        std::ignore = syncsLosing;

        setCategories(0U);
    };

    "the message phase is accounted for by its parts"_test = [] {
        // The message phase used to carry markers for only some of what it does, so a capture could say
        // how long the phase took but not where the time went. Every part is now marked except the work
        // guard's own handshake, which is what is left over. Asserted as an accounting rather than as
        // presence: each part must nest inside exactly one phase on its own worker, the parts together
        // must fit inside it, and a phase the guard refused must contain none of the guarded parts.
        reset();
        setCategories(categoryMask(Category::schedulerLoop, Category::lifecycle));

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{65536U}}, {"relative_deadline", 0.002f}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}, {"relative_deadline", 0.002f}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}, {"relative_deadline", 0.002f}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.runAndWait().has_value() >> fatal);

        const std::vector<Event> events = collect();
        exportTimeline("m4-message-phase");

        const auto isPart = [](Kind kind) {
            switch (kind) {
            case Kind::schedulerMessages:
            case Kind::removalCleanup:
            case Kind::zombieReap:
            case Kind::adopt:
            case Kind::stateSync:
            case Kind::blockMessages:
            case Kind::houseKeeping: return true;
            default: return false;
            }
        };
        const auto endOf = [](const Event& event) { return event.startNs + event.durationNs; };

        struct Phase {
            const Event*       record;
            std::uint64_t      partsNs = 0UL;
            std::array<int, 3> guarded{}; // removalCleanup, stateSync, blockMessages
            int                schedulerMessages = 0;
        };
        std::vector<Phase> phases;
        for (const Event& event : events) {
            if (event.kind == Kind::messagePhase) {
                phases.push_back(Phase{.record = &event});
            }
        }
        expect(gt(phases.size(), 0UZ) >> fatal) << "a running pool worker must enter the message phase";

        std::size_t            orphans = 0UZ;
        std::set<std::uint8_t> schedulerMessageWorkers;
        for (const Event& part : events) {
            if (!isPart(part.kind)) {
                continue;
            }
            if (part.kind == Kind::schedulerMessages) {
                schedulerMessageWorkers.insert(part.workerId);
            }
            const auto owner = std::ranges::find_if(phases, [&](const Phase& phase) { return phase.record->workerId == part.workerId && phase.record->startNs <= part.startNs && endOf(part) <= endOf(*phase.record); });
            if (owner == phases.end()) {
                ++orphans;
                continue;
            }
            owner->partsNs += part.durationNs;
            if (part.kind == Kind::schedulerMessages) {
                ++owner->schedulerMessages;
            } else if (part.kind == Kind::removalCleanup) {
                ++owner->guarded[0];
            } else if (part.kind == Kind::stateSync) {
                ++owner->guarded[1];
            } else if (part.kind == Kind::blockMessages) {
                ++owner->guarded[2];
            }
        }

        expect(eq(orphans, 0UZ)) << "every part must nest inside a message phase on its own worker";
        expect(eq(schedulerMessageWorkers.size(), 1UZ) >> fatal) << "the scheduler's own messages are handled by exactly one worker";
        const std::uint8_t schedulerWorker = *schedulerMessageWorkers.begin();

        std::size_t granted = 0UZ;
        for (const Phase& phase : phases) {
            expect(le(phase.partsNs, static_cast<std::uint64_t>(phase.record->durationNs))) << "the parts are consecutive, so together they cannot outlast their phase";
            const bool denied = (phase.record->flags & flag::kQuiescenceDenied) != 0U;
            const int  want   = denied ? 0 : 1;
            expect(phase.guarded == std::array{want, want, want}) << "a granted phase runs each guarded part exactly once, a refused one runs none";
            // Before the guard, so even a refused phase on that worker handles them.
            expect(eq(phase.schedulerMessages, phase.record->workerId == schedulerWorker ? 1 : 0)) << "worker 0 handles the scheduler's messages once in every phase, and no other worker does";
            granted += denied ? 0UZ : 1UZ;
        }
        expect(gt(granted, 0UZ)) << "at least one phase must have been granted, or the guarded parts were never exercised";

        setCategories(0U);
    };

    "a pool worker brackets its own life, and two workers are told apart"_test = [] {
        // Two independent chains so the graph partitions into two jobs and the pool runs two
        // workers. A single chain is one job, which would make the distinctness claim vacuous.
        reset();
        setCategories(categoryMask(Category::lifecycle));

        gr::Graph graph;
        for (const std::string& suffix : {std::string("a"), std::string("b")}) {
            auto& source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", "src" + suffix}, {"n_samples_max", gr::Size_t{32768U}}});
            auto& copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", "mid" + suffix}});
            auto& sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", "snk" + suffix}});
            std::ignore  = graph.connect<"out", "in">(source, copy);
            std::ignore  = graph.connect<"out", "in">(copy, sink);
        }

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.runAndWait().has_value() >> fatal);

        std::set<std::uint32_t> startedWorkers;
        std::set<std::uint32_t> stoppedWorkers;
        std::size_t             starts      = 0UZ;
        std::size_t             stops       = 0UZ;
        std::size_t             reaps       = 0UZ;
        std::size_t             adopts      = 0UZ;
        std::size_t             emptyStarts = 0UZ;
        for (const Event& event : collect()) {
            switch (event.kind) {
            case Kind::workerStart:
                ++starts;
                startedWorkers.insert(event.workerId);
                if (event.payload0 == 0U) {
                    ++emptyStarts;
                }
                break;
            case Kind::workerStop:
                ++stops;
                stoppedWorkers.insert(event.workerId);
                break;
            case Kind::zombieReap: ++reaps; break;
            case Kind::adopt: ++adopts; break;
            default: break;
            }
        }
        exportTimeline("t1-worker-lifecycle");

        expect(gt(starts, 0UZ) >> fatal) << "a pool run must record at least one worker starting";
        expect(eq(starts, stops)) << "every worker that started must also record stopping -- an unpaired start is the shape of a worker that died";
        expect(eq(startedWorkers.size(), starts)) << "each start must carry its own worker id, or two workers' records are indistinguishable";
        expect(startedWorkers == stoppedWorkers) << "the set of workers that stopped must be the set that started";
        expect(eq(emptyStarts, 0UZ)) << "a worker records how many blocks it owns, and a worker owning none returns before starting";

        // The claim this scenario exists to make. With one job the ids would trivially be one set of
        // one, and a hard-coded zero would pass; two jobs make the distinctness real.
        expect(ge(startedWorkers.size(), 2UZ)) << "two independent chains must give two workers -- if the partitioner changed, this scenario no longer tests what it claims";

        // Emitted every house-keeping pass on the working path, and asserted here because nothing
        // else in the suite reads them.
        expect(gt(reaps, 0UZ)) << "the zombie sweep must be recorded";
        expect(gt(adopts, 0UZ)) << "the adoption phase must be recorded";

        setCategories(0U);
        reset();
    };

    "a real graph can overrun its ring, and the dumped trace says how much it lost"_test = [] {
        // The synthetic wrap test in qa_Trace proves the ring overwrites. This proves the *reporting*
        // path a user actually meets: undersize the buffer, run a real graph, and see whether the
        // file admits it is incomplete. A trace that lost data and does not say so is the worst
        // outcome this layer has, because every conclusion drawn from it is quietly wrong.
        constexpr std::size_t kTinyRing = 64UZ;

        reset();
        expect(eq(setRingCapacity(kTinyRing), kTinyRing) >> fatal);
        setCategories(kAllCategories);

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{65536U}}});
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        // A pool of its own. A ring is sized when its thread first emits and never again, and the shared
        // pool's threads have usually emitted in an earlier scenario, so they keep a full-size ring and
        // never wrap. Whether this scenario held then depended on which threads earlier ones had warmed:
        // adding one more multi-threaded scenario ahead of it was enough to break it.
        constexpr std::string_view kOverrunPoolId = "qa_TraceScheduler_overrun";
        std::ignore                               = gr::thread_pool::Manager::instance().registerPool(std::string(kOverrunPoolId), // already registered is fine: its threads still have 64-record rings
                                          std::make_shared<gr::thread_pool::ThreadPoolWrapper>(std::make_unique<gr::thread_pool::BasicThreadPool>(std::string(kOverrunPoolId), gr::thread_pool::TaskType::CPU_BOUND, 2U, 2U), "CPU"));
        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> scheduler{gr::property_map{{"poolName", std::string(kOverrunPoolId)}}};
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.runAndWait().has_value() >> fatal);

        const RingStats stats = ringStats();
        expect(gt(stats.rings, 0UZ) >> fatal) << "the run must have emitted from at least one thread";
        expect(gt(stats.lost, 0UL) >> fatal) << "a 64-record ring cannot hold a whole run; if it did, this scenario no longer tests overrun";
        expect(le(stats.recorded, stats.rings * kTinyRing)) << "no ring may hold more than its capacity";

        const std::filesystem::path file    = std::filesystem::temp_directory_path() / std::format("qa_TraceScheduler_wrap_{}.gr4trace", ::getpid());
        const auto                  written = dump(file.string());
        expect(written.has_value() >> fatal);

        std::ifstream in(file, std::ios::binary);
        FileHeader    header{};
        in.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
        expect(gt(header.lostCount, 0UL)) << "the header must carry the loss, or a report cannot know the trace is partial";
        expect(eq(header.eventCount, written.value())) << "the header's count must match what was written";
        expect(eq(header.ringCount, stats.rings));

        std::filesystem::remove(file);
        setCategories(0U);
        std::ignore = setRingCapacity(kDefaultRingCapacity);
        reset();
    };

    "a dump requested while pool workers are running parks them and still produces a valid file"_test = [] {
        // The existing control-channel scenario drives an externalStep scheduler, where the work
        // quiescence guard has no workers to park and therefore proves nothing about it. Here there
        // are real workers mid-sweep when the dump arrives.
        reset();
        setCategories(0U);

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{0U}}} /* infinite: the dump must find the workers mid-sweep */);
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> scheduler;
        gr::MsgPortOut                                                       toScheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(toScheduler.connect(scheduler.msgIn).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

        gr::sendMessage<gr::message::Command::Set>(toScheduler, scheduler.unique_name, gr::scheduler::property::kTraceControl, {{"command", std::string("start")}, {"categories", gr::Size_t{kAllCategories}}});

        // Wait on an observable condition rather than on the clock: records appearing is the proof
        // that workers are live and emitting, which is the state the dump has to interrupt.
        bool emitting = false;
        for (std::size_t attempt = 0UZ; attempt < 10000UZ && !emitting; ++attempt) {
            emitting = ringStats().recorded > 0UL;
            std::this_thread::yield();
        }
        expect(emitting >> fatal) << "the pool must be emitting before the dump is requested, or the parking is untested";

        const std::filesystem::path file = std::filesystem::temp_directory_path() / std::format("qa_TraceScheduler_live_{}.gr4trace", ::getpid());
        gr::sendMessage<gr::message::Command::Set>(toScheduler, scheduler.unique_name, gr::scheduler::property::kTraceControl, {{"command", std::string("dump")}, {"path", file.string()}});

        bool written = false;
        for (std::size_t attempt = 0UZ; attempt < 100000UZ && !written; ++attempt) {
            written = std::filesystem::exists(file);
            std::this_thread::yield();
        }
        expect(written >> fatal) << "a dump requested over the channel must complete while the graph runs";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::REQUESTED_STOP);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        scheduler.waitDone();

        // Read after the run: a torn record would show up as a header that disagrees with the file.
        std::ifstream in(file, std::ios::binary);
        FileHeader    header{};
        in.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
        expect(eq(std::string_view(header.magic.data(), 8UZ), std::string_view("GR4TRACE"))) << "the file must be a well-formed trace, not a half-written one";
        expect(eq(header.eventBytes, static_cast<std::uint32_t>(sizeof(Event))));
        // Walk the variable-length entity section exactly as a reader would, then demand that what
        // remains is precisely the events the header promised. A dump that raced an emitter, or that
        // was truncated by the stop, fails this by a byte.
        for (std::uint64_t entity = 0UL; entity < header.entityCount; ++entity) {
            EntityRecord record{};
            in.read(reinterpret_cast<char*>(&record), sizeof(EntityRecord));
            in.seekg(std::streamoff{record.uniqueNameBytes} + std::streamoff{record.typeNameBytes}, std::ios::cur);
        }
        expect(in.good() >> fatal) << "the entity section must be complete";
        const std::uint64_t eventSectionStart = static_cast<std::uint64_t>(in.tellg());
        expect(eq(std::filesystem::file_size(file), eventSectionStart + header.eventCount * sizeof(Event))) << "the file's length must match what its header claims, or the dump raced the emitters";

        std::filesystem::remove(file);
        setCategories(0U);
        reset();
    };

    "a paused worker records that it is idle, and says why"_test = [] {
        // `idle` carries a four-valued reason that nothing had ever read back, so a wrong reason --
        // or a hard-coded one -- would have gone unnoticed. Only the pool-worker path emits it;
        // step() never reaches that code, so this must be a real pool and therefore asynchronous.
        reset();
        setCategories(0U);

        gr::Graph graph;
        auto&     source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{0U}}}); // infinite: the pause must find the graph still running
        auto&     copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
        auto&     sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore      = graph.connect<"out", "in">(source, copy);
        std::ignore      = graph.connect<"out", "in">(copy, sink);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);

        // A short idle period so a paused worker cycles promptly. The reflected key is the field
        // name, not the annotation's display name.
        expect(scheduler.settings().set({{"timeout_ms", gr::Size_t{5U}}}).empty() >> fatal);
        std::ignore = scheduler.settings().activateContext();
        std::ignore = scheduler.settings().applyStagedParameters();

        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

        const auto spinUntil = [](auto&& predicate) {
            for (std::size_t attempt = 0UZ; attempt < 20'000'000UZ; ++attempt) {
                if (predicate()) {
                    return true;
                }
                std::this_thread::yield();
            }
            return false;
        };

        expect(spinUntil([&scheduler] { return scheduler.isProcessing(); }) >> fatal) << "the pool must be running before it is paused";

        // RUNNING -> PAUSED is not a legal edge; the request is what a worker observes, and a worker
        // is what moves the scheduler the rest of the way.
        expect(scheduler.changeStateTo(gr::lifecycle::State::REQUESTED_PAUSE).has_value() >> fatal);
        expect(spinUntil([&scheduler] { return scheduler.state() == gr::lifecycle::State::PAUSED; }) >> fatal) << "the pause must be observed before anything is asserted about idling";

        // The mask is opened only now, which is what makes this scenario deterministic rather than a
        // race against a wrapping ring: every record that exists from here was emitted by a worker
        // that had already been told to pause. Waiting on the ring while it fills would be the
        // alternative, and reading a ring while its thread emits is undefined by contract.
        const RingStats before = ringStats();
        setCategories(categoryMask(Category::schedulerLoop));
        expect(spinUntil([&before] {
            const RingStats now = ringStats();
            return (now.recorded + now.lost) > (before.recorded + before.lost) + 8UL;
        })) << "a paused worker must keep recording, or it is not cycling at all";

        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::REQUESTED_STOP);
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        scheduler.waitDone();

        // Collected only now the pool is joined.
        std::size_t             pausedIdles = 0UZ;
        std::set<std::uint32_t> reasons;
        for (const Event& event : collect()) {
            if (event.kind == Kind::idle) {
                reasons.insert(event.payload0);
                if (event.payload0 == std::to_underlying(IdleReason::paused)) {
                    ++pausedIdles;
                }
            }
        }
        exportTimeline("t1-idle-paused");

        expect(gt(pausedIdles, 0UZ)) << "pausing a running pool must leave idle records carrying the paused reason";
        expect(!reasons.contains(std::to_underlying(IdleReason::noProgress))) << "noProgress belongs to singleThreadedBlocking; seeing it here would mean the reason is not taken from the branch it sits in";

        setCategories(0U);
        reset();
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
