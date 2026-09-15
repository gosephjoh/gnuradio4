#include <boost/ut.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
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
};

int main() { /* tests are statically registered as suites */ }
