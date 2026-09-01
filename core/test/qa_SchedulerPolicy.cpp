#include <boost/ut.hpp>

#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

#include <cstdint>
#include <limits>

using namespace boost::ut;
using namespace gr::scheduler;

static_assert(SchedulingPolicyLike<RoundRobinPolicy>);
static_assert(SchedulingPolicyLike<EarliestDeadlineFirstPolicy>);
static_assert(SchedulingPolicyLike<FixedPriorityPolicy>);
static_assert(RoundRobinPolicy::kStaticPriority);
static_assert(!RoundRobinPolicy::kNeedsRelease);
static_assert(!EarliestDeadlineFirstPolicy::kStaticPriority);
static_assert(EarliestDeadlineFirstPolicy::kNeedsRelease);
static_assert(FixedPriorityPolicy::kStaticPriority);
static_assert(FixedPriorityPolicy::kNeedsRelease);

// the scheduling policy is the third template axis and must default to round-robin for every
// assignment policy and every execution policy — including the pre-existing two-argument
// spelling, which pins the backward-compatible parameter order.
static_assert(Simple<>::schedulingPolicyName() == "RoundRobin");
static_assert(Simple<ExecutionPolicy::multiThreaded>::schedulingPolicyName() == "RoundRobin");
static_assert(Simple<ExecutionPolicy::externalStep>::schedulingPolicyName() == "RoundRobin");
static_assert(BreadthFirst<>::schedulingPolicyName() == "RoundRobin");
static_assert(BreadthFirst<ExecutionPolicy::multiThreaded, gr::profiling::Profiler>::schedulingPolicyName() == "RoundRobin");
static_assert(DepthFirst<>::schedulingPolicyName() == "RoundRobin");

namespace {
std::vector<std::shared_ptr<gr::BlockModel>> makeBlocks(gr::Graph& graph, std::size_t count) {
    for (std::size_t i = 0UZ; i < count; ++i) {
        std::ignore = graph.emplaceBlock<gr::testing::NullSource<float>>();
    }
    return {graph.blocks().begin(), graph.blocks().end()};
}
} // namespace

const boost::ut::suite<"SchedulingPolicy"> schedulingPolicyTests = [] {
    "round-robin keys on the block's position in the worker's list"_test = [] {
        gr::Graph              graph;
        const auto             blocks = makeBlocks(graph, 1UZ);
        const RoundRobinPolicy policy{};

        expect(eq(policy.key(*blocks.front(), SchedState{.index = 0UZ}), 0UZ));
        expect(eq(policy.key(*blocks.front(), SchedState{.index = 7UZ}), 7UZ));
    };

    "applyStaticOrder with round-robin is the identity permutation"_test = [] {
        gr::Graph  graph;
        auto       blocks = makeBlocks(graph, 8UZ);
        const auto before = blocks;

        gr::scheduler::detail::applyStaticOrder<RoundRobinPolicy>(blocks);

        expect(eq(blocks.size(), before.size()));
        expect(std::ranges::equal(blocks, before)) << "round-robin must not permute the worker's block list";
    };

    "applyStaticOrder tolerates empty and single-element lists"_test = [] {
        std::vector<std::shared_ptr<gr::BlockModel>> empty;
        gr::scheduler::detail::applyStaticOrder<RoundRobinPolicy>(empty);
        expect(empty.empty());

        gr::Graph  graph;
        auto       single = makeBlocks(graph, 1UZ);
        const auto only   = single.front();
        gr::scheduler::detail::applyStaticOrder<RoundRobinPolicy>(single);
        expect(eq(single.size(), 1UZ));
        expect(single.front() == only);
    };

    "EDF orders released jobs by absolute deadline"_test = [] {
        gr::Graph graph;
        const auto blocks = makeBlocks(graph, 1UZ);
        const EarliestDeadlineFirstPolicy policy{};

        const auto early = policy.key(*blocks.front(), SchedState{.index = 0UZ, .absoluteDeadlineNs = 100, .releaseOrder = 1UZ});
        const auto late  = policy.key(*blocks.front(), SchedState{.index = 0UZ, .absoluteDeadlineNs = 200, .releaseOrder = 0UZ});
        expect(early < late) << "deadline must dominate release and graph order";
    };

    "fixed priority sorts annotated blocks before unannotated blocks"_test = [] {
        gr::Graph graph;
        auto blocks = makeBlocks(graph, 3UZ);
        blocks[0]->metaInformation()[std::string(kPriorityKey)] = std::int64_t{20};
        blocks[1]->metaInformation()[std::string(kPriorityKey)] = std::int64_t{5};

        gr::scheduler::detail::applyStaticOrder<FixedPriorityPolicy>(blocks);

        expect(eq(readFixedPriority(*blocks[0]), std::int64_t{5}));
        expect(eq(readFixedPriority(*blocks[1]), std::int64_t{20}));
        expect(eq(readFixedPriority(*blocks[2]), std::numeric_limits<std::int64_t>::max()));
    };

    // the annotation is resolved once when the schedule is formed and carried in SchedState, so the
    // dispatch path never touches the metadata map; the policy must key off the cached value alone
    "fixed priority keys off the cached SchedState priority, not block metadata"_test = [] {
        gr::Graph  graph;
        const auto blocks = makeBlocks(graph, 1UZ);
        blocks.front()->metaInformation()[std::string(kPriorityKey)] = std::int64_t{7};

        const FixedPriorityPolicy policy{};
        expect(eq(std::get<0>(policy.key(*blocks.front(), SchedState{.index = 0UZ, .priority = 3})), std::int64_t{3})) << "the cached value must win";
        expect(eq(std::get<0>(policy.key(*blocks.front(), SchedState{})), std::numeric_limits<std::int64_t>::max())) << "an unset priority sorts last";
    };

    "readFixedPriority reports the annotation and its absence"_test = [] {
        gr::Graph  graph;
        const auto blocks = makeBlocks(graph, 2UZ);
        blocks[0]->metaInformation()[std::string(kPriorityKey)] = std::int64_t{-4};

        expect(eq(readFixedPriority(*blocks[0]), std::int64_t{-4}));
        expect(eq(readFixedPriority(*blocks[1]), std::numeric_limits<std::int64_t>::max()));
    };

    "a graph runs to completion under the default round-robin policy"_test = [] {
        constexpr gr::Size_t kSamples = 1024U;

        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", kSamples}});
        auto&     sink = graph.emplaceBlock<gr::testing::CountingSink<float>>();
        expect(graph.connect<"out", "in">(src, sink).has_value());

        gr::scheduler::Simple<> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.runAndWait().has_value());
        expect(eq(sink.count.value, kSamples));
    };

    "an explicitly round-robin-parameterised scheduler behaves identically"_test = [] {
        constexpr gr::Size_t kSamples = 1024U;

        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", kSamples}});
        auto&     sink = graph.emplaceBlock<gr::testing::CountingSink<float>>();
        expect(graph.connect<"out", "in">(src, sink).has_value());

        gr::scheduler::Simple<ExecutionPolicy::singleThreaded, gr::profiling::null::Profiler, RoundRobinPolicy> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.runAndWait().has_value());
        expect(eq(sink.count.value, kSamples));
    };
};

int main() { /* tests are statically executed */ }
