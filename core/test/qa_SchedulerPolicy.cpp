#include <boost/ut.hpp>

#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::scheduler;

static_assert(SchedulingPolicyLike<RoundRobinPolicy>);
static_assert(RoundRobinPolicy::kStaticPriority);
static_assert(!RoundRobinPolicy::kNeedsRelease);

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
