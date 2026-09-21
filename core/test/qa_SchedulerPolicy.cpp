#include <boost/ut.hpp>

#include <array>
#include <limits>

#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::scheduler;

static_assert(SchedulingPolicyLike<RoundRobinPolicy>);
static_assert(RoundRobinPolicy::kPriorityClass == PriorityClass::none, "round robin is time-sharing, not a priority discipline");
static_assert(FixedPriorityPolicy::kPriorityClass == PriorityClass::fixedTask);
static_assert(RateMonotonicPolicy::kPriorityClass == PriorityClass::fixedTask);
static_assert(hasStaticKey(RoundRobinPolicy::kPriorityClass) && !needsReleaseTracking(RoundRobinPolicy::kPriorityClass));
static_assert(hasStaticKey(FixedPriorityPolicy::kPriorityClass) && !needsReleaseTracking(FixedPriorityPolicy::kPriorityClass), "a task-level priority is constant across jobs, so it needs no release tracking");
static_assert(!hasStaticKey(PriorityClass::fixedJob) && needsReleaseTracking(PriorityClass::fixedJob), "a job-level priority derives from the release, so it presumes tracking it");

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

/// Orders blocks back-to-front. Exists only to prove that `applyStaticOrder` carries each block's
/// state along with it: under an identity permutation a desynchronised state vector is invisible.
struct ReversePolicy {
    static constexpr std::string_view             kName          = "Reverse";
    static constexpr gr::scheduler::PriorityClass kPriorityClass = gr::scheduler::PriorityClass::none; // ordering-only: this test block never runs a scheduler

    [[nodiscard]] constexpr std::size_t key(const gr::BlockModel&, const gr::scheduler::SchedState& state) const noexcept { return std::numeric_limits<std::size_t>::max() - state.index; }
};
static_assert(gr::scheduler::SchedulingPolicyLike<ReversePolicy>);

/// Appends its own name to a shared log on every invocation, so a test can observe the *order* and
/// the *count* of `work()` calls -- which is what distinguishes a priority-ordered sweep from
/// priority-dominant selection.
inline std::vector<std::string> gInvocationLog; // externalStep is single-threaded: no sync needed

template<typename T>
struct RecordingCopy : gr::Block<RecordingCopy<T>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(RecordingCopy, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) noexcept {
        gInvocationLog.push_back(std::string(this->name));
        const std::size_t n = std::min(input.size(), output.size());
        output.publish(n);
        std::ignore = input.consume(n);
        return gr::work::Status::OK;
    }
};

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

        std::vector<SchedState> states(blocks.size());
        for (std::size_t i = 0UZ; i < states.size(); ++i) {
            states[i] = SchedState{.index = i, .tieBreak = i, .batchCeiling = 100UZ + i};
        }
        const auto statesBefore = states;

        gr::scheduler::detail::applyStaticOrder<RoundRobinPolicy>(blocks, states);

        expect(eq(blocks.size(), before.size()));
        expect(std::ranges::equal(blocks, before)) << "round-robin must not permute the worker's block list";
        expect(std::ranges::equal(states, statesBefore, [](const SchedState& a, const SchedState& b) { return a.index == b.index && a.batchCeiling == b.batchCeiling; })) << "nor its parallel state";
    };

    "applyStaticOrder keeps blocks and their state aligned"_test = [] {
        // A policy that reverses the order, to prove the state follows the blocks. Getting this
        // wrong would hand each block another block's batch ceiling -- silently.
        gr::Graph               graph;
        auto                    blocks = makeBlocks(graph, 4UZ);
        std::vector<SchedState> states(blocks.size());
        for (std::size_t i = 0UZ; i < states.size(); ++i) {
            states[i] = SchedState{.index = i, .tieBreak = i, .batchCeiling = 10UZ * (i + 1UZ)};
        }
        const auto before = blocks;

        gr::scheduler::detail::applyStaticOrder<ReversePolicy>(blocks, states);

        expect(eq(blocks.size(), 4UZ));
        for (std::size_t i = 0UZ; i < blocks.size(); ++i) {
            expect(blocks[i] == before[before.size() - 1UZ - i]) << "the policy must have reversed the blocks";
            expect(eq(states[i].batchCeiling, 10UZ * (before.size() - i))) << "and each block must still carry its own ceiling";
            expect(eq(states[i].index, before.size() - 1UZ - i)) << "index stays the original position, so it remains a stable tie-breaker";
        }
    };

    "fixed priority runs the largest sched_priority first"_test = [] {
        // The sign convention in one assertion: GR4 counts larger as more urgent, `key()` is
        // minimum-first, so the policy negates. An inverted comparator still produces *a*
        // permutation, so the test must pin the direction rather than merely that order changed.
        gr::Graph               graph;
        auto                    blocks = makeBlocks(graph, 4UZ);
        std::vector<SchedState> states(blocks.size());
        const std::array        priorities{5, 40, 10, 20};
        for (std::size_t i = 0UZ; i < states.size(); ++i) {
            states[i] = SchedState{.index = i, .tieBreak = i, .userPriority = priorities[i]};
        }
        const auto before = blocks;

        gr::scheduler::detail::applyStaticOrder<FixedPriorityPolicy>(blocks, states);

        expect(blocks[0] == before[1]) << "priority 40 must run first";
        expect(blocks[1] == before[3]) << "then 20";
        expect(blocks[2] == before[2]) << "then 10";
        expect(blocks[3] == before[0]) << "and 5 last";
        expect(eq(states[0].userPriority, 40)) << "each block must keep its own priority through the permutation";
    };

    "fixed priority ignores a derived rank"_test = [] {
        // `FixedPriorityPolicy` is *absolute*: a block with no declared priority is unset, not
        // low-ranked. Keying on the derivation's blend instead would silently reorder blocks the
        // user never prioritised.
        gr::Graph               graph;
        auto                    blocks = makeBlocks(graph, 2UZ);
        std::vector<SchedState> states(blocks.size());
        states[0]         = SchedState{.index = 0UZ, .tieBreak = 0UZ, .priority = 99, .userPriority = 0}; // derived rank only
        states[1]         = SchedState{.index = 1UZ, .tieBreak = 1UZ, .priority = 1, .userPriority = 0};
        const auto before = blocks;

        gr::scheduler::detail::applyStaticOrder<FixedPriorityPolicy>(blocks, states);

        expect(std::ranges::equal(blocks, before)) << "with no user priorities the order must be untouched, whatever the derived ranks say";
    };

    "rate-monotonic runs the shorter period first"_test = [] {
        // The derivation ranks shorter periods higher, so keying on that rank is rate-monotonic.
        gr::Graph               graph;
        auto                    blocks = makeBlocks(graph, 3UZ);
        std::vector<SchedState> states(blocks.size());
        states[0]         = SchedState{.index = 0UZ, .tieBreak = 0UZ, .priority = 1}; // longest period -> lowest rank
        states[1]         = SchedState{.index = 1UZ, .tieBreak = 1UZ, .priority = 3}; // shortest period -> highest rank
        states[2]         = SchedState{.index = 2UZ, .tieBreak = 2UZ, .priority = 2};
        const auto before = blocks;

        gr::scheduler::detail::applyStaticOrder<RateMonotonicPolicy>(blocks, states);

        expect(blocks[0] == before[1]) << "the highest rank -- the shortest period -- runs first";
        expect(blocks[1] == before[2]);
        expect(blocks[2] == before[0]) << "and the longest period last";
    };

    "equal priorities fall back to registration order"_test = [] {
        // The degenerate case, and the one that looks identical to a completely broken priority
        // path: an unanchored graph derives no periods, so every priority is 0 and FP must behave
        // exactly like round-robin rather than permuting arbitrarily.
        gr::Graph               graph;
        auto                    blocks = makeBlocks(graph, 5UZ);
        std::vector<SchedState> states(blocks.size());
        for (std::size_t i = 0UZ; i < states.size(); ++i) {
            states[i] = SchedState{.index = i, .tieBreak = i};
        }
        const auto before = blocks;

        gr::scheduler::detail::applyStaticOrder<FixedPriorityPolicy>(blocks, states);
        expect(std::ranges::equal(blocks, before)) << "all-zero priorities must degenerate to the identity, not to an arbitrary permutation";

        gr::scheduler::detail::applyStaticOrder<RateMonotonicPolicy>(blocks, states);
        expect(std::ranges::equal(blocks, before)) << "and the same under rate-monotonic";
    };

    "applyStaticOrder tolerates empty and single-element lists"_test = [] {
        std::vector<std::shared_ptr<gr::BlockModel>> empty;
        std::vector<SchedState>                      noStates;
        gr::scheduler::detail::applyStaticOrder<RoundRobinPolicy>(empty, noStates);
        expect(empty.empty());

        gr::Graph               graph;
        auto                    single = makeBlocks(graph, 1UZ);
        std::vector<SchedState> oneState(1UZ);
        const auto              only = single.front();
        gr::scheduler::detail::applyStaticOrder<RoundRobinPolicy>(single, oneState);
        expect(eq(single.size(), 1UZ));
        expect(single.front() == only);
    };

    "an externalStep scheduler orders its shared job list by policy"_test = [] {
        // `step()` executes `(*_executionOrder)[0]` directly -- there is no worker-local copy
        // to order -- so without ordering the shared list at init, the same graph would run
        // priority-ordered on the pool path and in registration order here. `jobs()` is the
        // observable: it returns a copy of that shared list.
        gr::Graph graph;
        auto&     low  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("low")}, {"sched_priority", std::int32_t{1}}});
        auto&     high = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("high")}, {"sched_priority", std::int32_t{99}}});
        auto&     mid  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}, {"sched_priority", std::int32_t{50}}});
        expect(graph.connect<"out", "in">(low, high).has_value());
        expect(graph.connect<"out", "in">(high, mid).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, FixedPriorityPolicy> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

        const auto jobs = sched.jobs();
        expect(jobs != nullptr and !jobs->empty()) << fatal << "externalStep uses a single job list";
        const auto& jobList = (*jobs)[0];
        expect(eq(jobList.size(), 3UZ));
        expect(eq(jobList[0]->name(), std::string("high"))) << "priority 99 must be scheduled first";
        expect(eq(jobList[1]->name(), std::string("mid")));
        expect(eq(jobList[2]->name(), std::string("low"))) << "priority 1 last";

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "round-robin leaves the shared job list in registration order"_test = [] {
        // The behaviour-neutrality half: ordering the shared list must not perturb the default.
        gr::Graph graph;
        auto&     a = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("a")}, {"sched_priority", std::int32_t{1}}});
        auto&     b = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("b")}, {"sched_priority", std::int32_t{99}}});
        expect(graph.connect<"out", "in">(a, b).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep> sched; // default RoundRobinPolicy
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

        const auto  jobs    = sched.jobs();
        const auto& jobList = (*jobs)[0];
        expect(eq(jobList[0]->name(), std::string("a"))) << "round-robin keys on position, so priorities must not move anything";
        expect(eq(jobList[1]->name(), std::string("b")));

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "re-ordering an appended block restores priority order"_test = [] {
        // Adoption appends to the end of the worker's list, so the ordering has to be re-applied
        // or a newly adopted block runs last whatever its priority.
        //
        // N.B. this exercises `applyStaticOrder` on a list mutated the way `adoptBlocks()` mutates
        // it, not the scheduler's call site. Observing the call site would need a block that records
        // invocation order and a live adoption through the message path; that is not done here, so
        // the wiring is inferred rather than observed.
        gr::Graph               graph;
        auto                    blocks = makeBlocks(graph, 3UZ);
        std::vector<SchedState> states(blocks.size());
        states[0] = SchedState{.index = 0UZ, .tieBreak = 0UZ, .userPriority = 30};
        states[1] = SchedState{.index = 1UZ, .tieBreak = 1UZ, .userPriority = 20};
        states[2] = SchedState{.index = 2UZ, .tieBreak = 2UZ, .userPriority = 10};
        gr::scheduler::detail::applyStaticOrder<FixedPriorityPolicy>(blocks, states);
        expect(eq(states[0].userPriority, 30)) << "ordered to begin with";

        auto adopted = makeBlocks(graph, 1UZ); // a block appended by adoption, top priority
        blocks.push_back(adopted.front());
        states.push_back(SchedState{.index = blocks.size() - 1UZ, .tieBreak = blocks.size() - 1UZ, .userPriority = 99});
        expect(blocks.back() == adopted.front()) << "adoption appends, so it starts last";

        gr::scheduler::detail::applyStaticOrder<FixedPriorityPolicy>(blocks, states);

        expect(blocks.front() == adopted.front()) << "after re-ordering the adopted block must run first, not last";
        expect(eq(states.front().userPriority, 99));
        expect(eq(states.back().userPriority, 10)) << "and the rest keep their relative order";
    };

    "the tie-break travels with its block and decides equal keys"_test = [] {
        // M2f-4. Every key is equal here, so the tie-break alone orders the list -- and it is
        // deliberately not the position, so a comparator still keyed on `index` would fail.
        gr::Graph               graph;
        auto                    blocks = makeBlocks(graph, 3UZ);
        std::vector<SchedState> states(blocks.size());
        states[0]         = SchedState{.index = 0UZ, .tieBreak = 2UZ};
        states[1]         = SchedState{.index = 1UZ, .tieBreak = 0UZ};
        states[2]         = SchedState{.index = 2UZ, .tieBreak = 1UZ};
        const auto before = blocks;

        gr::scheduler::detail::applyStaticOrder<FixedPriorityPolicy>(blocks, states);

        expect(blocks[0] == before[1]) << "the smallest tie-break runs first";
        expect(blocks[1] == before[2]);
        expect(blocks[2] == before[0]);
        expect(eq(states[0].tieBreak, 0UZ)) << "each state followed its own block";
        expect(eq(states[0].index, 1UZ)) << "and `index` still names the original position";
    };

    "a data-topological tie-break orders equal priorities by data flow"_test = [] {
        // M2f-5, the feature end to end. No block declares a priority, so every key ties and the
        // setting decides the whole order. Registration order is chosen to contradict both
        // directions, so all three answers differ.
        const auto orderUnder = [](gr::scheduler::TieBreak tieBreak) {
            gr::Graph graph;
            auto&     mid  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
            auto&     sink = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("sink")}});
            auto&     src  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("src")}});
            expect(graph.connect<"out", "in">(src, mid).has_value());
            expect(graph.connect<"out", "in">(mid, sink).has_value());

            gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, FixedPriorityPolicy> sched;
            sched.tie_break = tieBreak;
            expect(sched.exchange(std::move(graph)).has_value());
            expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

            std::string order;
            for (const auto& block : (*sched.jobs())[0]) {
                order += (order.empty() ? "" : ", ") + std::string(block->name());
            }
            std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
            return order;
        };

        using enum gr::scheduler::TieBreak;
        expect(eq(orderUnder(registrationOrder), std::string("mid, sink, src"))) << "the default must reproduce the historic order exactly";
        expect(eq(orderUnder(upstreamFirst), std::string("src, mid, sink"))) << "producers before the blocks they feed";
        expect(eq(orderUnder(downstreamFirst), std::string("sink, mid, src"))) << "and the reverse drains first";
    };

    "the tie-break leaves round robin and dynamic-key policies alone"_test = [] {
        // The scope boundary, made executable. Round robin keys on the position itself and is the
        // baseline every measurement is taken against; EDF is never pre-sorted. Neither may move.
        const auto orderUnder = [](auto policyTag, gr::scheduler::TieBreak tieBreak) {
            using TPolicy = decltype(policyTag);
            gr::Graph graph;
            auto&     mid  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
            auto&     sink = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("sink")}});
            auto&     src  = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("src")}});
            expect(graph.connect<"out", "in">(src, mid).has_value());
            expect(graph.connect<"out", "in">(mid, sink).has_value());

            gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, TPolicy> sched;
            sched.tie_break = tieBreak;
            expect(sched.exchange(std::move(graph)).has_value());
            expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

            std::string order;
            for (const auto& block : (*sched.jobs())[0]) {
                order += (order.empty() ? "" : ", ") + std::string(block->name());
            }
            std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
            return order;
        };

        const std::string registered{"mid, sink, src"};
        expect(eq(orderUnder(RoundRobinPolicy{}, gr::scheduler::TieBreak::upstreamFirst), registered)) << "round robin must stay the historic sweep";
        expect(eq(orderUnder(EdfPolicy{}, gr::scheduler::TieBreak::upstreamFirst), registered)) << "a dynamic-key policy is not pre-sorted at all";
    };

    "round robin gives one turn each; fixed priority re-selects within a pass"_test = [] {
        // The distinction between the two disciplines, made executable. Round robin is
        // *time-sharing*: one `work()` call per block per pass, in list order. A fixed-priority
        // policy *selects*: after every successful call it returns to the highest-priority eligible
        // block, so a single pass can invoke the same block several times.
        //
        // The assertion is on repetition rather than on the exact interleaving, which a data
        // dependency makes topology-specific: `high` cannot run until `low` has produced.
        const auto passesUnder = [](auto policyTag) {
            using Policy = decltype(policyTag);
            gInvocationLog.clear();
            gr::Graph graph;
            auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{100000}}});
            auto&     low  = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::string("low")}, {"sched_priority", std::int32_t{1}}});
            auto&     high = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::string("high")}, {"sched_priority", std::int32_t{99}}});
            auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
            expect(graph.connect<"out", "in">(src, low).has_value());
            expect(graph.connect<"out", "in">(low, high).has_value());
            expect(graph.connect<"out", "in">(high, sink).has_value());

            gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, Policy> sched;
            expect(sched.exchange(std::move(graph)).has_value());
            expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
            expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());

            std::vector<std::vector<std::string>> passes;
            for (int i = 0; i < 6; ++i) {
                gInvocationLog.clear();
                std::ignore = sched.step();
                passes.push_back(gInvocationLog);
            }
            std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
            return passes;
        };

        const auto maxRepeat = [](const std::vector<std::string>& pass) {
            std::size_t worst = 0UZ;
            for (const std::string& name : {std::string("low"), std::string("high")}) {
                worst = std::max(worst, static_cast<std::size_t>(std::ranges::count(pass, name)));
            }
            return worst;
        };

        const auto roundRobin = passesUnder(RoundRobinPolicy{});
        const auto fixedPrio  = passesUnder(FixedPriorityPolicy{});

        for (const auto& pass : roundRobin) {
            expect(le(maxRepeat(pass), 1UZ)) << "round robin is time-sharing: no block gets a second turn within a pass";
        }
        const bool reselected = std::ranges::any_of(fixedPrio, [&](const auto& pass) { return maxRepeat(pass) > 1UZ; });
        expect(reselected) << "a fixed-priority policy must re-select after a successful call, so a block can run again within one pass";

        // and round robin must still sweep in list order, which here is the data-flow order
        const auto rrFirst = std::ranges::find_if(roundRobin, [](const auto& pass) { return pass.size() == 2UZ; });
        expect(rrFirst != roundRobin.end()) << fatal;
        expect(eq((*rrFirst)[0], std::string("low")));
        expect(eq((*rrFirst)[1], std::string("high")));
    };

    "the selection bound returns control to the worker"_test = [] {
        // The hang risk this bound exists to remove: house-keeping, message handling and lifecycle
        // checks all live *between* passes, so an unbounded selection loop would not merely starve
        // low-priority blocks -- it would stop the worker responding at all. Every `step()` must
        // return.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{100000}}});
        auto&     mid  = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::string("mid")}, {"sched_priority", std::int32_t{99}}});
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, FixedPriorityPolicy> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());

        gInvocationLog.clear();
        for (int i = 0; i < 20; ++i) {
            std::ignore = sched.step(); // must return; a hang here fails by timeout, which is the point
        }
        expect(!gInvocationLog.empty()) << "the graph must actually run";

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "an explicit selection bound caps the work done per pass"_test = [] {
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{100000}}});
        auto&     mid  = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::string("mid")}, {"sched_priority", std::int32_t{99}}});
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, FixedPriorityPolicy> sched;
        std::ignore = sched.settings().set({{"max_selections_per_pass", gr::Size_t{1}}});
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());

        std::size_t worstPass = 0UZ;
        for (int i = 0; i < 8; ++i) {
            gInvocationLog.clear();
            std::ignore = sched.step();
            worstPass   = std::max(worstPass, static_cast<std::size_t>(std::ranges::count(gInvocationLog, std::string("mid"))));
        }
        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(le(worstPass, 1UZ)) << std::format("a bound of one successful selection must permit at most one productive call per block per pass, saw {}", worstPass);
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
