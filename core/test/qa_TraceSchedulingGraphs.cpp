#include <boost/ut.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <limits>
#include <map>
#include <print>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>

#include <gnuradio-4.0/math/Math.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::scheduler;
using namespace gr::trace;

/**
 * @brief Cross-policy trace demonstrations over whole flowgraphs.
 *
 * The other trace suites each test one marker against a fixture built to provoke it. This one runs
 * the *same* three graphs under all four scheduling policies and asserts what the records say about
 * the differences between them -- the claims that are structural rather than incidental, and that no
 * single-marker test can reach because each one is a statement about two policies at once.
 *
 * Every run is driven through `externalStep` from the calling thread. A policy's decision sequence
 * is a property of the policy, and running it on a worker pool would add the operating system's
 * scheduling noise to a question that does not involve it.
 *
 * Captures are written to `GR4_TRACE_DEMO_DIR` when that names a directory, for offline rendering.
 * Nothing in the suite depends on the dump: it is a by-product, and its absence is not a failure.
 */
namespace {

constexpr gr::Size_t  kSamples = 8192U;
constexpr std::size_t kBatch   = 512UZ;
constexpr std::size_t kBuffer  = 4096UZ;
constexpr std::size_t kDepth   = 4UZ; /// stages in `deepChain`, and per branch in `fork`

/// Every block carries this, so a job is a fixed 512 samples and the record stream is legible.
gr::property_map pinned(gr::property_map settings) {
    settings["max_batch_size"] = static_cast<gr::Size_t>(kBatch);
    return settings;
}

std::string named(std::string_view stem, std::size_t instance) { return std::format("{}.{}", stem, instance); }

/// `ConstantSource -> MultiplyConst(2) -> DivideConst(2) -> CountingSink`, uniform attributes.
void buildStraightLine(gr::Graph& graph, std::size_t instance) {
    constexpr float kPeriod = 0.001f;

    auto& src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>(pinned({{"name", named("sl.src", instance)}, {"n_samples_max", kSamples}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));
    auto& mul  = graph.emplaceBlock<gr::blocks::math::MultiplyConst<float>>(pinned({{"name", named("sl.mul", instance)}, {"value", 2.0f}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));
    auto& div  = graph.emplaceBlock<gr::blocks::math::DivideConst<float>>(pinned({{"name", named("sl.div", instance)}, {"value", 2.0f}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));
    auto& sink = graph.emplaceBlock<gr::testing::CountingSink<float>>(pinned({{"name", named("sl.sink", instance)}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));

    const gr::EdgeParameters edge{.minBufferSize = kBuffer};
    std::ignore = graph.connect<"out", "in">(src, mul, edge);
    std::ignore = graph.connect<"out", "in">(mul, div, edge);
    std::ignore = graph.connect<"out", "in">(div, sink, edge);
}

/// A `kDepth`-stage cascade with periods graded down the chain and no user priority, which is what
/// gives rate monotonic a genuine ranking to derive while fixed priority still sees only zeroes.
void buildDeepChain(gr::Graph& graph, std::size_t instance) {
    const auto gradedPeriod = [](std::size_t stage) { return 0.010f / static_cast<float>(stage + 1UZ); };

    auto& src = graph.emplaceBlock<gr::testing::ConstantSource<float>>(pinned({{"name", named("dc.src", instance)}, {"n_samples_max", kSamples}, {"period", gradedPeriod(0UZ)}, {"relative_deadline", gradedPeriod(0UZ)}}));

    const gr::EdgeParameters              edge{.minBufferSize = kBuffer};
    gr::blocks::math::DivideConst<float>* previous = nullptr;

    for (std::size_t stage = 0UZ; stage < kDepth; ++stage) {
        const float period = gradedPeriod(stage + 1UZ);

        auto& mul = graph.emplaceBlock<gr::blocks::math::MultiplyConst<float>>(pinned({{"name", named(std::format("dc.mul{}", stage), instance)}, {"value", 2.0f}, {"period", period}, {"relative_deadline", period}}));
        auto& div = graph.emplaceBlock<gr::blocks::math::DivideConst<float>>(pinned({{"name", named(std::format("dc.div{}", stage), instance)}, {"value", 2.0f}, {"period", period}, {"relative_deadline", period}}));

        if (previous == nullptr) {
            std::ignore = graph.connect<"out", "in">(src, mul, edge);
        } else {
            std::ignore = graph.connect<"out", "in">(*previous, mul, edge);
        }
        std::ignore = graph.connect<"out", "in">(mul, div, edge);
        previous    = std::addressof(div);
    }

    const float sinkPeriod = gradedPeriod(kDepth + 1UZ);
    auto&       sink       = graph.emplaceBlock<gr::testing::CountingSink<float>>(pinned({{"name", named("dc.sink", instance)}, {"period", sinkPeriod}, {"relative_deadline", sinkPeriod}}));
    std::ignore            = graph.connect<"out", "in">(*previous, sink, edge);
}

/// One source fanning out to two branches with no join. The only shape here that sets
/// `sched_priority`, which is what makes it the one where fixed priority and rate monotonic must
/// agree: a user-set priority is copied verbatim rather than derived from the period.
void buildFork(gr::Graph& graph, std::size_t instance) {
    const gr::EdgeParameters edge{.minBufferSize = kBuffer};

    auto& src = graph.emplaceBlock<gr::testing::ConstantSource<float>>(pinned({{"name", named("fk.src", instance)}, {"n_samples_max", kSamples}, {"sched_priority", std::int32_t{10}}, {"period", 0.010f}, {"relative_deadline", 0.010f}}));

    const auto branch = [&](std::string_view stem, std::int32_t priority, float period) {
        gr::blocks::math::DivideConst<float>* previous = nullptr;
        for (std::size_t stage = 0UZ; stage < kDepth; ++stage) {
            const gr::property_map common{{"sched_priority", priority}, {"period", period}, {"relative_deadline", period}};

            gr::property_map mulSettings = common;
            mulSettings["name"]          = named(std::format("fk.{}.mul{}", stem, stage), instance);
            mulSettings["value"]         = 2.0f;

            gr::property_map divSettings = common;
            divSettings["name"]          = named(std::format("fk.{}.div{}", stem, stage), instance);
            divSettings["value"]         = 2.0f;

            auto& mul = graph.emplaceBlock<gr::blocks::math::MultiplyConst<float>>(pinned(mulSettings));
            auto& div = graph.emplaceBlock<gr::blocks::math::DivideConst<float>>(pinned(divSettings));

            if (previous == nullptr) {
                std::ignore = graph.connect<"out", "in">(src, mul, edge);
            } else {
                std::ignore = graph.connect<"out", "in">(*previous, mul, edge);
            }
            std::ignore = graph.connect<"out", "in">(mul, div, edge);
            previous    = std::addressof(div);
        }

        gr::property_map sinkSettings{{"sched_priority", priority}, {"period", period}, {"relative_deadline", period}};
        sinkSettings["name"] = named(std::format("fk.{}.sink", stem), instance);

        auto& sink  = graph.emplaceBlock<gr::testing::CountingSink<float>>(pinned(sinkSettings));
        std::ignore = graph.connect<"out", "in">(*previous, sink, edge);
    };

    branch("a", 20, 0.005f);
    branch("b", 5, 0.020f);
}

/// `straightLine`, emplaced sink-first. Identical graph, identical data flow, opposite registration
/// order -- the control for the claim that round robin's probing is set by registration order and
/// not by the topology.
void buildStraightLineReversed(gr::Graph& graph, std::size_t instance) {
    constexpr float kPeriod = 0.001f;

    auto& sink = graph.emplaceBlock<gr::testing::CountingSink<float>>(pinned({{"name", named("rv.sink", instance)}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));
    auto& div  = graph.emplaceBlock<gr::blocks::math::DivideConst<float>>(pinned({{"name", named("rv.div", instance)}, {"value", 2.0f}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));
    auto& mul  = graph.emplaceBlock<gr::blocks::math::MultiplyConst<float>>(pinned({{"name", named("rv.mul", instance)}, {"value", 2.0f}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));
    auto& src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>(pinned({{"name", named("rv.src", instance)}, {"n_samples_max", kSamples}, {"period", kPeriod}, {"relative_deadline", kPeriod}}));

    const gr::EdgeParameters edge{.minBufferSize = kBuffer};
    std::ignore = graph.connect<"out", "in">(src, mul, edge);
    std::ignore = graph.connect<"out", "in">(mul, div, edge);
    std::ignore = graph.connect<"out", "in">(div, sink, edge);
}

using Builder = void (*)(gr::Graph&, std::size_t);

struct Shape {
    std::string_view name;
    Builder          build;
};

constexpr std::array<Shape, 3UZ> kShapes{{
    {.name = "straightLine", .build = &buildStraightLine},
    {.name = "deepChain", .build = &buildDeepChain},
    {.name = "fork", .build = &buildFork},
}};

/// What one traced run produced, reduced to the quantities the predictions are about.
struct Census {
    std::map<Kind, std::size_t>     records;     /// how many of each kind
    std::map<EntityId, std::size_t> workByBlock; /// productive invocations per block
    std::map<EntityId, std::string> names;
    std::vector<Event>              work;             /// the `workEnd` records themselves, for timing questions
    std::size_t                     probeTotal = 0UZ; /// summed `payload0`, not the record count
    std::size_t                     lost       = 0UZ;
    std::size_t                     total      = 0UZ;

    [[nodiscard]] std::size_t of(Kind kind) const {
        const auto it = records.find(kind);
        return it == records.end() ? 0UZ : it->second;
    }
};

void collectEvent(const Event& event, void* user) noexcept {
    Census& census = *static_cast<Census*>(user);
    ++census.records[event.kind];
    ++census.total;
    if (event.kind == Kind::workEnd) {
        ++census.workByBlock[event.entity];
        census.work.push_back(event);
    } else if (event.kind == Kind::workProbe && event.payload0 != kSaturated) {
        census.probeTotal += event.payload0;
    }
}

void collectEntity(EntityId id, const EntityDescription& description, void* user) noexcept { static_cast<Census*>(user)->names[id] = std::string(description.uniqueName); }

/// Runs one shape under one policy to completion, single-threaded, and returns what was recorded.
/// The capture is dumped when `GR4_TRACE_DEMO_DIR` names a directory -- after the run, with the
/// scheduler stopped, which is the quiescence `dump()` requires.
template<SchedulingPolicyLike TPolicy>
Census runTraced(const Shape& shape, std::size_t instances, std::uint32_t mask) {
    reset();
    setCategories(mask);

    gr::Graph graph;
    for (std::size_t instance = 0UZ; instance < instances; ++instance) {
        shape.build(graph, instance);
    }

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, TPolicy> scheduler;
    expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);

    // Bounded by wall time rather than by a step count. A release-tracking policy gates releases on
    // an elapsed period, so a graph declaring a 10 ms period needs ~150 ms of real time to finish
    // however many times it is stepped -- a step budget large enough for the other policies expires
    // long before the periods do, and truncates the run without saying so. The deadline is a safety
    // net for a graph that cannot finish, not the thing that ends a healthy run.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    bool       finished = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            finished = true;
            break;
        }
    }
    expect(finished >> fatal) << std::format("{} x{} did not finish within its safety deadline", shape.name, instances);
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

    Census census;
    std::ignore = forEachEvent(&collectEvent, &census);
    std::ignore = forEachEntity(&collectEntity, &census);
    census.lost = ringStats().lost;

    if (const char* directory = std::getenv("GR4_TRACE_DEMO_DIR"); directory != nullptr && std::filesystem::is_directory(directory)) {
        const std::string path = std::format("{}/{}-{}-x{}.gr4trace", directory, shape.name, TPolicy::kName, instances);
        std::ignore            = gr::trace::dump(path);
    }

    setCategories(0U);
    return census;
}

/// The four policies under one call, so a claim about "every policy" is written once.
template<typename TVisitor>
void forEachPolicy(TVisitor&& visit) {
    visit.template operator()<RoundRobinPolicy>();
    visit.template operator()<FixedPriorityPolicy>();
    visit.template operator()<RateMonotonicPolicy>();
    visit.template operator()<EdfPolicy>();
}

} // namespace

const boost::ut::suite<"trace demonstrations across policies"> policyDemonstrations = [] {
    if constexpr (!gr::trace::kEnabled) {
        "a compiled-out build has nothing to demonstrate"_test = [] { expect(eq(runTraced<EdfPolicy>(kShapes[0], 1UZ, kAllCategories).total, 0UZ)); };
        return;
    }

    // Sized once, here, and deliberately not inside `runTraced`. A ring is allocated on a thread's
    // first enabled emission and `reset()` does not free it, so every later request is inert -- the
    // widest run in the suite (`fork` x4 with every category live) overruns the 65 536-record
    // default and would silently understate every count taken afterwards.
    const std::size_t adopted = setRingCapacity(1UZ << 20UZ);
    expect(ge(adopted, 1UZ << 20UZ) >> fatal) << std::format("the ring was clamped to {} records, which is not enough to hold these runs", adopted);

    "release tracking is reached by exactly the policies whose class calls for it"_test = [] {
        // `needsReleaseTracking` is a compile-time predicate, so this is a claim about which code
        // was *instantiated*, not about which branch ran. A trace is the only place it is visible
        // from outside, and it is worth pinning: the three release kinds are the machinery EDF's
        // correctness rests on, and a policy silently acquiring or losing them would change its
        // scheduling behaviour with nothing else to show for it.
        for (const Shape& shape : kShapes) {
            const Census roundRobin    = runTraced<RoundRobinPolicy>(shape, 1UZ, kAllCategories);
            const Census fixedPriority = runTraced<FixedPriorityPolicy>(shape, 1UZ, kAllCategories);
            const Census rateMonotonic = runTraced<RateMonotonicPolicy>(shape, 1UZ, kAllCategories);
            const Census edf           = runTraced<EdfPolicy>(shape, 1UZ, kAllCategories);

            for (const Census* census : {&roundRobin, &fixedPriority, &rateMonotonic}) {
                expect(eq(census->of(Kind::jobRelease), 0UZ)) << std::format("{}: a static-key policy released a job", shape.name);
                expect(eq(census->of(Kind::releaseScan), 0UZ)) << std::format("{}: a static-key policy scanned for releases", shape.name);
                expect(eq(census->of(Kind::select), 0UZ)) << std::format("{}: a static-key policy made a job-driven selection", shape.name);
            }

            expect(gt(edf.of(Kind::jobRelease), 0UZ)) << std::format("{}: EDF admitted no jobs at all", shape.name);
            expect(gt(edf.of(Kind::releaseScan), 0UZ)) << std::format("{}: EDF never scanned for releases", shape.name);
            expect(gt(edf.of(Kind::select), 0UZ)) << std::format("{}: EDF selected nothing", shape.name);
        }
    };

    "a released job is what EDF runs, and every run it makes is backed by one"_test = [] {
        // The `jobBacked` flag is set at the invocation, the release is recorded where the job was
        // admitted, and the two sites are far apart. That every productive EDF invocation carries
        // the flag is the end-to-end statement that the job machinery -- not `work()`-as-oracle --
        // is what decided the block could run.
        for (const Shape& shape : kShapes) {
            reset();
            setCategories(categoryMask(Category::work, Category::release, Category::select));

            gr::Graph graph;
            shape.build(graph, 0UZ);

            gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy> scheduler;
            expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
            expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
            expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
            for (std::size_t step = 0UZ; step < 20000UZ; ++step) {
                if (scheduler.step().status == gr::work::Status::DONE) {
                    break;
                }
            }
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

            struct Tally {
                std::size_t productive = 0UZ;
                std::size_t jobBacked  = 0UZ;
            } tally;
            std::ignore = forEachEvent(
                [](const Event& event, void* user) noexcept {
                    if (event.kind != Kind::workEnd) {
                        return;
                    }
                    Tally& counts = *static_cast<Tally*>(user);
                    ++counts.productive;
                    if ((event.flags & flag::kJobBacked) != 0U) {
                        ++counts.jobBacked;
                    }
                },
                &tally);

            expect(gt(tally.productive, 0UZ)) << std::format("{}: nothing ran", shape.name);
            expect(eq(tally.jobBacked, tally.productive)) << std::format("{}: {} of {} EDF invocations ran without a job behind them", shape.name, tally.productive - tally.jobBacked, tally.productive);
            setCategories(0U);
        }
    };

    "the eligibility oracle a policy uses is visible as its probe volume"_test = [] {
        // A block that cannot run answers `work()` with an INSUFFICIENT_* near-no-op, and those are
        // aggregated into `workProbe` rather than recorded one by one. The count is therefore the
        // price a policy pays for using `work()` itself as its eligibility test.
        //
        // EDF's zero is the structural claim and the only one asserted across every shape: a
        // released job *is* the eligibility answer, so the question is never put to `work()` at all.
        // The restart policies' non-zero counts are the other half -- the strict restart returns to
        // index 0 after every productive call and re-asks blocks that have just run and cannot run
        // again. Round robin is deliberately not asserted here; see the test below for why.
        for (const Shape& shape : kShapes) {
            const Census roundRobin    = runTraced<RoundRobinPolicy>(shape, 1UZ, kAllCategories);
            const Census fixedPriority = runTraced<FixedPriorityPolicy>(shape, 1UZ, kAllCategories);
            const Census rateMonotonic = runTraced<RateMonotonicPolicy>(shape, 1UZ, kAllCategories);
            const Census edf           = runTraced<EdfPolicy>(shape, 1UZ, kAllCategories);

            expect(eq(edf.probeTotal, 0UZ)) << std::format("{}: EDF probed {} times -- release tracking has stopped being the eligibility oracle", shape.name, edf.probeTotal);
            expect(gt(fixedPriority.probeTotal, 0UZ)) << std::format("{}: fixed priority probed nothing, which its strict restart cannot do", shape.name);
            expect(gt(rateMonotonic.probeTotal, 0UZ)) << std::format("{}: rate monotonic probed nothing, which its strict restart cannot do", shape.name);

            std::println("  {:<13} probes: RR {:>6}  FP {:>6}  RM {:>6}  EDF {:>6}", shape.name, roundRobin.probeTotal, fixedPriority.probeTotal, rateMonotonic.probeTotal, edf.probeTotal);
        }
    };

    "round robin's probing is set by registration order, not by the graph"_test = [] {
        // Round robin probes nothing on any of the shapes above, which looks at first like the
        // marker not reaching its loop. It is not: the flush is loop-agnostic and the accumulator is
        // shared. The cause is that those graphs are *emplaced in dataflow order*, so a flat sweep
        // visits every block after the one feeding it, and each finds its input already there. The
        // pipeline fills inside the first sweep rather than over several.
        //
        // The same graph emplaced sink-first proves it is registration order doing the work: now
        // every block is visited before its producer, so all but the source are starved on the way
        // down and the sweep costs three probes to move one batch.
        const Shape forward{.name = "straightLine", .build = &buildStraightLine};
        const Shape reversed{.name = "straightLineReversed", .build = &buildStraightLineReversed};

        const Census inOrder  = runTraced<RoundRobinPolicy>(forward, 1UZ, kAllCategories);
        const Census backward = runTraced<RoundRobinPolicy>(reversed, 1UZ, kAllCategories);

        std::println("  round robin probes: dataflow order {}, reversed {}", inOrder.probeTotal, backward.probeTotal);

        expect(eq(inOrder.probeTotal, 0UZ)) << "emplaced in dataflow order, a flat sweep should never meet a starved block";
        expect(gt(backward.probeTotal, inOrder.probeTotal)) << "emplaced backwards, every block is visited before its producer and must probe";
    };

    "the heap selector never silently reverts across any of these graphs"_test = [] {
        // The scratch is sized at the block count and none of these graphs mutates its block list,
        // so the fallback has no way to fire. It is asserted because the fallback is silent by
        // construction: were it to start firing, the run would still produce correct output and
        // nothing but this record would say the heap had stopped being used.
        for (const Shape& shape : kShapes) {
            for (const std::size_t instances : {1UZ, 4UZ}) {
                const Census scan = runTraced<EdfPolicy>(shape, instances, kAllCategories);
                expect(eq(scan.of(Kind::heapFallback), 0UZ)) << std::format("{} x{}: the ready-heap fell back to the linear scan", shape.name, instances);
            }
        }
    };

    "replication conserves work, so a policy's bias is visible only in time"_test = [] {
        // Four disjoint copies of one graph. Each copy must move the same 8192 samples through the
        // same blocks, so the *number* of invocations each receives is fixed by the data and not by
        // the policy -- a count that comes out even under every policy is arithmetic, not fairness,
        // and asserting it would be a test that cannot fail.
        //
        // What the policy does control is *when* each copy is served. The static-key loop restarts
        // at index 0 after every productive call, so it works down the block list from the first
        // copy each time; round robin sweeps all four evenly and EDF takes the globally earliest
        // deadline. That shows up as the instant each copy finishes, not as how much it did.
        const Shape           shape              = {.name = "deepChain", .build = &buildDeepChain};
        constexpr std::size_t kInstances         = 4UZ;
        constexpr std::size_t kBlocksPerInstance = 2UZ + 2UZ * kDepth; // source, sink, and a multiply/divide pair per stage

        // A block's configured name does not reach the capture -- the recorded unique name is
        // `type#id`, which carries no instance. Entity ids are assigned in registration order and
        // the copies are emplaced one after another, so the *rank* of an id is its registration
        // index and `rank / kBlocksPerInstance` is its copy. The entity count is checked against the
        // graph rather than assumed, so a change to either surfaces here.
        struct Spread {
            std::vector<std::size_t> invocations;
            std::vector<double>      completion; /// when this copy last ran, as a fraction of the run
        };

        const auto spreadOf = [&](const Census& census) {
            std::vector<EntityId> ids;
            for (const auto& [entity, count] : census.workByBlock) {
                ids.push_back(entity);
            }
            std::ranges::sort(ids);
            expect(eq(ids.size(), kInstances * kBlocksPerInstance) >> fatal) << "the instance mapping assumes every block ran and was interned exactly once";

            Spread                          spread{.invocations = std::vector<std::size_t>(kInstances, 0UZ), .completion = std::vector<double>(kInstances, 0.0)};
            std::vector<std::uint64_t>      lastNs(kInstances, 0UL);
            std::map<EntityId, std::size_t> instanceOf;
            for (std::size_t rank = 0UZ; rank < ids.size(); ++rank) {
                instanceOf[ids[rank]] = rank / kBlocksPerInstance;
                spread.invocations[rank / kBlocksPerInstance] += census.workByBlock.at(ids[rank]);
            }

            for (const Event& event : census.work) {
                const auto it = instanceOf.find(event.entity);
                if (it != instanceOf.end()) {
                    lastNs[it->second] = std::max(lastNs[it->second], event.startNs);
                }
            }

            const auto [first, last] = std::ranges::minmax(census.work | std::views::transform(&Event::startNs));
            const double span        = static_cast<double>(last - first);
            for (std::size_t instance = 0UZ; instance < kInstances; ++instance) {
                spread.completion[instance] = span == 0.0 ? 1.0 : static_cast<double>(lastNs[instance] - first) / span;
            }
            return spread;
        };

        const auto joined = [](const auto& values, std::string_view spec) {
            std::string text;
            for (const auto value : values) {
                text += spec == "int" ? std::format("{:>6}", value) : std::format("{:>6.2f}", static_cast<double>(value));
            }
            return text;
        };

        const Spread roundRobin    = spreadOf(runTraced<RoundRobinPolicy>(shape, kInstances, kAllCategories));
        const Spread edf           = spreadOf(runTraced<EdfPolicy>(shape, kInstances, kAllCategories));
        const Spread fixedPriority = spreadOf(runTraced<FixedPriorityPolicy>(shape, kInstances, kAllCategories));

        std::println("  deepChain x4, per copy (0..3):");
        std::println("    invocations   RR{}   EDF{}   FP{}", joined(roundRobin.invocations, "int"), joined(edf.invocations, "int"), joined(fixedPriority.invocations, "int"));
        std::println("    finished at   RR{}   EDF{}   FP{}", joined(roundRobin.completion, "f"), joined(edf.completion, "f"), joined(fixedPriority.completion, "f"));

        // The conservation claim, asserted so that the timing comparison beneath it is known to be
        // comparing equal amounts of work rather than different ones.
        for (const Spread* spread : {&roundRobin, &edf, &fixedPriority}) {
            const auto [least, most] = std::ranges::minmax(spread->invocations);
            expect(le(most - least, 1UZ)) << std::format("copies of one graph did materially different amounts of work ({} against {})", most, least);
        }

        // Round robin visits every block of every copy once per sweep, so the four copies advance in
        // lockstep and must finish together. This is the fairness statement the counts could not make.
        const auto [earliest, latest] = std::ranges::minmax(roundRobin.completion);
        expect(lt(latest - earliest, 0.05)) << std::format("round robin's four copies finished {:.2f} apart in a normalised run", latest - earliest);
    };

    "a release-tracking policy paces a source at the period it was given"_test = [] {
        // The sharpest quantitative check the trace supports, and the only one here whose expected
        // value comes from the configuration rather than from another run: `deepChain`'s source is
        // declared with a 10 ms period and has 8192/512 = 16 batches to move, so its releases must
        // be 10 ms apart and the run must take (16 - 1) x 10 ms whatever else is true.
        //
        // The three static-key policies ignore `period` entirely -- it reaches the scheduler only
        // through release tracking -- so the same graph under them finishes in about 1.5 ms. That
        // hundredfold difference is the temporal gate doing exactly what it is for, and is worth
        // pinning: a regression that dropped the gate would still deliver every sample, still pass
        // every other test in this suite, and only show up as a run that finished far too quickly.
        const Shape  shape{.name = "deepChain", .build = &buildDeepChain};
        const Census census = runTraced<EdfPolicy>(shape, 1UZ, categoryMask(Category::work));

        std::vector<EntityId> ids;
        for (const auto& [entity, count] : census.workByBlock) {
            ids.push_back(entity);
        }
        std::ranges::sort(ids);
        expect(!ids.empty() >> fatal);
        const EntityId source = ids.front(); // registration order: the source is emplaced first

        std::vector<std::uint64_t> starts;
        for (const Event& event : census.work) {
            if (event.entity == source) {
                starts.push_back(event.startNs);
            }
        }
        std::ranges::sort(starts);
        expect(eq(starts.size(), std::size_t{kSamples / kBatch}) >> fatal) << "the source should run once per batch";

        constexpr double kPeriodMs     = 10.0;
        double           worstGapError = 0.0;
        for (std::size_t i = 1UZ; i < starts.size(); ++i) {
            const double gapMs = static_cast<double>(starts[i] - starts[i - 1UZ]) / 1e6;
            worstGapError      = std::max(worstGapError, std::abs(gapMs - kPeriodMs));
        }

        const double makespanMs = static_cast<double>(starts.back() - starts.front()) / 1e6;
        std::println("  deepChain source: {} releases, worst gap error {:.3f} ms, makespan {:.1f} ms (predicted {:.1f})", starts.size(), worstGapError, makespanMs, kPeriodMs * static_cast<double>(starts.size() - 1UZ));

        // A generous tolerance: this is a soft real-time gate on a general-purpose kernel, and the
        // claim being made is that the pacing is the period rather than that it is hard.
        expect(lt(worstGapError, 2.0)) << std::format("a release was {:.3f} ms away from its 10 ms period", worstGapError);
        expect(gt(makespanMs, kPeriodMs * static_cast<double>(starts.size() - 1UZ) * 0.9)) << "the run finished sooner than the periods allow, so the temporal gate is not binding";
    };

    "no graph here mutates, so no admitted work is ever discarded"_test = [] {
        // `stateSync` re-derives every `SchedState`, discarding outstanding jobs with them. It rides
        // the house-keeping cadence rather than an actual mutation, so it fires on graphs that never
        // change -- this pins the consequence that matters: on a static graph it must throw nothing
        // away. A non-zero count here would mean jobs are being admitted and dropped unobserved.
        for (const Shape& shape : kShapes) {
            const Census census    = runTraced<EdfPolicy>(shape, 1UZ, kAllCategories);
            std::size_t  discarded = 0UZ;
            std::ignore            = forEachEvent(
                [](const Event& event, void* user) noexcept {
                    if (event.kind == Kind::stateSync && event.payload2 != 0U && event.payload2 != kSaturated) {
                        *static_cast<std::size_t*>(user) += event.payload2;
                    }
                },
                &discarded);
            expect(eq(discarded, 0UZ)) << std::format("{}: a re-sync discarded {} admitted jobs from a graph that never changed", shape.name, discarded);
        }
    };

    "a demonstration capture keeps every record it took"_test = [] {
        // A census computed over a window that silently moved is a wrong census. The graphs are
        // sized so that the default ring holds a whole run; this is what says so rather than
        // assuming it, and it is the precondition for every other assertion in this suite.
        for (const Shape& shape : kShapes) {
            const Census census = runTraced<EdfPolicy>(shape, 4UZ, kAllCategories);
            expect(eq(census.lost, 0UZ)) << std::format("{} x4 overran its ring by {} records -- every count in this suite is understated", shape.name, census.lost);
        }
    };
};

int main() { /* boost.ut runs the suites */ }
