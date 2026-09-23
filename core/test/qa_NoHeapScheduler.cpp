#include <atomic>
#include <cstdlib>
#include <memory_resource>
#include <new>

#include <boost/ut.hpp>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/MemoryAllocators.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;

// armed-window counter over ::operator new — same pattern as qa_Embedded but compiled with the
// normal toolchain (RTTI + exceptions on) so Graph/Scheduler headers compile.
namespace {
std::atomic<std::size_t> gGlobalNewCount{0UZ};
std::atomic<bool>        gGlobalNewArmed{false};

struct GlobalNewSentinel {
    std::size_t baseline;
    explicit GlobalNewSentinel() noexcept : baseline(gGlobalNewCount.load(std::memory_order_relaxed)) { gGlobalNewArmed.store(true, std::memory_order_release); }
    ~GlobalNewSentinel() { gGlobalNewArmed.store(false, std::memory_order_release); }

    GlobalNewSentinel(const GlobalNewSentinel&)            = delete;
    GlobalNewSentinel& operator=(const GlobalNewSentinel&) = delete;
    GlobalNewSentinel(GlobalNewSentinel&&)                 = delete;
    GlobalNewSentinel& operator=(GlobalNewSentinel&&)      = delete;

    [[nodiscard]] std::size_t delta() const noexcept { return gGlobalNewCount.load(std::memory_order_relaxed) - baseline; }
};
} // namespace

[[gnu::noinline]] void* operator new(std::size_t n) {
    void* p = std::malloc(n == 0UZ ? 1UZ : n);
    if (gGlobalNewArmed.load(std::memory_order_acquire)) {
        gGlobalNewCount.fetch_add(1UZ, std::memory_order_relaxed);
    }
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return p;
}

[[gnu::noinline]] void operator delete(void* p) noexcept { std::free(p); }
[[gnu::noinline]] void operator delete(void* p, std::size_t) noexcept { std::free(p); }

const boost::ut::suite<"Graph + Scheduler heap-discipline tracking"> _noHeapSched = [] {
    // no construction/wiring budget asserted: env-dependent (thread-pool worker count scales with cores); belongs to featGraphOnMcu.
    // the steady-state superloop below — the actual MCU promise — is env-independent (0 / 0 everywhere).

    // construction-time PMR routing: a Graph built from a ResourceProfile must place its OWN bookkeeping
    // containers (_blocks/_edges/_progress, name strings) on the profile, not the global heap. arena growth
    // is the discriminator — a body-level `_resources = resources` after delegation leaves it at 0.
    "Graph(ResourceProfile) routes its own containers to the profile arena"_test = [] {
        auto                arenaPtr = std::make_unique<gr::pmr::OwnedStaticArenaResource<1UZ << 23U>>();
        auto&               arena    = *arenaPtr;
        gr::ResourceProfile profile{.data = &arena, .tag = &arena, .mechanics = &arena};

        const std::size_t usedBefore = arena.used();
        gr::Graph         graph(profile);
        const std::size_t growth = arena.used() - usedBefore;

        std::fputs(std::format("[diag] Graph(profile) construction arena growth: {} bytes\n", growth).c_str(), stderr);
        expect(eq(graph.resources().mechanicsResource(), static_cast<std::pmr::memory_resource*>(&arena))) << "profile must be captured into _resources";
        expect(gt(growth, 0UZ)) << "Graph's own bookkeeping must allocate from the profile arena, not the global heap";
    };

    // MCU superloop: setup may allocate, but steady-state work() + messages must touch neither global new nor the
    // (non-reclaiming) bump arena. arena = default resource, so stray allocs show as growth; sentinel catches global new.
    "Graph + Scheduler steady-state superloop allocates no runtime memory"_test = [] {
        auto                arenaPtr = std::make_unique<gr::pmr::OwnedStaticArenaResource<1UZ << 23U>>();
        auto&               arena    = *arenaPtr;
        gr::ResourceProfile profile{.data = &arena, .tag = &arena, .mechanics = &arena};

        gr::Graph graph(profile);
        auto&     src = graph.emplaceBlock<gr::testing::NullSource<float>>();
        auto&     snk = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, snk, {.minBufferSize = 4096UZ}).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value()) << "externalStep start() must prime to RUNNING without spawning a worker";

        gr::allocator::pmr::ScopedDefaultResource scoped(&arena);
        for (int i = 0; i < 8; ++i) {   // warm-up: settle first-touch lazy state before measuring
            std::ignore = sched.step(); // step() drains scheduled messages and runs one work() pass
        }
        const std::size_t usedAfterWarmup = arena.used();

        std::size_t hits      = 0UZ;
        std::size_t performed = 0UZ;
        {
            GlobalNewSentinel sentinel;
            for (int cycle = 0; cycle < 10'000; ++cycle) {
                performed += sched.step().performed_work;
            }
            hits = sentinel.delta();
        }
        const std::size_t growth = arena.used() - usedAfterWarmup;
        std::fputs(std::format("[diag] steady-state global-new hits: {}, arena growth: {} bytes\n", hits, growth).c_str(), stderr);
        expect(gt(performed, 0UZ)) << "superloop must actually execute work";
        expect(eq(hits, 0UZ)) << "steady-state superloop must not reach ::operator new";
        expect(eq(growth, 0UZ)) << "steady-state superloop must not grow the bump arena";

        std::ignore = sched.changeStateTo(gr::lifecycle::State::REQUESTED_STOP);
        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };
};

namespace {
/// Reaches the per-worker moved-block lists, which are protected: `step()` never visits them, so the
/// superloop scenario above cannot say anything about their cost.
struct MovedBlocksProbe : gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded> {
    using Simple::_movedBlocks;
    using Simple::cleanupRemovedBlocks;
};

struct MovedBlocksFixture {
    MovedBlocksProbe                             sched;
    std::vector<std::shared_ptr<gr::BlockModel>> list;

    MovedBlocksFixture() {
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     copy = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, copy).has_value() >> fatal);
        expect(graph.connect<"out", "in">(copy, sink).has_value() >> fatal);
        expect(sched.exchange(std::move(graph)).has_value() >> fatal);
        sched._movedBlocks.resize(1UZ); // one worker; the pool path sizes this on worker start
        list.assign(sched.graph().blocks().begin(), sched.graph().blocks().end());
    }
};
} // namespace

const boost::ut::suite<"moved-block cleanup"> _movedBlockCleanup = [] {
    "clearing moved blocks allocates nothing when none were moved"_test = [] {
        // Every pool-worker pass calls this, and a block is moved between workers only by grouping and
        // ungrouping, which is rare. Building sets to erase nothing cost an allocation per block per pass.
        MovedBlocksFixture fixture;
        const std::size_t  before = fixture.list.size();
        {
            GlobalNewSentinel sentinel;
            fixture.sched.cleanupRemovedBlocks(0UZ, fixture.list);
            expect(eq(sentinel.delta(), 0UZ)) << "with nothing moved away, a pass must not allocate to find that out";
        }
        expect(eq(fixture.list.size(), before)) << "and must leave the worker's list alone";
    };

    "clearing moved blocks still removes the ones that were moved"_test = [] {
        // The guard against the fix being too eager: a moved block leaves the worker's list and the moved
        // list; one the worker never held stays listed as moved, for the worker that does hold it.
        MovedBlocksFixture                    fixture;
        const std::shared_ptr<gr::BlockModel> moved   = fixture.list[1];
        const auto                            foreign = std::make_shared<gr::BlockWrapper<gr::testing::Copy<float>>>();
        fixture.sched._movedBlocks[0].blocks          = {moved, foreign};

        fixture.sched.cleanupRemovedBlocks(0UZ, fixture.list);

        expect(eq(fixture.list.size(), 2UZ));
        expect(std::ranges::find(fixture.list, moved) == fixture.list.end()) << "a moved block must leave the worker's list";
        expect(eq(fixture.sched._movedBlocks[0].blocks.size(), 1UZ)) << "and the moved list, once this worker has let go of it";
        expect(fixture.sched._movedBlocks[0].blocks.front() == foreign) << "a block this worker never held stays listed as moved";
    };
};

int main() { /* tests are statically executed */ }
