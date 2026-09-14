#include <boost/ut.hpp>

#include <array>
#include <chrono>
#include <span>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::scheduler;

using Clock = std::chrono::steady_clock;

namespace {

constexpr Clock::duration seconds(double value) { return std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(value)); }

/// Owns the ring storage a `SchedState` spans into, so a test can exercise release without a
/// scheduler. Fixed size at construction: the span must not be invalidated.
struct StateFixture {
    std::vector<Job> storage;
    SchedState       state{};

    explicit StateFixture(std::size_t capacity) : storage(capacity) { state.jobs.storage = std::span<Job>{storage}; }
};

void activate(gr::BlockModel& block) {
    expect(block.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
    expect(block.changeStateTo(gr::lifecycle::State::RUNNING).has_value());
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
        expect(graph.connect<"out", "in">(src, copy).has_value());
        expect(graph.connect<"out", "in">(copy, sink).has_value());
        expect(graph.connectPendingEdges());
        source = graph.blocks()[0].get();
        mid    = graph.blocks()[1].get();
    }

    /// Only valid before activation: priming a RUNNING block is refused by design.
    ///
    /// N.B. `prime()` and `pump()` must not be mixed on the same edge. Priming a *connected* input
    /// attaches a second writer to a single-producer buffer, so a subsequent publish by the real
    /// producer advances the write cursor without the reader ever seeing those samples.
    void prime(std::size_t n) { expect(mid->primeInputPort(0UZ, n).has_value()); }

    void activateAll() {
        activate(*source);
        activate(*mid);
    }

    /// Data arriving *while running*, the only way it can: by the producer executing.
    void pump(std::size_t n) { std::ignore = source->work(n); }
};

/// Declares the `fixedJob` class purely to engage release tracking. Its key is the block's
/// position, so the ordering is the identity permutation and any behaviour seen under it comes from
/// the job machinery rather than from a new selection order (DEVLOG_M3 §5.9). Not a scheduling
/// policy anyone should use -- `EdfPolicy` is what will key on the deadlines this builds.
struct ReleaseProbePolicy {
    static constexpr std::string_view kName          = "ReleaseProbe";
    static constexpr PriorityClass    kPriorityClass = PriorityClass::fixedJob;

    [[nodiscard]] constexpr std::size_t key(const gr::BlockModel&, const SchedState& state) const noexcept { return state.index; }
};

static_assert(SchedulingPolicyLike<ReleaseProbePolicy>);

} // namespace

const boost::ut::suite<"job queue"> jobQueueTests = [] {
    "push, pop and wrap around fixed storage"_test = [] {
        std::array<Job, 3> storage{};
        JobQueue           queue{.storage = std::span<Job>{storage}};

        expect(queue.empty());
        expect(!queue.full());
        expect(eq(queue.capacity(), 3UZ));

        expect(queue.push(Job{.batch = 1UZ}));
        expect(queue.push(Job{.batch = 2UZ}));
        expect(queue.push(Job{.batch = 3UZ}));
        expect(queue.full());
        expect(!queue.push(Job{.batch = 4UZ})) << "a full ring refuses rather than overwriting";

        expect(eq(queue.front().batch, 1UZ));
        queue.pop();
        expect(eq(queue.front().batch, 2UZ));
        expect(queue.push(Job{.batch = 4UZ})) << "space freed by the pop wraps around";
        queue.pop();
        queue.pop();
        expect(eq(queue.front().batch, 4UZ));
        queue.pop();
        expect(queue.empty());
    };

    "an empty ring is inert"_test = [] {
        JobQueue queue{};
        expect(queue.empty());
        expect(queue.full()) << "zero capacity can accept nothing";
        expect(!queue.push(Job{.batch = 1UZ}));
        queue.pop(); // must not underflow
        expect(queue.empty());
    };
};

const boost::ut::suite<"job release"> releaseTests = [] {
    "the data gate withholds a release below the batch floor"_test = [] {
        Chain below;
        below.prime(3UZ);
        activate(*below.mid);

        StateFixture starved{4UZ};
        starved.state.batchFloor = 8UZ;
        releaseIfEligible(*below.mid, starved.state, Clock::now());
        expect(starved.state.jobs.empty()) << "3 samples against a floor of 8";

        Chain atFloor;
        atFloor.prime(8UZ);
        activate(*atFloor.mid);

        StateFixture ready{4UZ};
        ready.state.batchFloor = 8UZ;
        releaseIfEligible(*atFloor.mid, ready.state, Clock::now());
        expect(!ready.state.jobs.empty()) << "8 samples reaches the floor";
        expect(eq(ready.state.jobs.front().batch, 8UZ));
    };

    "the temporal gate withholds a release before the period elapses"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        const Clock::time_point now = Clock::now();

        StateFixture fixture{4UZ};
        fixture.state.periodSeconds = 10.0;
        fixture.state.lastRelease   = now;

        releaseIfEligible(*chain.mid, fixture.state, now + seconds(1.0));
        expect(fixture.state.jobs.empty()) << "one second into a ten-second period";

        releaseIfEligible(*chain.mid, fixture.state, now + seconds(10.5));
        expect(!fixture.state.jobs.empty()) << "past the period, and data is waiting";
    };

    "a zero period imposes no temporal gate"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        StateFixture fixture{4UZ};
        fixture.state.periodSeconds = 0.0;
        fixture.state.lastRelease   = Clock::now();

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(!fixture.state.jobs.empty()) << "unset period releases on data alone";
    };

    "release is sporadic -- the gate measures from the last actual release"_test = [] {
        Chain chain;
        chain.prime(512UZ);
        activate(*chain.mid);

        const Clock::time_point start = Clock::now();

        StateFixture fixture{8UZ};
        fixture.state.periodSeconds = 1.0;
        fixture.state.batchCeiling  = 4UZ;

        releaseIfEligible(*chain.mid, fixture.state, start);
        expect(eq(fixture.state.jobs.size, 1UZ));
        expect(eq(fixture.state.lastRelease.time_since_epoch().count(), start.time_since_epoch().count()));

        // Detected late: a grid model would back-date this to start + 1s, the sporadic model dates
        // it to when eligibility was actually observed.
        const Clock::time_point late = start + seconds(3.7);
        releaseIfEligible(*chain.mid, fixture.state, late);
        expect(eq(fixture.state.jobs.size, 2UZ));
        expect(eq(fixture.state.lastRelease.time_since_epoch().count(), late.time_since_epoch().count()));

        releaseIfEligible(*chain.mid, fixture.state, late + seconds(0.5));
        expect(eq(fixture.state.jobs.size, 2UZ)) << "half a period after the last release, still gated";
    };

    "the batch is min(unassigned, ceiling) and excludes prior jobs"_test = [] {
        Chain chain;
        chain.prime(100UZ);
        activate(*chain.mid);

        StateFixture fixture{8UZ};
        fixture.state.batchCeiling = 30UZ;

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.jobs.front().batch, 30UZ)) << "clipped by the ceiling";
        expect(eq(fixture.state.assignedSamples, 30UZ));

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.jobs.size, 2UZ));
        expect(eq(fixture.state.assignedSamples, 60UZ)) << "the second job claims different samples";

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.assignedSamples, 100UZ)) << "never more than the buffer holds";
        expect(eq(fixture.state.jobs.size, 4UZ));

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.jobs.size, 4UZ)) << "nothing unassigned is left";
    };

    "a released batch is frozen against later arrivals"_test = [] {
        Chain chain;
        chain.activateAll();
        chain.pump(16UZ);

        const std::size_t beforeRelease = chain.mid->availableInputSamples(true)[0UZ];
        expect(eq(beforeRelease, 16UZ));

        StateFixture fixture{4UZ};
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        const std::size_t atRelease = fixture.state.jobs.front().batch;
        expect(eq(atRelease, 16UZ));

        chain.pump(256UZ);
        expect(gt(chain.mid->availableInputSamples(true)[0UZ], atRelease)) << "more data really did arrive";
        expect(eq(fixture.state.jobs.front().batch, atRelease)) << "later samples do not resize an admitted job";
    };

    "retiring a job returns its whole assignment"_test = [] {
        Chain chain;
        chain.prime(100UZ);
        activate(*chain.mid);

        StateFixture fixture{4UZ};
        fixture.state.batchCeiling = 40UZ;
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.assignedSamples, 40UZ));

        retireFrontJob(fixture.state);
        expect(eq(fixture.state.assignedSamples, 0UZ)) << "the remainder returns to the pool, not a carried balance";
        expect(fixture.state.jobs.empty());

        retireFrontJob(fixture.state); // must be inert on an empty queue
        expect(eq(fixture.state.assignedSamples, 0UZ));
    };

    "the ledger fails closed when availability falls below what is committed"_test = [] {
        Chain chain;
        chain.prime(16UZ);
        activate(*chain.mid);

        StateFixture fixture{4UZ};
        fixture.state.assignedSamples = 1000UZ; // more than the buffer holds

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(fixture.state.jobs.empty()) << "saturating subtraction closes the gate instead of wrapping open";
    };

    "a full ring counts an overrun rather than dropping silently"_test = [] {
        Chain chain;
        chain.prime(100UZ);
        activate(*chain.mid);

        StateFixture fixture{2UZ};
        fixture.state.batchCeiling = 10UZ;

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(fixture.state.jobs.full());
        expect(eq(fixture.state.overruns, 0UZ));

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.jobs.size, 2UZ));
        expect(eq(fixture.state.overruns, 1UZ));
        expect(eq(fixture.state.assignedSamples, 20UZ)) << "a dropped release commits no samples";
    };

    "a block that is not running accumulates nothing"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        // deliberately not activated

        StateFixture fixture{4UZ};
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(fixture.state.jobs.empty());
    };

    "a finished block accumulates nothing"_test = [] {
        Chain chain;
        chain.prime(64UZ);
        activate(*chain.mid);

        StateFixture fixture{4UZ};
        fixture.state.finished = true;
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(fixture.state.jobs.empty());
    };

    "a source releases on the temporal gate, batched by output space"_test = [] {
        Chain chain;
        activate(*chain.source);

        StateFixture fixture{4UZ};
        fixture.state.batchCeiling = 256UZ;

        releaseIfEligible(*chain.source, fixture.state, Clock::now());
        expect(!fixture.state.jobs.empty()) << "no input to wait for";
        expect(eq(fixture.state.jobs.front().batch, 256UZ)) << "bounded by the ceiling, never SIZE_MAX";
        expect(lt(fixture.state.jobs.front().batch, gr::undefined_size));
    };
};

const boost::ut::suite<"job deadlines"> deadlineTests = [] {
    "an absolute deadline is the release instant plus the relative deadline"_test = [] {
        Chain chain;
        chain.prime(16UZ);
        activate(*chain.mid);

        const Clock::time_point now = Clock::now();

        StateFixture fixture{4UZ};
        fixture.state.relativeDeadlineSeconds = 0.25;

        releaseIfEligible(*chain.mid, fixture.state, now);
        expect(eq(fixture.state.jobs.front().releaseTime.time_since_epoch().count(), now.time_since_epoch().count()));
        expect(eq(fixture.state.jobs.front().absoluteDeadline.time_since_epoch().count(), (now + seconds(0.25)).time_since_epoch().count()));
    };

    "an implicit deadline falls back to the period"_test = [] {
        Chain chain;
        chain.prime(16UZ);
        activate(*chain.mid);

        const Clock::time_point now = Clock::now();

        StateFixture fixture{4UZ};
        fixture.state.periodSeconds = 0.5;

        releaseIfEligible(*chain.mid, fixture.state, now);
        expect(eq(fixture.state.jobs.front().absoluteDeadline.time_since_epoch().count(), (now + seconds(0.5)).time_since_epoch().count()));
    };

    "no period and no deadline sorts last, never first"_test = [] {
        Chain chain;
        chain.prime(16UZ);
        activate(*chain.mid);

        StateFixture fixture{4UZ};
        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.jobs.front().absoluteDeadline.time_since_epoch().count(), Clock::time_point::max().time_since_epoch().count()));
    };

    "an arbitrary deadline (D > T) legitimately holds several jobs outstanding"_test = [] {
        Chain chain;
        chain.prime(512UZ);
        activate(*chain.mid);

        const Clock::time_point start = Clock::now();

        StateFixture fixture{8UZ};
        fixture.state.periodSeconds           = 1.0;
        fixture.state.relativeDeadlineSeconds = 4.0; // D > T: by design, not overload
        fixture.state.batchCeiling            = 16UZ;

        for (std::size_t i = 0UZ; i < 3UZ; ++i) {
            releaseIfEligible(*chain.mid, fixture.state, start + seconds(static_cast<double>(i) * 1.5));
        }

        expect(eq(fixture.state.jobs.size, 3UZ)) << "three releases, none yet retired";
        expect(gt(fixture.state.jobs.front().absoluteDeadline.time_since_epoch().count(), (start + seconds(3.0)).time_since_epoch().count())) << "the first job is not yet late";
    };
};

const boost::ut::suite<"outstanding-job bound"> boundTests = [] {
    "the bound is the gating capacity divided by the batch floor"_test = [] {
        Chain chain;

        const std::size_t capacity = gatingCapacity(*chain.mid);
        expect(gt(capacity, 0UZ));
        expect(lt(capacity, gr::undefined_size));

        expect(eq(maxOutstandingJobs(*chain.mid, 1UZ), capacity));
        expect(eq(maxOutstandingJobs(*chain.mid, 8UZ), capacity / 8UZ));
        expect(eq(maxOutstandingJobs(*chain.mid, capacity * 2UZ), 1UZ)) << "clamped to at least one job";
        expect(eq(maxOutstandingJobs(*chain.mid, 0UZ), capacity)) << "a zero floor is treated as one";
    };

    "the cap clamps the derived bound without ever reaching zero"_test = [] {
        Chain             chain;
        const std::size_t capacity = gatingCapacity(*chain.mid);
        expect(gt(capacity, 64UZ)) << "otherwise the clamp below is not actually binding";

        expect(eq(maxOutstandingJobs(*chain.mid, 1UZ, 64UZ), 64UZ)) << "the cap binds";
        expect(eq(maxOutstandingJobs(*chain.mid, 1UZ, capacity * 2UZ), capacity)) << "a cap above the derived bound does nothing";
        expect(eq(maxOutstandingJobs(*chain.mid, 1UZ, 0UZ), 1UZ)) << "a zero cap still leaves room for one job";
        expect(eq(maxOutstandingJobs(*chain.mid, capacity, 64UZ), 1UZ)) << "the smaller of the two wins";
    };

    "the scheduler's cap bounds the arena it allocates"_test = [] {
        // The reason the cap exists: the derived bound is correct but reserves a ring per block
        // sized to the whole input buffer, which is megabytes for a graph of a few blocks.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{64U}}});
        auto&     copy = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::CountingSink<float>>();
        expect(graph.connect<"out", "in">(src, copy).has_value());
        expect(graph.connect<"out", "in">(copy, sink).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded, gr::profiling::null::Profiler, ReleaseProbePolicy> sched;
        expect(eq(sched.max_outstanding_jobs.value, decltype(sched)::kDefaultMaxOutstandingJobs));
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.runAndWait().has_value());
        expect(eq(sink.count.value, gr::Size_t{64U})) << "capping outstanding jobs does not lose samples";
    };

    "a source is bounded by its output capacity"_test = [] {
        Chain             chain;
        const std::size_t capacity = gatingCapacity(*chain.source);
        expect(gt(capacity, 0UZ)) << "an input-less block falls back to the output side";
        expect(lt(capacity, gr::undefined_size));
    };
};

const boost::ut::suite<"release-tracking scheduler"> integrationTests = [] {
    "a graph runs to completion under a release-tracking policy"_test = [] {
        constexpr gr::Size_t kSamples = 1024U;

        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", kSamples}});
        auto&     copy = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::CountingSink<float>>();
        expect(graph.connect<"out", "in">(src, copy).has_value());
        expect(graph.connect<"out", "in">(copy, sink).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded, gr::profiling::null::Profiler, ReleaseProbePolicy> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.runAndWait().has_value());
        expect(eq(sink.count.value, kSamples)) << "every sample arrives, as under round robin";
    };

    "event-driven detection carries data through the chain within a single pass"_test = [] {
        // The discriminator between the two detection points. The backstop runs once per pass, and
        // at the top of the first pass only the source is eligible -- its consumers have no input
        // yet. So if the sink has received anything after exactly one `step()`, the releases that
        // fed it can only have come from event-driven detection after each `work()`.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{4096U}}});
        auto&     copy = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::CountingSink<float>>();
        expect(graph.connect<"out", "in">(src, copy).has_value());
        expect(graph.connect<"out", "in">(copy, sink).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, ReleaseProbePolicy> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());

        std::ignore = sched.step();
        expect(gt(sink.count.value, gr::Size_t{0U})) << "successors released within the pass, not on the next one";

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a release-tracking policy leaves the shared job list in registration order"_test = [] {
        // `fixedJob` has no static key, so `applyStaticOrder` must not permute anything: the
        // successor indices built afterwards address positions, and a silent reorder would
        // mis-wire them.
        gr::Graph graph;
        auto&     a = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("a")}});
        auto&     b = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("b")}});
        expect(graph.connect<"out", "in">(a, b).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, ReleaseProbePolicy> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

        const auto jobs = sched.jobs();
        expect(jobs != nullptr and !jobs->empty()) << fatal;
        expect(eq((*jobs)[0][0]->name(), std::string("a")));
        expect(eq((*jobs)[0][1]->name(), std::string("b")));

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };
};

int main() { /* tests are statically executed */ }
