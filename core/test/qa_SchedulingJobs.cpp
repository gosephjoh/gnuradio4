#include <boost/ut.hpp>

#include <array>
#include <chrono>
#include <format>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>
#include <gnuradio-4.0/thread/thread_pool.hpp>

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
/// the job machinery rather than from a new selection order. Not a scheduling
/// policy anyone should use -- `EdfPolicy` is what will key on the deadlines this builds.
struct ReleaseProbePolicy {
    static constexpr std::string_view kName          = "ReleaseProbe";
    static constexpr PriorityClass    kPriorityClass = PriorityClass::fixedJob;

    [[nodiscard]] constexpr std::size_t key(const gr::BlockModel&, const SchedState& state) const noexcept { return state.index; }
};

static_assert(SchedulingPolicyLike<ReleaseProbePolicy>);

/// Appends its own name to a shared log on every invocation, so a test can observe selection order
/// directly rather than inferring it.
inline std::vector<std::string> gInvocationLog; // externalStep is single-threaded: no sync needed

template<typename T>
struct RecordingCopy : gr::Block<RecordingCopy<T>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(RecordingCopy, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) {
        gInvocationLog.emplace_back(this->name);
        const std::size_t n = std::min(input.size(), output.size());
        for (std::size_t i = 0UZ; i < n; ++i) {
            output[i] = input[i];
        }
        output.publish(n);
        std::ignore = input.consume(n);
        return gr::work::Status::OK;
    }
};

[[nodiscard]] std::size_t positionOf(std::string_view name) {
    const auto it = std::ranges::find(gInvocationLog, name);
    return it == gInvocationLog.end() ? std::numeric_limits<std::size_t>::max() : static_cast<std::size_t>(std::distance(gInvocationLog.begin(), it));
}

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

const boost::ut::suite<"earliest deadline first"> edfTests = [] {
    "a nearer deadline wins against a lower registration index"_test = [] {
        // Two independent chains, interleaved so that deadline order contradicts *position* order.
        // `b` is registered after `a` but carries the nearer deadline, so EDF must run it first --
        // whereas selecting the first block holding a job, which is what the loop did before the key
        // was consulted at all, would run `a`.
        gInvocationLog.clear();

        gr::Graph graph;
        auto&     srcA  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("srcA")}, {"n_samples_max", gr::Size_t{256U}}, {"relative_deadline", 0.010f}});
        auto&     a     = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::string("a")}, {"relative_deadline", 0.040f}});
        auto&     sinkA = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::string("sinkA")}, {"relative_deadline", 0.050f}});
        auto&     srcB  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("srcB")}, {"n_samples_max", gr::Size_t{256U}}, {"relative_deadline", 0.020f}});
        auto&     b     = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::string("b")}, {"relative_deadline", 0.030f}});
        auto&     sinkB = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::string("sinkB")}, {"relative_deadline", 0.060f}});

        expect(graph.connect<"out", "in">(srcA, a).has_value());
        expect(graph.connect<"out", "in">(a, sinkA).has_value());
        expect(graph.connect<"out", "in">(srcB, b).has_value());
        expect(graph.connect<"out", "in">(b, sinkB).has_value());

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());
        std::ignore = sched.step();
        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);

        expect(positionOf("a") != std::numeric_limits<std::size_t>::max()) << fatal << "both copies must have run";
        expect(positionOf("b") != std::numeric_limits<std::size_t>::max()) << fatal;
        expect(lt(positionOf("b"), positionOf("a"))) << "0.030 s deadline must precede 0.040 s, whatever the registration order";
    };

    "the key follows the front job, and an empty queue sorts last"_test = [] {
        const EdfPolicy policy{};

        std::array<Job, 4> storage{};
        SchedState         state{};
        state.jobs.storage = std::span<Job>{storage};

        gr::Graph graph;
        auto&     block       = graph.emplaceBlock<gr::testing::Copy<float>>();
        std::ignore           = block;
        gr::BlockModel& model = *graph.blocks()[0];

        expect(eq(policy.key(model, state), Clock::time_point::max().time_since_epoch().count())) << "an empty queue must not sort first";

        const Clock::time_point near = Clock::now() + seconds(1.0);
        const Clock::time_point far  = near + seconds(10.0);
        expect(state.jobs.push(Job{.batch = 1UZ, .absoluteDeadline = near}));
        expect(state.jobs.push(Job{.batch = 1UZ, .absoluteDeadline = far}));

        expect(eq(policy.key(model, state), near.time_since_epoch().count())) << "the front job sets the key";
        state.jobs.pop();
        expect(eq(policy.key(model, state), far.time_since_epoch().count())) << "retiring it re-keys the block";
    };

    "equal keys break on registration order"_test = [] {
        const EdfPolicy         policy{};
        const Clock::time_point deadline = Clock::now() + seconds(1.0);

        std::array<Job, 1> lhsStorage{};
        std::array<Job, 1> rhsStorage{};
        // A dynamic-key policy is never pre-sorted and never given a topological tie-break, so its
        // `tieBreak` is always the block's own position -- which is what `syncSchedStates` writes.
        SchedState lhs{.index = 7UZ, .tieBreak = 7UZ};
        SchedState rhs{.index = 2UZ, .tieBreak = 2UZ};
        lhs.jobs.storage = std::span<Job>{lhsStorage};
        rhs.jobs.storage = std::span<Job>{rhsStorage};
        expect(lhs.jobs.push(Job{.batch = 1UZ, .absoluteDeadline = deadline}));
        expect(rhs.jobs.push(Job{.batch = 1UZ, .absoluteDeadline = deadline}));

        gr::Graph graph;
        auto&     block       = graph.emplaceBlock<gr::testing::Copy<float>>();
        std::ignore           = block;
        gr::BlockModel& model = *graph.blocks()[0];

        expect(selectsBefore(policy, model, rhs, model, lhs)) << "index 2 before index 7 on an exact tie";
        expect(!selectsBefore(policy, model, lhs, model, rhs));
    };
};

namespace {

/// Runs one interleaved two-chain graph under the given selector and returns the invocation order.
/// Deadlines are set explicitly and 10 ms apart, far wider than any clock jitter between the two
/// runs, so the expected order is a property of the policy rather than of the timing.
std::vector<std::string> runUnder(gr::scheduler::SelectionStrategy strategy, std::size_t chains, std::size_t steps) {
    gInvocationLog.clear();

    gr::Graph graph;
    for (std::size_t c = 0UZ; c < chains; ++c) {
        const float deadline = 0.010f * static_cast<float>(chains - c); // later chains are more urgent
        auto&       src      = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::format("src{}", c)}, {"n_samples_max", gr::Size_t{512U}}, {"relative_deadline", deadline}});
        auto&       mid      = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::format("mid{}", c)}, {"relative_deadline", deadline}});
        auto&       sink     = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::format("sink{}", c)}, {"relative_deadline", deadline}});
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
    }

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy> sched;
    sched.selection_strategy = strategy;
    expect(sched.exchange(std::move(graph)).has_value());
    expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
    expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());
    for (std::size_t i = 0UZ; i < steps; ++i) {
        std::ignore = sched.step();
    }
    std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);

    return gInvocationLog;
}

} // namespace

const boost::ut::suite<"selector equivalence"> selectorTests = [] {
    "both selectors are offered and differ only in how the minimum is found"_test = [] {
        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy> sched;
        expect(sched.selection_strategy.value == gr::scheduler::SelectionStrategy::linearScan) << "the scan stays the default";
    };

    "the ready heap selects the same sequence as the linear scan"_test = [] {
        // This is what licenses the benchmark to be a comparison rather than two unrelated numbers,
        // and the guard against the heap drifting as either side is changed.
        for (std::size_t chains : {2UZ, 5UZ, 9UZ}) {
            const std::vector<std::string> scanned = runUnder(gr::scheduler::SelectionStrategy::linearScan, chains, 4UZ);
            const std::vector<std::string> heaped  = runUnder(gr::scheduler::SelectionStrategy::readyHeap, chains, 4UZ);

            expect(!scanned.empty()) << fatal << std::format("{} chains produced no invocations", chains);
            expect(eq(scanned.size(), heaped.size())) << std::format("{} chains: invocation counts differ", chains);
            expect(scanned == heaped) << std::format("{} chains: selection order differs\n  scan: {}\n  heap: {}", chains, std::format("{}", scanned), std::format("{}", heaped));
        }
    };

    "the heap honours the deadline order too"_test = [] {
        // The same contradiction between deadline order and registration order as the scan test,
        // run through the heap: chain 1 is registered later but is the more urgent.
        const std::vector<std::string> log = runUnder(gr::scheduler::SelectionStrategy::readyHeap, 2UZ, 1UZ);
        gInvocationLog                     = log;
        expect(positionOf("mid1") != std::numeric_limits<std::size_t>::max()) << fatal;
        expect(positionOf("mid0") != std::numeric_limits<std::size_t>::max()) << fatal;
        expect(lt(positionOf("mid1"), positionOf("mid0"))) << "the nearer deadline runs first under the heap as well";
    };
};

namespace {

/// All chains share one deadline, so the sources -- released together by the backstop, which takes a
/// single timestamp per pass -- carry *identical* absolute deadlines. That is the only way to make
/// the tie-break observable: elsewhere release times differ and so do the deadlines derived from them.
std::vector<std::string> runTied(gr::scheduler::SelectionStrategy strategy, std::size_t chains) {
    gInvocationLog.clear();

    gr::Graph graph;
    for (std::size_t c = 0UZ; c < chains; ++c) {
        auto& src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::format("tsrc{}", c)}, {"n_samples_max", gr::Size_t{256U}}, {"relative_deadline", 0.05f}});
        auto& mid  = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::format("tmid{}", c)}, {"relative_deadline", 0.05f}});
        auto& sink = graph.emplaceBlock<gr::testing::CountingSink<float>>({{"name", std::format("tsink{}", c)}, {"relative_deadline", 0.05f}});
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
    }

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy> sched;
    sched.selection_strategy = strategy;
    expect(sched.exchange(std::move(graph)).has_value());
    expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
    expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());
    std::ignore = sched.step();
    std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);

    return gInvocationLog;
}

} // namespace

const boost::ut::suite<"selector tie-breaking"> tieTests = [] {
    "equal deadlines resolve identically under both selectors"_test = [] {
        for (std::size_t chains : {3UZ, 6UZ}) {
            const std::vector<std::string> scanned = runTied(gr::scheduler::SelectionStrategy::linearScan, chains);
            const std::vector<std::string> heaped  = runTied(gr::scheduler::SelectionStrategy::readyHeap, chains);

            expect(!scanned.empty()) << fatal << std::format("{} tied chains produced no invocations", chains);
            expect(scanned == heaped) << std::format("{} tied chains: order differs\n  scan: {}\n  heap: {}", chains, std::format("{}", scanned), std::format("{}", heaped));
        }
    };

    "a tie resolves to the lower registration index"_test = [] {
        const std::vector<std::string> log = runTied(gr::scheduler::SelectionStrategy::linearScan, 3UZ);
        gInvocationLog                     = log;
        expect(lt(positionOf("tmid0"), positionOf("tmid1"))) << "registration order decides an exact tie";
        expect(lt(positionOf("tmid1"), positionOf("tmid2")));
    };
};

namespace {

/// `n_batches` is `min(pool->maxThreads(), nBlocks)`, so without pinning the worker count -- and
/// therefore the whole topology under test -- depends on the host's core count. A bounded pool
/// selected by name makes it the same everywhere.
constexpr std::string_view kTwoThreadPool = "qa_sched_jobs_two";

void registerTwoThreadPool() {
    using namespace gr::thread_pool;
    static std::once_flag once;
    std::call_once(once, [] {
        auto pool = std::make_shared<ThreadPoolWrapper>(std::make_unique<BasicThreadPool>(std::string(kTwoThreadPool), TaskType::CPU_BOUND, 2U, 2U), "CPU");
        Manager::instance().replacePool(std::string(kTwoThreadPool), std::move(pool));
    });
}

enum class Placement : std::uint8_t {
    crossWorker, /// emplaced chain-by-chain: with two workers every edge crosses one
    sameWorker   /// emplaced rank-by-rank: each chain lands entirely on one worker
};

/// Two three-block chains, placed by emplacement order (all assignment policies stripe by index,
/// so worker == index % nWorkers). Returns the two sinks: their `count` is a plain diagnostics
/// member written inside `processBulk`, so it has to be read from the block, not from the settings
/// store, which never sees it.
std::pair<gr::testing::CountingSink<float>*, gr::testing::CountingSink<float>*> buildTwoChains(gr::Graph& graph, Placement placement, gr::Size_t samples) {
    const gr::property_map srcA{{"name", std::string("srcA")}, {"n_samples_max", samples}, {"relative_deadline", 0.010f}};
    const gr::property_map srcB{{"name", std::string("srcB")}, {"n_samples_max", samples}, {"relative_deadline", 0.020f}};
    const gr::property_map midA{{"name", std::string("midA")}, {"relative_deadline", 0.010f}};
    const gr::property_map midB{{"name", std::string("midB")}, {"relative_deadline", 0.020f}};
    const gr::property_map sinkA{{"name", std::string("sinkA")}};
    const gr::property_map sinkB{{"name", std::string("sinkB")}};

    gr::testing::CountingSink<float>* outA = nullptr;
    gr::testing::CountingSink<float>* outB = nullptr;

    const auto wire = [&graph](auto& src, auto& mid, auto& sink) {
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
    };

    if (placement == Placement::crossWorker) {
        auto& a0 = graph.emplaceBlock<gr::testing::ConstantSource<float>>(srcA);
        auto& a1 = graph.emplaceBlock<gr::testing::Copy<float>>(midA);
        auto& a2 = graph.emplaceBlock<gr::testing::CountingSink<float>>(sinkA);
        auto& b0 = graph.emplaceBlock<gr::testing::ConstantSource<float>>(srcB);
        auto& b1 = graph.emplaceBlock<gr::testing::Copy<float>>(midB);
        auto& b2 = graph.emplaceBlock<gr::testing::CountingSink<float>>(sinkB);
        wire(a0, a1, a2);
        wire(b0, b1, b2);
        outA = std::addressof(a2);
        outB = std::addressof(b2);
    } else {
        auto& a0 = graph.emplaceBlock<gr::testing::ConstantSource<float>>(srcA);
        auto& b0 = graph.emplaceBlock<gr::testing::ConstantSource<float>>(srcB);
        auto& a1 = graph.emplaceBlock<gr::testing::Copy<float>>(midA);
        auto& b1 = graph.emplaceBlock<gr::testing::Copy<float>>(midB);
        auto& a2 = graph.emplaceBlock<gr::testing::CountingSink<float>>(sinkA);
        auto& b2 = graph.emplaceBlock<gr::testing::CountingSink<float>>(sinkB);
        wire(a0, a1, a2);
        wire(b0, b1, b2);
        outA = std::addressof(a2);
        outB = std::addressof(b2);
    }
    return {outA, outB};
}

/// Runs the fixture to completion and returns what each sink received. Only totals are returned:
/// nothing about ordering survives concurrency, so nothing about ordering is asserted. `runAndWait()`
/// joins the workers, so reading the counters afterwards needs no synchronisation of its own.
template<typename TPolicy>
std::pair<gr::Size_t, gr::Size_t> runThreaded(Placement placement, gr::Size_t samples, gr::scheduler::SelectionStrategy strategy = gr::scheduler::SelectionStrategy::linearScan, std::size_t* observedWorkers = nullptr) {
    registerTwoThreadPool();

    gr::Graph  graph;
    const auto sinks = buildTwoChains(graph, placement, samples);

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, TPolicy> sched{{"poolName", kTwoThreadPool}};
    sched.selection_strategy = strategy;
    expect(sched.exchange(std::move(graph)).has_value());

    if (observedWorkers != nullptr) {
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        const auto jobs  = sched.jobs();
        *observedWorkers = jobs == nullptr ? 0UZ : jobs->size();
    }

    expect(sched.runAndWait().has_value()) << "the graph must run to completion";

    return {sinks.first->count.value, sinks.second->count.value};
}

} // namespace

constexpr gr::Size_t kThreadedSamples = 4096U;

const boost::ut::suite<"multi-threaded release tracking"> threadedTests = [] {
    "the fixture really does get two workers"_test = [] {
        std::size_t workers = 0UZ;
        std::ignore         = runThreaded<EdfPolicy>(Placement::crossWorker, kThreadedSamples, gr::scheduler::SelectionStrategy::linearScan, &workers);
        expect(eq(workers, 2UZ)) << "otherwise the placement below means nothing";
    };

    "cross-worker chains deliver every sample"_test = [] {
        // Every edge crosses a worker boundary, so successor lists are empty by construction and the
        // per-sweep backstop is the only thing that can release a consumer.
        const auto [a, b] = runThreaded<EdfPolicy>(Placement::crossWorker, kThreadedSamples);
        expect(eq(a, kThreadedSamples)) << "chain A lost samples across the worker boundary";
        expect(eq(b, kThreadedSamples)) << "chain B lost samples across the worker boundary";
    };

    "same-worker chains deliver every sample"_test = [] {
        const auto [a, b] = runThreaded<EdfPolicy>(Placement::sameWorker, kThreadedSamples);
        expect(eq(a, kThreadedSamples));
        expect(eq(b, kThreadedSamples));
    };

    "both selectors agree under threads"_test = [] {
        const auto [scanA, scanB] = runThreaded<EdfPolicy>(Placement::crossWorker, kThreadedSamples, gr::scheduler::SelectionStrategy::linearScan);
        const auto [heapA, heapB] = runThreaded<EdfPolicy>(Placement::crossWorker, kThreadedSamples, gr::scheduler::SelectionStrategy::readyHeap);
        expect(eq(scanA, heapA));
        expect(eq(scanB, heapB));
        expect(eq(heapA, kThreadedSamples));
        expect(eq(heapB, kThreadedSamples));
    };

    "task-level fixed priority works across workers today"_test = [] {
        // `RateMonotonicPolicy` is `fixedTask`, so `hasStaticKey` holds, the list is pre-sorted and
        // the polling selection loop runs -- no release tracking, and therefore none of the
        // cross-worker problem. It is the deadline-derived ordering that *is* available under
        // threads right now.
        const auto [crossA, crossB] = runThreaded<RateMonotonicPolicy>(Placement::crossWorker, kThreadedSamples);
        expect(eq(crossA, kThreadedSamples));
        expect(eq(crossB, kThreadedSamples));

        const auto [sameA, sameB] = runThreaded<RateMonotonicPolicy>(Placement::sameWorker, kThreadedSamples);
        expect(eq(sameA, kThreadedSamples));
        expect(eq(sameB, kThreadedSamples));
    };

    "round robin is unaffected"_test = [] {
        // The behaviour-neutrality baseline: the same graphs under the default policy, which does no
        // release tracking at all.
        const auto [crossA, crossB] = runThreaded<RoundRobinPolicy>(Placement::crossWorker, kThreadedSamples);
        expect(eq(crossA, kThreadedSamples));
        expect(eq(crossB, kThreadedSamples));

        const auto [sameA, sameB] = runThreaded<RoundRobinPolicy>(Placement::sameWorker, kThreadedSamples);
        expect(eq(sameA, kThreadedSamples));
        expect(eq(sameB, kThreadedSamples));
    };
};

const boost::ut::suite<"end-of-stream readiness"> eosTests = [] {
    "work(1) on an ended, empty port reports DONE"_test = [] {
        // The assumption the whole termination argument rests on. If an ended but
        // empty port answers INSUFFICIENT_INPUT_ITEMS instead, a waived-floor job would never finish
        // the block and the worker would spin.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{8U}}});
        auto&     copy = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, copy).has_value());
        expect(graph.connect<"out", "in">(copy, sink).has_value());
        expect(graph.connectPendingEdges());

        gr::BlockModel& srcModel = *graph.blocks()[0];
        gr::BlockModel& midModel = *graph.blocks()[1];
        activate(srcModel);
        activate(midModel);

        for (std::size_t i = 0UZ; i < 4UZ && srcModel.state() == gr::lifecycle::State::RUNNING; ++i) {
            std::ignore = srcModel.work(64UZ);
        }
        for (std::size_t i = 0UZ; i < 8UZ && midModel.availableInputSamples(true)[0UZ] > 0UZ; ++i) {
            std::ignore = midModel.work(64UZ);
        }

        expect(eq(midModel.availableInputSamples(true)[0UZ], 0UZ)) << fatal << "the port must be empty for this to mean anything";
        expect(midModel.state() == gr::lifecycle::State::RUNNING) << fatal << "and the block still running";

        const gr::work::Result result = midModel.work(1UZ);
        expect(result.status == gr::work::Status::DONE) << std::format("an ended, empty port must terminate the block; got status {}", static_cast<int>(result.status));
    };
};

namespace {

/// `src -> copy -> sink` driven until the source has stopped and published its end-of-stream tag,
/// leaving the copy holding `samples` samples with EOS pending behind them.
struct EndedChain {
    gr::Graph       graph;
    gr::BlockModel* src = nullptr;
    gr::BlockModel* mid = nullptr;

    explicit EndedChain(gr::Size_t samples) {
        auto& source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", samples}});
        auto& copy   = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto& sink   = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(source, copy).has_value());
        expect(graph.connect<"out", "in">(copy, sink).has_value());
        expect(graph.connectPendingEdges());

        src = graph.blocks()[0].get();
        mid = graph.blocks()[1].get();
        activate(*src);
        activate(*mid);

        for (std::size_t i = 0UZ; i < 8UZ && src->state() == gr::lifecycle::State::RUNNING; ++i) {
            std::ignore = src->work(2UZ * static_cast<std::size_t>(samples));
        }
        expect(src->state() != gr::lifecycle::State::RUNNING) << fatal << "the source must have stopped";
        expect(mid->inputStreamEnded()) << fatal << "and its end-of-stream tag must have reached the consumer";
        expect(eq(mid->availableInputSamples(true)[0UZ], static_cast<std::size_t>(samples))) << fatal;
    }
};

} // namespace

const boost::ut::suite<"end-of-stream release guards"> eosGuardTests = [] {
    constexpr gr::Size_t kLeftover = 8U;

    "end of stream waives the batch floor"_test = [] {
        EndedChain chain{kLeftover};

        StateFixture fixture{4UZ};
        fixture.state.batchFloor = 64UZ; // far above what is left, so the gate is shut for ever

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(!fixture.state.jobs.empty()) << fatal << "an ended stream must release what is left";
        expect(eq(fixture.state.jobs.front().batch, static_cast<std::size_t>(kLeftover))) << "the drain job takes the remainder";
    };

    "the floor is waived only while the ring is empty"_test = [] {
        // A shut gate can equally mean the samples are committed to outstanding jobs rather than
        // absent. Waiving then would hand the same samples to a second job.
        EndedChain chain{kLeftover};

        StateFixture fixture{4UZ};
        fixture.state.batchFloor = 64UZ;

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.jobs.size, 1UZ)) << fatal;
        expect(eq(fixture.state.assignedSamples, static_cast<std::size_t>(kLeftover)));

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(eq(fixture.state.jobs.size, 1UZ)) << "the waiver must not fire again while a job is outstanding";
        expect(eq(fixture.state.assignedSamples, static_cast<std::size_t>(kLeftover))) << "and must not commit the same samples twice";
    };

    "the temporal gate is waived on the terminal path"_test = [] {
        // Otherwise shutdown latency would scale with the longest period in the graph.
        EndedChain chain{kLeftover};

        StateFixture fixture{4UZ};
        fixture.state.batchFloor    = 64UZ;
        fixture.state.periodSeconds = 1000.0;
        fixture.state.lastRelease   = Clock::now(); // a full period away from being due

        releaseIfEligible(*chain.mid, fixture.state, Clock::now());
        expect(!fixture.state.jobs.empty()) << "a terminating block does not wait out a period to run its last job";
    };

    "a stopped block is marked finished, a paused one is not"_test = [] {
        // A source that stops itself is never released again, so nothing else would ever report DONE
        // for it and a worker would wait on it for ever. A paused block may yet resume.
        Chain paused;
        paused.prime(64UZ);
        activate(*paused.mid);
        expect(paused.mid->changeStateTo(gr::lifecycle::State::REQUESTED_PAUSE).has_value());
        expect(paused.mid->changeStateTo(gr::lifecycle::State::PAUSED).has_value());

        StateFixture pausedState{4UZ};
        releaseIfEligible(*paused.mid, pausedState.state, Clock::now());
        expect(!pausedState.state.finished) << "a paused block must not be written off";
        expect(pausedState.state.jobs.empty()) << "nor accumulate jobs while paused";

        Chain stopped;
        stopped.prime(64UZ);
        activate(*stopped.mid);
        expect(stopped.mid->changeStateTo(gr::lifecycle::State::REQUESTED_STOP).has_value());

        StateFixture stoppedState{4UZ};
        releaseIfEligible(*stopped.mid, stoppedState.state, Clock::now());
        expect(stoppedState.state.finished) << "a block that is shutting down is finished for scheduling purposes";
    };
};

namespace {

/// Reports ERROR without consuming or publishing, so a test can reach the selectors' error path.
template<typename T>
struct FailingCopy : gr::Block<FailingCopy<T>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(FailingCopy, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) const noexcept {
        std::ignore = input.consume(0UZ);
        output.publish(0UZ);
        return gr::work::Status::ERROR;
    }
};

/// One `step()` over `chains` independent source→RecordingCopy→sink chains, returning how many
/// recorded blocks ran. `bound` is `max_selections_per_pass`.
std::size_t recordedRunsInOneStep(gr::Size_t bound, std::size_t chains, gr::scheduler::SelectionStrategy strategy) {
    gInvocationLog.clear();

    gr::Graph graph;
    for (std::size_t c = 0UZ; c < chains; ++c) {
        auto& src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::format("bsrc{}", c)}, {"n_samples_max", gr::Size_t{512U}}});
        auto& mid  = graph.emplaceBlock<RecordingCopy<float>>({{"name", std::format("bmid{}", c)}});
        auto& sink = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::format("bsink{}", c)}});
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
    }

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy> sched;
    sched.selection_strategy      = strategy;
    sched.max_selections_per_pass = bound;
    expect(sched.exchange(std::move(graph)).has_value());
    expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
    expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());
    std::ignore = sched.step();
    std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);

    return gInvocationLog.size();
}

} // namespace

const boost::ut::suite<"dynamic selector error and bound"> selectorEdgeTests = [] {
    "a block reporting ERROR aborts the pass"_test = [] {
        for (const auto strategy : {gr::scheduler::SelectionStrategy::linearScan, gr::scheduler::SelectionStrategy::readyHeap}) {
            gr::Graph graph;
            auto&     src     = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{512U}}});
            auto&     failing = graph.emplaceBlock<FailingCopy<float>>();
            auto&     sink    = graph.emplaceBlock<gr::testing::NullSink<float>>();
            expect(graph.connect<"out", "in">(src, failing).has_value());
            expect(graph.connect<"out", "in">(failing, sink).has_value());

            gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, EdfPolicy> sched;
            sched.selection_strategy = strategy;
            expect(sched.exchange(std::move(graph)).has_value());
            expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
            expect(sched.changeStateTo(gr::lifecycle::State::RUNNING).has_value());

            const gr::work::Result result = sched.step();
            expect(result.status == gr::work::Status::ERROR) << "the selector must surface a block's ERROR, not swallow it";

            std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
        }
    };

    "max_selections_per_pass bounds the dynamic loop"_test = [] {
        // M2e covered the bound for the static-key loop; the dynamic loop re-derives it and never
        // checked. Four chains are released together by the backstop, so a tight bound has to cut
        // the pass short and a generous one must not.
        constexpr std::size_t kChains = 4UZ;

        for (const auto strategy : {gr::scheduler::SelectionStrategy::linearScan, gr::scheduler::SelectionStrategy::readyHeap}) {
            const std::size_t tight    = recordedRunsInOneStep(2U, kChains, strategy);
            const std::size_t generous = recordedRunsInOneStep(64U, kChains, strategy);

            expect(le(tight, 2UZ)) << "a bound of two cannot admit more than two selections";
            expect(eq(generous, kChains)) << "and a generous bound lets every chain's middle block run";
            expect(lt(tight, generous)) << "so the bound is actually binding";
        }
    };
};

namespace {

/// Feeds its own asynchronous input from its output, so it is its own successor. Asynchronous, so
/// the loop port never gates the block and no feedback priming is needed.
template<typename T>
struct SelfLoop : gr::Block<SelfLoop<T>> {
    gr::PortIn<T>            in;
    gr::PortIn<T, gr::Async> loop;
    gr::PortOut<T>           out;

    GR_MAKE_REFLECTABLE(SelfLoop, in, loop, out);

    std::size_t processed = 0UZ;

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::InputSpanLike auto& fed, gr::OutputSpanLike auto& output) noexcept {
        const std::size_t n = std::min(input.size(), output.size());
        for (std::size_t i = 0UZ; i < n; ++i) {
            output[i] = input[i];
        }
        processed += n;
        output.publish(n);
        std::ignore = input.consume(n);
        std::ignore = fed.consume(fed.size()); // drain the feedback so the loop buffer cannot fill
        return gr::work::Status::OK;
    }
};

} // namespace

const boost::ut::suite<"feedback self-successor"> selfLoopTests = [] {
    "a block that is its own successor runs correctly under both selectors"_test = [] {
        // `onNewlyReady` skips the block currently running: it is out of the heap and is re-pushed
        // once its job completes, so without that guard a self-edge inserts a duplicate entry and the
        // block can be popped a second time with an empty queue.
        constexpr gr::Size_t kSamples = 2048U;

        for (const auto strategy : {gr::scheduler::SelectionStrategy::linearScan, gr::scheduler::SelectionStrategy::readyHeap}) {
            gr::Graph graph;
            auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", kSamples}});
            auto&     loop = graph.emplaceBlock<SelfLoop<float>>();
            expect(graph.connect<"out", "in">(src, loop).has_value());
            expect(graph.connect<"out", "loop">(loop, loop).has_value()) << fatal << "the self-edge must connect";

            gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded, gr::profiling::null::Profiler, EdfPolicy> sched;
            sched.selection_strategy = strategy;
            expect(sched.exchange(std::move(graph)).has_value());
            expect(sched.runAndWait().has_value()) << "a self-edge must not wedge or crash the selector";
            expect(eq(loop.processed, static_cast<std::size_t>(kSamples))) << "and every sample must still be processed exactly once";
        }
    };
};

int main() { /* tests are statically executed */ }
