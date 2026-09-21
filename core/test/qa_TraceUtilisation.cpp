#include <boost/ut.hpp>

#include <cstdint>
#include <cstdlib>
#include <format>
#include <map>
#include <set>
#include <span>
#include <string>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>
#include <gnuradio-4.0/TraceReport.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::trace;

/**
 * Worker CPU utilisation, built from hand-made records.
 *
 * The decomposition is arithmetic over a set of intervals, so it is testable without a scheduler at
 * all — and it must be, because a real worker produces different durations on every run. The
 * timing-dependent half of this feature is covered separately by invariants; here every answer is
 * known by construction.
 */
namespace {

[[nodiscard]] Event started(std::uint8_t worker, std::uint64_t atNs) { return Event{.startNs = atNs, .kind = Kind::workerStart, .workerId = worker}; }

[[nodiscard]] Event stopped(std::uint8_t worker, std::uint64_t atNs) { return Event{.startNs = atNs, .kind = Kind::workerStop, .workerId = worker}; }

/// `cpuUs` is the on-core time the worker loop consumed, as the emitter records it.
[[nodiscard]] Event stoppedWithCpu(std::uint8_t worker, std::uint64_t atNs, std::uint32_t cpuUs) { return Event{.startNs = atNs, .payload2 = cpuUs, .kind = Kind::workerStop, .workerId = worker, .flags = flag::kThreadCpuValid}; }

[[nodiscard]] Event interval(std::uint8_t worker, Kind kind, std::uint64_t atNs, std::uint32_t durationNs) { return Event{.startNs = atNs, .durationNs = durationNs, .kind = kind, .workerId = worker}; }

[[nodiscard]] Event probe(std::uint8_t worker, std::uint32_t count, std::uint32_t probeNs, EntityId entity = EntityId{1U}) { return Event{.payload0 = count, .payload1 = probeNs, .entity = entity, .kind = Kind::workProbe, .workerId = worker}; }

/// A worker whose life divides exactly: 1000 ns long, 600 sweeping (of which 400 productive and 100
/// probing), 100 in messages, 200 idle, leaving 100 unaccounted.
[[nodiscard]] std::vector<Event> knownWorker(std::uint8_t worker = 0U) {
    return {
        started(worker, 1000UL),                            //
        interval(worker, Kind::messagePhase, 1000UL, 100U), //
        interval(worker, Kind::sweep, 1100UL, 600U),        //
        interval(worker, Kind::workEnd, 1150UL, 400U),      //
        probe(worker, 3U, 100U),                            //
        interval(worker, Kind::idle, 1700UL, 200U),         //
        stopped(worker, 2000UL),                            //
    };
}

[[nodiscard]] const WorkerUtilisation& only(const std::vector<WorkerUtilisation>& all) {
    expect(eq(all.size(), 1UZ) >> fatal) << "expected exactly one worker in the capture";
    return all.front();
}

} // namespace

const boost::ut::suite<"TraceUtilisation"> traceUtilisationTests = [] {
    "a worker's life decomposes into terms that are each recoverable"_test = [] {
        const WorkerUtilisation w = only(workerUtilisation(knownWorker(), 0UL));
        expect(w.computed >> fatal) << w.reason;

        expect(eq(w.lifetimeNs, 1000UL));
        expect(eq(w.messagePhaseNs, 100UL));
        expect(eq(w.sweepNs, 600UL));
        expect(eq(w.idleNs, 200UL));
        expect(eq(w.blockProductiveNs, 400UL));
        expect(eq(w.blockProbeNs, 100UL));
        expect(eq(w.blockNs(), 500UL));
        expect(eq(w.unaccountedNs, 100UL)) << "1000 - (100 message + 600 sweep + 200 idle)";
    };

    "productive and probe time are separate terms, and both are block execution"_test = [] {
        // Probing is time spent inside work() that produced nothing. It is not scheduler overhead --
        // folding it there would understate what the blocks cost and overstate the scheduler.
        const WorkerUtilisation w = only(workerUtilisation(knownWorker(), 0UL));
        expect(w.computed >> fatal);
        expect(eq(w.blockProductiveNs, 400UL));
        expect(eq(w.blockProbeNs, 100UL));
        expect(eq(w.blockNs(), w.blockProductiveNs + w.blockProbeNs));
        expect(neq(w.blockProbeNs, 0UL)) << "a capture with probes must not report them as zero";
    };

    "occupancy and utilisation use different denominators"_test = [] {
        const WorkerUtilisation w = only(workerUtilisation(knownWorker(), 0UL));
        expect(w.computed >> fatal);

        // 500 of 1000 ns alive; 500 of the 800 ns it was not deliberately waiting.
        expect(eq(w.occupancy, 0.5)) << "block time over the whole life";
        expect(w.hasUtilisation >> fatal);
        expect(eq(w.utilisation, 0.625)) << "block time over the life minus idle";
        expect(gt(w.utilisation, w.occupancy)) << "excluding idle can only raise the ratio";
    };

    "utilisation is refused for a worker that only ever idled"_test = [] {
        // Occupancy of 0 % is true and meaningful. "Share of active time" has no meaning when there
        // was no active time, and reporting it as 0 % would present an idle worker as a busy one
        // that achieved nothing.
        const std::vector<Event> allIdle{
            started(0U, 0UL),                     //
            interval(0U, Kind::idle, 0UL, 1000U), //
            stopped(0U, 1000UL),                  //
        };
        const WorkerUtilisation w = only(workerUtilisation(allIdle, 0UL));
        expect(w.computed >> fatal) << w.reason;
        expect(eq(w.occupancy, 0.0));
        expect(!w.hasUtilisation) << "the denominator would be zero, so the ratio is undefined rather than zero";
        expect(eq(w.idleNs, 1000UL));
    };

    "the two clocks give on-core time and preemption"_test = [] {
        // Alive 1000 ns, on-core for 700, of which 200 was deliberate waiting -- so 100 ns was taken
        // away involuntarily.
        // Scaled so the microsecond field lands exactly: a 1 000 000 ns life, 700 000 ns on-core.
        const std::vector<Event> scaled{
            started(0U, 0UL),                               //
            interval(0U, Kind::messagePhase, 0UL, 100000U), //
            interval(0U, Kind::sweep, 100000UL, 600000U),   //
            interval(0U, Kind::workEnd, 150000UL, 400000U), //
            probe(0U, 3U, 100000U),                         //
            interval(0U, Kind::idle, 700000UL, 200000U),    //
            stoppedWithCpu(0U, 1000000UL, 700000U / 1000U), //
        };
        const WorkerUtilisation w = only(workerUtilisation(scaled, 0UL));
        expect(w.computed >> fatal) << w.reason;
        expect(w.hasThreadCpuTime >> fatal);
        expect(eq(w.threadCpuNs, 700000UL));
        expect(eq(w.preemptionNs, 100000UL)) << "off-core 300000 ns, of which 200000 was idle";
        expect(!w.preemptionClamped);
    };

    "idle time is not counted as preemption"_test = [] {
        // A worker that slept most of its life and was never preempted must report zero preemption,
        // not the whole of its off-core time.
        const std::vector<Event> sleepy{
            started(0U, 0UL),                               //
            interval(0U, Kind::sweep, 0UL, 100000U),        //
            interval(0U, Kind::workEnd, 0UL, 100000U),      //
            interval(0U, Kind::idle, 100000UL, 900000U),    //
            stoppedWithCpu(0U, 1000000UL, 100000U / 1000U), // on-core only while sweeping
        };
        const WorkerUtilisation w = only(workerUtilisation(sleepy, 0UL));
        expect(w.computed >> fatal) << w.reason;
        expect(eq(w.idleNs, 900000UL));
        expect(eq(w.preemptionNs, 0UL)) << "off-core 900000 ns is exactly the idle, so none of it was involuntary";
        expect(!w.preemptionClamped);
    };

    "a capture that lost records is refused outright"_test = [] {
        // The terms would cover a truncated window while the lifetime spans the whole run, so every
        // ratio understates by an unknowable amount. Refusing here also removes the interval-clipping
        // problem: a clipped capture can no longer reach the arithmetic at all.
        const WorkerUtilisation w = only(workerUtilisation(knownWorker(), 17UL));
        expect(!w.computed) << "a wrapped ring cannot be divided into percentages";
        expect(w.reason.find("17 records were lost") != std::string::npos) << w.reason;
    };

    "a capture with no worker loop is refused, naming externalStep"_test = [] {
        // step() runs on the caller's thread, so there is no worker whose lifetime this could be a
        // fraction of. The condition is the missing pair; the reason consults the sweep flag so the
        // message names the policy rather than the symptom.
        const std::vector<Event> stepped{
            Event{.startNs = 0UL, .durationNs = 500U, .kind = Kind::sweep, .workerId = 0U, .flags = flag::kViaStep},
            interval(0U, Kind::workEnd, 100UL, 300U),
        };
        const WorkerUtilisation w = only(workerUtilisation(stepped, 0UL));
        expect(!w.computed) << "there is no worker thread to describe";
        expect(w.reason.find("externalStep") != std::string::npos) << "the refusal must name the policy: " << w.reason;

        // ... and without that flag the refusal still fires, just less specifically.
        const std::vector<Event> noPair{interval(0U, Kind::sweep, 0UL, 500U)};
        const WorkerUtilisation  v = only(workerUtilisation(noPair, 0UL));
        expect(!v.computed);
        expect(v.reason.find("lifecycle") != std::string::npos) << v.reason;
    };

    "a reversed or duplicated lifetime is refused"_test = [] {
        const std::vector<Event> backwards{started(0U, 2000UL), stopped(0U, 1000UL)};
        const WorkerUtilisation  b = only(workerUtilisation(backwards, 0UL));
        expect(!b.computed) << "an unsigned subtraction would otherwise give an enormous plausible lifetime";
        expect(b.reason.find("does not follow") != std::string::npos) << b.reason;

        const std::vector<Event> twice{started(0U, 0UL), started(0U, 10UL), stopped(0U, 1000UL)};
        const WorkerUtilisation  t = only(workerUtilisation(twice, 0UL));
        expect(!t.computed) << "two starts means the pair is ambiguous";
        expect(t.reason.find("exactly one") != std::string::npos) << t.reason;

        const std::vector<Event> zero{started(0U, 1000UL), stopped(0U, 1000UL)};
        const WorkerUtilisation  z = only(workerUtilisation(zero, 0UL));
        expect(!z.computed) << "a zero lifetime would divide by zero";
    };

    "a containment violation is refused, and the reason carries the numbers"_test = [] {
        // The markers are structurally nested, so this cannot happen by measurement skew -- only by
        // overlapping scopes, a nesting change, mixed workers, or a bug here. A bare "refused" would
        // be indistinguishable from an over-eager check, so the arithmetic goes in the message.
        const std::vector<Event> tooMuchBlock{
            started(0U, 0UL),                       //
            interval(0U, Kind::sweep, 0UL, 100U),   //
            interval(0U, Kind::workEnd, 0UL, 400U), // cannot exceed its own sweep
            stopped(0U, 1000UL),                    //
        };
        const WorkerUtilisation b = only(workerUtilisation(tooMuchBlock, 0UL));
        expect(!b.computed);
        expect(b.reason.find("exceeds") != std::string::npos) << b.reason;
        expect(b.reason.find("400") != std::string::npos) << "the offending block total must appear: " << b.reason;
        expect(b.reason.find("100") != std::string::npos) << "and the sweep total it broke: " << b.reason;

        const std::vector<Event> tooMuchTotal{
            started(0U, 0UL),                              //
            interval(0U, Kind::sweep, 0UL, 600U),          //
            interval(0U, Kind::messagePhase, 600UL, 600U), // 600 + 600 > 1000
            stopped(0U, 1000UL),                           //
        };
        const WorkerUtilisation t = only(workerUtilisation(tooMuchTotal, 0UL));
        expect(!t.computed);
        expect(t.reason.find("1000 ns lifetime") != std::string::npos) << t.reason;
    };

    "a saturated interval is refused rather than summed"_test = [] {
        // Saturation makes a term a lower bound. It can never cause a false containment failure --
        // it only shrinks a sum -- but every percentage derived from it understates, so it is caught
        // by name rather than indirectly by an invariant that would report the wrong cause.
        const std::vector<Event> saturated{
            started(0U, 0UL),                           //
            interval(0U, Kind::sweep, 0UL, kSaturated), //
            stopped(0U, 10'000'000'000UL),              //
        };
        const WorkerUtilisation w = only(workerUtilisation(saturated, 0UL));
        expect(!w.computed);
        expect(w.reason.find("4.295") != std::string::npos) << w.reason;

        const std::vector<Event> saturatedProbe{
            started(0U, 0UL),                      //
            interval(0U, Kind::sweep, 0UL, 1000U), //
            probe(0U, 1U, kSaturated),             //
            stopped(0U, 10'000'000'000UL),         //
        };
        expect(!only(workerUtilisation(saturatedProbe, 0UL)).computed) << "a clipped probe total is equally unusable";
    };

    "on-core time exceeding the lifetime is refused"_test = [] {
        // No single thread can consume more CPU-seconds than wall-seconds elapsed, so this is either
        // the two clocks swapped or a delta against the wrong anchor.
        const std::vector<Event> impossible{
            started(0U, 0UL),                     //
            stoppedWithCpu(0U, 1000000UL, 5000U), // 5 ms of CPU in a 1 ms life
        };
        const WorkerUtilisation w = only(workerUtilisation(impossible, 0UL));
        expect(!w.computed);
        expect(w.reason.find("no single thread can do") != std::string::npos) << w.reason;
    };

    "the saturation bucket names no thread and is refused"_test = [] {
        const std::vector<Event> folded{started(kWorkerOverflow, 0UL), stopped(kWorkerOverflow, 1000UL)};
        const WorkerUtilisation  w = only(workerUtilisation(folded, 0UL));
        expect(!w.computed);
        expect(w.reason.find("saturation bucket") != std::string::npos) << w.reason;
    };

    "workers are reported separately, not averaged"_test = [] {
        std::vector<Event> two = knownWorker(0U);
        for (const Event& e : knownWorker(1U)) {
            two.push_back(e);
        }
        const std::vector<WorkerUtilisation> all = workerUtilisation(two, 0UL);
        expect(eq(all.size(), 2UZ) >> fatal) << "one entry per worker";
        expect(eq(all[0].workerId, std::uint8_t{0U}));
        expect(eq(all[1].workerId, std::uint8_t{1U}));
        for (const WorkerUtilisation& w : all) {
            expect(w.computed >> fatal) << w.reason;
            expect(eq(w.lifetimeNs, 1000UL)) << "each worker's own lifetime, not a shared one";
            expect(eq(w.blockNs(), 500UL));
        }
    };

    "one refused worker does not invalidate the others"_test = [] {
        std::vector<Event> mixed = knownWorker(0U);
        mixed.push_back(started(1U, 2000UL)); // worker 1 never stopped
        const std::vector<WorkerUtilisation> all = workerUtilisation(mixed, 0UL);
        expect(eq(all.size(), 2UZ) >> fatal);
        expect(all[0].computed) << "a sound worker must still be reported: " << all[0].reason;
        expect(!all[1].computed) << "and the unsound one refused on its own";
    };

    "a saturated on-core reading is refused, not believed"_test = [] {
        // The CPU delta is stored in microseconds in a 32-bit word, so it saturates past 71 minutes.
        // A saturated reading is a lower bound like any other, and believing it would report the
        // difference between the true CPU time and the clipped one as preemption that never happened.
        //
        // Reachable without the lost-records refusal intervening: a long run with only
        // Category::lifecycle live emits two records per worker, so the ring never wraps.
        const std::vector<Event> longRun{
            started(0U, 0UL),                                    //
            stoppedWithCpu(0U, 7'200'000'000'000UL, kSaturated), // two hours alive, CPU clipped
        };
        const WorkerUtilisation w = only(workerUtilisation(longRun, 0UL));
        expect(!w.computed) << "a clipped on-core reading cannot be divided into percentages";
        expect(w.reason.find("on-core") != std::string::npos) << "the refusal must name what was clipped: " << w.reason;
    };

    "the preemption clamp fires and says so"_test = [] {
        // Idling is a blocking wait, so on-core time plus idle should not exceed the lifetime. Where
        // it does, an assumption behind the preemption figure has bent -- an idle reason that spins
        // rather than blocks, or clock skew -- and that is recorded rather than hidden behind the
        // clamp. Without a test that provokes it, the flag is indistinguishable from one never set.
        const std::vector<Event> spinning{
            started(0U, 0UL),                               //
            interval(0U, Kind::idle, 0UL, 900000U),         // claims 900 us of waiting ...
            stoppedWithCpu(0U, 1000000UL, 950000U / 1000U), // ... while holding a core for 950 us
        };
        const WorkerUtilisation w = only(workerUtilisation(spinning, 0UL));
        expect(w.computed >> fatal) << w.reason;
        expect(w.preemptionClamped) << "off-core (50 us) is less than the idle claimed (900 us), so the clamp must fire";
        expect(eq(w.preemptionNs, 0UL)) << "and the reported value must be clamped rather than wrapping negative";
    };

    "an absent thread-CPU clock is absent, not zero"_test = [] {
        // A zero on-core time would read as "100 % preempted", which is the most misleading value
        // the field could take on a platform that simply cannot measure it.
        const WorkerUtilisation w = only(workerUtilisation(knownWorker(), 0UL));
        expect(w.computed >> fatal);
        expect(!w.hasThreadCpuTime) << "the flag is clear, so the reading must not be believed";
        expect(eq(w.threadCpuNs, 0UL));
        expect(eq(w.preemptionNs, 0UL)) << "and no preemption may be inferred from a reading that does not exist";
    };
};

/**
 * A real worker loop, which `externalStep` cannot provide: `step()` runs on the caller's thread, so
 * there is no worker whose lifetime a fraction could be taken of.
 *
 * `singleThreaded` runs the *same* loop as the thread pool, just on the calling thread — so the
 * mechanism is fully exercised with no second thread to race. Durations vary between runs, so
 * nothing here asserts a value: these are relationships that must hold however the machine behaves.
 */
namespace {

using LiveScheduler = gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded>;

[[nodiscard]] std::vector<Event> runLive(std::uint32_t categories, gr::Size_t nSamples) {
    reset();
    setCategories(categories);

    gr::Graph graph;
    auto&     src = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", nSamples}});
    auto&     mid = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("mid")}});
    auto&     snk = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
    std::ignore   = graph.connect<"out", "in">(src, mid);
    std::ignore   = graph.connect<"out", "in">(mid, snk);

    LiveScheduler scheduler;
    std::ignore = scheduler.exchange(std::move(graph));
    std::ignore = scheduler.runAndWait();

    std::vector<Event> events;
    std::ignore = forEachEvent([](const Event& e, void* user) noexcept { static_cast<std::vector<Event>*>(user)->push_back(e); }, &events);
    if (const char* directory = std::getenv("GR4_TRACE_ARTEFACT_DIR"); directory != nullptr) {
        std::ignore = gr::trace::dump(std::format("{}/t4h-utilisation.gr4trace", directory));
    }
    setCategories(0U);
    return events;
}

} // namespace

const boost::ut::suite<"TraceUtilisationLive"> traceUtilisationLiveTests = [] {
    if constexpr (!kEnabled) {
        return;
    }

    "a real worker loop yields a decomposition that holds together"_test = [] {
        const std::vector<Event>             events = runLive(categoryMask(Category::work, Category::schedulerLoop, Category::lifecycle), gr::Size_t{200000U});
        const std::vector<WorkerUtilisation> all    = workerUtilisation(events, 0UL);
        expect(eq(all.size(), 1UZ) >> fatal) << "singleThreaded runs exactly one worker";

        const WorkerUtilisation& w = all.front();
        expect(w.computed >> fatal) << w.reason;

        // A · containment. These are exact, not approximate: the markers are structurally nested, so
        // there is no skew to absorb and a violation by one nanosecond is a genuine fault.
        expect(le(w.blockNs(), w.sweepNs)) << "blocks run inside sweeps";
        expect(le(w.messagePhaseNs + w.sweepNs + w.idleNs, w.lifetimeNs)) << "the loop's phases fit inside the life";
        expect(ge(w.unaccountedNs, 0UL));
        expect(ge(w.occupancy, 0.0) and le(w.occupancy, 1.0)) << "a fraction of a life cannot exceed it";
        if (w.hasUtilisation) {
            expect(ge(w.utilisation, 0.0) and le(w.utilisation, 1.0));
        }

        // B · tightness. Containment is blind to a term going missing -- these are what notice.
        expect(gt(w.lifetimeNs, 0UL));
        expect(gt(w.blockProductiveNs, 0UL)) << "a run that moved 200k samples must show block time";
        expect(gt(w.sweepNs, 0UL));
        expect(ge(w.sweepNs, w.blockNs()));
        const std::uint64_t accounted = w.messagePhaseNs + w.sweepNs + w.idleNs;
        expect(ge(accounted * 2UL, w.lifetimeNs)) << "a busy run accounts for well over half its life; far less means a term was dropped";
    };

    "the two clocks agree with each other and with the wall"_test = [] {
        const std::vector<Event> events = runLive(categoryMask(Category::work, Category::schedulerLoop, Category::lifecycle), gr::Size_t{200000U});
        const WorkerUtilisation& w      = workerUtilisation(events, 0UL).front();
        expect(w.computed >> fatal) << w.reason;
        expect(w.hasThreadCpuTime >> fatal) << "this platform has CLOCK_THREAD_CPUTIME_ID, so the reading must be present";

        // D1 · the strongest single assertion available: exact, universal, load-independent, and it
        // relates two measurements taken by entirely different mechanisms.
        expect(le(w.threadCpuNs, w.lifetimeNs)) << "a single thread cannot consume more CPU-seconds than wall-seconds elapsed";
        expect(gt(w.threadCpuNs, 0UL)) << "a run that did work must have held a core";
        expect(ge(w.preemptionNs, 0UL));
        expect(!w.preemptionClamped) << "the clamp firing means idle exceeded off-core, so an assumption bent";
    };

    "a short-lived worker still cannot consume more CPU than it was alive"_test = [] {
        // D1 on the tightest margin available. The clocks must be read so that the on-core interval
        // nests strictly *inside* the wall lifetime -- wall first at the start, wall last at the stop.
        // Reading the CPU clock first instead adds its own syscall, ~235 ns, to the on-core side only;
        // a millisecond run absorbs that in the natural gap between the two clocks, and a run this
        // short does not. The longer scenarios above passed with that fault present.
        for (const gr::Size_t samples : {gr::Size_t{64U}, gr::Size_t{256U}, gr::Size_t{1024U}}) {
            const std::vector<Event>             events = runLive(categoryMask(Category::work, Category::schedulerLoop, Category::lifecycle), samples);
            const std::vector<WorkerUtilisation> all    = workerUtilisation(events, 0UL);
            expect(eq(all.size(), 1UZ) >> fatal);
            const WorkerUtilisation& w = all.front();
            expect(w.computed >> fatal) << "at " << samples << " samples: " << w.reason;
            expect(le(w.threadCpuNs, w.lifetimeNs)) << "at " << samples << " samples the on-core time must still fit inside the life";
        }
    };

    "occupancy responds to the work actually done"_test = [] {
        // C4 · the differential. Every other invariant is satisfied by a constant; only this notices
        // that the metric moves with the thing it claims to measure.
        const std::uint32_t      mask         = categoryMask(Category::work, Category::schedulerLoop, Category::lifecycle);
        const std::vector<Event> small        = runLive(mask, gr::Size_t{20000U});
        const std::uint64_t      smallBlockNs = workerUtilisation(small, 0UL).front().blockNs();
        const std::vector<Event> large        = runLive(mask, gr::Size_t{200000U});
        const std::uint64_t      largeBlockNs = workerUtilisation(large, 0UL).front().blockNs();

        expect(gt(smallBlockNs, 0UL) >> fatal);
        expect(gt(largeBlockNs, smallBlockNs * 3UL)) << "ten times the samples must show as substantially more block time, not a constant";
    };

    "the analysis is deterministic for a given capture"_test = [] {
        // C5 · whatever the run did, reading it twice must say the same thing.
        const std::vector<Event>             events = runLive(categoryMask(Category::work, Category::schedulerLoop, Category::lifecycle), gr::Size_t{50000U});
        const std::vector<WorkerUtilisation> first  = workerUtilisation(events, 0UL);
        const std::vector<WorkerUtilisation> second = workerUtilisation(events, 0UL);
        expect(eq(first.size(), second.size()) >> fatal);
        expect(eq(first.front().lifetimeNs, second.front().lifetimeNs));
        expect(eq(first.front().blockNs(), second.front().blockNs()));
        expect(eq(first.front().occupancy, second.front().occupancy));
    };

    "several real workers are each described on their own terms"_test = [] {
        // The first test in this milestone to run the worker loop on pool threads rather than the
        // calling one. Values are not asserted -- with several threads they are not even stable in
        // shape, since the job split depends on the pool size -- but the relationships must hold for
        // every worker independently, and the attribution must not blur between them.
        reset();
        setCategories(categoryMask(Category::work, Category::schedulerLoop, Category::lifecycle));

        gr::Graph graph;
        auto&     src = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{200000U}}});
        auto&     a   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("a")}});
        auto&     b   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", std::string("b")}});
        auto&     snk = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore   = graph.connect<"out", "in">(src, a);
        std::ignore   = graph.connect<"out", "in">(a, b);
        std::ignore   = graph.connect<"out", "in">(b, snk);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> scheduler;
        std::ignore = scheduler.exchange(std::move(graph));
        std::ignore = scheduler.runAndWait();

        std::vector<Event> events;
        std::ignore = forEachEvent([](const Event& e, void* user) noexcept { static_cast<std::vector<Event>*>(user)->push_back(e); }, &events);
        setCategories(0U);

        const std::vector<WorkerUtilisation> all = workerUtilisation(events, 0UL);
        expect(gt(all.size(), 0UZ) >> fatal) << "a pool-driven run must produce at least one worker";

        std::set<std::uint8_t> ids;
        std::size_t            computed = 0UZ;
        for (const WorkerUtilisation& w : all) {
            expect(ids.insert(w.workerId).second) << "worker ids must be distinct -- one entry each, never merged";
            expect(neq(w.workerId, kWorkerOverflow)) << "the saturation bucket names no single thread";
            if (!w.computed) {
                continue; // a worker that emitted nothing but its own bracket is not a fault
            }
            ++computed;
            // F1 - every per-worker invariant holds for each worker independently.
            expect(le(w.blockNs(), w.sweepNs)) << "worker " << w.workerId << ": blocks run inside sweeps";
            expect(le(w.messagePhaseNs + w.sweepNs + w.idleNs, w.lifetimeNs)) << "worker " << w.workerId;
            expect(ge(w.occupancy, 0.0) and le(w.occupancy, 1.0)) << "worker " << w.workerId;
            if (w.hasThreadCpuTime) {
                expect(le(w.threadCpuNs, w.lifetimeNs)) << "worker " << w.workerId << ": on-core cannot exceed alive";
            }
        }
        expect(gt(computed, 0UZ) >> fatal) << "at least one worker must have a usable decomposition";

        // F4 - each block belongs to one job set, so its records must come from one worker. A block
        // surfacing under two means the identity or the worker attribution is wrong, which is the
        // thing arguing-from-the-code could establish but never observe under real concurrency.
        std::map<EntityId, std::set<std::uint8_t>> workersPerBlock;
        for (const Event& event : events) {
            if (event.kind == Kind::workEnd && event.entity != kNoEntity) {
                workersPerBlock[event.entity].insert(event.workerId);
            }
        }
        expect(gt(workersPerBlock.size(), 0UZ) >> fatal) << "block records must be attributed to blocks";
        for (const auto& [entity, workers] : workersPerBlock) {
            expect(eq(workers.size(), 1UZ)) << "entity " << entity << " ran under " << workers.size() << " workers; a block belongs to exactly one job set";
        }
    };

    "per-block execution sums to the worker's productive total"_test = [] {
        // E5 · the two sides are built from the same records via different keys -- one grouped by
        // worker, the other by block -- so a mismatch means the two attributions disagree, which is
        // a direct check on the identity plumbing.
        const std::vector<Event> events = runLive(categoryMask(Category::work, Category::schedulerLoop, Category::lifecycle), gr::Size_t{50000U});
        const WorkerUtilisation& w      = workerUtilisation(events, 0UL).front();
        expect(w.computed >> fatal) << w.reason;

        std::map<EntityId, std::uint64_t> byBlock;
        std::uint64_t                     unattributed = 0UL;
        for (const Event& event : events) {
            if (event.kind != Kind::workEnd) {
                continue;
            }
            if (event.entity == kNoEntity) {
                unattributed += event.durationNs;
                continue;
            }
            byBlock[event.entity] += event.durationNs;
        }
        std::uint64_t summed = unattributed;
        for (const auto& [entity, ns] : byBlock) {
            summed += ns;
        }
        expect(eq(summed, w.blockProductiveNs)) << "grouping by block and by worker must reach the same total";
        expect(eq(unattributed, 0UL)) << "every invocation must name its block, or the identity was never pushed down";
        expect(gt(byBlock.size(), 1UZ)) << "a three-block chain must show more than one block running";
    };
};

int main() { /* suites run via registration */ }
