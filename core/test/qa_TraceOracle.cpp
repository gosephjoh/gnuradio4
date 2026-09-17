#include <boost/ut.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceReport.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;

/**
 * The replay oracle: given a capture, decide whether the scheduler picked the block it should have.
 *
 * This is the one thing in the trace thrust that makes a claim about the *scheduler* rather than
 * about the tracer, and the distinction matters when it fails. A violation can mean the scheduler
 * chose wrongly, or it can mean the oracle rebuilt the ready set wrongly -- and an oracle that cannot
 * tell those apart is worse than none, because the first thing it will do is cry wolf about EDF.
 *
 * The premises it rests on are asserted separately, in `qa_TraceSchedulingPolicy.cpp`, before this
 * file relies on them: that the ready set is ever larger than one, that entity ids ascend with
 * registration order, that a block's records name one worker, and that record loss is visible.
 */
namespace {

using namespace gr::trace;

/// A capture the oracle refuses to judge, and why. Refusing is a first-class answer: a verdict drawn
/// from an incomplete capture is worse than no verdict.
struct Verdict {
    bool        usable = false;
    std::string refusal;
    std::size_t decisions  = 0UZ; /// selections examined
    std::size_t contended  = 0UZ; /// ... of which had a real choice
    std::size_t violations = 0UZ;
    std::string firstViolation;
};

/**
 * Replays a capture against earliest-deadline semantics.
 *
 * It mirrors the scheduler's own bookkeeping rather than approximating it: a release queues a job, a
 * selection must name the minimum-key ready block, an execution retires that block's head job, and a
 * block reporting DONE has its remaining jobs discarded -- which the scheduler does too, and which an
 * oracle that ignored it would mistake for a queue that never drained.
 *
 * Scoped to one worker and to a job-driven policy. A ready set is per worker, so pooling threads
 * would invent violations; and the tie-break is by registration index, which entity ids track only
 * where the block list is never reordered.
 */
[[nodiscard]] Verdict replay(std::span<const Event> events, std::uint64_t lostRecords) {
    Verdict verdict;
    if (lostRecords > 0UL) {
        verdict.refusal = "the capture lost records, so a release may be missing and a block would look as though it was never ready";
        return verdict;
    }

    std::set<std::uint32_t> workers;
    bool                    sawSelect = false, sawRelease = false, sawWork = false;
    for (const Event& e : events) {
        if (e.kind == Kind::select || e.kind == Kind::jobRelease || (e.kind == Kind::workEnd && (e.flags & flag::kJobBacked) != 0U)) {
            workers.insert(e.workerId);
        }
        sawSelect |= e.kind == Kind::select;
        sawRelease |= e.kind == Kind::jobRelease;
        sawWork |= e.kind == Kind::workEnd;
    }
    if (!sawSelect || !sawRelease || !sawWork) {
        verdict.refusal = "the capture needs selection, release and work records together; ordering cannot be checked from any two of them";
        return verdict;
    }
    if (workers.size() > 1UZ) {
        verdict.refusal = "records span more than one worker; a ready set is per worker and pooling them would invent violations";
        return verdict;
    }
    if (workers.contains(kWorkerOverflow)) {
        verdict.refusal = "a record carries the worker-overflow sentinel, which names no worker";
        return verdict;
    }

    constexpr std::uint64_t                       kNoDeadline = std::numeric_limits<std::uint64_t>::max();
    std::map<EntityId, std::deque<std::uint64_t>> queued; // absolute deadlines, in release order

    for (const Event& e : events) {
        switch (e.kind) {
        case Kind::jobRelease: {
            if (e.payload1 == kSaturated) {
                verdict.refusal = "a release carries an unrepresentable deadline; its ordering key cannot be reconstructed";
                return verdict;
            }
            queued[e.entity].push_back(e.payload1 == kUnsetDeadline ? kNoDeadline : e.startNs + e.payload1);
            break;
        }
        case Kind::select: {
            ++verdict.decisions;
            const auto chosen = queued.find(e.entity);
            if (chosen == queued.end() || chosen->second.empty()) {
                ++verdict.violations;
                if (verdict.firstViolation.empty()) {
                    verdict.firstViolation = std::format("entity {} was selected while holding no released job", e.entity);
                }
                break;
            }
            const std::uint64_t chosenKey = chosen->second.front();

            std::size_t   ready      = 0UZ;
            EntityId      bestEntity = 0U;
            std::uint64_t bestKey    = kNoDeadline;
            bool          haveBest   = false;
            for (const auto& [entity, deadlines] : queued) {
                if (deadlines.empty()) {
                    continue;
                }
                ++ready;
                // The scheduler's tie-break is the lower registration index, and ids ascend with it.
                if (!haveBest || deadlines.front() < bestKey || (deadlines.front() == bestKey && entity < bestEntity)) {
                    bestKey    = deadlines.front();
                    bestEntity = entity;
                    haveBest   = true;
                }
            }
            verdict.contended += ready > 1UZ ? 1UZ : 0UZ;
            if (haveBest && (chosenKey > bestKey || (chosenKey == bestKey && e.entity > bestEntity))) {
                ++verdict.violations;
                if (verdict.firstViolation.empty()) {
                    verdict.firstViolation = std::format("entity {} ran with deadline {}, while entity {} was ready with the earlier deadline {}", e.entity, chosenKey, bestEntity, bestKey);
                }
            }
            break;
        }
        case Kind::workEnd: {
            if ((e.flags & flag::kJobBacked) == 0U) {
                break;
            }
            auto queue = queued.find(e.entity);
            if (queue == queued.end() || queue->second.empty()) {
                break;
            }
            queue->second.pop_front();
            if (e.status == static_cast<std::int8_t>(gr::work::Status::DONE)) {
                // Mirrors the scheduler discarding jobs a finished block can never run. **Currently
                // unexercised**: measured against the contended capture, queue depth never exceeds
                // one, so the queue is already empty after the pop and removing this changes no
                // verdict. Kept because it mirrors the scheduler rather than because a test proves it
                // matters -- exercising it needs releases outpacing executions, which this graph shape
                // does not produce.
                queue->second.clear();
            }
            break;
        }
        default: break;
        }
    }

    verdict.usable = true;
    return verdict;
}

/// `ValueMap::at()` indexes by position, not by key, so a report field is read through `find`.
template<typename T>
[[nodiscard]] T fieldOr(const gr::property_map& map, std::string_view key, T fallback) {
    const auto it = map.find(key);
    return it != map.end() ? (*it).second.value_or(std::move(fallback)) : fallback;
}

void collectingConsumer(const Event& event, void* user) noexcept { static_cast<std::vector<Event>*>(user)->push_back(event); }

[[nodiscard]] std::vector<Event> collect() {
    std::vector<Event> events;
    std::ignore = forEachEvent(collectingConsumer, &events);
    return events;
}

/// Earliest-deadline, inverted: it runs the job due *last*. Used to prove the oracle rejects a wrong
/// scheduler, because an oracle never shown to reject one is not evidence that the real policy is
/// right.
struct LatestDeadlineFirstPolicy {
    static constexpr std::string_view             kName          = "LatestDeadlineFirst";
    static constexpr gr::scheduler::PriorityClass kPriorityClass = gr::scheduler::PriorityClass::fixedJob;

    [[nodiscard]] constexpr std::chrono::steady_clock::rep key(const gr::BlockModel&, const gr::scheduler::SchedState& state) const noexcept {
        if (state.jobs.empty()) {
            return std::numeric_limits<std::chrono::steady_clock::rep>::max(); // empty sorts last, as EDF does
        }
        return -state.jobs.front().absoluteDeadline.time_since_epoch().count();
    }
};
static_assert(gr::scheduler::SchedulingPolicyLike<LatestDeadlineFirstPolicy>);

/// Two independent chains with different deadlines, so the selector faces a real choice.
template<typename TPolicy>
void runContended(float deadlineA, float deadlineB, gr::Size_t samples = 4096U) {
    gr::Graph  graph;
    const auto branch = [&graph](const std::string& suffix, float deadline, gr::Size_t n) {
        auto& source = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", "src" + suffix}, {"n_samples_max", n}, {"relative_deadline", deadline}});
        auto& copy   = graph.emplaceBlock<gr::testing::Copy<float>>({{"name", "mid" + suffix}, {"relative_deadline", deadline}});
        auto& sink   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", "snk" + suffix}, {"relative_deadline", deadline}});
        std::ignore  = graph.connect<"out", "in">(source, copy);
        std::ignore  = graph.connect<"out", "in">(copy, sink);
    };
    branch("A", deadlineA, samples);
    branch("B", deadlineB, samples);

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep, gr::profiling::null::Profiler, TPolicy> scheduler;
    expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
    expect(scheduler.settings().set({{"max_work_items", gr::Size_t{256U}}}).empty() >> fatal);
    std::ignore = scheduler.settings().activateContext();
    std::ignore = scheduler.settings().applyStagedParameters();
    expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
    expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
    for (std::size_t pass = 0UZ; pass < 64UZ; ++pass) {
        if (scheduler.step().status == gr::work::Status::DONE) {
            break;
        }
    }
    std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
}

} // namespace

const boost::ut::suite<"TraceOracle"> oracleTests = [] {
    if constexpr (!kEnabled) {
        "a compiled-out build has nothing to judge"_test = [] {
            const Verdict verdict = replay(std::span<const Event>{}, 0UL);
            expect(!verdict.usable) << "an empty capture cannot support an ordering claim";
            expect(!verdict.refusal.empty()) << "and the refusal must say why";
        };
        return;
    }

    "the oracle accepts a correct earliest-deadline run"_test = [] {
        reset();
        setCategories(categoryMask(Category::select, Category::release, Category::work));
        runContended<gr::scheduler::EdfPolicy>(0.010f, 0.020f);

        const std::vector<Event> events  = collect();
        const Verdict            verdict = replay(events, ringStats().lost);

        expect(verdict.usable >> fatal) << "a clean capture must be judgeable: " << verdict.refusal;
        expect(gt(verdict.decisions, 0UZ) >> fatal) << "the run must have made selections";
        expect(gt(verdict.contended, 0UZ) >> fatal) << "and some must have been real choices, or the verdict is vacuous";
        expect(eq(verdict.violations, 0UZ)) << "earliest-deadline scheduling must never run a job while a more urgent one is ready: " << verdict.firstViolation;

        setCategories(0U);
        reset();
    };

    "the oracle rejects a scheduler that runs the least urgent job"_test = [] {
        // The scenario that makes the one above worth anything. An oracle never shown to reject a
        // wrong scheduler is not evidence that the right one is right.
        reset();
        setCategories(categoryMask(Category::select, Category::release, Category::work));
        runContended<LatestDeadlineFirstPolicy>(0.010f, 0.020f);

        const Verdict verdict = replay(collect(), ringStats().lost);

        expect(verdict.usable >> fatal) << verdict.refusal;
        expect(gt(verdict.contended, 0UZ) >> fatal) << "the inverted policy must still have faced choices, or it never had the chance to choose wrongly";
        expect(gt(verdict.violations, 0UZ)) << "a policy that deliberately runs the job due last must be caught doing it";
        expect(!verdict.firstViolation.empty()) << "and the verdict must name a concrete violation, not merely count one";

        setCategories(0U);
        reset();
    };

    "the oracle refuses a capture that lost records"_test = [] {
        // Loss is fed in rather than provoked. A ring is created at whatever capacity was set when its
        // thread first emitted, so shrinking the capacity cannot make *this* thread's ring wrap -- and
        // the behaviour under test is the refusal, not the ring. That record loss is observable on a
        // real graph is established separately, in the scheduling-policy suite.
        reset();
        setCategories(categoryMask(Category::select, Category::release, Category::work));
        runContended<gr::scheduler::EdfPolicy>(0.010f, 0.020f);

        const std::vector<Event> events = collect();
        expect(replay(events, 0UL).usable >> fatal) << "the same capture must be judgeable when nothing was lost, or this compares two different things";

        const Verdict verdict = replay(events, 1UL);
        expect(!verdict.usable) << "one evicted record is enough to make a release invisible, and a block that never appears ready cannot be judged";
        expect(verdict.refusal.contains("lost")) << "and the refusal must name the reason";
        expect(eq(verdict.violations, 0UZ)) << "a refused capture must report no verdict at all, rather than a verdict with a caveat";

        setCategories(0U);
        reset();
    };

    "the oracle refuses a capture missing any of the three record kinds"_test = [] {
        reset();
        setCategories(categoryMask(Category::select, Category::release)); // no work records
        runContended<gr::scheduler::EdfPolicy>(0.010f, 0.020f, 2048U);

        const Verdict verdict = replay(collect(), ringStats().lost);
        expect(!verdict.usable) << "without executions the queue never drains, so every later selection would look wrong";
        expect(verdict.refusal.contains("selection, release and work")) << "the refusal must say what was missing";

        setCategories(0U);
        reset();
    };

    "the report marks a lossy capture unreliable"_test = [] {
        reset();
        setCategories(categoryMask(Category::deadline, Category::release, Category::work));
        runContended<gr::scheduler::EdfPolicy>(1.0e-6f, 1.0e-6f, 2048U);
        const std::vector<Event> events = collect();

        expect(fieldOr<bool>(report(events, 0UL), "reliable", false)) << "the same capture is reliable when nothing was lost";

        const gr::property_map lossy = report(events, 3UL);
        expect(!fieldOr<bool>(lossy, "reliable", true)) << "a capture that dropped records understates every figure in it and must say so";
        expect(fieldOr<std::string>(lossy, "reliable_reason", std::string{}).contains("evicted")) << "and name the reason rather than merely flagging it";

        setCategories(0U);
        reset();
    };

    "the report reports a disagreement rather than splitting the difference"_test = [] {
        // Built by hand, because the disagreement this detects -- a released job discarded before it
        // ran -- is what the scheduler does on a re-sync, and provoking it through a graph would make
        // the test depend on the house-keeping cadence. The arithmetic under test is the comparison,
        // and a synthetic capture exercises it exactly.
        //
        // One release with a 1000 ns deadline, one execution completing 5000 ns later. Reconstruction
        // therefore sees a miss. No live `deadlineMiss` record accompanies it, as though the marker
        // had been unable to record one -- which is precisely the asymmetry the cross-check exists to
        // surface.
        constexpr std::uint64_t  kRelease = 1'000'000UL;
        const std::vector<Event> synthetic{
            Event{.startNs = kRelease, .payload0 = 64U, .payload1 = 1000U, .payload2 = 64U, .entity = 1U, .kind = Kind::jobRelease},
            Event{.startNs = kRelease + 1000UL, .durationNs = 4000U, .payload0 = 64U, .payload1 = 64U, .entity = 1U, .kind = Kind::workEnd, .flags = flag::kJobBacked},
        };

        const gr::property_map summary = report(synthetic, 0UL);
        expect(eq(fieldOr<std::uint64_t>(summary, "misses_reconstructed", 0UL), 1UL) >> fatal) << "a completion 5000 ns after a release with a 1000 ns deadline is late by reconstruction";
        expect(eq(fieldOr<std::uint64_t>(summary, "misses_live", 99UL), 0UL)) << "and no live record accompanies it in this capture";
        expect(eq(fieldOr<std::string>(summary, "cross_check", std::string{}), std::string("disagree"))) << "the two methods disagree, and the report must say so";
        expect(fieldOr<std::string>(summary, "cross_check_reason", std::string{}).contains("discarded")) << "naming the cause a reader should go looking for";

        // The agreeing case, so the check is not simply always reporting disagreement.
        std::vector<Event> agreeing = synthetic;
        agreeing.push_back(Event{.startNs = kRelease + 5000UL, .payload0 = 4000U, .payload1 = 5000U, .payload2 = 64U, .entity = 1U, .kind = Kind::deadlineMiss, .flags = flag::kDeadlineMissed});
        expect(eq(fieldOr<std::string>(report(agreeing, 0UL), "cross_check", std::string{}), std::string("agree"))) << "with the live record present the two agree, and that must be reported too";
    };

    "the report refuses a response-time distribution it cannot support"_test = [] {
        reset();
        setCategories(categoryMask(Category::deadline)); // misses only: no releases to attribute them to
        runContended<gr::scheduler::EdfPolicy>(1.0e-6f, 1.0e-6f, 2048U);

        const gr::property_map summary = report(collect(), ringStats().lost);
        expect(gt(fieldOr<std::uint64_t>(summary, "deadline_misses", 0UL), 0UL) >> fatal) << "misses must still be counted from their own records alone";
        expect(eq(fieldOr<std::string>(summary, "response_time", std::string{}), std::string("unavailable"))) << "a half-populated distribution that looks complete is the failure this avoids";
        // Specific, not merely non-empty. Both refusal branches used to mention releases, so a check
        // for that word passed whichever one fired -- and the mutation removing the release check
        // escaped because the *other* branch answered in its place.
        expect(fieldOr<std::string>(summary, "response_time_reason", std::string{}).contains("no release records")) << "the refusal must name the missing releases specifically, not any absent record kind";

        setCategories(0U);
        reset();
    };

    "the report reconstructs response times and cross-checks them against the live misses"_test = [] {
        reset();
        setCategories(categoryMask(Category::deadline, Category::release, Category::work));
        runContended<gr::scheduler::EdfPolicy>(1.0e-6f, 1.0e-6f, 2048U);

        const gr::property_map summary = report(collect(), ringStats().lost);
        expect(eq(fieldOr<std::string>(summary, "response_time", std::string{}), std::string("reconstructed")) >> fatal);
        expect(gt(fieldOr<std::uint64_t>(summary, "response_time_samples", 0UL), 0UL)) << "releases and executions must have paired up";
        expect(fieldOr<bool>(summary, "reliable", false)) << "a capture that lost nothing is reliable";
        expect(eq(fieldOr<std::string>(summary, "cross_check", std::string{}), std::string("agree"))) //
            << "the live and reconstructed miss counts must agree; they can only differ if a released job was discarded before running or records were lost";

        setCategories(0U);
        reset();
    };
};

int main() { /* tests are statically registered as suites */ }
