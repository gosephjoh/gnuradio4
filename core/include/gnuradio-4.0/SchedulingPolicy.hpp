#ifndef GNURADIO_SCHEDULINGPOLICY_HPP
#define GNURADIO_SCHEDULINGPOLICY_HPP

#include <algorithm>
#include <cassert>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <span>
#include <string_view>
#include <vector>

#include <gnuradio-4.0/BlockModel.hpp>
#include <gnuradio-4.0/Trace.hpp>

namespace gr::scheduler {

/**
 * @brief Per-block scheduling state, held by the worker alongside its block list.
 *
 * `index` is the block's position in the worker's list at the time the schedule was formed.
 * Serves both as round-robin key and as the deterministic tie-breaker for other policies.
 * Deadline- and priority-carrying fields are to be added alongside relevant policies.
 */
/// "Nothing bounds this batch" -- the value the scheduler passes to `work()` when no block-level,
/// scheduler-level or port-level ceiling applies.
inline constexpr std::size_t kUnboundedBatch = std::numeric_limits<std::size_t>::max();

/**
 * @brief Which scheduling discipline a policy belongs to.
 *
 * The standard real-time taxonomy, by *what priority is a property of*:
 *
 * - **fixed task priority** — the priority belongs to the block and is the same for every job it
 *   ever runs (fixed priority, rate-monotonic, deadline-monotonic);
 * - **fixed job priority** — constant within a job but differing between jobs of the same block,
 *   because it derives from the release (earliest-deadline-first);
 * - **dynamic priority** — may change *during* a job (least-laxity-first).
 *
 * Round robin belongs to none of them: it is time-sharing and has no notion of priority at all,
 * which is why it needs its own value rather than being folded in as a degenerate case.
 *
 * **Terminology.** `fixedTask`/`fixedJob` are the formal names -- task-level fixed and job-level
 * fixed priority. In real-time systems usage these are colloquially shortened to *static* and
 * *dynamic* priority, so "EDF is a dynamic-priority policy" is idiomatic rather than incorrect.
 * The enumerators use the formal names only because this enum also has to name the genuinely
 * dynamic class, where the colloquial shorthand would collide. `hasStaticKey()` below keeps the
 * colloquial sense, which is the one that matters mechanically.
 *
 * Two mechanical questions follow from the class rather than being declared separately, because
 * the alignment is definitional, not incidental: a task-level priority cannot change after
 * assignment, so the order can be precomputed; and a job-level priority is *derived* from the
 * release, so it presumes release tracking.
 */
enum class PriorityClass : std::uint8_t {
    none,      /// time-sharing; no priority (round robin)
    fixedTask, /// priority is a property of the block, constant across its jobs (FP, RM)
    fixedJob,  /// priority constant within a job, differing between jobs (EDF)
    dynamic    /// priority may change during a job (LLF)
};

/// How a worker picks the minimum-key block among those eligible. Not a property of the policy: the
/// same policy must be runnable both ways so the two can be measured against each other.
///
/// Only meaningful where `hasStaticKey` is false. A static-key policy has its list pre-sorted by
/// `applyStaticOrder`, so "first eligible" already *is* "minimum key" and a heap would buy nothing.
enum class SelectionStrategy : std::uint8_t {
    linearScan, /// O(n) per selection, no auxiliary state
    readyHeap   /// O(log n) per selection over a heap rebuilt once per pass
};

/// How the block list is ordered where two blocks compare equal under a task-level fixed-priority
/// policy (`PriorityClass::fixedTask` -- fixed priority and rate monotonic). Applied once, when the
/// list is first ordered; nothing during a pass consults it.
///
/// Round robin is deliberately outside this: it keys on the position itself, so it never ties, and
/// it has to keep meaning "the historic sweep" to remain the baseline the other policies are
/// measured against.
enum class TieBreak : std::uint8_t {
    registrationOrder, /// the block's position in its worker's job list -- the historic behaviour
    upstreamFirst,     /// data-topological: a producer is ordered before every block it feeds
    downstreamFirst    /// the reverse: drain what is already in flight before refilling it
};

[[nodiscard]] constexpr std::string_view toString(TieBreak tieBreak) noexcept {
    switch (tieBreak) {
    case TieBreak::upstreamFirst: return "upstream_first";
    case TieBreak::downstreamFirst: return "downstream_first";
    case TieBreak::registrationOrder: break;
    }
    return "registration_order";
}

/// Whether the ordering key is fixed once the schedule is formed, and the list can be pre-sorted.
[[nodiscard]] constexpr bool hasStaticKey(PriorityClass priorityClass) noexcept { return priorityClass == PriorityClass::none || priorityClass == PriorityClass::fixedTask; }

/// Whether the policy needs job-release and absolute-deadline bookkeeping.
[[nodiscard]] constexpr bool needsReleaseTracking(PriorityClass priorityClass) noexcept { return priorityClass == PriorityClass::fixedJob || priorityClass == PriorityClass::dynamic; }

/// Whether the worker selects the highest-priority eligible block each time, rather than sweeping.
[[nodiscard]] constexpr bool selectsByPriority(PriorityClass priorityClass) noexcept { return priorityClass != PriorityClass::none; }

/// One released job: a unit of work admitted at a particular instant, with its batch frozen at
/// release so that samples arriving before it executes cannot change what it was admitted to do.
struct Job {
    std::size_t                           batch = 0UZ;
    std::chrono::steady_clock::time_point releaseTime{};      /// detection instant, never a nominal one
    std::chrono::steady_clock::time_point absoluteDeadline{}; /// frozen with the batch, so a mid-run
                                                              /// `relative_deadline` change cannot move it
};

/// Fixed-capacity ring over storage owned elsewhere: the scheduler allocates one arena during setup
/// and hands each block a slice, so releasing a job never allocates. Capacity is per block and
/// derived from the gating input capacity divided by the block's batch floor, not a constant.
struct JobQueue {
    std::span<Job> storage{};
    std::size_t    head = 0UZ;
    std::size_t    size = 0UZ;

    [[nodiscard]] constexpr bool        empty() const noexcept { return size == 0UZ; }
    [[nodiscard]] constexpr bool        full() const noexcept { return storage.empty() || size >= storage.size(); }
    [[nodiscard]] constexpr std::size_t capacity() const noexcept { return storage.size(); }

    [[nodiscard]] constexpr Job&       front() noexcept { return storage[head]; }
    [[nodiscard]] constexpr const Job& front() const noexcept { return storage[head]; }

    [[nodiscard]] constexpr bool push(const Job& job) noexcept {
        if (full()) {
            return false;
        }
        storage[(head + size) % storage.size()] = job;
        ++size;
        return true;
    }

    constexpr void pop() noexcept {
        if (empty()) {
            return;
        }
        head = (head + 1UZ) % storage.size();
        --size;
    }

    constexpr void clear() noexcept {
        head = 0UZ;
        size = 0UZ;
    }
};

struct SchedState {
    std::size_t index = 0UZ;

    /// What orders this block against another of equal key. It is the block's position -- `index`
    /// -- unless a task-level fixed-priority policy was asked for a data-topological order, which is
    /// the only case where the two differ (`TieBreak`, `SchedulerBase::tie_break`). Resolved during
    /// setup, so no pass pays for the choice, and kept apart from `index` so that round robin --
    /// which keys on the position itself -- cannot be moved by a tie-break setting.
    ///
    /// N.B. a `SchedState` built by hand must set this beside `index`: the values have to be
    /// distinct within a job list, or the ordering stops being a strict total order.
    std::size_t tieBreak = 0UZ;

    /// Interned trace identity, assigned in `syncSchedStates()` and `kNoEntity` where tracing is
    /// compiled out. Cached here rather than looked up per marker because this struct is already
    /// beside the block in the worker's hot loop, and because an id derived from the block's address
    /// survives `applyStaticOrder`'s permutation -- which a position-derived one would not.
    gr::trace::EntityId entityId = gr::trace::kNoEntity;

    /// Which worker owns this state, for markers emitted from code that has the state but not the
    /// worker -- `releaseIfEligible()` is a free function and would otherwise attribute every release
    /// to worker 0. Assigned beside `entityId` in `syncSchedStates()`, which already receives it.
    std::uint8_t workerId = 0U;

    /// Per-invocation batch ceiling, resolved once during setup and handed to `work()` as its
    /// requested work. Ceiling only: a batch *floor* has no enforcement path at this layer, since
    /// `work()` takes an upper bound and clamps it up to the block's release threshold.
    std::size_t batchCeiling = kUnboundedBatch;

    /// Resolved scheduling priority: the user's value where one was set, otherwise the
    /// rate-monotonic rank derived from periods. Larger is more urgent, matching GR4's per-port
    /// `priority` convention; the *policy* is where that flips into a minimum-first key.
    std::int32_t priority = 0;

    /// The block's own declared `sched_priority`, `0` meaning unset. Kept separate from `priority`
    /// because an *absolute* priority scheme must treat "no user value" as unset rather than
    /// inheriting a derived rank -- and because, being block-local, it survives adoption intact.
    std::int32_t userPriority = 0;

    /// Unproductive `work()` invocations this sweep, and what they cost. Aggregated rather than
    /// recorded one record at a time: under round robin and fixed priority `work()` doubles as the
    /// eligibility oracle, so most invocations do nothing, and a record each would bury the
    /// productive ones. Flushed to one `workProbe` per block per sweep and zeroed.
    ///
    /// Reset wholesale by `syncSchedStates()` on the house-keeping cadence, which is harmless: these
    /// are per-sweep quantities and the sweep that filled them has already flushed them.
    std::uint32_t probeCount = 0U;
    std::uint64_t probeNs    = 0UL;

    /// Set when the block has reported `DONE`, so a selection loop can skip it instead of
    /// re-probing it on every restart. Cleared whenever the states are re-derived, which is the
    /// right scope: a graph mutation invalidates the conclusion.
    bool finished = false;

    /// Whether the backstop release scan must evaluate this block on its next pass. Cleared only when
    /// the last evaluation found the data gate shut, since then nothing but new input, the block's own
    /// execution, a lifecycle change or a re-sync can open it -- and each of those sets this again or
    /// is checked directly. True by default, so every re-derivation starts with a full scan.
    bool releaseCheckDue = true;

    /// Evaluated on every pass regardless: a source, which no producer's walk can reach, or a block fed
    /// from another worker, whose producer must not write this worker's state.
    bool releaseCheckedEveryPass = false;

    /// Release bookkeeping, used only where the policy's class calls for it
    /// (`needsReleaseTracking`). A round-robin scheduler leaves all of it untouched and pays only
    /// the storage, which is fixed and small.
    JobQueue jobs{};

    /// Zero-initialised on purpose: it makes the temporal gate vacuous on the first sweep, so a
    /// block's first release is decided by data alone.
    std::chrono::steady_clock::time_point lastRelease{};

    /// Samples already committed to released-but-unexecuted jobs, so two jobs cannot claim the
    /// same data. Read through `unassignedSamples()`, which saturates rather than wrapping.
    std::size_t assignedSamples = 0UZ;

    /// Minimum input the block needs before it can run: the data gate's threshold.
    std::size_t batchFloor = 1UZ;

    /// Resolved period and relative deadline in seconds, `0` meaning unset. A zero period imposes
    /// no temporal gate, leaving release governed by data alone.
    double periodSeconds           = 0.0;
    double relativeDeadlineSeconds = 0.0;

    /// Releases dropped because the ring was full -- a real backlog signal, not an error: the
    /// samples stay unassigned and are offered again on a later detection.
    ///
    /// N.B. the buffer-derived bound alone could never be reached by a block with inputs, so a
    /// non-zero count used to imply a bookkeeping defect. The `max_outstanding_jobs` clamp removes
    /// that guarantee deliberately, and with it the distinction: under a clamp every block can
    /// saturate, and this counts how often the cap, rather than the data, was the binding
    /// constraint.
    std::size_t overruns = 0UZ;

    /// Indices, within this worker's own list, of the blocks this one feeds. Event-driven release
    /// detection walks these after a `work()` that produced output; cross-worker edges are absent
    /// by construction and are covered by the per-sweep backstop instead.
    std::span<const std::size_t> successors{};
};

/**
 * @brief Selects which of a worker's runnable blocks executes next.
 *
 * All supported policies are expressed as *minimum key first*: `key()` maps a block to an
 * ordering value, and the worker runs the smallest. `RoundRobinPolicy` keys on the block's
 * position, so its ordering is the identity permutation and reproduces the historic sweep.
 *
 * A policy declares only its `kPriorityClass`; whether its key can be precomputed and whether it
 * needs release bookkeeping follow from that (`hasStaticKey`, `needsReleaseTracking`).
 */
template<typename T>
concept SchedulingPolicyLike = requires(const T& policy, const BlockModel& block, const SchedState& state) {
    { T::kName } -> std::convertible_to<std::string_view>;
    { T::kPriorityClass } -> std::convertible_to<PriorityClass>;
    { policy.key(block, state) } -> std::totally_ordered;
};

/**
 * @brief Cyclic sweep of the worker's blocks in assignment order. Mimics GR4's original poolWorker.
 *
 * The worker visits every block once per pass, in the order the assignment policy
 * (`Simple`/`BreadthFirst`/`DepthFirst`) placed them in its job list, and repeats. Ordering is
 * therefore by *position*, not by when a block became runnable.
 *
 * N.B. this is not first-in-first-out with respect to job *release*: a true FIFO policy would
 * order by the time each block became eligible, which requires the release tracking that arrives
 * with the deadline-driven policies.
 */
struct RoundRobinPolicy {
    static constexpr std::string_view kName          = "RoundRobin";
    static constexpr PriorityClass    kPriorityClass = PriorityClass::none;

    [[nodiscard]] constexpr std::size_t key(const BlockModel& /*block*/, const SchedState& state) const noexcept { return state.index; }
};

static_assert(SchedulingPolicyLike<RoundRobinPolicy>);

/**
 * @brief Runs blocks in order of the user's declared `sched_priority`, largest first.
 *
 * "Absolute" fixed priority: it keys on what the user asked for and nothing else. A block with no
 * declared priority is unset, not low-ranked-by-derivation -- it ties with every other unset block
 * and falls back to registration order. Use `RateMonotonicPolicy` to have priorities derived
 * instead.
 *
 * The negation is the whole of the sign convention: GR4 counts larger as more urgent, while
 * `key()` is minimum-first.
 */
struct FixedPriorityPolicy {
    static constexpr std::string_view kName          = "FixedPriority";
    static constexpr PriorityClass    kPriorityClass = PriorityClass::fixedTask;

    [[nodiscard]] constexpr std::int64_t key(const BlockModel& /*block*/, const SchedState& state) const noexcept { return -static_cast<std::int64_t>(state.userPriority); }
};

static_assert(SchedulingPolicyLike<FixedPriorityPolicy>);

/**
 * @brief Earliest deadline first: runs the released job whose absolute deadline is nearest.
 *
 * The key is the *front* job's deadline, not the block's: EDF orders jobs, and a block's queue is
 * FIFO, so its next job is always the front one. `key()` is already minimum-first and an earlier
 * deadline is more urgent, so unlike the priority policies this needs no sign flip.
 *
 * A block with no released job returns the maximum representable instant. It is never selected --
 * the selectors skip empty queues -- but leaving the key undefined would invite the mirror of the
 * trap an unset *priority* sets, where a default-constructed `0` sorts first and starves everything.
 */
struct EdfPolicy {
    static constexpr std::string_view kName          = "EarliestDeadlineFirst";
    static constexpr PriorityClass    kPriorityClass = PriorityClass::fixedJob;

    [[nodiscard]] constexpr std::chrono::steady_clock::rep key(const BlockModel& /*block*/, const SchedState& state) const noexcept { return (state.jobs.empty() ? std::chrono::steady_clock::time_point::max() : state.jobs.front().absoluteDeadline).time_since_epoch().count(); }
};

static_assert(SchedulingPolicyLike<EdfPolicy>);

/// The one ordering every selector must use: minimum key first, ties broken by `tieBreak` -- the
/// block's position, unless `SchedulerBase::tie_break` asked a task-level fixed-priority policy for
/// a data-topological order.
///
/// Shared deliberately. A linear scan breaking ties by scan position and a heap breaking them by
/// insertion order would disagree on equal keys, and the two would stop being comparable for reasons
/// unrelated to either being wrong. N.B. the ready heap (`Scheduler.hpp`) breaks ties on the block's
/// *position*, which agrees with this only because the policies that use it keep `tieBreak == index`
/// -- they are never pre-sorted, and never given a topological tie-break. Giving a dynamic-key
/// policy one means bringing that comparator here too.
template<typename TPolicy>
[[nodiscard]] bool selectsBefore(const TPolicy& policy, const BlockModel& lhsBlock, const SchedState& lhs, const BlockModel& rhsBlock, const SchedState& rhs) {
    const auto lhsKey = policy.key(lhsBlock, lhs);
    const auto rhsKey = policy.key(rhsBlock, rhs);
    return lhsKey == rhsKey ? lhs.tieBreak < rhs.tieBreak : lhsKey < rhsKey;
}

/**
 * @brief Runs blocks in rate-monotonic order: the shorter a block's period, the sooner it runs.
 *
 * Keys on the priority the derivation assigned -- a rank over periods, with a user-set
 * `sched_priority` overriding it for that block. A block whose period could not be derived (no
 * rate anchor, or adopted after `init()`) has priority `0` and therefore runs last.
 */
struct RateMonotonicPolicy {
    static constexpr std::string_view kName          = "RateMonotonic";
    static constexpr PriorityClass    kPriorityClass = PriorityClass::fixedTask;

    [[nodiscard]] constexpr std::int64_t key(const BlockModel& /*block*/, const SchedState& state) const noexcept { return -static_cast<std::int64_t>(state.priority); }
};

static_assert(SchedulingPolicyLike<RateMonotonicPolicy>);

namespace detail {

/**
 * Arranges `blocks` into the order dictated by a static-key policy. Ties break on the original
 * position, making the comparison a strict total order — so an unstable sort is deterministic
 * and no stable-sort scratch buffer is needed. For a policy that keys on the position itself
 * (`RoundRobinPolicy`) this is the identity permutation.
 */
template<SchedulingPolicyLike TPolicy>
void applyStaticOrder(std::vector<std::shared_ptr<BlockModel>>& blocks, std::vector<SchedState>& states) {
    if constexpr (!hasStaticKey(TPolicy::kPriorityClass)) {
        return; // dynamic keys are re-evaluated per pass, not pre-sorted
    } else {
        if (blocks.size() < 2UZ || states.size() != blocks.size()) {
            return;
        }

        const TPolicy            policy{};
        std::vector<std::size_t> order(blocks.size());
        std::iota(order.begin(), order.end(), 0UZ);

        // The sort below is unstable, so it is deterministic only if the comparison is a strict
        // total order -- which rests entirely on the tie-breaks being distinct. A duplicate makes
        // the resulting permutation arbitrary, and the symptom would be an intermittent reordering
        // far from here, so it is caught at the source instead.
        [[maybe_unused]] const auto tieBreaksAreDistinct = [&states] {
            std::vector<std::size_t> tieBreaks;
            tieBreaks.reserve(states.size());
            std::ranges::transform(states, std::back_inserter(tieBreaks), &SchedState::tieBreak);
            std::ranges::sort(tieBreaks);
            return std::ranges::adjacent_find(tieBreaks) == tieBreaks.end();
        };
        assert(tieBreaksAreDistinct() && "SchedState::tieBreak must be distinct within a job list");

        std::ranges::sort(order, [&policy, &blocks, &states](std::size_t lhs, std::size_t rhs) { return selectsBefore(policy, *blocks[lhs], states[lhs], *blocks[rhs], states[rhs]); });

        // `states` carries a payload now, so it must follow the same permutation -- a policy that
        // reorders blocks while their state stays put would hand each block another's batch.
        // `index` is deliberately *not* renumbered: it is the original position, which is what
        // makes it a deterministic tie-breaker.
        std::vector<std::shared_ptr<BlockModel>> reorderedBlocks;
        std::vector<SchedState>                  reorderedStates;
        reorderedBlocks.reserve(blocks.size());
        reorderedStates.reserve(states.size());
        for (std::size_t position : order) {
            reorderedBlocks.push_back(blocks[position]);
            reorderedStates.push_back(states[position]);
        }
        blocks = std::move(reorderedBlocks);
        states = std::move(reorderedStates);
    }
}

} // namespace detail

} // namespace gr::scheduler

#endif // GNURADIO_SCHEDULINGPOLICY_HPP
