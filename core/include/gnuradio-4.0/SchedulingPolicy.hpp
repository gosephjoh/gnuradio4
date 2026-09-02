#ifndef GNURADIO_SCHEDULINGPOLICY_HPP
#define GNURADIO_SCHEDULINGPOLICY_HPP

#include <algorithm>
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

/// Whether the ordering key is fixed once the schedule is formed, and the list can be pre-sorted.
[[nodiscard]] constexpr bool hasStaticKey(PriorityClass priorityClass) noexcept { return priorityClass == PriorityClass::none || priorityClass == PriorityClass::fixedTask; }

/// Whether the policy needs job-release and absolute-deadline bookkeeping (§3.4).
[[nodiscard]] constexpr bool needsReleaseTracking(PriorityClass priorityClass) noexcept { return priorityClass == PriorityClass::fixedJob || priorityClass == PriorityClass::dynamic; }

/// Whether the worker selects the highest-priority eligible block each time, rather than sweeping.
[[nodiscard]] constexpr bool selectsByPriority(PriorityClass priorityClass) noexcept { return priorityClass != PriorityClass::none; }

struct SchedState {
    std::size_t index = 0UZ;

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

    /// Set when the block has reported `DONE`, so a selection loop can skip it instead of
    /// re-probing it on every restart. Cleared whenever the states are re-derived, which is the
    /// right scope: a graph mutation invalidates the conclusion.
    bool finished = false;
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

        const auto keyAt = [&policy, &blocks, &states](std::size_t position) { return policy.key(*blocks[position], states[position]); };
        std::ranges::sort(order, [&keyAt](std::size_t lhs, std::size_t rhs) {
            const auto lhsKey = keyAt(lhs);
            const auto rhsKey = keyAt(rhs);
            return lhsKey == rhsKey ? lhs < rhs : lhsKey < rhsKey;
        });

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
