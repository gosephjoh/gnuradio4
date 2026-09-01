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
#include <string_view>
#include <tuple>
#include <vector>

#include <gnuradio-4.0/BlockModel.hpp>

namespace gr::scheduler {

/// Per-block fixed-priority annotation. Smaller values are more urgent; an omitted priority sorts last.
inline constexpr std::string_view kPriorityKey = "gr:priority";

/**
 * @brief Reads a block's fixed-priority annotation from its metadata.
 *
 * Cold path only: the result belongs in `SchedState::priority` once the schedule is formed. Reading it per
 * selection costs a string-keyed hash lookup on every candidate of every dispatch pass, which dominates the
 * decision itself at small batch sizes.
 *
 * N.B. `(*it).second` is bound to the non-owning 8-byte `ValueView`; materialising an owning `pmt::Value`
 * here would allocate from the polymorphic memory resource just to read one integer.
 */
[[nodiscard]] inline std::int64_t readFixedPriority(const BlockModel& block) {
    const auto& meta = block.metaInformation();
    if (const auto it = meta.find(kPriorityKey); it != meta.end()) {
        const auto entry = (*it).second;
        return entry.value_or<std::int64_t>(std::numeric_limits<std::int64_t>::max());
    }
    return std::numeric_limits<std::int64_t>::max();
}

/**
 * @brief Per-block scheduling state, held by the worker alongside its block list.
 *
 * `index` is the block's position in the worker's list at the time the schedule was formed. 
 * Serves both as round-robin key and as the deterministic tie-breaker for other policies. 
 * Deadline- and priority-carrying fields are to be added alongside relevant policies.
 */
struct SchedState {
    std::size_t   index              = 0UZ;
    std::int64_t  absoluteDeadlineNs = std::numeric_limits<std::int64_t>::max();
    std::uint64_t releaseOrder       = std::numeric_limits<std::uint64_t>::max();
    std::int64_t  priority           = std::numeric_limits<std::int64_t>::max();
};

/**
 * @brief Selects which of a worker's runnable blocks executes next.
 *
 * All supported policies are expressed as *minimum key first*: `key()` maps a block to an
 * ordering value, and the worker runs the smallest. `RoundRobinPolicy` keys on the block's
 * position, so its ordering is the identity permutation and reproduces the historic sweep.
 *
 * - `kStaticPriority` — the priority key never changes after the schedule is formed, so priority 
                         ordering should be established once instead of re-evaluated every pass.
 * - `kNeedsRelease`   — the policy requires job-release/deadline bookkeeping.
 */
template<typename T>
concept SchedulingPolicyLike = requires(const T& policy, const BlockModel& block, const SchedState& state) {
    { T::kName } -> std::convertible_to<std::string_view>;
    { T::kStaticPriority } -> std::convertible_to<bool>;
    { T::kNeedsRelease } -> std::convertible_to<bool>;
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
    static constexpr std::string_view kName           = "RoundRobin";
    static constexpr bool             kStaticPriority = true;
    static constexpr bool             kNeedsRelease   = false;

    [[nodiscard]] constexpr std::size_t key(const BlockModel& /*block*/, const SchedState& state) const noexcept { return state.index; }
};

static_assert(SchedulingPolicyLike<RoundRobinPolicy>);

/**
 * @brief Dynamic earliest-deadline-first ordering for released jobs.
 *
 * The job-aware scheduler supplies the absolute deadline and release sequence in `SchedState`.
 * Release order and then graph order make simultaneous deadlines deterministic.
 */
struct EarliestDeadlineFirstPolicy {
    static constexpr std::string_view kName           = "EarliestDeadlineFirst";
    static constexpr bool             kStaticPriority = false;
    static constexpr bool             kNeedsRelease   = true;

    [[nodiscard]] constexpr auto key(const BlockModel& /*block*/, const SchedState& state) const noexcept {
        return std::tuple{state.absoluteDeadlineNs, state.releaseOrder, state.index};
    }
};

/**
 * @brief Static, non-preemptive fixed-priority ordering for released jobs.
 *
 * A smaller `gr:priority` value is more urgent. Unannotated blocks run after annotated blocks,
 * with graph order providing a deterministic tie-breaker.
 */
struct FixedPriorityPolicy {
    static constexpr std::string_view kName           = "FixedPriority";
    static constexpr bool             kStaticPriority = true;
    static constexpr bool             kNeedsRelease   = true;

    [[nodiscard]] constexpr auto key(const BlockModel& /*block*/, const SchedState& state) const noexcept { return std::pair{state.priority, state.index}; }
};

static_assert(SchedulingPolicyLike<EarliestDeadlineFirstPolicy>);
static_assert(SchedulingPolicyLike<FixedPriorityPolicy>);

namespace detail {

/**
 * Arranges `blocks` into the order dictated by a static-key policy. Ties break on the original
 * position, making the comparison a strict total order — so an unstable sort is deterministic
 * and no stable-sort scratch buffer is needed. For a policy that keys on the position itself
 * (`RoundRobinPolicy`) this is the identity permutation.
 */
template<SchedulingPolicyLike TPolicy>
void applyStaticOrder(std::vector<std::shared_ptr<BlockModel>>& blocks) {
    if constexpr (!TPolicy::kStaticPriority) {
        return; // dynamic keys are re-evaluated per pass, not pre-sorted
    } else {
        if (blocks.size() < 2UZ) {
            return;
        }

        const TPolicy            policy{};
        std::vector<std::size_t> order(blocks.size());
        std::iota(order.begin(), order.end(), 0UZ);

        // resolve each block's static key once here rather than O(N log N) times inside the comparator
        std::vector<SchedState> states;
        states.reserve(blocks.size());
        for (std::size_t position = 0UZ; position < blocks.size(); ++position) {
            states.push_back(SchedState{.index = position, .priority = readFixedPriority(*blocks[position])});
        }

        const auto keyAt = [&policy, &blocks, &states](std::size_t position) { return policy.key(*blocks[position], states[position]); };
        std::ranges::sort(order, [&keyAt](std::size_t lhs, std::size_t rhs) {
            const auto lhsKey = keyAt(lhs);
            const auto rhsKey = keyAt(rhs);
            return lhsKey == rhsKey ? lhs < rhs : lhsKey < rhsKey;
        });

        std::vector<std::shared_ptr<BlockModel>> reordered;
        reordered.reserve(blocks.size());
        std::ranges::transform(order, std::back_inserter(reordered), [&blocks](std::size_t position) { return blocks[position]; });
        blocks = std::move(reordered);
    }
}

} // namespace detail

} // namespace gr::scheduler

#endif // GNURADIO_SCHEDULINGPOLICY_HPP
