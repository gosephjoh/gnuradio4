#ifndef GNURADIO_SCHEDULINGPOLICY_HPP
#define GNURADIO_SCHEDULINGPOLICY_HPP

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <iterator>
#include <memory>
#include <numeric>
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
struct SchedState {
    std::size_t index = 0UZ;
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

        const auto keyAt = [&policy, &blocks](std::size_t position) { return policy.key(*blocks[position], SchedState{.index = position}); };
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
