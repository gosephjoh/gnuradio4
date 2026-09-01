#ifndef EDF_SCHEDULER_PROTOTYPE_HPP
#define EDF_SCHEDULER_PROTOTYPE_HPP

// verbatim copy of ~/gr4_test/edf_scheduler_prototype.hpp (2025-07-31): the hand-built static-priority
// prototype that reorders the round-robin sweep by topological depth at init. Included in
// bm_DeadlineWorkload as a comparison point against the release-aware policies.

#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/BlockModel.hpp>

#include <vector>
#include <queue>
#include <memory>
#include <chrono>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <ranges>
#include <string>

namespace gr::scheduler {

/**
 * @brief Deadline Policy used to calculate block priority in EDF Scheduling.
 */
enum class EDFPolicy {
    StaticTopologicalDepth, // Earliest deadline based on topological path distance from Source to Sink
    DynamicBufferStarvation // Earliest deadline based on buffer fill ratio (time-to-overflow / underflow)
};

/**
 * @brief Structure representing a block task with an associated deadline.
 */
struct EDFTask {
    std::shared_ptr<gr::BlockModel> block;
    double deadline_ms; // Deadline timestamp or remaining budget in milliseconds
    std::size_t graph_depth;

    // Min-heap ordering: Smallest deadline_ms has HIGHEST priority
    bool operator>(const EDFTask& other) const noexcept {
        if (deadline_ms == other.deadline_ms) {
            return graph_depth > other.graph_depth; // Tie-breaker: shallower blocks first
        }
        return deadline_ms > other.deadline_ms;
    }
};

/**
 * @brief Prototype Earliest Deadline First (EDF) Scheduler for GNU Radio 4.0.
 * 
 * Implements real-time EDF block ordering. Blocks with the earliest deadline
 * (closest target latency or highest buffer urgency) are scheduled and executed first.
 */
template<ExecutionPolicy execution = ExecutionPolicy::singleThreaded, profiling::ProfilerLike TProfiler = profiling::null::Profiler>
struct EDF : SchedulerBase<EDF<execution, TProfiler>, execution, TProfiler> {
    using Description = Doc<R""(Earliest Deadline First (EDF) Scheduler for GNU Radio 4.0.
    Orders block execution dynamically by prioritizing blocks with the nearest deadline budget.)"">;

    using SchedulerBase<EDF<execution, TProfiler>, execution, TProfiler>::SchedulerBase;

    EDFPolicy policy = EDFPolicy::StaticTopologicalDepth;
    std::unordered_map<std::shared_ptr<gr::BlockModel>, double> block_deadlines;

    void setPolicy(EDFPolicy p) noexcept {
        policy = p;
    }

    /**
     * @brief Computes topological depths and deadlines for all blocks in the flattened graph.
     */
    void customInit() {
        using block_t = std::shared_ptr<gr::BlockModel>;
        [[maybe_unused]] const auto pe = this->_profilerHandler->startCompleteEvent("scheduler_edf.init");

        gr::Graph flatGraph = gr::graph::flatten(*this->_graph);
        const auto adjacencyList = graph::computeAdjacencyList(flatGraph);
        const auto sourceBlocks = graph::findSourceBlocks(adjacencyList);

        // Calculate topological depth via BFS
        std::unordered_map<block_t, std::size_t> depths;
        std::queue<std::pair<block_t, std::size_t>> q;

        for (const auto& src : sourceBlocks) {
            q.push({src, 0UZ});
            depths[src] = 0UZ;
        }

        while (!q.empty()) {
            auto [curr, depth] = q.front();
            q.pop();

            if (adjacencyList.contains(curr)) {
                for (const auto& edges : adjacencyList.at(curr) | std::views::values) {
                    for (const auto* edge : edges) {
                        const auto& dst = edge->destinationBlock();
                        if (depths.find(dst) == depths.end() || depths[dst] < depth + 1) {
                            depths[dst] = depth + 1;
                            q.push({dst, depth + 1});
                        }
                    }
                }
            }
        }

        // Build Min-Heap Priority Queue for EDF Tasks
        std::priority_queue<EDFTask, std::vector<EDFTask>, std::greater<EDFTask>> edfQueue;

        for (const auto& block : flatGraph.blocks()) {
            std::size_t depth = depths.contains(block) ? depths[block] : 0UZ;
            
            // Assign deadline based on selected policy:
            // For StaticTopologicalDepth: Earlier blocks in the pipeline have earlier deadlines (D = depth * 1.0 ms)
            double deadline = static_cast<double>(depth) * 1.0; 

            edfQueue.push(EDFTask{
                .block = block,
                .deadline_ms = deadline,
                .graph_depth = depth
            });

            block_deadlines[block] = deadline;
        }

        // Convert Priority Queue into GR4 execution order batches
        std::size_t n_batches = 1UZ;
        if (execution == ExecutionPolicy::multiThreaded) {
            n_batches = std::min(static_cast<std::size_t>(this->_pool->maxThreads()), flatGraph.blocks().size());
        }

        std::lock_guard lock(this->_executionOrderMutex);
        std::lock_guard guard(this->_adoptionBlocksMutex);
        this->_adoptionBlocks.clear();
        this->_adoptionBlocks.resize(n_batches);
        this->_executionOrder->clear();
        this->_executionOrder->reserve(n_batches);

        for (std::size_t b = 0; b < n_batches; ++b) {
            this->_executionOrder->emplace_back(std::vector<block_t>());
        }

        std::size_t idx = 0;
        while (!edfQueue.empty()) {
            EDFTask task = edfQueue.top();
            edfQueue.pop();
            (*this->_executionOrder)[idx % n_batches].push_back(task.block);
            idx++;
        }
    }

    void customReset() {
        customInit();
    }
};

} // namespace gr::scheduler

#endif // EDF_SCHEDULER_PROTOTYPE_HPP
