#ifndef GNURADIO_EARLIEST_DEADLINE_FIRST_HPP
#define GNURADIO_EARLIEST_DEADLINE_FIRST_HPP

#include <gnuradio-4.0/JobTracking.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/thread/thread_affinity.hpp>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <format>
#include <numeric>
#include <set>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(_POSIX_THREADS) && not defined(__EMSCRIPTEN__) && not defined(__APPLE__)
#include <sched.h>
#include <sys/resource.h>
#endif

namespace gr::scheduler {

/// per-block relative deadline in nanoseconds, read from BlockModel::metaInformation()
inline constexpr std::string_view kDeadlineKey = "gr:deadline_ns";

/// optional per-block worst-case execution time in nanoseconds, read from BlockModel::metaInformation()
inline constexpr std::string_view kWcetKey = "gr:wcet_ns";

/// EDF-specific entries recognised inside SchedulerBase::sched_settings
inline constexpr std::string_view kDefaultDeadlineKey       = "default_deadline_ns";
inline constexpr std::string_view kPrecedenceKey            = "precedence_deadlines";
inline constexpr std::string_view kPartitioningKey           = "deadline_partitioning";
inline constexpr std::string_view kImplicitDeadlineKey       = "implicit_deadlines";
inline constexpr std::string_view kBufferPressureDeadlineKey = "buffer_pressure_deadlines";
inline constexpr std::string_view kRealTimePriorityKey       = "rt_priority"; // 0 disables; otherwise SCHED_FIFO priority
inline constexpr std::string_view kTraceLevelKey            = "trace_level";      // 0: counters only, 1: job events, 2: everything (default)
inline constexpr std::string_view kVerifySelectionKey       = "verify_selection"; // debug: count heap selections that disagree with a linear scan
inline constexpr std::string_view kHeapSelectionKey         = "heap_selection";   // 1: heap always, 0: linear scan always, -1 (default): heap once a worker holds more than kHeapThreshold tasks

/**
 * @brief Non-preemptive job-aware scheduler, using EDF unless another release-aware policy is supplied.
 *
 * Each dispatch pass selects the ready block with the earliest absolute deadline and invokes work() on that
 * block alone, rather than sweeping the whole block list. Readiness is evaluated from port occupancy without
 * calling work(), so blocks that cannot make progress cost a predicate rather than an invocation.
 *
 * GR4 blocks run to completion, so this is non-preemptive EDF: the blocking factor is the duration of the
 * longest single work() call, bounded in practice by the inherited max_work_items setting.
 *
 * Under ExecutionPolicy::multiThreaded each worker owns a disjoint slice of the graph and runs its own policy
 * queue over it, i.e. partitioned rather than global scheduling. Task state is indexed by runnerID and
 * therefore needs no synchronisation on the dispatch path.
 *
 * EDF-specific settings are latched from sched_settings at init()/reset() rather than tracked live, so they
 * must be applied before the graph is exchanged in. settingsChanged() is deliberately not overridden: the base
 * uses it to switch thread pools, and hiding it would silently break that.
 */
template<ExecutionPolicy execution = ExecutionPolicy::singleThreaded, typename TClock = std::chrono::steady_clock, profiling::ProfilerLike TProfiler = profiling::null::Profiler, SchedulingPolicyLike TPolicy = EarliestDeadlineFirstPolicy>
requires(TPolicy::kNeedsRelease)
struct EarliestDeadlineFirst : SchedulerBase<EarliestDeadlineFirst<execution, TClock, TProfiler, TPolicy>, execution, TProfiler, TPolicy> {
    using Description = Doc<R""(Non-preemptive job-aware scheduler with fixed batch releases, deadline accounting, and tracing.)"">;
    using base_t      = SchedulerBase<EarliestDeadlineFirst<execution, TClock, TProfiler, TPolicy>, execution, TProfiler, TPolicy>;
    using Clock       = TClock;
    using TimePoint   = typename TClock::time_point;
    using Duration    = std::chrono::nanoseconds;
    using DeadlineMap = std::unordered_map<const BlockModel*, Duration>;
    using RateMap     = std::unordered_map<const BlockModel*, float>;
    using BlockEdge   = std::pair<const BlockModel*, const BlockModel*>;

    struct Task {
        std::shared_ptr<BlockModel> block;
        JobParameters               parameters{};
        JobState<TClock>            jobs{};
        ProductionLog<TClock>       production{};        // deadlines carried by this block's released output, for its consumers to inherit
        Duration                    relativeDeadline{};
        Duration                    effectiveDeadline{}; // relativeDeadline after precedence modification; ORDERING ONLY, never miss accounting
        TimePoint                   absoluteDeadline{};  // deadline of the oldest outstanding job (inherited or own); valid while one is pending
        std::int64_t                fixedPriority      = std::numeric_limits<std::int64_t>::max();
        std::uint64_t               nDispatches        = 0UZ;
        std::uint64_t               firstDispatchOrder = std::numeric_limits<std::uint64_t>::max();
        std::uint64_t               nInheritanceGaps   = 0UZ; // release groups for which a feeding log had nothing to inherit (own deadline used)
        std::uint64_t               outputNumerator    = 1UZ; // output units per work unit = outputNumerator / outputDenominator (resampling ratio)
        std::uint64_t               outputDenominator  = 1UZ;
        bool                        retired            = false; // work() reported DONE; excluded from selection

        // Incremental-scan state. A block's port occupancy only changes when it runs or when a graph
        // neighbour runs, and the release observation is idempotent for unchanged occupancy, so a task that
        // is not dirty needs neither a release observation nor a readiness probe.
        std::vector<std::size_t> consumers{};              // indices into this runner's task vector
        std::vector<std::size_t> producers{};              // indices into this runner's task vector
        std::vector<std::size_t> feeds{};                  // producers over forward edges on this runner: the logs this block inherits deadlines from
        std::vector<std::size_t> fed{};                    // consumers over forward edges on this runner: their completed units bound what the log keeps
        bool                     dirty       = true;
        bool                     queued      = false;      // sits in the selector's dirty queue
        bool                     ready       = false;      // cached result of isReady(), valid while !dirty
        bool                     alwaysDirty = false;      // async ports, or a neighbour on another worker
        bool                     eligible    = false;      // released, ready and not retired; valid while !dirty
        bool                     outputLimited = false;     // `available` was capped by output room, not by input
        std::int64_t             keyDeadlineNs = std::numeric_limits<std::int64_t>::max();  // valid while !dirty
        std::uint64_t            keyReleaseNs  = std::numeric_limits<std::uint64_t>::max(); // valid while !dirty

        static constexpr std::size_t kNotStalled = std::numeric_limits<std::size_t>::max();
        std::size_t stalledAtProgress = kNotStalled; // a withdrawn premature release parks the task until progress moves
        TimePoint   stalledUntil{};                  // ... and, for a source with a known period, until it can be due again
        TimePoint   lastProduction{};                // last dispatch at which a source actually produced
        TimePoint   nextDue{};                       // estimated nominal instant of the source's next job (valid once it has produced)

        /// cumulative work units -> cumulative output units, i.e. what a consumer counts as input units
        [[nodiscard]] std::uint64_t outputUnits(std::uint64_t workUnits) const noexcept { return workUnits * outputNumerator / outputDenominator; }
    };

    // nMisses / maxLateness / maxResponseTime are per job: response = completion - release, where release is the
    // nominal due instant for a paced source with a known period and the arrival (next-probe) instant for every other
    // block. A job misses when it completes after its absolute deadline, which it inherits from the producer jobs
    // whose output it consumes (never later than its own release + relative deadline), so a miss at a sink is the
    // end-to-end tardiness the benchmarks measure there. Jobs that never complete are not counted.
    struct Statistics {
        std::uint64_t nDispatches      = 0UZ; // work() calls issued by policy selection
        std::uint64_t nMisses          = 0UZ; // jobs completed after their (inherited or own) absolute deadline
        Duration      maxLateness{};
        std::uint64_t nSelections      = 0UZ; // dispatch passes that found a ready released job
        std::uint64_t nSweeps          = 0UZ; // dispatch passes that used the bounded fallback sweep
        std::uint64_t nProbes          = 0UZ; // port probes issued on the dispatch path
        std::uint64_t nReleased        = 0UZ;
        std::uint64_t nCompleted       = 0UZ;
        std::uint64_t nCancelled       = 0UZ;
        std::uint64_t nWithdrawn       = 0UZ; // premature releases taken back on a zero-work dispatch
        std::uint64_t nPending         = 0UZ;
        std::uint64_t nReleaseOverflows = 0UZ;
        Duration      maxResponseTime{};
        std::uint64_t nInheritanceGaps     = 0UZ; // release groups that found no producer record to inherit a deadline from
        std::uint64_t nProductionMerges    = 0UZ; // production-log overflows; each one makes some inherited deadline pessimistic
        std::uint64_t nSelectionMismatches = 0UZ; // heap selections that disagreed with a linear scan (only counted with verify_selection)
    };

    /// per-block counters: the always-on, allocation-free alternative to a full event trace
    struct BlockStatistics {
        std::string   name;
        std::size_t   batchSize = 0UZ;
        Duration      period{};
        Duration      relativeDeadline{};
        Duration      effectiveDeadline{};
        std::uint64_t nDispatches      = 0UZ;
        std::uint64_t nReleased        = 0UZ;
        std::uint64_t nCompleted       = 0UZ;
        std::uint64_t nMissed          = 0UZ;
        std::uint64_t nWithdrawn       = 0UZ;
        std::uint64_t nInheritanceGaps = 0UZ;
        Duration      maxResponseTime{};
        Duration      meanResponseTime{};
        Duration      maxLateness{};
        std::array<std::uint64_t, JobState<TClock>::kHistogramBuckets> responseHistogram{}; // see JobState::kHistogramBuckets
    };

    // Per-worker selection state: an indexed min-heap of the eligible tasks keyed by the policy, the queue of
    // tasks whose key may have moved since they were last probed, and the list of parked tasks. A selection then
    // costs O(dirty * log n) rather than a scan over every task. The heap holds exactly the tasks a linear scan
    // would consider and orders them by the same key -- which ends in the task index, so it is a total order --
    // hence it yields the very task the scan would; `verify_selection` asserts that at run time.
    struct Selector {
        using Key = decltype(std::declval<const TPolicy&>().key(std::declval<const BlockModel&>(), std::declval<const SchedState&>()));
        static constexpr std::size_t kNotInHeap = std::numeric_limits<std::size_t>::max();

        std::vector<std::size_t> heap;        // task indices ordered by key; heap[0] is the most urgent
        std::vector<std::size_t> position;    // task index -> heap slot, or kNotInHeap
        std::vector<Key>         keys;        // valid while the task is in the heap
        std::vector<std::size_t> dirtyQueue;  // tasks to (re-)probe before the next selection
        std::vector<std::size_t> parked;      // tasks waiting for progress and/or time before they may be re-probed
        std::vector<std::size_t> alwaysDirty; // async ports or off-worker neighbours: re-probed on every selection

        void reset(std::size_t nTasks) {
            heap.clear();
            heap.reserve(nTasks);
            position.assign(nTasks, kNotInHeap);
            keys.assign(nTasks, Key{});
            dirtyQueue.clear();
            dirtyQueue.reserve(nTasks);
            parked.clear();
            parked.reserve(nTasks);
            alwaysDirty.clear();
        }

        [[nodiscard]] bool        empty() const noexcept { return heap.empty(); }
        [[nodiscard]] std::size_t top() const noexcept { return heap.front(); }
        [[nodiscard]] bool        contains(std::size_t index) const noexcept { return position[index] != kNotInHeap; }

        void update(std::size_t index, const Key& key) {
            keys[index] = key;
            if (position[index] == kNotInHeap) {
                position[index] = heap.size();
                heap.push_back(index);
                siftUp(position[index]);
                return;
            }
            siftDown(siftUp(position[index]));
        }

        void erase(std::size_t index) noexcept {
            const std::size_t slot = position[index];
            if (slot == kNotInHeap) {
                return;
            }
            const std::size_t last = heap.size() - 1UZ;
            if (slot != last) {
                swapSlots(slot, last);
            }
            heap.pop_back();
            position[index] = kNotInHeap;
            if (slot < heap.size()) {
                siftDown(siftUp(slot));
            }
        }

    private:
        [[nodiscard]] bool less(std::size_t slotA, std::size_t slotB) const noexcept { return keys[heap[slotA]] < keys[heap[slotB]]; }

        void swapSlots(std::size_t slotA, std::size_t slotB) noexcept {
            std::swap(heap[slotA], heap[slotB]);
            position[heap[slotA]] = slotA;
            position[heap[slotB]] = slotB;
        }

        std::size_t siftUp(std::size_t slot) noexcept {
            while (slot > 0UZ) {
                const std::size_t parent = (slot - 1UZ) / 2UZ;
                if (!less(slot, parent)) {
                    break;
                }
                swapSlots(slot, parent);
                slot = parent;
            }
            return slot;
        }

        std::size_t siftDown(std::size_t slot) noexcept {
            const std::size_t n = heap.size();
            while (true) {
                const std::size_t left     = 2UZ * slot + 1UZ;
                const std::size_t right    = left + 1UZ;
                std::size_t       smallest = slot;
                if (left < n && less(left, smallest)) {
                    smallest = left;
                }
                if (right < n && less(right, smallest)) {
                    smallest = right;
                }
                if (smallest == slot) {
                    return slot;
                }
                swapSlots(slot, smallest);
                slot = smallest;
            }
        }
    };

    // The null profiler's handler methods are no-ops, but their arguments are still evaluated at every call
    // site: a std::string of the block name plus an initializer_list of arg_value, heap-allocated and thrown
    // away per dispatch. Constructing those payloads is only worth it when a real profiler is attached.
    static constexpr bool kTracing = !std::same_as<TProfiler, profiling::null::Profiler>;

    static constexpr Duration kFallbackDeadline = std::chrono::milliseconds(10);
    // Below this many tasks per worker a linear scan over the eligible set is cheaper than keeping the heap
    // ordered on every probe (measured on an 8-block chain at 64-sample batches: the heap's per-probe sift and
    // key bookkeeping cost more than the scan it replaces); above it the O(log n) selection wins.
    static constexpr std::size_t kHeapThreshold = 32UZ;
    static constexpr Duration kMinimumDeadline  = std::chrono::nanoseconds(1); // floor for the precedence fixpoint

    using base_t::base_t;

    std::vector<std::vector<Task>> tasksPerRunner; // indexed by runnerID; disjoint, so lock-free on the dispatch path
    std::vector<Selector>          selectorPerRunner;
    std::vector<std::uint64_t>     nSelectionsPerRunner;
    std::vector<std::uint64_t>     nSweepsPerRunner;
    std::vector<std::uint64_t>     nProbesPerRunner; // port probes on the dispatch path; per worker so no write is shared
    std::vector<std::uint64_t>     nMismatchesPerRunner;
    std::vector<std::uint8_t>      rtAppliedPerRunner;
    RateMap                        propagatedRates;
    std::unordered_map<const BlockModel*, std::vector<const BlockModel*>> consumersOf;  // block -> blocks it feeds
    std::unordered_map<const BlockModel*, std::vector<const BlockModel*>> producersOf;  // block -> blocks feeding it
    std::set<BlockEdge>                                                   backEdges;    // loop-closing edges: no deadline flows across them
    std::unordered_map<const BlockModel*, std::size_t>                    bufferBounds; // block -> smallest buffer on any of its edges
    Duration                       defaultDeadline          = kFallbackDeadline;
    bool                           precedenceDeadlines      = true;
    bool                           deadlinePartitioning     = true;
    bool                           implicitDeadlines        = true;  // unannotated deadline equals the fixed-batch period
    bool                           bufferPressureDeadlines  = false; // optional secondary EDF key adjustment
    bool                           verifySelection          = false; // debug: cross-check every heap selection against a linear scan
    int                            heapSelection            = -1;    // see kHeapSelectionKey
    int                            traceLevel               = 2;     // 0: counters only; 1: job events, no candidate lists or idle pokes; 2: everything
    int                            realTimePriority         = 0;     // opt-in: 0 leaves worker threads on the default policy
    std::size_t                    nInfeasibleDeadlines     = 0UZ;   // precedence constraints that cannot meet their deadlines
    std::uint64_t                  nextDispatchOrder        = 0UZ;
    TimePoint                      traceEpoch{};                    // reference instant for trace time stamps

    void customInit() {
        [[maybe_unused]] const auto pe = this->_profilerHandler->startCompleteEvent("edf.init");

        readSchedulerSettings();

        const gr::Graph                               flatGraph = gr::graph::flatten(*this->_graph);
        const std::vector<std::shared_ptr<BlockModel>> blockList(flatGraph.blocks().begin(), flatGraph.blocks().end());
        const std::size_t                             nBatches = batchCount(blockList.size());
        backEdges                                              = collectBackEdges(flatGraph);
        bufferBounds                                           = computeBufferBounds(flatGraph);
        propagatedRates                                        = computePropagatedRates(flatGraph, blockList);
        std::tie(consumersOf, producersOf)                     = computeAdjacency(flatGraph);

        // deadlines are a whole-graph property, so they are resolved before partitioning; the partitioner then
        // has the effective deadlines it needs to balance urgency across workers
        const DeadlineMap effective = computeEffectiveDeadlines(flatGraph, blockList);

        JobLists jobs = deadlinePartitioning ? partitionByUrgency(blockList, nBatches, effective) : detail::batchBlocks(blockList, nBatches);

        std::lock_guard lock(this->_executionOrderMutex);
        std::lock_guard guard(this->_adoptionBlocksMutex);
        this->_adoptionBlocks.clear();
        this->_adoptionBlocks.resize(nBatches);
        *this->_executionOrder = std::move(jobs);

        tasksPerRunner.assign(nBatches, {});
        selectorPerRunner.assign(nBatches, {});
        nSelectionsPerRunner.assign(nBatches, 0UZ);
        nSweepsPerRunner.assign(nBatches, 0UZ);
        nProbesPerRunner.assign(nBatches, 0UZ);
        nMismatchesPerRunner.assign(nBatches, 0UZ);
        rtAppliedPerRunner.assign(nBatches, 0U);
        nextDispatchOrder = 0UZ;

        const TimePoint now = TClock::now();
        traceEpoch          = now;
        for (std::size_t runnerID = 0UZ; runnerID < nBatches; ++runnerID) {
            syncTasks(runnerID, (*this->_executionOrder)[runnerID]);
            for (Task& task : tasksPerRunner[runnerID]) {
                task.effectiveDeadline = effective.at(task.block.get());
                task.absoluteDeadline  = now + task.effectiveDeadline;
            }
        }
    }

    void customReset() { customInit(); }

    work::Result dispatchOnce(std::size_t runnerID, const std::vector<std::shared_ptr<BlockModel>>& blocks) {
        if (runnerID >= tasksPerRunner.size()) {
            return this->traverseBlockListOnce(blocks); // graph changed under us; fall back until the next init()
        }

        applyRealTimePriorityOnce(runnerID);

        syncTasks(runnerID, blocks);
        std::vector<Task>& tasks    = tasksPerRunner[runnerID];
        Selector&          selector = selectorPerRunner[runnerID];

        auto* trace = this->_profiler.forThisThread();

        // One pass performs up to as many selections as there are blocks, which is exactly the amount of work
        // the historic round-robin sweep does per worker-loop iteration. Each selection still independently
        // picks the most urgent released job, so the dispatch order is unchanged; only the number of returns
        // to the worker loop differs. Dispatching a single block per pass instead made every block pay the
        // loop's per-iteration cost -- work guard, message ratio, removed-block cleanup, task resync -- once
        // per work() call rather than once per sweep, which dominated the scheduler at small batch sizes.
        const std::size_t maxDispatches         = std::max(1UZ, tasks.size());
        std::size_t       performedWorkThisPass = 0UZ;
        std::size_t       requestedWorkThisPass = 0UZ;

        TimePoint now = TClock::now();
        for (std::size_t dispatch = 0UZ; dispatch < maxDispatches; ++dispatch) {
            Task* selected = selectNextReady(runnerID, now, trace);

            if (selected == nullptr) {
                if (dispatch > 0UZ) {
                    break; // already made progress this pass; the next pass re-evaluates or sweeps
                }
                // No complete fixed batch is ready. A bounded sweep preserves GR4's DONE detection and drains
                // a final partial batch while still feeding every observed release/completion into the tracker.
                nSweepsPerRunner[runnerID]++;
                markAllDirty(runnerID);
                const work::Result swept = trackedFallbackSweep(runnerID, trace);
                if (swept.status != work::Status::DONE) {
                    std::ranges::for_each(tasks, [](Task& task) { task.retired = false; });
                }
                return swept;
            }
            const auto index = static_cast<std::size_t>(selected - tasks.data());
            nSelectionsPerRunner[runnerID]++;
            selected->nDispatches++;
            if (selected->firstDispatchOrder == std::numeric_limits<std::uint64_t>::max()) {
                selected->firstDispatchOrder = gr::atomic_ref(nextDispatchOrder).fetch_add(1UZ);
            }

            const std::size_t requestedWork   = std::min(this->max_work_items.value, selected->parameters.batchSize);
            const auto        missesBefore    = selected->jobs.nMissed;
            const auto        completedBefore = selected->jobs.nCompleted;

            // sampled before work() so a timer tick that fires during the call still counts as fresh progress
            const bool        timeGatedRelease   = selected->parameters.source || !selected->parameters.tracked;
            const std::size_t progressBeforeWork = timeGatedRelease ? this->_graph->progress().value() : 0UZ;

            work::Result result{};
            if constexpr (kTracing) {
                if (traceLevel >= 2) {
                    auto dispatchEvent = trace->startCompleteEvent("scheduler.job.dispatch", "scheduler", {
                        {"block", std::string(selected->block->name())},
                        {"batch", traceInt(requestedWork)},
                        {"priority", traceInt(selected->fixedPriority)},
                        {"now_ms", traceMs(nanosecondsOf(now))},
                        {"deadline_ms", traceMs(selected->keyDeadlineNs)},
                        {"release_ms", traceMs(static_cast<std::int64_t>(selected->keyReleaseNs))},
                        {"candidates", eligibleCandidates(tasks, *selected)},
                    });
                    result = selected->block->work(requestedWork);
                    dispatchEvent.finish();
                } else if (traceLevel == 1) {
                    auto dispatchEvent = trace->startCompleteEvent("scheduler.job.dispatch", "scheduler", {
                        {"block", std::string(selected->block->name())},
                        {"batch", traceInt(requestedWork)},
                        {"priority", traceInt(selected->fixedPriority)},
                        {"now_ms", traceMs(nanosecondsOf(now))},
                        {"deadline_ms", traceMs(selected->keyDeadlineNs)},
                        {"release_ms", traceMs(static_cast<std::int64_t>(selected->keyReleaseNs))},
                    });
                    result = selected->block->work(requestedWork);
                    dispatchEvent.finish();
                } else {
                    result = selected->block->work(requestedWork);
                }
            } else {
                result = selected->block->work(requestedWork);
            }
            const auto [reportedRequest, performedWork, status] = result;
            now = TClock::now(); // also serves as the next selection's release-observation instant
            observeCompletion(*selected, performedWork, now, completedBefore, missesBefore, trace);
            if (performedWork > 0UZ && selected->parameters.source) {
                noteProduction(*selected, performedWork, now);
            }

            // A consumer gained input, so its availability genuinely moved. A producer only gained output
            // room: unless output room was what capped it, available = min(input, output) is unchanged and
            // the release observation would be a no-op, making the probe pure waste. The block that just ran
            // is re-probed because it may have another complete batch waiting.
            markDirty(runnerID, index);
            for (const std::size_t consumer : selected->consumers) {
                markDirty(runnerID, consumer);
            }
            for (const std::size_t producer : selected->producers) {
                if (tasks[producer].outputLimited) {
                    markDirty(runnerID, producer);
                }
            }

            if (status == work::Status::ERROR) {
                return {reportedRequest, performedWorkThisPass + performedWork, work::Status::ERROR};
            }
            if (status == work::Status::DONE) {
                retire(*selected, trace);
                selector.erase(index);
            } else if (performedWork == 0UZ && timeGatedRelease) {
                // The probe can only see that the block *may* run (output room, message arrival), not that it
                // is due: a paced source that is not due yet performs no work but keeps the earliest deadline,
                // so re-selecting it would spin while starving every other task. Withdraw the premature
                // release -- withdrawn, not cancelled, so the next observation may re-release it -- and park the
                // task until global progress moves: work performed elsewhere, or the wake a paced source's
                // timer thread raises when its next chunk falls due.
                withdrawPremature(*selected, trace);
                selected->stalledAtProgress = progressBeforeWork;
                selected->stalledUntil      = repokeNotBefore(*selected, now);
                selected->eligible          = false;
                selected->dirty             = false;
                selector.erase(index);
                selector.parked.push_back(index);
            }

            requestedWorkThisPass += requestedWork;
            performedWorkThisPass += performedWork;
        }

        return {requestedWorkThisPass, performedWorkThisPass, work::Status::OK};
    }

    // aggregated across workers; read while stopped or under work quiescence, as the dispatch path writes
    // these fields without synchronisation.
    [[nodiscard]] Statistics statistics() const {
        Statistics summary;
        for (const std::vector<Task>& tasks : tasksPerRunner) {
            for (const Task& task : tasks) {
                summary.nDispatches += task.nDispatches;
                summary.nMisses += task.jobs.nMissed;
                summary.maxLateness = std::max(summary.maxLateness, task.jobs.maxLateness);
                summary.nReleased += task.jobs.nReleased;
                summary.nCompleted += task.jobs.nCompleted;
                summary.nCancelled += task.jobs.nCancelled;
                summary.nWithdrawn += task.jobs.nWithdrawn;
                summary.nPending += task.jobs.nPendingJobs();
                summary.nReleaseOverflows += task.jobs.nReleaseOverflows;
                summary.maxResponseTime = std::max(summary.maxResponseTime, task.jobs.maxResponseTime);
                summary.nInheritanceGaps += task.nInheritanceGaps;
                summary.nProductionMerges += task.production.nMerged;
            }
        }
        summary.nSelections          = std::accumulate(nSelectionsPerRunner.begin(), nSelectionsPerRunner.end(), std::uint64_t{0});
        summary.nSweeps              = std::accumulate(nSweepsPerRunner.begin(), nSweepsPerRunner.end(), std::uint64_t{0});
        summary.nProbes              = std::accumulate(nProbesPerRunner.begin(), nProbesPerRunner.end(), std::uint64_t{0});
        summary.nSelectionMismatches = std::accumulate(nMismatchesPerRunner.begin(), nMismatchesPerRunner.end(), std::uint64_t{0});
        return summary;
    }

    // per-block view of the same counters plus each block's response-time histogram; same read conditions
    [[nodiscard]] std::vector<BlockStatistics> blockStatistics() const {
        std::vector<BlockStatistics> summary;
        for (const std::vector<Task>& tasks : tasksPerRunner) {
            for (const Task& task : tasks) {
                summary.push_back(BlockStatistics{
                    .name              = std::string(task.block->name()),
                    .batchSize         = task.parameters.batchSize,
                    .period            = task.parameters.period,
                    .relativeDeadline  = task.relativeDeadline,
                    .effectiveDeadline = task.effectiveDeadline,
                    .nDispatches       = task.nDispatches,
                    .nReleased         = task.jobs.nReleased,
                    .nCompleted        = task.jobs.nCompleted,
                    .nMissed           = task.jobs.nMissed,
                    .nWithdrawn        = task.jobs.nWithdrawn,
                    .nInheritanceGaps  = task.nInheritanceGaps,
                    .maxResponseTime   = task.jobs.maxResponseTime,
                    .meanResponseTime  = task.jobs.nCompleted > 0UZ ? task.jobs.totalResponseTime / static_cast<Duration::rep>(task.jobs.nCompleted) : Duration::zero(),
                    .maxLateness       = task.jobs.maxLateness,
                    .responseHistogram = task.jobs.responseHistogram,
                });
            }
        }
        return summary;
    }

    [[nodiscard]] std::uint64_t totalDeadlineMisses() const { return statistics().nMisses; }

    [[nodiscard]] std::uint64_t totalDispatches() const { return statistics().nDispatches; }

    [[nodiscard]] const Task* findTask(std::string_view blockName) const {
        for (const std::vector<Task>& tasks : tasksPerRunner) {
            const auto match = std::ranges::find_if(tasks, [blockName](const Task& task) { return task.block->name() == blockName; });
            if (match != tasks.end()) {
                return std::addressof(*match);
            }
        }
        return nullptr;
    }

private:
    void readSchedulerSettings() {
        const property_map& settings = this->sched_settings.value;
        defaultDeadline              = readDuration(settings, kDefaultDeadlineKey, defaultDeadline);
        if (const auto it = settings.find(kPrecedenceKey); it != settings.end()) {
            const auto       entry = (*it).second;
            precedenceDeadlines    = entry.value_or<bool>(true);
        }
        if (const auto it = settings.find(kPartitioningKey); it != settings.end()) {
            const auto       entry = (*it).second;
            deadlinePartitioning   = entry.value_or<bool>(true);
        }
        if (const auto it = settings.find(kImplicitDeadlineKey); it != settings.end()) {
            const auto       entry = (*it).second;
            implicitDeadlines      = entry.value_or<bool>(true);
        }
        if (const auto it = settings.find(kBufferPressureDeadlineKey); it != settings.end()) {
            const auto       entry  = (*it).second;
            bufferPressureDeadlines = entry.value_or<bool>(false);
        }
        if (const auto it = settings.find(kVerifySelectionKey); it != settings.end()) {
            const auto entry = (*it).second;
            verifySelection  = entry.value_or<bool>(false);
        }
        if (const auto it = settings.find(kHeapSelectionKey); it != settings.end()) {
            const auto entry = (*it).second;
            heapSelection    = static_cast<int>(std::clamp<std::int64_t>(entry.value_or<std::int64_t>(-1), -1, 1));
        }
        if (const auto it = settings.find(kTraceLevelKey); it != settings.end()) {
            const auto entry = (*it).second;
            traceLevel       = static_cast<int>(std::clamp<std::int64_t>(entry.value_or<std::int64_t>(2), 0, 2));
        }
        if (const auto it = settings.find(kRealTimePriorityKey); it != settings.end()) {
            const auto       entry    = (*it).second;
            const auto       requested = static_cast<int>(entry.value_or<std::int64_t>(0));
            realTimePriority          = realTimePriorityIsAttainable(requested) ? requested : 0;
        }
    }

    // GR4's setThreadSchedulingParameter() calls gr::log::fatal(), which is [[noreturn]], on an out-of-range
    // priority — so the range and the RLIMIT_RTPRIO ceiling are checked here first. A scheduler must decline to
    // go real-time, never abort the process for asking.
    [[nodiscard]] static bool realTimePriorityIsAttainable(int priority) {
#if defined(_POSIX_THREADS) && not defined(__EMSCRIPTEN__) && not defined(__APPLE__)
        if (priority <= 0) {
            return false;
        }
        const int minPriority = sched_get_priority_min(SCHED_FIFO);
        const int maxPriority = sched_get_priority_max(SCHED_FIFO);
        if (priority < minPriority || priority > maxPriority) {
            gr::log::warning("EDF: rt_priority {} outside SCHED_FIFO range [{}, {}]; staying on the default policy", priority, minPriority, maxPriority);
            return false;
        }
        struct rlimit limit {};
        if (getrlimit(RLIMIT_RTPRIO, std::addressof(limit)) != 0 || static_cast<rlim_t>(priority) > limit.rlim_cur) {
            gr::log::warning("EDF: rt_priority {} exceeds the RLIMIT_RTPRIO ceiling; staying on the default policy", priority);
            return false;
        }
        return true;
#else
        std::ignore = priority;
        return false;
#endif
    }

    // runs on the worker thread itself, which is the only place its scheduling policy can be set; attainability
    // was already established in readSchedulerSettings(), so this cannot abort.
    void applyRealTimePriorityOnce(std::size_t runnerID) {
        if (realTimePriority <= 0 || runnerID >= rtAppliedPerRunner.size() || rtAppliedPerRunner[runnerID] != 0U) {
            return;
        }
        rtAppliedPerRunner[runnerID] = 1U;
#if defined(_POSIX_THREADS) && not defined(__EMSCRIPTEN__) && not defined(__APPLE__)
        gr::thread_pool::thread::setThreadSchedulingParameter(gr::thread_pool::thread::Policy::FIFO, realTimePriority);
#endif
    }

    [[nodiscard]] std::size_t batchCount(std::size_t nBlocks) const {
        if constexpr (execution == ExecutionPolicy::multiThreaded) {
            return std::max(1UZ, std::min(static_cast<std::size_t>(this->_pool->maxThreads()), nBlocks));
        } else {
            return 1UZ;
        }
    }

    // A primed feedback edge carries no intra-iteration precedence constraint: GR4 injects initial samples via
    // calculateLoopPrimingSize()/primeLoop(), so the consumer reads the previous iteration's data rather than
    // waiting on this one. Excluding the closing edge of each detected cycle therefore leaves a genuine DAG,
    // which is what the deadline transform below requires. detectFeedbackLoops() records each cycle's edges in
    // traversal order, so the closing (back-)edge is the last of them.
    // Running a block affects its two sides differently, and that asymmetry is worth keeping: a consumer
    // gains input, whereas a producer only gains output room. Back edges are included deliberately, since a
    // primed feedback edge still carries data between the two blocks.
    using Adjacency = std::unordered_map<const BlockModel*, std::vector<const BlockModel*>>;

    [[nodiscard]] static std::pair<Adjacency, Adjacency> computeAdjacency(const gr::Graph& flatGraph) {
        Adjacency consumers;
        Adjacency producers;
        for (const gr::Edge& edge : flatGraph.edges()) {
            const BlockModel* source      = edge.sourceBlock().get();
            const BlockModel* destination = edge.destinationBlock().get();
            if (source == nullptr || destination == nullptr) {
                continue;
            }
            consumers[source].push_back(destination);
            producers[destination].push_back(source);
        }
        const auto deduplicate = [](Adjacency& map) {
            for (auto& [block, list] : map) {
                std::ranges::sort(list);
                const auto duplicates = std::ranges::unique(list);
                list.erase(duplicates.begin(), duplicates.end());
            }
        };
        deduplicate(consumers);
        deduplicate(producers);
        return {std::move(consumers), std::move(producers)};
    }

    // Resolves each task's neighbour list to indices within this worker's task vector. A neighbour owned by
    // another worker cannot be tracked from here, so the task is pinned dirty instead; the same applies to
    // async ports, whose occupancy can change without any block in this graph running. Forward (non-loop)
    // edges between in-worker tasks additionally become deadline-inheritance feeds. The selector is rebuilt
    // from scratch: every task starts dirty, and a task that was parked stays parked.
    void linkNeighbours(std::vector<Task>& tasks, Selector& selector) const {
        std::unordered_map<const BlockModel*, std::size_t> position;
        position.reserve(tasks.size());
        for (std::size_t index = 0UZ; index < tasks.size(); ++index) {
            position.emplace(tasks[index].block.get(), index);
        }

        selector.reset(tasks.size());
        for (std::size_t index = 0UZ; index < tasks.size(); ++index) {
            Task& task = tasks[index];
            task.consumers.clear();
            task.producers.clear();
            task.feeds.clear();
            task.fed.clear();
            task.dirty       = true;
            task.queued      = false;
            task.eligible    = false;
            task.alwaysDirty = task.block->hasAsyncInputPorts() || task.block->hasAsyncOutputPorts();

            const auto resolve = [&](const Adjacency& map, std::vector<std::size_t>& into, std::vector<std::size_t>& forward, bool neighbourIsProducer) {
                const auto found = map.find(task.block.get());
                if (found == map.end()) {
                    return;
                }
                for (const BlockModel* neighbour : found->second) {
                    const auto at = position.find(neighbour);
                    if (at == position.end()) {
                        task.alwaysDirty = true; // lives on another worker; its effect on us is unobservable here
                        continue;
                    }
                    into.push_back(at->second);
                    const BlockEdge edge = neighbourIsProducer ? BlockEdge{neighbour, task.block.get()} : BlockEdge{task.block.get(), neighbour};
                    if (!backEdges.contains(edge)) {
                        forward.push_back(at->second);
                    }
                }
            };
            resolve(consumersOf, task.consumers, task.fed, false);
            resolve(producersOf, task.producers, task.feeds, true);

            if (task.alwaysDirty) {
                selector.alwaysDirty.push_back(index);
            }
            if (task.stalledAtProgress != Task::kNotStalled) {
                selector.parked.push_back(index);
            } else {
                task.queued = true;
                selector.dirtyQueue.push_back(index);
            }
        }
    }

    // smallest buffer on any edge touching a block: an upper bound on the batch a job can ever be released with
    [[nodiscard]] static std::unordered_map<const BlockModel*, std::size_t> computeBufferBounds(const gr::Graph& flatGraph) {
        std::unordered_map<const BlockModel*, std::size_t> bounds;
        for (const gr::Edge& edge : flatGraph.edges()) {
            const std::size_t size = edge.minBufferSize();
            if (size == 0UZ || size == std::numeric_limits<std::size_t>::max()) {
                continue;
            }
            for (const BlockModel* block : {static_cast<const BlockModel*>(edge.sourceBlock().get()), static_cast<const BlockModel*>(edge.destinationBlock().get())}) {
                if (block == nullptr) {
                    continue;
                }
                if (const auto [at, inserted] = bounds.try_emplace(block, size); !inserted) {
                    at->second = std::min(at->second, size);
                }
            }
        }
        return bounds;
    }

    // the batch an unannotated block gets: the scheduler's work quantum, or -- when that is unbounded -- one full
    // buffer, the same quantum round-robin processes per work() call. 0 when neither is known.
    [[nodiscard]] std::size_t defaultBatchFor(const BlockModel* block) const {
        std::size_t batch = this->max_work_items.value;
        if (const auto bound = bufferBounds.find(block); bound != bufferBounds.end()) {
            batch = std::min(batch, bound->second);
        }
        return batch == std::numeric_limits<std::size_t>::max() ? 0UZ : batch;
    }

    [[nodiscard]] static std::set<BlockEdge> collectBackEdges(const gr::Graph& flatGraph) {
        std::set<BlockEdge> backEdges;
        for (const graph::FeedbackLoop& loop : gr::graph::detectFeedbackLoops(flatGraph)) {
            if (loop.edges.empty()) {
                continue;
            }
            const gr::Edge& closing = loop.edges.back();
            backEdges.emplace(closing.sourceBlock().get(), closing.destinationBlock().get());
        }
        return backEdges;
    }

    /**
     * Propagates source work rates through the acyclic part of the flowgraph. A normal block's work unit is
     * an input sample, while its output rate is scaled by output_chunk_size / input_chunk_size. At a join the
     * slowest synchronous input governs the rate. Local sample-rate or period annotations are authoritative;
     * propagation only fills gaps. The result is deliberately best effort until GR4 exposes port-level rate
     * equations as a graph service.
     */
    [[nodiscard]] RateMap computePropagatedRates(const gr::Graph& flatGraph, const std::vector<std::shared_ptr<BlockModel>>& blockList) const {
        RateMap                      rates;
        std::set<const BlockModel*>  pinned;

        for (const std::shared_ptr<BlockModel>& block : blockList) {
            const bool isSource = block->blockInputTypes().empty();
            if (const std::optional<float> declared = detail::declaredSampleRate(isSource ? block->outputMetaInfos(true) : block->inputMetaInfos(true)); declared.has_value()) {
                rates[block.get()] = *declared;
                pinned.insert(block.get());
                continue;
            }

            if (const std::uint64_t periodNs = detail::readUnsigned(block->metaInformation(), kPeriodKey); periodNs > 0UZ) {
                const JobParameters parameters = deriveJobParameters(*block, defaultDeadline, std::nullopt, defaultBatchFor(block.get()));
                rates[block.get()] = static_cast<float>(static_cast<double>(parameters.batchSize) * 1e9 / static_cast<double>(periodNs));
                pinned.insert(block.get());
            }
        }

        // `backEdges` (member) was resolved from this graph in customInit() before either pass runs
        for (std::size_t round = 0UZ; round < blockList.size(); ++round) {
            bool changed = false;
            for (const gr::Edge& edge : flatGraph.edges()) {
                BlockModel* source      = edge.sourceBlock().get();
                BlockModel* destination = edge.destinationBlock().get();
                if (backEdges.contains(BlockEdge{source, destination}) || pinned.contains(destination)) {
                    continue;
                }

                const auto sourceRate = rates.find(source);
                if (sourceRate == rates.end()) {
                    continue;
                }

                float outputRate = sourceRate->second;
                if (!source->blockInputTypes().empty()) {
                    const gr::Ratio ratio = source->resamplingRatio();
                    if (ratio.numerator <= 0 || ratio.denominator <= 0) {
                        continue;
                    }
                    outputRate *= static_cast<float>(ratio.denominator) / static_cast<float>(ratio.numerator);
                }
                if (!(outputRate > 0.f)) {
                    continue;
                }

                const auto [destinationRate, inserted] = rates.try_emplace(destination, outputRate);
                if (inserted) {
                    changed = true;
                } else if (outputRate < destinationRate->second) {
                    destinationRate->second = outputRate;
                    changed                 = true;
                }
            }
            if (!changed) {
                break;
            }
        }
        return rates;
    }

    [[nodiscard]] std::optional<float> propagatedRateFor(const BlockModel* block) const {
        if (const auto rate = propagatedRates.find(block); rate != propagatedRates.end()) {
            return rate->second;
        }
        return std::nullopt;
    }

    // Deadline modification for precedence-constrained EDF: a block inherits the urgency of everything
    // downstream of it, via
    //   d*(i) = min( d(i), min over successors j of ( d*(j) - C(j) ) )
    // Classical EDF assumes independent jobs; dataflow blocks are ordered by their edges, so without this a
    // source feeding an urgent sink would be scheduled as if it were slack. Relaxation runs over the acyclic
    // graph left after removing back-edges, so it converges within the longest path length; hitting
    // kMinimumDeadline now means the task set is genuinely infeasible rather than that the iteration ran away,
    // and is counted rather than silently clamped.
    //
    // [1] J. Blazewicz, "Scheduling dependent tasks with different arrival times to meet deadlines", in
    //     Modelling and Performance Evaluation of Computer Systems, North-Holland, 1976, pp. 57-65.
    // [2] H. Chetto, M. Silly, and T. Bouchentouf, "Dynamic scheduling of real-time tasks under precedence
    //     constraints", Real-Time Systems, vol. 2, no. 3, pp. 181-194, 1990.
    [[nodiscard]] DeadlineMap computeEffectiveDeadlines(const gr::Graph& flatGraph, const std::vector<std::shared_ptr<BlockModel>>& blockList) {
        DeadlineMap effective;
        DeadlineMap cost;
        for (const std::shared_ptr<BlockModel>& block : blockList) {
            const JobParameters parameters = deriveJobParameters(*block, defaultDeadline, propagatedRateFor(block.get()), defaultBatchFor(block.get()));
            effective[block.get()] = relativeDeadlineFor(*block, parameters);
            cost[block.get()]      = readDuration(block->metaInformation(), kWcetKey, Duration::zero());
        }

        nInfeasibleDeadlines = 0UZ;
        if (!precedenceDeadlines) {
            return effective;
        }

        // `backEdges` (member) was resolved from this graph in customInit() before either pass runs

        for (std::size_t round = 0UZ; round < blockList.size(); ++round) {
            bool changed = false;
            for (const gr::Edge& edge : flatGraph.edges()) {
                const BlockModel* sourceBlock      = edge.sourceBlock().get();
                const BlockModel* destinationBlock = edge.destinationBlock().get();
                if (backEdges.contains(BlockEdge{sourceBlock, destinationBlock})) {
                    continue;
                }

                const auto source      = effective.find(sourceBlock);
                const auto destination = effective.find(destinationBlock);
                if (source == effective.end() || destination == effective.end()) {
                    continue;
                }

                const Duration required = destination->second - cost.at(destinationBlock);
                if (required < kMinimumDeadline) {
                    nInfeasibleDeadlines++;
                }
                if (const Duration candidate = std::max(kMinimumDeadline, required); candidate < source->second) {
                    source->second = candidate;
                    changed        = true;
                }
            }
            if (!changed) {
                break;
            }
        }
        return effective;
    }

    // Greedy least-loaded assignment over blocks ordered by descending utilisation C(i)/d*(i), the standard
    // first-fit-decreasing heuristic for partitioned EDF. GR4's own stride slicing is arbitrary with respect to
    // deadlines and will happily co-locate every urgent block on one worker.
    [[nodiscard]] JobLists partitionByUrgency(const std::vector<std::shared_ptr<BlockModel>>& blockList, std::size_t nBatches, const DeadlineMap& effective) const {
        std::vector<std::size_t> order(blockList.size());
        std::iota(order.begin(), order.end(), 0UZ);

        const auto utilisation = [&](std::size_t index) {
            const BlockModel* block    = blockList[index].get();
            const Duration    deadline = std::max(kMinimumDeadline, effective.at(block));
            const Duration    wcet     = readDuration(block->metaInformation(), kWcetKey, Duration::zero());
            // with no declared WCET every block contributes equally, so the partitioner degrades to balancing
            // block counts while still grouping by urgency
            const double numerator = wcet > Duration::zero() ? static_cast<double>(wcet.count()) : 1.0;
            return numerator / static_cast<double>(deadline.count());
        };

        std::ranges::sort(order, [&](std::size_t lhs, std::size_t rhs) { return utilisation(lhs) > utilisation(rhs); });

        JobLists            result(nBatches);
        std::vector<double> loadPerBatch(nBatches, 0.0);
        for (const std::size_t index : order) {
            const auto lightest = std::ranges::min_element(loadPerBatch);
            const auto batch    = static_cast<std::size_t>(std::distance(loadPerBatch.begin(), lightest));
            result[batch].push_back(blockList[index]);
            loadPerBatch[batch] += utilisation(index);
        }
        return result;
    }

    [[nodiscard]] static Duration readDuration(const property_map& map, std::string_view key, Duration fallback) {
        if (const auto it = map.find(key); it != map.end()) {
            const auto          entry = (*it).second;
            const std::uint64_t ns    = entry.value_or<std::uint64_t>(0UZ);
            if (ns > 0UZ) {
                return Duration(static_cast<std::int64_t>(ns));
            }
        }
        return fallback;
    }

    [[nodiscard]] Task makeTask(const std::shared_ptr<BlockModel>& block) const {
        const JobParameters parameters = deriveJobParameters(*block, defaultDeadline, propagatedRateFor(block.get()), defaultBatchFor(block.get()));
        const Duration      relative   = relativeDeadlineFor(*block, parameters);
        const gr::Ratio     ratio      = block->resamplingRatio();
        const bool          scaled     = !parameters.source && ratio.numerator > 0 && ratio.denominator > 0;
        return Task{
            .block             = block,
            .parameters        = parameters,
            .relativeDeadline  = relative,
            .effectiveDeadline = relative,
            .absoluteDeadline  = TClock::now() + relative,
            .fixedPriority     = readFixedPriority(*block),
            .outputNumerator   = scaled ? static_cast<std::uint64_t>(ratio.denominator) : 1UZ,
            .outputDenominator = scaled ? static_cast<std::uint64_t>(ratio.numerator) : 1UZ,
        };
    }

    // block adoption and removal mutate a worker's list mid-run, so the task table is reconciled rather than
    // assumed stable; per-task statistics survive as long as the block does.
    void syncTasks(std::size_t runnerID, const std::vector<std::shared_ptr<BlockModel>>& blocks) {
        std::vector<Task>& tasks = tasksPerRunner[runnerID];
        if (tasks.size() == blocks.size() && std::ranges::equal(tasks, blocks, {}, &Task::block)) {
            return;
        }

        std::vector<Task> reconciled;
        reconciled.reserve(blocks.size());
        for (const std::shared_ptr<BlockModel>& block : blocks) {
            const auto known = std::ranges::find(tasks, block, &Task::block);
            reconciled.push_back(known != tasks.end() ? *known : makeTask(block));
        }
        tasks = std::move(reconciled);
        linkNeighbours(tasks, selectorPerRunner[runnerID]);
    }

    void markDirty(std::size_t runnerID, std::size_t index) {
        Task& task = tasksPerRunner[runnerID][index];
        task.dirty = true;
        if (!task.queued) {
            task.queued = true;
            selectorPerRunner[runnerID].dirtyQueue.push_back(index);
        }
    }

    void markAllDirty(std::size_t runnerID) {
        for (std::size_t index = 0UZ; index < tasksPerRunner[runnerID].size(); ++index) {
            markDirty(runnerID, index);
        }
    }

    // Progress alone is a poor wake signal when many paced sources share a worker: every real dispatch moves
    // it, and each move re-arms every parked source for another fruitless poke. Under a static-priority key
    // those pokes outrank lower tasks' real work and can consume the whole slack. A source with a known period
    // therefore also parks in time. The bound must err early -- an early re-arm costs one poke, a late one
    // costs a deadline, and under EDF a late-observed release even carries a later deadline, so lateness would
    // feed on itself. Hence the earlier of two estimates, clamped to a window after the source was just found
    // not due: at least period/16 (bounds the poke rate) and at most period/8 (bounds the observation delay,
    // so a source that fell behind is polled its way back rather than being re-armed ever later).
    [[nodiscard]] static TimePoint repokeNotBefore(const Task& task, TimePoint now) {
        const Duration period = task.parameters.period;
        if (!task.parameters.source || period <= Duration::zero() || task.parameters.periodFromFallback) {
            return now;
        }
        const TimePoint fromLastProduction = task.lastProduction + period / 2;
        const TimePoint fromNominal        = task.nextDue - period / 4;
        return std::clamp(std::min(fromLastProduction, fromNominal), now + period / 16, now + period / 8);
    }

    // Tracks the nominal schedule of a source: after producing, the next job is due no later than one period
    // from now, and no later than one period per produced batch after the previous estimate.
    static void noteProduction(Task& task, std::size_t performedWork, TimePoint now) {
        const Duration period = task.parameters.period;
        const auto     nJobs  = static_cast<Duration::rep>(std::max(1UZ, performedWork / std::max(1UZ, task.parameters.batchSize)));
        const bool     first  = task.lastProduction == TimePoint{};
        task.nextDue          = first ? now + period : std::min(task.nextDue + period * nJobs, now + period);
        task.lastProduction   = now;
    }

    template<profiling::ProfilerHandlerLike THandler>
    [[nodiscard]] Task* selectNextReady(std::size_t runnerID, TimePoint now, THandler trace) {
        std::vector<Task>& tasks    = tasksPerRunner[runnerID];
        Selector&          selector = selectorPerRunner[runnerID];

        // parked tasks return to the probe queue once their time gate has passed and progress has moved
        std::optional<std::size_t> progressNow; // fetched lazily; only parked tasks need it
        for (std::size_t slot = 0UZ; slot < selector.parked.size();) {
            const std::size_t index = selector.parked[slot];
            Task&             task  = tasks[index];
            if (now < task.stalledUntil) {
                ++slot; // cannot be due yet
                continue;
            }
            if (!progressNow.has_value()) {
                progressNow = this->_graph->progress().value();
            }
            if (*progressNow == task.stalledAtProgress) {
                ++slot; // nothing has happened that could make the block producible
                continue;
            }
            task.stalledAtProgress = Task::kNotStalled;
            selector.parked[slot]  = selector.parked.back();
            selector.parked.pop_back();
            markDirty(runnerID, index); // re-probe so the release is re-observed at its true instant
        }
        for (const std::size_t index : selector.alwaysDirty) {
            markDirty(runnerID, index);
        }

        // Everything the key is built from -- release instant, deadline, readiness -- can only move when the
        // block's occupancy or job state moves, so only queued tasks are probed, in index order so that the
        // outcome is the one a full scan in list order would produce, and their heap entries updated.
        const bool useHeap = heapSelection == 1 || (heapSelection < 0 && tasks.size() > kHeapThreshold);
        if (selector.dirtyQueue.size() > 1UZ) {
            std::ranges::sort(selector.dirtyQueue);
        }
        for (const std::size_t index : selector.dirtyQueue) {
            Task& task  = tasks[index];
            task.queued = false;
            if (task.stalledAtProgress != Task::kNotStalled) {
                continue; // parked: left dirty, re-queued on revival
            }
            nProbesPerRunner[runnerID]++;
            const Probe probe = probeBlock(*task.block);
            observeReleases(tasks, task, now, trace, probe.available);
            task.ready         = probe.ready;
            task.outputLimited = probe.outputLimited;
            task.dirty         = false;

            const typename JobState<TClock>::PendingRelease* oldest = task.jobs.oldestPending();
            task.eligible                                           = !task.retired && oldest != nullptr && task.ready;
            if (task.eligible) {
                // ordered by the job's inherited deadline, or by the precedence-tightened own deadline when that
                // is earlier (it is what carries downstream urgency to a producer whose consumers annotate tighter
                // deadlines than it inherits from further upstream)
                const Duration  keyRelative = bufferPressureDeadlines ? pressureAdjusted(*task.block, task.effectiveDeadline) : task.effectiveDeadline;
                const TimePoint keyDeadline = std::min(oldest->deadline, oldest->releaseTime + keyRelative);
                task.absoluteDeadline       = oldest->deadline;
                task.keyDeadlineNs          = nanosecondsOf(keyDeadline);
                task.keyReleaseNs           = static_cast<std::uint64_t>(nanosecondsOf(oldest->releaseTime));
                if (useHeap) {
                    selector.update(index, keyOf(task, index));
                }
            } else if (useHeap) {
                selector.erase(index);
            }
        }
        selector.dirtyQueue.clear();

        if (!useHeap) {
            return linearSelection(tasks);
        }
        Task* selected = selector.empty() ? nullptr : std::addressof(tasks[selector.top()]);
        if (verifySelection && selected != linearSelection(tasks)) {
            nMismatchesPerRunner[runnerID]++;
        }
        return selected;
    }

    [[nodiscard]] static typename Selector::Key keyOf(const Task& task, std::size_t index) {
        return TPolicy{}.key(*task.block, SchedState{
                                              .index              = index,
                                              .absoluteDeadlineNs = task.keyDeadlineNs,
                                              .releaseOrder       = task.keyReleaseNs,
                                              .priority           = task.fixedPriority,
                                          });
    }

    // the reference implementation the heap is checked against: the eligible task with the smallest key
    [[nodiscard]] static Task* linearSelection(std::vector<Task>& tasks) {
        Task*                                selected = nullptr;
        std::optional<typename Selector::Key> selectedKey;
        for (std::size_t index = 0UZ; index < tasks.size(); ++index) {
            Task& task = tasks[index];
            if (!task.eligible) {
                continue;
            }
            const auto key = keyOf(task, index);
            if (!selectedKey.has_value() || key < *selectedKey) {
                selected    = std::addressof(task);
                selectedKey = key;
            }
        }
        return selected;
    }

    [[nodiscard]] static std::int64_t nanosecondsOf(TimePoint instant) noexcept { return std::chrono::duration_cast<Duration>(instant.time_since_epoch()).count(); }

    [[nodiscard]] Duration relativeDeadlineFor(BlockModel& block, const JobParameters& parameters) const {
        const Duration annotated = readDuration(block.metaInformation(), kDeadlineKey, Duration::zero());
        if (annotated > Duration::zero()) {
            return annotated;
        }
        if (implicitDeadlines && parameters.period > Duration::zero()) {
            return parameters.period;
        }
        return defaultDeadline;
    }

    // absolute steady-clock nanoseconds -> milliseconds since customInit(), the unit trace viewers show
    [[nodiscard]] double traceMs(std::int64_t absoluteNs) const noexcept {
        return static_cast<double>(absoluteNs - std::chrono::duration_cast<Duration>(traceEpoch.time_since_epoch()).count()) / 1e6;
    }

    // every other task that was eligible at this selection, with its deadline: the alternatives the policy
    // passed over, so a trace can be audited for earliest-deadline-first ordering decision by decision
    [[nodiscard]] std::string eligibleCandidates(const std::vector<Task>& tasks, const Task& selected) const {
        std::string listing;
        for (const Task& task : tasks) {
            if (!task.eligible || std::addressof(task) == std::addressof(selected)) {
                continue;
            }
            listing += std::format("{}{}@{:.3f}", listing.empty() ? "" : " ", task.block->name(), traceMs(task.keyDeadlineNs));
        }
        return listing;
    }

    template<std::integral TValue>
    [[nodiscard]] static int traceInt(TValue value) noexcept {
        if (std::in_range<int>(value)) {
            return static_cast<int>(value);
        }
        if constexpr (std::signed_integral<TValue>) {
            if (value < 0) {
                return std::numeric_limits<int>::min();
            }
        }
        return std::numeric_limits<int>::max();
    }

    template<profiling::ProfilerHandlerLike THandler>
    void observeReleases(std::vector<Task>& tasks, Task& task, TimePoint now, THandler trace, std::size_t probedAvailable) {
        if (task.retired) {
            return;
        }

        std::size_t available = probedAvailable;
        if (task.parameters.source) {
            // Output capacity only says whether a source can run; it is not a count of source arrivals.
            available = !task.jobs.hasPendingJob() && available >= task.parameters.batchSize ? task.parameters.batchSize : 0UZ;
        } else if (!task.parameters.tracked || available == std::numeric_limits<std::size_t>::max()) {
            // Message-only and otherwise occupancy-invisible blocks cannot expose a release edge. Keep one
            // best-effort job outstanding instead of manufacturing an unbounded release count.
            available = task.jobs.hasPendingJob() ? 0UZ : task.parameters.batchSize;
        }

        // A paced source's job is due at its nominal instant, not at whichever probe happened to notice output room
        // for it. Once the source has produced, nextDue -- maintained from actual production and never earlier
        // than the true grid -- is that instant, so the release is stamped nextDue: when detection ran late the
        // stamp is still the due time, and when a probe found room before the job was due (the usual premature
        // release, withdrawn once the source refuses to run) the stamp is not the poke but the instant the data
        // will actually exist. Every consumer inherits this stamp's deadline, so an early stamp would count misses
        // at every sink that the sink's own clock does not see. Every other block is released by its producer's
        // completion, which the very next probe observes, so `now` already is the arrival instant there.
        const bool      nominalKnown = task.parameters.source && task.parameters.period > Duration::zero() && !task.parameters.periodFromFallback && task.lastProduction != TimePoint{};
        const TimePoint releaseAt    = nominalKnown ? task.nextDue : now;
        const std::size_t batchSize  = task.parameters.batchSize;

        // Each job's deadline is fixed here. A source's is its own: release + D. Any other job inherits the
        // earliest deadline among the producer jobs whose output it consumes -- the record covering its first
        // input unit, since deadlines never decrease along a stream -- capped by its own release + D. Jobs that
        // fall inside the same producer records share a deadline and are released as one group.
        std::uint64_t remaining = task.jobs.releasable(available, batchSize);
        while (remaining > 0UZ) {
            const std::uint64_t firstUnit = task.jobs.nextReleaseUnit(batchSize);
            TimePoint           deadline  = releaseAt + task.relativeDeadline;
            std::uint64_t       nJobs     = remaining;
            bool                inherited = false;
            for (const std::size_t feed : task.feeds) {
                const std::optional<typename ProductionLog<TClock>::Cover> found = tasks[feed].production.cover(firstUnit);
                if (!found.has_value()) {
                    // nothing logged for these units yet (a producer ran in the fallback sweep ahead of its own
                    // release observation, or its log was truncated by a withdrawal); nor for any later unit, so
                    // the whole group falls back to the precedence-tightened own deadline
                    task.nInheritanceGaps++;
                    deadline = std::min(deadline, releaseAt + task.effectiveDeadline);
                    continue;
                }
                inherited = true;
                deadline  = std::min(deadline, found->deadline);
                nJobs     = std::min(nJobs, std::max<std::uint64_t>(1UZ, (found->validUntil - firstUnit) / batchSize));
            }
            task.jobs.release(nJobs, releaseAt, deadline);
            publishDeadline(tasks, task, deadline);
            if constexpr (kTracing) {
                if (traceLevel >= 1) {
                    trace->instantEvent("scheduler.job.release", "scheduler", {
                        {"block", std::string(task.block->name())},
                        {"jobs", traceInt(nJobs)},
                        {"batch", traceInt(batchSize)},
                        {"deadline_ms", traceMs(nanosecondsOf(deadline))},
                        {"relative_ms", static_cast<double>(task.effectiveDeadline.count()) / 1e6},
                        {"release_ms", traceMs(nanosecondsOf(releaseAt))},
                        {"own_ms", static_cast<double>(task.relativeDeadline.count()) / 1e6},
                        {"inherited", inherited ? 1 : 0},
                    });
                }
            }
            remaining -= nJobs;
        }

        task.absoluteDeadline = task.jobs.earliestDeadline().value_or(TimePoint::max());
    }

    // Extends the block's production log to the units its released jobs will publish, so in-worker consumers can
    // inherit `deadline`. Records every such consumer has already completed are dropped on the way.
    void publishDeadline(std::vector<Task>& tasks, Task& task, TimePoint deadline) {
        if (task.fed.empty()) {
            return; // nobody on this worker inherits from it
        }
        std::uint64_t retireBefore = std::numeric_limits<std::uint64_t>::max();
        for (const std::size_t consumer : task.fed) {
            const Task& fed = tasks[consumer];
            retireBefore    = std::min(retireBefore, fed.jobs.nCompleted * static_cast<std::uint64_t>(fed.parameters.batchSize));
        }
        task.production.publishUntil(task.outputUnits(task.jobs.nextReleaseUnit(task.parameters.batchSize)), deadline, retireBefore);
    }

    template<profiling::ProfilerHandlerLike THandler>
    void observeCompletion(Task& task, std::size_t performedWork, TimePoint now, std::uint64_t completedBefore, std::uint64_t missesBefore, THandler trace) {
        TimePoint lastRetiredDeadline{};
        task.jobs.observeCompletion(performedWork, task.parameters.batchSize, now, [&lastRetiredDeadline](std::uint64_t, TimePoint deadline) { lastRetiredDeadline = deadline; });
        task.absoluteDeadline = task.jobs.earliestDeadline().value_or(TimePoint::max());
        const std::uint64_t newlyCompleted = task.jobs.nCompleted - completedBefore;
        if (newlyCompleted > 0UZ) {
            if constexpr (kTracing) {
                if (traceLevel >= 1) {
                    trace->instantEvent("scheduler.job.complete", "scheduler", {
                        {"block", std::string(task.block->name())},
                        {"jobs", traceInt(newlyCompleted)},
                        {"work", traceInt(performedWork)},
                        {"deadline_ms", traceMs(nanosecondsOf(lastRetiredDeadline))},
                    });
                }
            }
        }

        const std::uint64_t newlyMissed = task.jobs.nMissed - missesBefore;
        if (newlyMissed > 0UZ) {
            if constexpr (kTracing) {
                if (traceLevel >= 1) {
                    trace->instantEvent("scheduler.job.deadline_miss", "scheduler", {
                        {"block", std::string(task.block->name())},
                        {"jobs", traceInt(newlyMissed)},
                        {"lateness_ns", traceInt(task.jobs.maxLateness.count())},
                        {"deadline_ms", traceMs(nanosecondsOf(lastRetiredDeadline))},
                        {"completed_ms", traceMs(nanosecondsOf(now))},
                    });
                    trace->counterEvent("scheduler.deadline_misses", "scheduler", {{"count", traceInt(task.jobs.nMissed)}});
                }
            }
        }
    }

    template<profiling::ProfilerHandlerLike THandler>
    void traceWithdrawal(const Task& task, std::uint64_t nJobs, std::string_view reason, THandler trace) const {
        if constexpr (kTracing) {
            if (traceLevel >= 1 && nJobs > 0UZ) {
                trace->instantEvent("scheduler.job.withdraw", "scheduler", {
                    {"block", std::string(task.block->name())},
                    {"jobs", traceInt(nJobs)},
                    {"reason", std::string(reason)},
                });
            }
        } else {
            std::ignore = trace;
        }
    }

    // work() reported DONE: outstanding jobs will never complete and are cancelled (they stay counted in nReleased)
    template<profiling::ProfilerHandlerLike THandler>
    void retire(Task& task, THandler trace) const {
        task.retired                 = true;
        task.eligible                = false;
        const std::uint64_t pending = task.jobs.nPendingJobs();
        task.jobs.cancelPending();
        traceWithdrawal(task, pending, "done", trace);
    }

    // a release the block could not act on (not due yet): taken back so the next observation may re-release it,
    // and the units it announced are struck from the production log
    template<profiling::ProfilerHandlerLike THandler>
    void withdrawPremature(Task& task, THandler trace) const {
        const std::uint64_t withdrawnBefore = task.jobs.nWithdrawn;
        task.jobs.withdrawPendingReleases();
        task.production.truncate(task.outputUnits(task.jobs.nextReleaseUnit(task.parameters.batchSize)));
        traceWithdrawal(task, task.jobs.nWithdrawn - withdrawnBefore, "not_due", trace);
    }

    template<profiling::ProfilerHandlerLike THandler>
    [[nodiscard]] work::Result trackedFallbackSweep(std::size_t runnerID, THandler trace) {
        std::vector<Task>& tasks                  = tasksPerRunner[runnerID];
        std::size_t        performedWorkAllBlocks = 0UZ;
        bool               unfinishedBlocksExist  = false;

        for (Task& task : tasks) {
            if (task.retired) {
                continue;
            }

            const TimePoint now = TClock::now();
            if (task.stalledAtProgress != Task::kNotStalled && now < task.stalledUntil) {
                unfinishedBlocksExist = true; // parked in time: poking it would only repeat the withdrawn release
                continue;
            }
            observeReleases(tasks, task, now, trace, probeBlock(*task.block).available);
            const std::size_t requestedWork   = std::min(this->max_work_items.value, task.parameters.batchSize);
            const auto        completedBefore = task.jobs.nCompleted;
            const auto        missesBefore    = task.jobs.nMissed;

            work::Result result{};
            if constexpr (kTracing) {
                if (traceLevel >= 2) {
                    auto dispatchEvent = trace->startCompleteEvent("scheduler.job.fallback", "scheduler", {
                        {"block", std::string(task.block->name())},
                        {"batch", traceInt(requestedWork)},
                    });
                    result = task.block->work(requestedWork);
                    dispatchEvent.finish();
                } else {
                    result = task.block->work(requestedWork);
                    if (traceLevel == 1 && result.performed_work > 0UZ) {
                        // idle pokes are the bulk of a trace and carry no information; only productive ones are kept
                        trace->instantEvent("scheduler.job.fallback", "scheduler", {
                            {"block", std::string(task.block->name())},
                            {"batch", traceInt(requestedWork)},
                            {"work", traceInt(result.performed_work)},
                        });
                    }
                }
            } else {
                result = task.block->work(requestedWork);
            }
            const auto [reportedRequest, performedWork, status] = result;
            performedWorkAllBlocks += performedWork;
            if (performedWork > 0UZ) {
                task.nDispatches++;
                if (task.firstDispatchOrder == std::numeric_limits<std::uint64_t>::max()) {
                    task.firstDispatchOrder = gr::atomic_ref(nextDispatchOrder).fetch_add(1UZ);
                }
            }
            const TimePoint completedAt = TClock::now();
            if (performedWork > 0UZ && task.parameters.source) {
                noteProduction(task, performedWork, completedAt); // keeps nextDue honest when the sweep, not selection, ran the source
            }
            observeCompletion(task, performedWork, completedAt, completedBefore, missesBefore, trace);

            if (status == work::Status::ERROR) {
                return {reportedRequest, performedWorkAllBlocks, work::Status::ERROR};
            }
            if (status == work::Status::DONE) {
                retire(task, trace);
            } else {
                unfinishedBlocksExist = true;
            }

            // A completed traversal is only needed to prove global quiescence for DONE detection, and that is
            // precisely the case where every element performs nothing. Once one element has made progress the
            // sweep's job is done: continuing would execute the whole block list back-to-back and stretch the
            // non-preemptive blocking term from one work() call to one full sweep, which is the very bound the
            // deadline policy exists to keep. Return to selection instead.
            if (performedWorkAllBlocks > 0UZ) {
                return {this->max_work_items, performedWorkAllBlocks, work::Status::OK};
            }
        }

        return {this->max_work_items, performedWorkAllBlocks, unfinishedBlocksExist ? work::Status::OK : work::Status::DONE};
    }

public:
    // Optional EDF key adjustment from buffer pressure: the more input a block has queued, the sooner its upstream
    // producer stalls, so backlog is treated as urgency. Sources declare no input and keep their stated
    // deadline. This is a heuristic — it needs no annotation, which is the point, but its value on real
    // flowgraphs is unmeasured and it is therefore opt-in.
    [[nodiscard]] Duration pressureAdjusted(BlockModel& block, Duration base) const {
        const std::span<const std::size_t> available = block.availableInputSamples(false);
        if (available.empty()) {
            return base;
        }
        const std::size_t backlog = *std::ranges::max_element(available);
        return std::max(kMinimumDeadline, Duration(base.count() / static_cast<Duration::rep>(1UZ + backlog)));
    }

private:
    // readiness without invoking work(): every synchronous port must clear its minimum requirement. Blocks with
    // asynchronous ports do not honour that contract, so they are always offered to the selector.
    [[nodiscard]] static bool isReady(BlockModel& block) {
        if (block.hasAsyncInputPorts() || block.hasAsyncOutputPorts()) {
            return true;
        }
        return satisfiesMinimum(block.availableInputSamples(true), block.minInputRequirements()) //
               && satisfiesMinimum(block.availableOutputSamples(true), block.minOutputRequirements());
    }

    /// Work units available to the block plus whether it can run, resolved from a single fetch of each port
    /// span. availableWorkUnits() and isReady() each fetch the input and output occupancy separately; on the
    /// dispatch path they are always wanted together, and the spans are the expensive part.
    struct Probe {
        std::size_t available     = 0UZ;
        bool        ready         = false;
        bool        outputLimited = false; // output room, not input data, is what caps `available`
    };

    [[nodiscard]] static Probe probeBlock(BlockModel& block) {
        const std::span<const std::size_t>       availableInput  = block.availableInputSamples(true);
        const std::span<const std::size_t>       availableOutput = block.availableOutputSamples(true);
        const std::span<const gr::port::BitMask> inputTypes      = block.blockInputTypes();
        const std::span<const gr::port::BitMask> outputTypes     = block.blockOutputTypes();

        const std::size_t fromInput  = detail::minAvailableOnConnectedSyncPorts(availableInput, inputTypes);
        const std::size_t outputRoom = detail::minAvailableOnConnectedSyncPorts(availableOutput, outputTypes);

        std::size_t available     = fromInput; // sink: nothing downstream can throttle it
        bool        outputLimited = false;
        if (outputRoom != std::numeric_limits<std::size_t>::max()) {
            const gr::Ratio   ratio      = block.resamplingRatio();
            const std::size_t fromOutput = (ratio.numerator <= 0 || ratio.denominator <= 0) //
                                               ? outputRoom
                                               : (outputRoom * static_cast<std::size_t>(ratio.numerator)) / static_cast<std::size_t>(ratio.denominator);
            available                    = std::min(fromInput, fromOutput);
            outputLimited                = fromOutput <= fromInput;
        }

        const bool ready = block.hasAsyncInputPorts() || block.hasAsyncOutputPorts() //
                           || (satisfiesMinimum(availableInput, block.minInputRequirements()) && satisfiesMinimum(availableOutput, block.minOutputRequirements()));

        return Probe{.available = available, .ready = ready, .outputLimited = outputLimited};
    }

    [[nodiscard]] static bool satisfiesMinimum(std::span<const std::size_t> available, std::span<const std::size_t> required) {
        const std::size_t nPorts = std::min(available.size(), required.size());
        for (std::size_t port = 0UZ; port < nPorts; ++port) {
            if (available[port] < std::max(1UZ, required[port])) {
                return false;
            }
        }
        return true;
    }
};

/// Fixed-priority variant of the same fixed-batch release/completion model used by EDF.
template<ExecutionPolicy execution = ExecutionPolicy::singleThreaded, typename TClock = std::chrono::steady_clock, profiling::ProfilerLike TProfiler = profiling::null::Profiler>
using FixedPriority = EarliestDeadlineFirst<execution, TClock, TProfiler, FixedPriorityPolicy>;

} // namespace gr::scheduler

#endif // GNURADIO_EARLIEST_DEADLINE_FIRST_HPP
