#ifndef GNURADIO_SCHEDULER_HPP
#define GNURADIO_SCHEDULER_HPP

#include <bit>
#include <chrono>
#include <mutex>
#include <queue>
#include <set>
#include <unordered_set>

#include <thread>
#include <utility>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Graph_yaml_importer.hpp>
#include <gnuradio-4.0/LifeCycle.hpp>
#include <gnuradio-4.0/Message.hpp>
#include <gnuradio-4.0/Port.hpp>
#include <gnuradio-4.0/Profiler.hpp>
#include <gnuradio-4.0/SchedulerModel.hpp> // nested-scheduler dispatch (detail::asSchedulerModel)
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/TraceFile.hpp>
#include <gnuradio-4.0/meta/indirect.hpp>
#include <gnuradio-4.0/meta/reflection.hpp>
#include <gnuradio-4.0/thread/thread_pool.hpp>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#include <emscripten/threading.h>
#endif

// Under Windows windows.h defines ERROR as 0.  This messes the ERROR function work::status::ERROR.
#ifdef _WIN32
#ifdef ERROR
#undef ERROR
#endif // #ifdef ERROR
#endif // #ifdef _WIN32

template<typename T>
inline void waitUntilChanged(gr::Sequence& sequence, T oldValue, [[maybe_unused]] unsigned int delay_ms = 1U) {
    if (sequence.value() != oldValue) {
        return;
    }
    do {
#ifdef __EMSCRIPTEN__
#ifdef __EMSCRIPTEN_PTHREADS__
        sequence.wait(oldValue); // only works in worker threads with PThreads
#else
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms)); // fallback spin sleep
#endif
#else
        sequence.wait(oldValue); // C++ native
#endif
    } while (sequence.value() == oldValue);
}

namespace gr::scheduler {
using namespace gr::message;

namespace property {

inline static const char* const kEmplaceBlock  = "EmplaceBlock";
inline static const char* const kRemoveBlock   = "RemoveBlock";
inline static const char* const kReplaceBlock  = "ReplaceBlock";
inline static const char* const kGroupBlocks   = "GroupBlocks";
inline static const char* const kUngroupBlocks = "UngroupBlocks";
inline static const char* const kEmplaceEdge   = "EmplaceEdge";
inline static const char* const kRemoveEdge    = "RemoveEdge";

inline static const char* const kTraceControl = "TraceControl";

inline static const char* const kBlockEmplaced   = "BlockEmplaced";
inline static const char* const kBlockRemoved    = "BlockRemoved";
inline static const char* const kBlockReplaced   = "BlockReplaced";
inline static const char* const kBlocksGrouped   = "BlocksGrouped";
inline static const char* const kBlocksUngrouped = "BlocksUngrouped";
inline static const char* const kEdgeEmplaced    = "EdgeEmplaced";
inline static const char* const kEdgeRemoved     = "EdgeRemoved";

inline static const char* const kGraphGRC           = "GraphGRC";
inline static const char* const kSchedulerInspect   = "SchedulerInspect";
inline static const char* const kSchedulerInspected = "SchedulerInspected";
} // namespace property

enum class ExecutionPolicy {
    singleThreaded,         /// runs the whole graph on the calling thread in one owned loop; no thread pool
    multiThreaded,          /// splits the blocks into job sets dispatched across thread-pool workers
    singleThreadedBlocking, /// single-threaded, but blocks with a time-out when no block made progress (CPU/battery power-saving)
    externalStep            /// external drive (MCU/freestanding): no owned loop, watchdog, or thread pool; the application calls step() in its superloop
};

using JobLists = std::vector<std::vector<std::shared_ptr<BlockModel>>>;

template<typename Derived, ExecutionPolicy execution = ExecutionPolicy::singleThreaded, profiling::ProfilerLike TProfiler = profiling::null::Profiler, SchedulingPolicyLike TPolicy = RoundRobinPolicy>
struct SchedulerBase : Block<Derived> {
    friend class lifecycle::StateMachine<Derived>;
    using TaskExecutor = gr::thread_pool::TaskExecutor;
    using enum block::Category;

private:
    static consteval void _forbid_reserved_overrides() {
        using Base = SchedulerBase<Derived, execution, TProfiler, TPolicy>;
        // Lifecycle callback functions init/start/stop/pause/resume/reset need to remain reserved to SchedulerBase<Derived, ...>.
        // Do NOT re-implement them in the Derived custom user-defined scheduler.
        static_assert(std::same_as<decltype(&Derived::init), decltype(&Base::init)>, "Derived defines 'init()' (reserved). Use 'customInit()' instead.");
        static_assert(std::same_as<decltype(&Derived::start), decltype(&Base::start)>, "Derived defines 'start()' (reserved). Use 'customStart()' instead.");
        static_assert(std::same_as<decltype(&Derived::stop), decltype(&Base::stop)>, "Derived defines 'stop()' (reserved). Use 'customStop()' instead.");
        static_assert(std::same_as<decltype(&Derived::pause), decltype(&Base::pause)>, "Derived defines 'pause()' (reserved). Use 'customPause()' instead.");
        static_assert(std::same_as<decltype(&Derived::resume), decltype(&Base::resume)>, "Derived defines 'resume()' (reserved). Use 'customResume()' instead.");
        static_assert(std::same_as<decltype(&Derived::reset), decltype(&Base::reset)>, "Derived defines 'reset()' (reserved). Use 'customReset()' instead.");
    }

    gr::Graph* findTargetSubGraph(const gr::property_map& data) {
        auto it = data.find("_targetGraph");
        if (it == data.end()) {
            return std::addressof(*_graph);
        } else if ((*it).second.value_or(std::string()) == _graph->unique_name || (*it).second.value_or(std::string()) == this->unique_name) {
            return std::addressof(*_graph);
        } else {
            const auto targetGraphName = (*it).second.value_or(std::string_view{});
            if (targetGraphName.empty()) {
                return nullptr;
            }
            auto result = graph::findBlock(*_graph, std::string_view(targetGraphName));
            if (!result) {
                return nullptr;
            }

            if (result.value()->typeName() != "gr::Graph") {
                return nullptr;
            }

            return static_cast<gr::Graph*>(result.value()->raw());
        }
    }

protected:
    using ProfileHandle = decltype(std::declval<TProfiler&>().forThisThread());

    // similar to std::stop_token but guarantees that the cooperating thread
    // will not do any work after stop is requested. Performance makes sense for
    // threads which mostly sleep / do little actual work. Requires that the
    // thread cooperate by sleeping on the sleepVariable.
    class WatchdogStopContext {
    private:
        std::condition_variable _sleepVariable;
        std::mutex              _requestedMutex;
        bool                    _stopRequested = false;

    public:
        [[nodiscard]] std::unique_lock<std::mutex> lock() { return std::unique_lock{_requestedMutex}; }
        [[nodiscard]] std::condition_variable&     sleepVariable() { return _sleepVariable; }
        void                                       requestStop() {
            if (gr::atomic_ref(_stopRequested).load_acquire()) {
                return; // hint, no need to take mutex
            }

            bool wasRequested = false;
            {
                auto lock    = std::unique_lock{_requestedMutex};
                wasRequested = !_stopRequested;
                gr::atomic_ref(_stopRequested).store_release(true);
            }
            if (wasRequested) {
                _sleepVariable.notify_all();
            }
        }

        /// only to be called while lock() is held
        [[nodiscard]] bool stopRequested() const { return _stopRequested; }
    };

    struct WatchdogThreadHandle {
        std::mutex                           mutex;
        std::shared_ptr<WatchdogStopContext> context;

        void stop() {
            auto lock = std::unique_lock{mutex};
            if (context) {
                context->requestStop();
            }
        }

        /// Stops the watchdog and creates a context for a new thread
        std::shared_ptr<WatchdogStopContext> stopOldThreadAndGetNewContext() {
            auto currentThreadContextLock = std::unique_lock{mutex};
            if (context) {
                context->requestStop();
            }
            context = std::make_shared<WatchdogStopContext>();
            return context;
        }
    };

    meta::indirect<gr::Graph>     _graph{};
    std::size_t                   _graphGeneration{0}; // incremented on exchange()
    TProfiler                     _profiler{};
    ProfileHandle                 _profilerHandler{_profiler.forThisThread()};
    std::shared_ptr<TaskExecutor> _pool{gr::thread_pool::Manager::instance().defaultCpuPool()};
    std::shared_ptr<gr::Sequence> _nRunningJobs = std::allocate_shared<gr::Sequence>(std::pmr::polymorphic_allocator<gr::Sequence>(this->_resources.mechanicsResource()));
    mutable std::recursive_mutex  _executionOrderMutex; // only used when modifying and copying the graph->local job list
    std::shared_ptr<JobLists>     _executionOrder = std::allocate_shared<JobLists>(std::pmr::polymorphic_allocator<JobLists>(this->_resources.mechanicsResource()));

    WatchdogThreadHandle _lastWatchDogThread;

    std::mutex                               _zombieBlocksMutex;
    std::vector<std::shared_ptr<BlockModel>> _zombieBlocks;

    // moved blocks are similar to zombies but their lifecycle state is not
    // modified, and they are guaranteed to never be referenced/worked on by
    // their worker thread again if they are placed into _movedBlocks during a
    // WorkQuiescenceGuard. They are used to move blocks from one scheduler to
    // another without stopping the whole graph.
    struct MovedBlockList {
        mutable std::unique_ptr<std::mutex>      mutex = std::make_unique<std::mutex>();
        std::vector<std::shared_ptr<BlockModel>> blocks;
    };
    std::vector<MovedBlockList> _movedBlocks;

    // for blocks that were added while scheduler was running. They need to be adopted by a thread
    std::mutex _adoptionBlocksMutex;
    // fixed-sized vector indexed by runnerId. Cheaper than a map.
    std::vector<std::vector<std::shared_ptr<BlockModel>>> _adoptionBlocks;

    MsgPortOutForChildren    _toChildMessagePort;
    MsgPortInFromChildren    _fromChildMessagePort;
    std::vector<gr::Message> _pendingMessagesToChildren;
    bool                     _messagePortsConnected = false;

    std::shared_ptr<BatchStrategy> _batchStrategy = std::make_shared<NominalBatchStrategy>();
    SchedulingAnalysis             _schedulingAnalysis{};

    /// Per-block scheduling state, mirroring `_executionOrder`'s shape and rebuilt with it under
    /// `_executionOrderMutex`. Built during setup so that `step()` -- which has no worker-local
    /// storage and must not allocate -- can index it directly.
    std::vector<std::vector<SchedState>> _schedStates{};

    /// Backing storage the per-block `JobQueue`s and successor lists span into. One entry per
    /// worker, sized and filled during setup and never touched afterwards, which is what keeps job
    /// release allocation-free in steady state. Only populated for policies that track releases.
    std::vector<std::vector<Job>>         _jobArena{};
    std::vector<std::vector<std::size_t>> _successorArena{};

    /// One heap slot per block. The heap is rebuilt each pass and a block appears in it at most
    /// once, so the block count is an exact bound and the scratch never has to grow.
    using PolicyKey = decltype(std::declval<const TPolicy&>().key(std::declval<const BlockModel&>(), std::declval<const SchedState&>()));

    struct ReadyEntry {
        PolicyKey   key{};
        std::size_t index = 0UZ;
    };

    std::vector<std::vector<ReadyEntry>> _readyHeapArena{};

    /// Multiplier applied to the block count when `max_selections_per_pass` is left at auto. A
    /// tunable heuristic, not a derived quantity: larger favours fidelity to the priority order,
    /// smaller bounds how long message handling, adoption and lifecycle transitions wait.
    static constexpr std::size_t kDefaultSelectionMultiplier = 4UZ;

public:
    /// Default clamp on a block's outstanding jobs. A tunable, not a derived quantity: the
    /// buffer-derived bound (`maxOutstandingJobs`) is correct but reserves megabytes for a depth no
    /// realistic graph reaches, and depth grows only while a released block goes unselected.
    static constexpr gr::Size_t kDefaultMaxOutstandingJobs = 64U;

private:
    std::atomic_flag _processingScheduledMessages;
    bool             _workQuiescenceRequested{false};
    std::size_t      _nWorkersInWork{0};

    void rebuildProfiler(const profiling::Options& opt) {
        std::destroy_at(std::addressof(_profiler));
        std::construct_at(std::addressof(_profiler), opt);
        _profilerHandler = _profiler.forThisThread();
    }

    void registerPropertyCallbacks() noexcept {
        _forbid_reserved_overrides();
        using PropertyCallback                            = BlockBase::PropertyCallback;
        auto& callbacks                                   = this->propertyCallbacks;
        callbacks[scheduler::property::kRemoveBlock]      = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackRemoveBlock);
        callbacks[scheduler::property::kGroupBlocks]      = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackGroupBlocks);
        callbacks[scheduler::property::kUngroupBlocks]    = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackUngroupBlocks);
        callbacks[scheduler::property::kRemoveEdge]       = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackRemoveEdge);
        callbacks[scheduler::property::kEmplaceEdge]      = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackEmplaceEdge);
        callbacks[graph::property::kInspectBlock]         = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackInspectBlock);
        callbacks[scheduler::property::kEmplaceBlock]     = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackEmplaceBlock);
        callbacks[scheduler::property::kReplaceBlock]     = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackReplaceBlock);
        callbacks[scheduler::property::kGraphGRC]         = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackGraphGRC);
        callbacks[scheduler::property::kSchedulerInspect] = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackSchedulerInspect);
        callbacks[scheduler::property::kTraceControl]     = static_cast<PropertyCallback>(&SchedulerBase::propertyCallbackTraceControl);
        this->settings().updateActiveParameters();
    }

public:
    using base_t = Block<Derived>;

    Annotated<gr::Size_t, "timeout", Unit<"ms">, Doc<"sleep timeout to wait if graph has made no progress ">>                                                                                                                                                                  timeout_ms                      = 100U;
    Annotated<gr::Size_t, "watchdog_timeout", Unit<"ms">, Doc<"sleep timeout for watchdog">>                                                                                                                                                                                   watchdog_timeout                = 1000U;
    Annotated<gr::Size_t, "timeout_inactivity_count", Doc<"number of inactive cycles w/o progress before sleep is triggered">>                                                                                                                                                 timeout_inactivity_count        = 5U;
    Annotated<gr::Size_t, "process_stream_to_message_ratio", Doc<"number of stream to msg processing">>                                                                                                                                                                        process_stream_to_message_ratio = 16U;
    Annotated<HouseKeepPolicy, "house_keeping_policy", Doc<"when to run buffer housekeeping: Light = none, Normal = riding the message-handling gate">>                                                                                                                        house_keeping_policy            = HouseKeepPolicy::Normal;
    Annotated<HouseKeepDepth, "house_keeping_depth", Doc<"per-pass reclaim depth: Shallow = clear() only (keeps slot capacity → steady-state tag publish is allocation-free), Deep = clear() + shrink_to_fit() (returns idle-slot memory, but re-allocates on next publish)">> house_keeping_depth             = HouseKeepDepth::Shallow;
    Annotated<std::string, "pool name", Doc<"default pool name">>                                                                                                                                                                                                              poolName                        = std::string(gr::thread_pool::kDefaultCpuPoolId);
    Annotated<std::size_t, "max_work_items", Doc<"number of work items per work scheduling interval (controls latency)">>                                                                                                                                                      max_work_items                  = std::numeric_limits<std::size_t>::max(); // TODO: check whether we can keep this std::size_t or more consistently to gr::Size_t
    Annotated<property_map, "sched_settings", Doc<"scheduler implementation specific settings">>                                                                                                                                                                               sched_settings{};

    Annotated<gr::Size_t, "max_selections_per_pass", Doc<"priority-class policies: cap on successful work() calls before returning to house-keeping (0: auto = 4 x block count)">>                        max_selections_per_pass = 0U;
    Annotated<SelectionStrategy, "selection_strategy", Doc<"dynamic-key policies: how the next block is picked -- linearScan (O(n), no auxiliary state) or readyHeap (O(log n), heap rebuilt per pass)">> selection_strategy      = SelectionStrategy::linearScan;
    Annotated<gr::Size_t, "max_outstanding_jobs", Doc<"release-tracking policies: cap on a block's outstanding jobs, clamping the buffer-derived bound (0: uncapped)">>                                   max_outstanding_jobs    = kDefaultMaxOutstandingJobs;

    Annotated<gr::Size_t, "trace_categories", Doc<"bitmask of live trace-marker groups (0: tracing off). Inert unless the trace layer was compiled in">>                                                                    trace_categories  = 0U;
    Annotated<gr::Size_t, "trace_buffer_size", Doc<"records retained per emitting thread; rounded up to a power of two, capped at 4 GiB/thread (a clamp is reported). Takes effect for threads that have not yet emitted">> trace_buffer_size = 65536U;

    GR_MAKE_REFLECTABLE(SchedulerBase, timeout_ms, watchdog_timeout, timeout_inactivity_count, process_stream_to_message_ratio, house_keeping_policy, house_keeping_depth, max_work_items, max_selections_per_pass, max_outstanding_jobs, selection_strategy, trace_categories, trace_buffer_size, poolName, sched_settings);

    constexpr static block::Category blockCategory = block::Category::ScheduledBlockGroup;

    [[nodiscard]] static constexpr auto executionPolicy() { return execution; }

    [[nodiscard]] static constexpr std::string_view schedulingPolicyName() { return TPolicy::kName; }

    /// Per-block periods/deadlines/priorities derived at init(); empty until the graph is initialised.
    [[nodiscard]] const SchedulingAnalysis& schedulingAnalysis() const noexcept { return _schedulingAnalysis; }

    /// Replaces the batch-operating-point strategy; takes effect at the next init()/refresh.
    void setBatchStrategy(std::shared_ptr<BatchStrategy> strategy) {
        if (strategy) {
            _batchStrategy = std::move(strategy);
        }
    }

    /**
     * Re-derives the scheduling attributes for the current graph. Read-only with respect to the
     * blocks: derived values are kept here rather than written back, so re-deriving on a later
     * start() cannot mistake a previously derived value for one the user set.
     */
    void refreshSchedulingAnalysis() {
        [[maybe_unused]] const auto pe = _profilerHandler->startCompleteEvent("scheduler_base.deriveSchedulingAttributes");

        gr::Graph flatGraph = gr::graph::flatten(*_graph);

        // sample which attributes the user set *before* deriving anything (see SchedulingAnalysis.hpp)
        std::unordered_map<const BlockModel*, UserSetAttributes> userSet;
        userSet.reserve(flatGraph.blocks().size());
        for (const std::shared_ptr<BlockModel>& block : flatGraph.blocks()) {
            userSet.emplace(block.get(), userSetFromSettings(*block));
        }

        _schedulingAnalysis = deriveSchedulingAttributes(
            flatGraph, *_batchStrategy,
            [&userSet](const BlockModel& block) {
                const auto it = userSet.find(std::addressof(block));
                return it == userSet.end() ? UserSetAttributes{} : it->second;
            },
            max_work_items); // the scheduler's own ceiling is what a block inherits when it sets none
    }

    /// Resolves each block's cached scheduling state from the analysis. A block the analysis does
    /// not know -- one adopted at run time, after the last derivation -- falls back to the
    /// scheduler's own ceiling, which is what every block received before per-block batches existed.
    void syncSchedStates(const std::vector<std::shared_ptr<BlockModel>>& blocks, std::vector<SchedState>& states, std::uint8_t workerId = 0U) const {
        states.resize(blocks.size());
        for (std::size_t i = 0UZ; i < blocks.size(); ++i) {
            // The user's own declaration is block-local, so it survives adoption intact -- which is
            // what makes an *absolute* priority scheme exact for adopted blocks where a
            // rate-monotonic one cannot be.
            const std::int32_t userPriority = static_cast<std::int32_t>(gr::scheduler::detail::settingAsDouble(*blocks[i], "sched_priority", 0.0));

            std::size_t  ceiling  = kUnboundedBatch;
            std::int32_t priority = userPriority; // a derived rank needs the graph; absent one, the user's value or 0
            if (const DerivedAttributes* attributes = _schedulingAnalysis.find(*blocks[i]); attributes != nullptr) {
                ceiling  = attributes->executionCeiling;
                priority = attributes->priority;
            } else {
                // A block adopted at run time is absent from the analysis, and stays absent: the
                // derivation runs from init() only, which a running scheduler never re-enters. So
                // resolve its ceiling directly rather than handing it the scheduler's -- every
                // input to `executionCeiling` is block-local (its own `max_batch_size`, the
                // scheduler ceiling, its own ports), so no graph pass is needed. Falling back to
                // `max_work_items` would silently discard an explicit per-block ceiling for the
                // rest of the run, breaking "user-set values always win" for exactly the
                // blocks whose configuration arrived most recently.
                ceiling = _batchStrategy->resolve(*blocks[i], static_cast<std::size_t>(max_work_items)).executionCeiling;
            }
            // Interned whenever tracing is *compiled in*, not only while a capture is live: the
            // identities have to exist already when a category is switched on, or every capture would
            // open with `kNoEntity` records until the next house-keeping pass re-synced. A known block
            // costs one hash lookup and no allocation; with tracing compiled out the
            // `if constexpr` leaves nothing at all.
            gr::trace::EntityId entityId = gr::trace::kNoEntity;
            if constexpr (gr::trace::kEnabled) {
                entityId = gr::trace::intern(std::addressof(*blocks[i]), //
                    gr::trace::EntityDescription{.uniqueName = blocks[i]->uniqueName(), .typeName = blocks[i]->typeName(), .workerId = workerId, .nInputPorts = static_cast<std::uint16_t>(blocks[i]->dynamicInputPortsSize()), .nOutputPorts = static_cast<std::uint16_t>(blocks[i]->dynamicOutputPortsSize())});
            }

            states[i] = SchedState{.index = i, .entityId = entityId, .workerId = workerId, .batchCeiling = ceiling, .priority = priority, .userPriority = userPriority};

            if constexpr (needsReleaseTracking(TPolicy::kPriorityClass)) {
                // The gates' inputs. A block the analysis does not know keeps period and deadline at
                // zero, which leaves it released on data alone -- the same
                // deliberately-imperfect treatment adopted blocks already receive for priority.
                states[i].batchFloor = gr::scheduler::detail::releaseThreshold(*blocks[i]);
                if (const DerivedAttributes* attributes = _schedulingAnalysis.find(*blocks[i]); attributes != nullptr) {
                    states[i].batchFloor              = std::max(attributes->batchFloor, 1UZ);
                    states[i].periodSeconds           = static_cast<double>(attributes->period);
                    states[i].relativeDeadlineSeconds = static_cast<double>(attributes->relativeDeadline);
                }
            }
        }
    }

    void rebuildSchedStates() {
        std::lock_guard lock(_executionOrderMutex);
        if (!_executionOrder) {
            _schedStates.clear();
            return;
        }
        _schedStates.resize(_executionOrder->size());
        for (std::size_t job = 0UZ; job < _executionOrder->size(); ++job) {
            syncSchedStates((*_executionOrder)[job], _schedStates[job], static_cast<std::uint8_t>(job));

            // Order the *shared* list too, not only the worker-local copies. `step()` executes
            // `(*_executionOrder)[0]` directly and has no worker-local copy to order, so without
            // this the same graph would run priority-ordered on the pool path and in registration
            // order under `externalStep` -- a scheduler silently ignoring its own policy parameter.
            // Done here because `applyStaticOrder` allocates and `step()` must not; init() is setup,
            // which the no-heap contract exempts. Each job list is sorted independently: reordering
            // *across* lists would override the assignment policy's grouping.
            gr::scheduler::detail::applyStaticOrder<TPolicy>((*_executionOrder)[job], _schedStates[job]);
        }

        if constexpr (needsReleaseTracking(TPolicy::kPriorityClass)) {
            _jobArena.resize(_executionOrder->size());
            _successorArena.resize(_executionOrder->size());
            _readyHeapArena.resize(_executionOrder->size());
            for (std::size_t job = 0UZ; job < _executionOrder->size(); ++job) {
                buildReleaseStorage((*_executionOrder)[job], _schedStates[job], _jobArena[job], _successorArena[job], _readyHeapArena[job]);
            }
        }
    }

    /// Sizes the job arena and the successor lists for one worker's blocks and hands each block a
    /// span into them.
    ///
    /// Must follow every `syncSchedStates()` / `applyStaticOrder()` pair: the former resets the
    /// spans wholesale, and the latter permutes the positions that successor indices address. There
    /// are two state paths -- `_schedStates` for `step()`, and the worker-local copies `poolWorker`
    /// derives -- and both must go through here, or a policy that tracks releases finds every job
    /// ring empty and never runs a block at all.
    void buildReleaseStorage(const std::vector<std::shared_ptr<BlockModel>>& blocks, std::vector<SchedState>& states, std::vector<Job>& jobArena, std::vector<std::size_t>& successorArena, std::vector<ReadyEntry>& readyHeap) const {
        jobArena.clear();
        successorArena.clear();
        readyHeap.assign(blocks.size(), ReadyEntry{});
        if (blocks.empty()) {
            return;
        }

        const gr::Graph                flatGraph = gr::graph::flatten(*_graph);
        const gr::graph::AdjacencyList adjacency = gr::graph::computeAdjacencyList(flatGraph);

        std::unordered_map<const BlockModel*, std::size_t> localIndex;
        localIndex.reserve(blocks.size());
        for (std::size_t i = 0UZ; i < blocks.size(); ++i) {
            localIndex.emplace(blocks[i].get(), i);
        }

        std::vector<std::size_t> successorOffsets(blocks.size() + 1UZ, 0UZ);
        std::vector<std::size_t> jobOffsets(blocks.size() + 1UZ, 0UZ);

        for (std::size_t i = 0UZ; i < blocks.size(); ++i) {
            if (const auto entry = adjacency.find(blocks[i]); entry != adjacency.end()) {
                for (const std::vector<const gr::Edge*>& edges : entry->second | std::views::values) {
                    for (const gr::Edge* edge : edges) {
                        // A successor on another worker is deliberately absent: its state belongs to
                        // that worker and must not be written from here. The per-sweep backstop
                        // covers those edges.
                        if (const auto local = localIndex.find(edge->destinationBlock().get()); local != localIndex.end()) {
                            successorArena.push_back(local->second);
                        }
                    }
                }
            }
            successorOffsets[i + 1UZ] = successorArena.size();

            const std::size_t cap = max_outstanding_jobs == 0U ? kUnboundedBatch : static_cast<std::size_t>(max_outstanding_jobs);
            jobArena.resize(jobArena.size() + gr::scheduler::maxOutstandingJobs(*blocks[i], std::max(states[i].batchFloor, 1UZ), cap));
            jobOffsets[i + 1UZ] = jobArena.size();
        }

        // Spans are taken only now: both vectors are complete, so no later growth can invalidate them.
        for (std::size_t i = 0UZ; i < blocks.size(); ++i) {
            states[i].successors   = std::span<const std::size_t>{successorArena}.subspan(successorOffsets[i], successorOffsets[i + 1UZ] - successorOffsets[i]);
            states[i].jobs.storage = std::span<Job>{jobArena}.subspan(jobOffsets[i], jobOffsets[i + 1UZ] - jobOffsets[i]);
        }
    }

    void requestWorkQuiescence() {
        gr::atomic_ref(_workQuiescenceRequested).store_release(true);
        // _nWorkersInWork load may not be reordered before _workQuiescenceRequested store
        std::atomic_thread_fence(std::memory_order_seq_cst);
        while (gr::atomic_ref(_nWorkersInWork).load_acquire() > 0) {
            std::this_thread::yield();
        }
    }

    void releaseWorkQuiescence() { gr::atomic_ref(_workQuiescenceRequested).store_release(false); }

    /// Invokes blockUntilWorking(), do not call when scheduler is not running or being started
    /// as it will block until it sees a worker thread/call to poolWorker
    void requestWorkQuiescenceAll() {
        requestWorkQuiescence();
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [](auto& block) {
            if (block->blockCategory() == block::Category::ScheduledBlockGroup) {
                if (auto* sm = detail::asSchedulerModel(*block)) {
                    sm->blockUntilWorking();
                    sm->requestWorkQuiescenceAll();
                }
            }
        });
    }

    void releaseWorkQuiescenceAll() {
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [](auto& block) {
            if (block->blockCategory() == block::Category::ScheduledBlockGroup) {
                if (auto* sm = detail::asSchedulerModel(*block)) {
                    sm->releaseWorkQuiescenceAll();
                }
            }
        });
        releaseWorkQuiescence();
    }

    /// If this scheduler is INITIALISED or RUNNING, this function will block the calling thread until the scheduler
    /// has begun spawning its worker threads (though the worker threads may not have started yet). This is useful
    /// when spawning another thread to call start(), because a scheduler's start() observes _graph and modifies
    /// ports before it spawns workers, so this can be used to wait until that is done.
    ///
    /// Do not call this on a scheduler which has been INITIALISED but has not (been started | is planned to be
    /// started by another thread), as this will block indefinitely in that case, waiting for another thread to
    /// issue changeStateTo(RUNNING). Calling this via the SchedulerModel, which manages the starting thread, and
    /// can check if it has been spawned, prevents this issue.
    void blockUntilWorking() {
        using enum lifecycle::State;
        if constexpr (executionPolicy() == ExecutionPolicy::externalStep) {
            return; // you are doing step() so you know when it is working :)
        }
        while (_nRunningJobs->value() == 0UZ) {
            const auto currentState = this->state();
            if (currentState != INITIALISED && currentState != RUNNING) {
                return;
            }
            std::this_thread::yield();
        }
    }

    template<typename SchedulerType>
    struct WorkQuiescenceGuard {
        SchedulerType* _scheduler;
        explicit WorkQuiescenceGuard(SchedulerType* s) : _scheduler(s) { _scheduler->requestWorkQuiescenceAll(); }
        ~WorkQuiescenceGuard() { _scheduler->releaseWorkQuiescenceAll(); }
        WorkQuiescenceGuard(const WorkQuiescenceGuard&)            = delete;
        WorkQuiescenceGuard& operator=(const WorkQuiescenceGuard&) = delete;
    };

    struct WorkGuard {
        SchedulerBase* _scheduler;
        bool           _needsDecrement         = false;
        bool           _isWorking              = false;
        WorkGuard(const WorkGuard&)            = delete;
        WorkGuard& operator=(const WorkGuard&) = delete;
        explicit WorkGuard(SchedulerBase* s) : _scheduler(s) {
            if (!gr::atomic_ref(_scheduler->_workQuiescenceRequested).load_acquire()) {
                gr::atomic_ref(_scheduler->_nWorkersInWork).fetch_add(1UZ);
                // incrementing _nWorkersInWork may not be reordered after loading _workQuiescenceRequested
                std::atomic_thread_fence(std::memory_order_seq_cst);
                _needsDecrement  = true;
                this->_isWorking = !gr::atomic_ref(_scheduler->_workQuiescenceRequested).load_acquire();
            }
        }
        ~WorkGuard() {
            if (_needsDecrement) {
                gr::atomic_ref(_scheduler->_nWorkersInWork).fetch_sub(1UZ);
            }
        }

        constexpr explicit operator bool() const { return _isWorking; }
    };

    SchedulerBase() : base_t(gr::property_map()) { registerPropertyCallbacks(); }

    SchedulerBase(std::initializer_list<std::pair<std::string_view, Value>> initParameter) noexcept(false) : base_t(initParameter) {
        registerPropertyCallbacks();
        std::ignore = this->settings().set(property_map{initParameter});
        std::ignore = this->settings().activateContext();
        std::ignore = this->settings().applyStagedParameters();
    }

    explicit SchedulerBase(property_map initParameters) noexcept(false) : base_t(initParameters) {
        registerPropertyCallbacks();
        std::ignore = this->settings().set(initParameters);
        std::ignore = this->settings().activateContext();
        std::ignore = this->settings().applyStagedParameters();
    }

    ~SchedulerBase() {
        if (this->state() == lifecycle::RUNNING) {
            if (auto e = this->changeStateTo(lifecycle::REQUESTED_STOP); !e) {
                std::println(std::cerr, "Failed to stop execution at destruction of scheduler: {} ({})", e.error().message, e.error().srcLoc());
                std::abort();
            }
        }
        waitDone();

        _lastWatchDogThread.stop();

        _executionOrder.reset(); // force earlier crashes if this is accessed after destruction (e.g. from thread that was kept running)
    }

    [[nodiscard]] std::expected<meta::indirect<Graph>, Error> exchange(meta::indirect<Graph>&& newGraph, const profiling::Options& option = {}) {
        using enum lifecycle::State;
        const auto oldState = this->state();
        if (lifecycle::isActive(oldState)) { // need to stop running scheduler
            if (auto result = this->changeStateTo(REQUESTED_STOP); !result) {
                return std::unexpected(result.error());
            }
            waitDone(); // wait for all jobs to complete

            if (auto result = this->changeStateTo(STOPPED); !result) {
                return std::unexpected(result.error());
            }
        }

        if (this->state() == ERROR || this->state() == STOPPED) {
            reset(); // reset internal states
        }

        // Watchdogs may read from _graph, potentially during the exchange() below
        _lastWatchDogThread.stop();

        gr::atomic_ref(_graphGeneration).fetch_add(1);

        auto oldGraph = std::exchange(_graph, std::move(newGraph));

        if ((option != profiling::Options{})) { // need to update profiler
            rebuildProfiler(option);
        }

        if (_pool->name() != std::string_view(poolName.value)) { // sync pool with (possibly updated) poolName setting
            const std::string_view requested{poolName.value};
            if (auto r = gr::thread_pool::Manager::instance().get(requested); r) {
                _pool = *r;
            } else {
                this->emitErrorMessage("exchange(poolName)", std::format("unknown thread pool '{}': {}; keeping existing pool '{}'", requested, r.error().message, _pool->name()));
            }
        }

        // restore the original lifecycle state
        if (lifecycle::isActive(oldState)) {
            if (auto result = this->changeStateTo(INITIALISED); !result) { // Need to go to INITIALISED first
                return std::unexpected(result.error());
            }
            if (auto result = this->changeStateTo(RUNNING); !result) {
                return std::unexpected(result.error());
            }

            if (oldState == REQUESTED_PAUSE) {
                if (auto result = this->changeStateTo(REQUESTED_PAUSE); !result) {
                    return std::unexpected(result.error());
                }
            } else if (oldState == PAUSED) {
                if (auto result = this->changeStateTo(REQUESTED_PAUSE); !result) {
                    return std::unexpected(result.error());
                }
                if (auto result = this->changeStateTo(PAUSED); !result) {
                    return std::unexpected(result.error());
                }
            }
        }
        return oldGraph;
    }

    [[nodiscard]] const gr::Graph& graph() const noexcept { return *_graph; }
    [[nodiscard]] gr::Graph&       graph() noexcept { return *_graph; }

    [[nodiscard]] const TProfiler& profiler() const noexcept { return _profiler; }

    [[nodiscard]] bool isProcessing() const
    requires(executionPolicy() == ExecutionPolicy::multiThreaded)
    {
        return _nRunningJobs->value() > 0UZ;
    }

    void settingsChanged(const property_map& /*oldSettings*/, const property_map& newSettings) noexcept {
        if constexpr (gr::trace::kEnabled) {
            // Capacity first, then categories. A ring is allocated when a thread first emits, and a
            // thread only emits once some category is live -- so setting the capacity afterwards would
            // leave every thread that had already started on the previous size.
            if (newSettings.contains("trace_buffer_size")) {
                const std::size_t requested = static_cast<std::size_t>(trace_buffer_size);
                const std::size_t adopted   = gr::trace::setRingCapacity(requested);
                if (adopted < requested) {
                    // Said out loud rather than swallowed. A silently shrunk buffer is a capture that
                    // quietly loses the oldest records, which is the failure this layer exists to avoid.
                    this->emitErrorMessage("settingsChanged(trace_buffer_size)", std::format("requested {} records per thread, clamped to {} by the {}-byte ceiling", requested, adopted, gr::trace::ringCapacityLimitBytes()));
                }
            }
            if (newSettings.contains("trace_categories")) {
                gr::trace::setCategories(static_cast<std::uint32_t>(trace_categories));
            }
        }

        if (!newSettings.contains("poolName")) {
            return;
        }
        const std::string_view requested{poolName.value};
        if (!_pool || _pool->name() == requested) {
            return;
        }
        if (auto r = gr::thread_pool::Manager::instance().get(requested); r) {
            _pool = *r;
        } else {
            this->emitErrorMessage("settingsChanged(poolName)", std::format("unknown thread pool '{}': {}; keeping existing pool '{}'", requested, r.error().message, _pool->name()));
        }
    }

    void stateChanged(lifecycle::State newState) { this->notifyListeners(block::property::kLifeCycleState, {{"state", std::string(gr::meta::enumName(newState).value_or(""))}}); }

    [[nodiscard]] std::span<std::shared_ptr<BlockModel>>       blocks() noexcept { return _graph->blocks(); }
    [[nodiscard]] std::span<const std::shared_ptr<BlockModel>> blocks() const noexcept { return _graph->blocks(); }
    [[nodiscard]] std::span<Edge>                              edges() noexcept { return _graph->edges(); }
    [[nodiscard]] std::span<const Edge>                        edges() const noexcept { return _graph->edges(); }

    void connectBlockMessagePorts() {
        const auto available = _graph->msgIn.streamReader().available();
        if (available != 0UZ) {
            ReaderSpanLike auto msgInSpan = _graph->msgIn.streamReader().get<SpanReleasePolicy::ProcessAll>(available);
            _pendingMessagesToChildren.insert(_pendingMessagesToChildren.end(), msgInSpan.begin(), msgInSpan.end());
        }

        _fromChildMessagePort.materialiseDefaultBuffer(_graph->resources().dataResource(), _graph->resources().tagResource());
        auto toSchedulerBuffer = _fromChildMessagePort.buffer();
        if (!_toChildMessagePort.connect(_graph->msgIn)) {
            this->emitErrorMessage("connectBlockMessagePorts()", "Failed to connect scheduler input message port to graph msgIn");
        }
        _graph->msgOut.setBuffer(toSchedulerBuffer.streamBuffer, toSchedulerBuffer.tagBuffer);

        graph::forEachBlock<TransparentBlockGroup>(*_graph, [this, &toSchedulerBuffer](auto& block) {
            if (!_toChildMessagePort.connect(*block->msgIn)) {
                this->emitErrorMessage("connectBlockMessagePorts()", std::format("Failed to connect scheduler input message port to child '{}'", block->uniqueName()));
            }

            block->msgOut->setBuffer(toSchedulerBuffer.streamBuffer, toSchedulerBuffer.tagBuffer);
        });

        // Forward any messages to children that were received before the scheduler was initialised
        _messagePortsConnected = true;

        WriterSpanLike auto msgSpan = _toChildMessagePort.streamWriter().template reserve<SpanReleasePolicy::ProcessAll>(_pendingMessagesToChildren.size());
        std::ranges::move(_pendingMessagesToChildren, msgSpan.begin());
        _pendingMessagesToChildren.clear();
    }

    void processMessages(gr::MsgPortInBuiltin& port, std::span<const gr::Message> messages) {
        base_t::processMessages(port, messages); // filters messages and calls own property handler

        for (const gr::Message& msg : messages) {
            if (msg.serviceName != this->unique_name && msg.serviceName != this->name && msg.endpoint != block::property::kLifeCycleState) {
                // only forward wildcard, non-scheduler messages, and non-lifecycle messages (N.B. the latter is exclusively handled by the scheduler)
                if (_messagePortsConnected) {
                    WriterSpanLike auto msgSpan = _toChildMessagePort.streamWriter().template reserve<SpanReleasePolicy::ProcessAll>(1UZ);
                    msgSpan[0]                  = msg;
                } else {
                    // if not yet connected, keep messages to children in cache and forward when connecting
                    _pendingMessagesToChildren.push_back(msg);
                }
            }
        }
    }

    void processScheduledMessages() {
        if (std::atomic_flag_test_and_set_explicit(&_processingScheduledMessages, std::memory_order_acquire)) {
            return;
        }

        on_scope_exit _ = [&] { std::atomic_flag_clear_explicit(&_processingScheduledMessages, std::memory_order_release); };

        base_t::processScheduledMessages(); // filters messages and calls own property handler

        // Process messages in the graph
        _graph->processScheduledMessages();
        if (_nRunningJobs->value() == 0UZ) {
            graph::forEachBlock<TransparentBlockGroup>(*_graph, [](auto& block) { block->processScheduledMessages(); });
        }

        ReaderSpanLike auto messagesFromChildren = _fromChildMessagePort.streamReader().get();
        if (messagesFromChildren.size() == 0) {
            return;
        }

        if (this->msgOut.buffer().streamBuffer.n_readers() == 0) {
            // nobody is listening on messages -> escalate otherwise-ignored child errors
            for (const auto& msg : messagesFromChildren) {
                if (!msg.data.has_value()) {
#if __cpp_exceptions
                    throw gr::exception(std::format("scheduler {}: throwing ignored exception {:t}", this->name, msg.data.error()));
#else
                    gr::log::error("scheduler {}: ignored child error {:t}", this->name, msg.data.error());
#endif
                }
            }
            return;
        }

        {
            WriterSpanLike auto msgSpan = this->msgOut.streamWriter().template reserve<SpanReleasePolicy::ProcessAll>(messagesFromChildren.size());
            std::ranges::copy(messagesFromChildren, msgSpan.begin());
            msgSpan.publish(messagesFromChildren.size());
        } // to force publish
        if (!messagesFromChildren.consume(messagesFromChildren.size())) {
            this->emitErrorMessage("process child return messages", "Failed to consume messages from child message port");
        }
    }

    std::expected<void, Error> runAndWait() {
        using enum lifecycle::State;
        [[maybe_unused]] const auto pe = this->_profilerHandler->startCompleteEvent("scheduler_base.runAndWait");
        processScheduledMessages(); // make sure initial subscriptions are processed
        if (this->state() == STOPPED || this->state() == ERROR) {
            if (auto e = this->changeStateTo(INITIALISED); !e) {
                this->emitErrorMessage("runAndWait() -> LifecycleState", e.error());
                return std::unexpected(e.error());
            }
        }
        if (this->state() == IDLE) {
            if (auto e = this->changeStateTo(INITIALISED); !e) {
                this->emitErrorMessage("runAndWait() -> LifecycleState", e.error());
                return std::unexpected(e.error());
            }
        }
        if (auto e = this->changeStateTo(RUNNING); !e) {
            this->emitErrorMessage("runAndWait() -> LifecycleState", e.error());
            return std::unexpected(e.error());
        }

        // N.B. the transition to lifecycle::State::RUNNING will for the ExecutionPolicy:
        // * singleThreaded[Blocking] naturally block in the calling thread
        // * multiThreaded[Blocking] spawn two worker and block on 'waitDone()'
        waitDone();
        processScheduledMessages();

        if (this->state() == RUNNING) {
            if (auto e = this->changeStateTo(REQUESTED_STOP); !e) {
                this->emitErrorMessage("runAndWait() -> LifecycleState", e.error());
                return std::unexpected(e.error());
            }
        }
        if (this->state() == REQUESTED_STOP) {
            if (auto e = this->changeStateTo(STOPPED); !e) {
                this->emitErrorMessage("runAndWait() -> LifecycleState", e.error());
            }
        }
        processScheduledMessages();
        return {};
    }

    void waitDone(bool isCalledFromWorker = false) {
        [[maybe_unused]] const auto pe = _profilerHandler->startCompleteEvent("scheduler_base.waitDone");
        while (_nRunningJobs->value() > (isCalledFromWorker ? 1UZ : 0UZ)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
        }
        // _nRunningJobs == 0 is the only quiescence point shared by the runAndWait /
        // exchange / dtor teardown paths (stop() runs at REQUESTED_STOP, before the
        // workers join). One Aggressive pass here returns slot storage to the
        // allocator/pool while no work() can touch the buffers. Light opts out of all
        // scheduler-driven housekeeping (intrinsic writer-pressure path only).
        if (house_keeping_policy.value != HouseKeepPolicy::Light) {
            graph::forEachBlock<TransparentBlockGroup>(*_graph, [](auto& block) { block->houseKeeping(HouseKeepPolicy::Aggressive, HouseKeepDepth::Deep); });
        }
    }

    /// Returns a deep copy of jobs to avoid data races on the std::vectors that make up the JobLists.
    /// It is unsafe to access any fields of the blocks which work() may be mutating, if the
    /// scheduler is running.
    [[nodiscard]] std::shared_ptr<JobLists> jobs() const noexcept {
        std::unique_lock guard(_executionOrderMutex);
        if (!_executionOrder) {
            return {};
        }
        return std::make_shared<JobLists>(std::as_const(*_executionOrder));
    }

    // external-drive superloop entry; precondition: start() has primed the graph to RUNNING.
    [[nodiscard]] work::Result step()
    requires(executionPolicy() == ExecutionPolicy::externalStep)
    {
        processScheduledMessages();
        return traverseBlockListOnce((*_executionOrder)[0], _schedStates.empty() ? std::span<SchedState>{} : std::span<SchedState>{_schedStates[0]}, _readyHeapArena.empty() ? std::span<ReadyEntry>{} : std::span<ReadyEntry>{_readyHeapArena[0]});
    }

    /*
     * Removes blocks which may currently be owned by this scheduler. While the
     * block may not be destroyed immediately, it is guaranteed to never be
     * dereferenced again by this scheduler after this function returns.
     *
     * This only makes sense to call while worker threads are not working, so
     * one or both of the scheduler being stopped or work quiescence requested.
     */
    void removeBlocks(std::span<const std::shared_ptr<BlockModel>> blocksToRemoveSpan) {
        if (lifecycle::isActive(this->state())) {
            removeBlocksWhileRunning(blocksToRemoveSpan);
        } else {
            // to avoid UB in the case of a customInit() that reads from
            // residual state in these, and to prevent lengthening the
            // lifetimes of the blocks behind the shared ptrs in unexpected
            // ways, just clear these immediately.
            std::scoped_lock guard(_adoptionBlocksMutex, _executionOrderMutex);
            _adoptionBlocks.clear();
            if (_executionOrder) {
                _executionOrder->clear();
            }
        }
    }

protected:
    void disconnectAllEdges() {
        _graph->disconnectAllEdges();
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [](auto& block) {
            if (block->blockCategory() == TransparentBlockGroup) {
                auto* graph = static_cast<GraphWrapper<gr::Graph>*>(block.get());
                graph->blockRef().disconnectAllEdges();
            }
        });
    }

    bool connectPendingEdges() {
        auto primeFeedbackPorts = [&](const gr::Graph& graph) {
            std::vector<graph::FeedbackLoop> feedbackLoops = gr::graph::detectFeedbackLoops(graph);
            for (auto& loop : feedbackLoops) {
                if (std::expected<std::size_t, Error> nPrimeSamples = gr::graph::calculateLoopPrimingSize(loop); nPrimeSamples) {
                    if (auto ret = gr::graph::primeLoop(loop, nPrimeSamples.value()); !ret) {
                        this->emitErrorMessage("connectPendingEdges()", std::format("failed to prime feedback loop: {}\nloop: {}", ret.error(), loop.edges));
                    }
                } else {
                    this->emitErrorMessage("connectPendingEdges()", std::format("failed to prime feedback loop: {}\nloop: {}", nPrimeSamples.error(), loop.edges));
                }
            }
        };

        bool result = _graph->connectPendingEdges();
        primeFeedbackPorts(gr::graph::flatten(*_graph)); // need to flatten graph due to potential loops from within the subgraph to blocks in the parents.
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [&result, &primeFeedbackPorts](auto& block) {
            if (block->blockCategory() == TransparentBlockGroup) {
                auto* graph = static_cast<GraphWrapper<gr::Graph>*>(block.get());
                result      = result && graph->blockRef().connectPendingEdges();
                primeFeedbackPorts(gr::graph::flatten(graph->blockRef()));
            }
        });
        return result;
    }

    /// N.B. `states` is parallel to `blocks`; each entry supplies that block's batch ceiling.
    /// Do not reach for `max_work_items` directly here -- routing every batch decision through the
    /// resolver is what keeps the executor and the derivation from disagreeing.
    ///
    /// Two loops, selected by the policy's `PriorityClass`:
    ///
    /// - **`none`** (round robin) sweeps the list once, calling `work()` on every block. Time
    ///   sharing: each block gets one turn per pass.
    /// - **`fixed`** selects the highest-priority *eligible* block each time, which is what
    ///   fixed-priority scheduling means. The list is already priority-sorted, so "highest
    ///   priority eligible" is "the earliest index that can run".
    work::Result traverseBlockListOnce(const std::vector<std::shared_ptr<BlockModel>>& blocks, std::span<SchedState> states, std::span<ReadyEntry> readyHeap = {}, std::uint8_t traceWorkerId = 0U) const {
        std::size_t performedWorkAllBlocks = 0UZ;
        bool        unfinishedBlocksExist  = false; // i.e. at least one block returned OK, INSUFFICIENT_INPUT_ITEMS, or INSUFFICIENT_OUTPU_ITEMS

        const auto ceilingFor = [&](std::size_t i) { return i < states.size() ? states[i].batchCeiling : static_cast<std::size_t>(max_work_items); };

        // Hoisted once per pass rather than tested per invocation: the mask is a relaxed load, and a
        // category switched on mid-pass simply takes effect on the next one (mask changes are not
        // synchronised with emitters by design). `states` may be shorter than `blocks`, or empty --
        // `step()` passes an empty span -- so identity falls back to `kNoEntity`, which is a valid
        // worker-scoped record rather than an out-of-range read.
        [[maybe_unused]] const bool traceWork  = gr::trace::kEnabled && gr::trace::categoryEnabled(gr::trace::Category::work);
        [[maybe_unused]] const auto entityFor  = [&](std::size_t i) { return i < states.size() ? states[i].entityId : gr::trace::kNoEntity; };
        [[maybe_unused]] const auto traceEnter = [&](std::size_t i, std::size_t requested, gr::trace::LoopKind loopKind, std::uint8_t extraFlags) -> std::uint64_t {
            if (!traceWork) {
                return 0UL;
            }
            const std::uint64_t entered = gr::trace::now();
            gr::trace::emit(gr::trace::Event{.startNs = entered, .payload0 = gr::trace::saturate(requested), .entity = entityFor(i), .kind = gr::trace::Kind::workBegin, .workerId = traceWorkerId, .flags = static_cast<std::uint8_t>(std::to_underlying(loopKind) | extraFlags)});
            return entered;
        };
        // Emitted only for an invocation that did something. The predicate is on `status`, never on
        // `performed_work`: an all-asynchronous-input block reports `performed_work == requestedWork`
        // having consumed nothing, and a source that publishes everything and returns DONE reports
        // zero. Either would be recorded backwards.
        //
        // An unmatched `workBegin` therefore means one of two things, and a reader can tell them
        // apart: an unproductive probe, accounted for by the `workProbe` that follows in the same
        // sweep; or -- if it is the last record on its ring -- a `work()` that never returned.
        [[maybe_unused]] const auto traceLeave = [&](std::size_t i, std::uint64_t entered, std::size_t requested, std::size_t performed, work::Status status, gr::trace::LoopKind loopKind, std::uint8_t extraFlags) {
            if (!traceWork) {
                return;
            }
            if (status == work::Status::INSUFFICIENT_INPUT_ITEMS || status == work::Status::INSUFFICIENT_OUTPUT_ITEMS) {
                // Aggregated, not recorded. The second clock read is bought deliberately: differencing
                // adjacent records would fold the scheduler overhead between invocations into the probe
                // cost, and separating those two is the entire reason `work()` emits a pair at all.
                if (i < states.size()) {
                    ++states[i].probeCount;
                    states[i].probeNs += gr::trace::now() - entered;
                }
                return;
            }
            gr::trace::emit(gr::trace::Event{.startNs = entered, .durationNs = gr::trace::durationOf(entered, gr::trace::now()), .payload0 = gr::trace::saturate(requested), .payload1 = gr::trace::saturate(performed), .entity = entityFor(i), .kind = gr::trace::Kind::workEnd, .workerId = traceWorkerId, .status = static_cast<std::int8_t>(status), .flags = static_cast<std::uint8_t>(std::to_underlying(loopKind) | extraFlags)});
        };

        // One sweep marker here rather than one at each caller: `traverseBlockListOnce` *is* the sweep,
        // and `poolWorker` is unreachable under `externalStep` (only singleThreaded, its blocking
        // variant and multiThreaded call it), so which driver produced this pass is a compile-time
        // fact and needs no parameter.
        constexpr std::uint8_t            kSweepFlags = (executionPolicy() == ExecutionPolicy::externalStep) ? gr::trace::flag::kViaStep : std::uint8_t{0U};
        [[maybe_unused]] gr::trace::Scope sweepScope{gr::trace::Event{.kind = gr::trace::Kind::sweep, .workerId = traceWorkerId, .flags = kSweepFlags}};

        // Flushed on *every* exit path, the two ERROR returns included, so a pass that failed still
        // reports what its probing cost -- which is exactly the pass someone will be looking at.
        // `on_scope_exit` rather than a line before each return: three call sites that must not drift
        // is how the ERROR path ends up silently uninstrumented.
        //
        // Declared *after* `sweepScope`, so it destructs *before* it: the probe records and the
        // sweep's own payload are both in place by the time the scope emits.
        [[maybe_unused]] on_scope_exit flushProbes = [&] {
            if constexpr (gr::trace::kEnabled) {
                sweepScope.event().payload0 = static_cast<std::uint32_t>(blocks.size());
                sweepScope.event().payload1 = gr::trace::saturate(performedWorkAllBlocks);
                sweepScope.event().status   = static_cast<std::int8_t>(unfinishedBlocksExist ? work::Status::OK : work::Status::DONE);
                if (!traceWork) {
                    return;
                }
                for (std::size_t i = 0UZ; i < std::min(blocks.size(), states.size()); ++i) {
                    if (states[i].probeCount == 0U) {
                        continue;
                    }
                    gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(), .payload0 = states[i].probeCount, .payload1 = gr::trace::saturate(states[i].probeNs), .entity = states[i].entityId, .kind = gr::trace::Kind::workProbe, .workerId = traceWorkerId});
                    states[i].probeCount = 0U;
                    states[i].probeNs    = 0UL;
                }
            }
        };

        // The per-pass selection bound exists to keep a worker responsive: house-keeping, messages,
        // adoption and lifecycle checks all live between passes. Whether the default multiplier of
        // four ever actually binds is an unprofiled question (RT section 8.3), and this is the record
        // that answers it. Round robin has no bound and never calls this, so it compiles nothing.
        [[maybe_unused]] const auto traceSelectionBoundHit = [&](std::size_t boundValue, std::size_t blockCount) {
            if constexpr (gr::trace::kEnabled) {
                gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(),
                    .payload0                             = gr::trace::saturate(boundValue),
                    .payload1                             = gr::trace::saturate(blockCount), //
                    .kind                                 = gr::trace::Kind::selectionBoundHit,
                    .workerId                             = traceWorkerId});
            }
        };

        // Releases, and answers whether it did. The queue length is the only honest witness: a
        // release can happen with the ring already non-empty, so "was empty, now is not" would
        // undercount. Compiled away entirely when tracing is out, which is why the call is written
        // twice rather than the count being taken unconditionally.
        [[maybe_unused]] const auto releaseAndCount = [](BlockModel& block, SchedState& state, std::chrono::steady_clock::time_point now, std::uint8_t pathFlag) -> std::size_t {
            if constexpr (gr::trace::kEnabled) {
                const std::size_t before = state.jobs.size;
                gr::scheduler::releaseIfEligible(block, state, now, pathFlag);
                return state.jobs.size > before ? 1UZ : 0UZ;
            } else {
                gr::scheduler::releaseIfEligible(block, state, now, pathFlag);
                return 0UZ;
            }
        };

        // Backstop release pass. Covers what event-driven detection cannot reach: sources, which
        // have no producer to trigger them, and blocks fed across a worker boundary, whose
        // upstream must not write this worker's state. It also catches the event trigger's own
        // misses -- a block that publishes its last samples and returns DONE reports
        // `performed_work == 0` -- so it is load-bearing, not merely a fallback.
        if constexpr (needsReleaseTracking(TPolicy::kPriorityClass)) {
            const std::chrono::steady_clock::time_point now      = std::chrono::steady_clock::now();
            const std::size_t                           nScanned = std::min(blocks.size(), states.size());

            // A complete record, and emitted whether or not anything was released. A scan that finds
            // nothing is the *cost* side of "did event-driven detection earn its keep" -- recording
            // only the productive scans would make both paths look free and answer the question wrong.
            [[maybe_unused]] gr::trace::Scope scanScope{gr::trace::Event{.payload0 = gr::trace::saturate(nScanned), .kind = gr::trace::Kind::releaseScan, .workerId = traceWorkerId}};
            [[maybe_unused]] std::size_t      nReleased = 0UZ;

            for (std::size_t i = 0UZ; i < nScanned; ++i) {
                nReleased += releaseAndCount(*blocks[i], states[i], now, 0U /* backstop */);
            }
            if constexpr (gr::trace::kEnabled) {
                scanScope.event().payload1 = gr::trace::saturate(nReleased);
            }
        }

        /// Event-driven detection: a block's output arriving is what makes its consumers eligible,
        /// so their release is stamped within one block execution of the write rather than one
        /// sweep. One hop only -- no transitive cascade, so a feedback loop cannot spin it.
        [[maybe_unused]] const auto releaseSuccessorsOf = [&](std::size_t producer, auto&& onNewlyReady) {
            if (producer >= states.size()) {
                return;
            }
            const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

            // `entity` is the producer whose output triggered this walk, where the backstop's scan
            // carries `kNoEntity`. That is what lets a report attribute a walk's cost to the block
            // that caused it rather than to the sweep it happened in.
            [[maybe_unused]] gr::trace::Scope scanScope{gr::trace::Event{.payload0 = gr::trace::saturate(states[producer].successors.size()), .entity = states[producer].entityId, .kind = gr::trace::Kind::releaseScan, .workerId = traceWorkerId, .flags = gr::trace::flag::kViaSuccessorWalk}};
            [[maybe_unused]] std::size_t      nReleased = 0UZ;

            for (std::size_t successor : states[producer].successors) {
                if (successor < blocks.size() && successor < states.size()) {
                    // The empty-to-non-empty transition is what a heap selector needs to hear about:
                    // a block already holding a job is already in the heap.
                    const bool wasEmpty = states[successor].jobs.empty();
                    nReleased += releaseAndCount(*blocks[successor], states[successor], now, gr::trace::flag::kViaSuccessorWalk);
                    if (wasEmpty && !states[successor].jobs.empty()) {
                        onNewlyReady(successor);
                    }
                }
            }
            if constexpr (gr::trace::kEnabled) {
                scanScope.event().payload1 = gr::trace::saturate(nReleased);
            }
        };

        if constexpr (!selectsByPriority(TPolicy::kPriorityClass)) {
            for (std::size_t i = 0UZ; i < blocks.size(); ++i) {
                const std::size_t   ceiling                         = ceilingFor(i);
                const std::uint64_t entered                         = traceEnter(i, ceiling, gr::trace::LoopKind::roundRobin, 0U);
                const auto [requested_work, performed_work, status] = blocks[i]->work(ceiling);
                traceLeave(i, entered, requested_work, performed_work, status, gr::trace::LoopKind::roundRobin, 0U);
                performedWorkAllBlocks += performed_work;

                if (status == work::Status::ERROR) {
                    return {requested_work, performedWorkAllBlocks, work::Status::ERROR};
                } else if (status != work::Status::DONE) {
                    unfinishedBlocksExist = true;
                }
            }
        } else if constexpr (hasStaticKey(TPolicy::kPriorityClass)) {
            // Fixed-priority selection over a list `applyStaticOrder` has already sorted, so the
            // earliest index that can run *is* the highest-priority eligible block. `work()` doubles
            // as the eligibility oracle: a block that cannot run returns performed_work == 0 with an
            // INSUFFICIENT_* status, which is a cheap near-no-op.
            //
            // The bound is what keeps the worker responsive: house-keeping, message handling,
            // adoption and lifecycle checks all live between passes, so an unbounded loop would not
            // merely starve low-priority blocks, it would hang the worker.
            const std::size_t nBlocks    = blocks.size();
            const std::size_t bound      = max_selections_per_pass == 0U ? kDefaultSelectionMultiplier * nBlocks : static_cast<std::size_t>(max_selections_per_pass);
            std::size_t       selections = 0UZ;
            std::size_t       index      = 0UZ;

            while (index < nBlocks && selections < bound) {
                if (index < states.size() && states[index].finished) {
                    ++index; // already DONE: skipped by marker rather than compacted out of the list
                    continue;
                }

                const std::size_t   ceiling                         = ceilingFor(index);
                const std::uint64_t entered                         = traceEnter(index, ceiling, gr::trace::LoopKind::fixedPriority, 0U);
                const auto [requested_work, performed_work, status] = blocks[index]->work(ceiling);
                traceLeave(index, entered, requested_work, performed_work, status, gr::trace::LoopKind::fixedPriority, 0U);
                performedWorkAllBlocks += performed_work;

                if (status == work::Status::ERROR) {
                    return {requested_work, performedWorkAllBlocks, work::Status::ERROR};
                }
                if (status == work::Status::DONE) {
                    if (index < states.size()) {
                        states[index].finished = true;
                    }
                    ++index;
                    continue;
                }

                unfinishedBlocksExist = true;
                if (performed_work > 0UZ) {
                    ++selections;
                    index = 0UZ; // strict restart: a still-runnable higher-priority block runs again
                } else {
                    ++index; // not eligible right now; try the next-highest priority
                }
            }
            if (selections >= bound) {
                // The strict restart makes this *more* likely here than on the job-driven paths, not
                // less, which is why the marker is gated on `selectsByPriority` rather than on
                // release tracking. A bound hit here comes with no `select` records beside it.
                traceSelectionBoundHit(bound, nBlocks);
            }
        } else {
            // Job-driven selection for policies whose key is a property of the *job* -- the key
            // changes as jobs are released and retired, so the list cannot be pre-sorted and the
            // ordering has to be re-derived.
            //
            // A released job is the eligibility answer, which is what retires the probing the
            // fixed-priority loop above relies on: no job, no run, and no `work()` call to find out.
            //
            // Two selectors, chosen at run time so the same policy and graph can be measured both
            // ways. They must agree on every decision -- the shared `selectsBefore` ordering is what
            // makes that true even for equal keys -- so the only difference is how the minimum is
            // found: a scan over every eligible block, or a heap.
            //
            // N.B. neither has a strict restart. That was a way of approximating "highest priority
            // eligible" over a pre-sorted list; taking the minimum outright reconsiders every block
            // on each iteration by construction, so a restart would only repeat work.
            const TPolicy     policy{};
            const std::size_t nBlocks = std::min(blocks.size(), states.size());
            const std::size_t bound   = max_selections_per_pass == 0U ? kDefaultSelectionMultiplier * blocks.size() : static_cast<std::size_t>(max_selections_per_pass);
            const bool        useHeap = selection_strategy == SelectionStrategy::readyHeap && readyHeap.size() >= nBlocks;
            if constexpr (gr::trace::kEnabled) {
                // RT defect B3: a readyHeap request silently reverts to the linear scan when the
                // scratch is too small, and nothing says so. Level-triggered rather than latched --
                // the scratch is resized on the same house-keeping pass that grows the block list, so
                // the condition is transient at worst on the pool path and a *repeated* record is
                // itself the finding rather than noise to suppress.
                if (selection_strategy == SelectionStrategy::readyHeap && !useHeap) {
                    gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(),
                        .payload0                             = gr::trace::saturate(nBlocks),
                        .payload1                             = gr::trace::saturate(readyHeap.size()), //
                        .kind                                 = gr::trace::Kind::heapFallback,
                        .workerId                             = traceWorkerId});
                }
            }
            std::size_t selections = 0UZ;
            std::size_t heapSize   = 0UZ;
            std::size_t running    = nBlocks;

            const auto eligible = [&](std::size_t i) { return !states[i].finished && !states[i].jobs.empty(); };

            // `std::push_heap` builds a *max* heap, so the comparator is reversed to put the
            // smallest key on top. Ties fall to the lower registration index, matching the scan.
            const auto worse = [](const ReadyEntry& lhs, const ReadyEntry& rhs) { return lhs.key == rhs.key ? lhs.index > rhs.index : lhs.key > rhs.key; };

            const auto pushReady = [&](std::size_t i) {
                if (heapSize >= readyHeap.size()) {
                    return; // cannot happen while the scratch is sized at the block count
                }
                readyHeap[heapSize] = ReadyEntry{.key = policy.key(*blocks[i], states[i]), .index = i};
                ++heapSize;
                std::push_heap(readyHeap.begin(), readyHeap.begin() + static_cast<std::ptrdiff_t>(heapSize), worse);
            };

            // A block released while it is *running* must not be pushed here: it is already out of
            // the heap and is re-pushed after its job completes. Without this a feedback edge, where
            // a block is its own successor, would insert a duplicate entry.
            const auto onNewlyReady = [&](std::size_t i) {
                if (useHeap && i != running) {
                    pushReady(i);
                }
            };

            // One record per selection decision, emitted *before* the call it authorises, so a
            // reader sees release -> select -> workBegin -> workEnd in that order and the replay
            // oracle can line a decision up against the ready set that produced it.
            //
            // `readySetSize` is what the selector itself believed was ready: the heap's population
            // for the heap path, the eligible count for the scan. Those are not quite the same
            // quantity -- a heap may hold an entry whose job has since been retired -- which is
            // exactly why a skipped stale entry is flagged rather than silently corrected.
            [[maybe_unused]] bool       staleSkipped = false;
            [[maybe_unused]] const auto traceSelect  = [&](std::size_t chosen, std::size_t readySetSize, std::size_t heapPopulation, std::uint8_t pathFlag) {
                if constexpr (gr::trace::kEnabled) {
                    gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(),
                         .payload0                             = gr::trace::saturate(readySetSize),
                         .payload1                             = gr::trace::saturate(selections),
                         .payload2                             = gr::trace::saturate(heapPopulation), //
                         .entity                               = states[chosen].entityId,
                         .kind                                 = gr::trace::Kind::select,
                         .workerId                             = traceWorkerId, //
                         .flags                                = static_cast<std::uint8_t>(pathFlag | (staleSkipped ? gr::trace::flag::kStaleEntrySkipped : 0U))});
                    staleSkipped = false;
                }
            };

            // Nothing was ready. Round robin spins when idle too, so the claim that a job-driven
            // worker is at parity with it is an argument until this is counted.
            [[maybe_unused]] const auto traceSelectEmpty = [&] {
                if constexpr (gr::trace::kEnabled) {
                    gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(),
                        .payload0                             = gr::trace::saturate(nBlocks),
                        .payload1                             = gr::trace::saturate(selections), //
                        .kind                                 = gr::trace::Kind::selectEmpty,
                        .workerId                             = traceWorkerId});
                }
            };

            const auto runOne = [&](std::size_t chosen) -> std::optional<work::Result> {
                running                                             = chosen;
                const std::size_t   batch                           = states[chosen].jobs.front().batch;
                const std::uint64_t entered                         = traceEnter(chosen, batch, gr::trace::LoopKind::jobDriven, gr::trace::flag::kJobBacked);
                const auto [requested_work, performed_work, status] = blocks[chosen]->work(batch);
                traceLeave(chosen, entered, requested_work, performed_work, status, gr::trace::LoopKind::jobDriven, gr::trace::flag::kJobBacked);
                performedWorkAllBlocks += performed_work;
                ++selections; // every iteration consumed a released job, successful or not

                gr::scheduler::retireFrontJob(states[chosen]);

                // Unconditional, and ahead of the DONE branch. `performed_work` cannot be used as the
                // trigger: `computePerformedWork()` returns 0 for any status other than OK, so a
                // source that publishes its whole output and finishes in one call reports zero having
                // published everything. Gating on it stranded that data with the producer already
                // marked finished, so no later pass could free it.
                releaseSuccessorsOf(chosen, onNewlyReady);
                running = nBlocks;

                if (status == work::Status::ERROR) {
                    return work::Result{requested_work, performedWorkAllBlocks, work::Status::ERROR};
                }
                if (status == work::Status::DONE) {
                    states[chosen].finished = true;
                    states[chosen].jobs.clear(); // jobs it can never execute
                    states[chosen].assignedSamples = 0UZ;
                    return std::nullopt;
                }

                unfinishedBlocksExist = true;
                return std::nullopt;
            };

            // A block that has not reported DONE keeps the pass unfinished, as round robin achieves by
            // polling `work()` and getting INSUFFICIENT_* back from the starved ones. Concluding DONE
            // because no block on *this* worker holds a job is sound only for a single worker: across
            // a boundary the producer is another thread that may not have run yet, and the consumer's
            // worker would exit before any data arrived.
            //
            // This terminates only because end-of-stream waives the batch floor (`releaseIfEligible`):
            // without that a starved block could never run, never finish, and the pass would never be
            // DONE. The two rules are one change.
            const auto markUnfinished = [&] {
                for (std::size_t i = 0UZ; i < nBlocks; ++i) {
                    if (!states[i].finished) {
                        unfinishedBlocksExist = true;
                        return;
                    }
                }
            };

            if (useHeap) {
                // Rebuilt once per pass rather than carried across them. That costs O(n) a pass and
                // buys the invariant that a block appears at most once, which is what removes any
                // need for stale-entry handling or a position map.
                for (std::size_t i = 0UZ; i < nBlocks; ++i) {
                    if (eligible(i)) {
                        readyHeap[heapSize] = ReadyEntry{.key = policy.key(*blocks[i], states[i]), .index = i};
                        ++heapSize;
                    }
                }
                std::make_heap(readyHeap.begin(), readyHeap.begin() + static_cast<std::ptrdiff_t>(heapSize), worse);

                while (selections < bound && heapSize > 0UZ) {
                    std::pop_heap(readyHeap.begin(), readyHeap.begin() + static_cast<std::ptrdiff_t>(heapSize), worse);
                    --heapSize;
                    const std::size_t chosen = readyHeap[heapSize].index;

                    // A popped entry may be stale. `onNewlyReady` avoids creating duplicates, but
                    // that is an argument about one call site, whereas `runOne` would read
                    // `jobs.front()` of an emptied ring. Validating here makes the loop correct
                    // however an entry came to be there.
                    if (!eligible(chosen)) {
                        if constexpr (gr::trace::kEnabled) {
                            staleSkipped = true;
                        }
                        continue;
                    }

                    traceSelect(chosen, heapSize + 1UZ, heapSize, gr::trace::flag::kViaHeap);
                    if (const std::optional<work::Result> failure = runOne(chosen); failure.has_value()) {
                        return *failure;
                    }
                    if (eligible(chosen)) {
                        pushReady(chosen); // re-keyed to whatever job is now at its head
                    }
                }
                if (heapSize == 0UZ && selections < bound) {
                    traceSelectEmpty(); // exhausted rather than bounded: the two exits are different findings
                } else if (selections >= bound) {
                    traceSelectionBoundHit(bound, nBlocks);
                }
                markUnfinished();
            } else {
                while (selections < bound) {
                    std::size_t                  chosen     = nBlocks;
                    [[maybe_unused]] std::size_t readyCount = 0UZ;
                    for (std::size_t i = 0UZ; i < nBlocks; ++i) {
                        if (!eligible(i)) {
                            continue;
                        }
                        if constexpr (gr::trace::kEnabled) {
                            ++readyCount;
                        }
                        if (chosen == nBlocks || gr::scheduler::selectsBefore(policy, *blocks[i], states[i], *blocks[chosen], states[chosen])) {
                            chosen = i;
                        }
                    }

                    if (chosen == nBlocks) {
                        traceSelectEmpty();
                        break; // nothing released this pass
                    }

                    traceSelect(chosen, readyCount, 0UZ, 0U /* linear scan */);
                    if (const std::optional<work::Result> failure = runOne(chosen); failure.has_value()) {
                        return *failure;
                    }
                }
                if (selections >= bound) {
                    traceSelectionBoundHit(bound, nBlocks);
                }
                markUnfinished();
            }
        }
#ifdef __EMSCRIPTEN__
        std::this_thread::sleep_for(std::chrono::microseconds(10u)); // workaround for incomplete std::atomic implementation (at least it seems for nodejs)
#endif
        return {max_work_items, performedWorkAllBlocks, unfinishedBlocksExist ? work::Status::OK : work::Status::DONE};
    }

    void init() {
        [[maybe_unused]] const auto pe = _profilerHandler->startCompleteEvent("scheduler_base.init");
        base_t::processScheduledMessages(); // make sure initial subscriptions are processed
        connectBlockMessagePorts();

        if constexpr (requires(Derived& d) { d.customInit(); }) {
            static_cast<Derived*>(this)->customInit();
        }

        refreshSchedulingAnalysis();
        rebuildSchedStates();
    }

    void reset() {
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [this](auto& block) { this->emitErrorMessageIfAny("reset() -> LifecycleState", block->changeStateTo(lifecycle::INITIALISED)); });
        disconnectAllEdges();

        if constexpr (requires(Derived& d) { d.customReset(); }) {
            static_cast<Derived*>(this)->customReset();
        }
    }

    void start() {
        using enum gr::lifecycle::State;

        disconnectAllEdges();
        if (auto result = connectPendingEdges(); !result) {
            this->emitErrorMessage("start()", "Failed to connect blocks in graph");
        }
        if (this->state() == IDLE) {
            if (auto result = this->changeStateTo(INITIALISED); !result) { // Need to go to INITIALISED first
                this->emitErrorMessage("start()", result.error());
            }
        }

        std::unique_lock lock(_executionOrderMutex);

        // initialize _movedBlocks so that it can be safely indexed by runner
        // ID from different threads while the scheduler is running
        _movedBlocks.resize(_executionOrder->size());

        graph::forEachBlock<TransparentBlockGroup>(*_graph, [this](auto& block) { //
            if (block->blockCategory() == ScheduledBlockGroup) {
                // We don't simply move to RUNNING, as schedulers block. This code path
                // uses a separate thread.
                auto* schedulerModel = detail::asSchedulerModel(*block);
                if (schedulerModel) {
                    schedulerModel->start();
                } else {
                    this->emitErrorMessage("start()", std::format("ScheduledBlockGroup is not a SchedulerModel {}", block->uniqueName()));
                }
            } else {
                this->emitErrorMessageIfAny("LifecycleState -> RUNNING", block->changeStateTo(lifecycle::RUNNING));
            }
        });

        if constexpr (executionPolicy() == ExecutionPolicy::externalStep) { // no watchdog/pool; the application drives step()
            if constexpr (requires(Derived& d) { d.customStart(); }) {
                static_cast<Derived*>(this)->customStart();
            }
            return;
        }

        // start watchdog
        auto ioThreadPool = gr::thread_pool::Manager::defaultIoPool();

        ioThreadPool->execute([this, context = _lastWatchDogThread.stopOldThreadAndGetNewContext()] { this->runWatchDog(context, watchdog_timeout.value, timeout_inactivity_count.value); });

        // it is possible that a stray thread is still running due to it
        // dispatching an exchange(), stopping only threads other than itself
        waitDone();

        assert(!_executionOrder->empty());
        if constexpr (executionPolicy() == ExecutionPolicy::singleThreaded || executionPolicy() == ExecutionPolicy::singleThreadedBlocking) {
            // in multithreaded mode, poolWorkers operate while _executionOrderMutex is not held. let's do that same behavior here
            // because propertyCallbackUngroupBlocks() relies on being able to modify the _executionOrder of another running scheduler
            lock.unlock();
            on_scope_exit exit = [&lock] { lock.lock(); };

            static_cast<Derived*>(this)->poolWorker(0UZ, _executionOrder);
        } else { // run on processing thread pool
            [[maybe_unused]] const auto pe           = _profilerHandler->startCompleteEvent("scheduler_base.runOnPool");
            auto                        jobListsCopy = _executionOrder;
            for (std::size_t runnerID = 0UZ; runnerID < _executionOrder->size(); runnerID++) {
                _pool->execute([this, runnerID, jobListsCopy]() { static_cast<Derived*>(this)->poolWorker(runnerID, jobListsCopy); });
            }
            if (!_executionOrder->empty()) {
                _nRunningJobs->wait(0UZ); // waits until at least one pool worker started
            }
        }
        if constexpr (requires(Derived& d) { d.customStart(); }) {
            static_cast<Derived*>(this)->customStart();
        }
    }

    void poolWorker(const std::size_t runnerID, std::shared_ptr<std::vector<std::vector<std::shared_ptr<BlockModel>>>> jobList) {
        using enum lifecycle::State;
        std::shared_ptr<gr::Sequence> progress     = _graph->_progress; // life-time guaranteed
        std::shared_ptr<gr::Sequence> nRunningJobs = _nRunningJobs;

        nRunningJobs->incrementAndGet();
        nRunningJobs->notify_all();

        on_scope_exit decrement = [nRunningJobs] {
            std::ignore = nRunningJobs->subAndGet(1UZ);
            nRunningJobs->notify_all();
        };

        gr::thread_pool::thread::setThreadName(std::format("pW{}-{}", runnerID, gr::meta::shorten_type_name(this->unique_name)));

        [[maybe_unused]] auto profiler_handler = _profiler.forThisThread();

        std::vector<std::shared_ptr<BlockModel>> localBlockList;
        {
            assert(jobList->size() > runnerID);
            std::lock_guard                          lock(_executionOrderMutex);
            std::vector<std::shared_ptr<BlockModel>> blocks = jobList->at(runnerID);
            localBlockList.reserve(blocks.size());
            std::ranges::copy(blocks, std::back_inserter(localBlockList));
        }
        std::vector<SchedState> localStates;
        // Worker-local release storage. `_jobArena` belongs to the `step()` path and must not be
        // shared: two workers spanning into one vector would alias, and the arena has to be re-sized
        // whenever this worker's list changes.
        std::vector<Job>         localJobArena;
        std::vector<std::size_t> localSuccessorArena;
        std::vector<ReadyEntry>  localReadyHeap;

        syncSchedStates(localBlockList, localStates, gr::trace::workerIdOf(runnerID));
        gr::scheduler::detail::applyStaticOrder<TPolicy>(localBlockList, localStates); // no-op for RoundRobinPolicy: its key is the position itself
        if constexpr (needsReleaseTracking(TPolicy::kPriorityClass)) {
            buildReleaseStorage(localBlockList, localStates, localJobArena, localSuccessorArena, localReadyHeap);
        }

        if (localBlockList.empty()) {
            return;
        }

        // Placed here rather than at function entry because `localBlockList` is only known once the
        // job list has been copied, and "how many blocks did this worker own" is the first thing a
        // reader wants from a worker's first record.
        [[maybe_unused]] std::size_t sweepCount = 0UZ;
        // Previous pass's block list, by address, so a re-sync can say whether anything actually
        // changed. Only populated when tracing is compiled in.
        [[maybe_unused]] std::vector<const void*> traceListFingerprint;
        if constexpr (gr::trace::kEnabled) {
            gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(), .payload0 = static_cast<std::uint32_t>(localBlockList.size()), .payload1 = static_cast<std::uint32_t>(gr::trace::currentCpu()), .kind = gr::trace::Kind::workerStart, .workerId = gr::trace::workerIdOf(runnerID)});
        }
        [[maybe_unused]] on_scope_exit traceWorkerStop = [&] {
            if constexpr (gr::trace::kEnabled) {
                gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(), .payload0 = gr::trace::saturate(sweepCount), .payload1 = static_cast<std::uint32_t>(gr::trace::ringStats().lost), .kind = gr::trace::Kind::workerStop, .workerId = gr::trace::workerIdOf(runnerID)});
            }
        };

        const auto            initialGeneration  = gr::atomic_ref(_graphGeneration).load_acquire();
        [[maybe_unused]] auto currentProgress    = this->_graph->progress().value();
        std::size_t           inactiveCycleCount = 0UZ;
        std::size_t           msgToCount         = 0UZ;
        auto                  activeState        = this->state();
        do {
            [[maybe_unused]] auto pe = profiler_handler->startCompleteEvent("scheduler_base.work");
            if constexpr (executionPolicy() == ExecutionPolicy::singleThreadedBlocking) {
                // optionally tracking progress and block if there is none
                currentProgress = progress->value();
            }

            // Process messages either when the ratio gate opens, or immediately when any entry-point port has
            // pending traffic. This keeps the ratio's amortisation of empty-queue checks while giving arriving
            // messages single-iteration latency (important for multi-hop sub-scheduler message paths).
            const bool hasMessagesToProcess = msgToCount == 0UZ || //
                                              (runnerID == 0UZ && (this->msgIn.available() > 0UZ || _fromChildMessagePort.available() > 0UZ));
            if (hasMessagesToProcess) {
                [[maybe_unused]] gr::trace::Scope messageScope{gr::trace::Event{.payload0 = gr::trace::saturate(msgToCount), .payload1 = static_cast<std::uint32_t>(localBlockList.size()), .kind = gr::trace::Kind::messagePhase, .workerId = gr::trace::workerIdOf(runnerID)}};
                if (runnerID == 0UZ) {
                    this->processScheduledMessages(); // execute the scheduler- and Graph-specific message handler only once globally
                    if (initialGeneration != gr::atomic_ref(_graphGeneration).load_acquire()) {
                        return; // we called exchange()
                    }
                }

                // although we do not do work() on child blocks inside of this WorkGuard, these operations modify
                // Ports. cleanupZombieBlocks will destroy ports, processScheduledMessages may access
                // ports, and housekeeping resizes port buffers. cleanupRemovedBlocks and adoptBlocks must be in
                // this block to preserve adoption/removal ordering. Some messages, such as grouping/ungrouping
                // and emplacing and removing edges, may modify these ports while work quiescence is requested.
                WorkGuard isWorking(this);
                if (!isWorking) {
                    // Not a wait: the guard denies the pass outright while a structural change is in
                    // flight. Recorded so that a stall shows up as "quiescence was requested" rather
                    // than as an unexplained gap between sweeps.
                    if constexpr (gr::trace::kEnabled) {
                        messageScope.event().flags |= gr::trace::flag::kQuiescenceDenied;
                        gr::trace::emit(gr::trace::Event{.startNs = gr::trace::now(), .kind = gr::trace::Kind::quiescenceWait, .workerId = gr::trace::workerIdOf(runnerID)});
                    }
                }
                if (isWorking) {
                    // we must always clean up removed blocks before accessing localBlockList
                    cleanupRemovedBlocks(runnerID, localBlockList);

                    // Zombies are cleaned per-thread, as we remove from the localBlockList as well.
                    // Cleaning zombies has low priority, so uses process_stream_to_message_ratio (a different ratio could be introduced)
                    {
                        [[maybe_unused]] gr::trace::Scope reapScope{gr::trace::Event{.payload0 = gr::trace::saturate(localBlockList.size()), .kind = gr::trace::Kind::zombieReap, .workerId = gr::trace::workerIdOf(runnerID)}};
                        cleanupZombieBlocks(localBlockList);
                        if constexpr (gr::trace::kEnabled) {
                            reapScope.event().payload1 = gr::trace::saturate(localBlockList.size());
                        }
                    }

                    {
                        [[maybe_unused]] gr::trace::Scope adoptScope{gr::trace::Event{.payload0 = gr::trace::saturate(localBlockList.size()), .kind = gr::trace::Kind::adopt, .workerId = gr::trace::workerIdOf(runnerID)}};
                        adoptBlocks(runnerID, localBlockList);
                        if constexpr (gr::trace::kEnabled) {
                            adoptScope.event().payload1 = gr::trace::saturate(localBlockList.size());
                        }
                    }

                    // Removal, zombie cleanup and adoption all mutate `localBlockList`, so the
                    // parallel state must be re-derived before it is indexed again. Unconditionally:
                    // a removal and an adoption in the same pass leave the size unchanged while the
                    // *contents* differ, so a size comparison would silently hand each block its
                    // neighbour's ceiling. This runs on the house-keeping cadence, not per pass.
                    // The re-sync rides the message/house-keeping cadence, not an actual mutation, and
                    // it assigns whole states -- so it also discards every outstanding job and resets
                    // each block's last-release time. Whether that is a real cost depends on how often
                    // it fires with nothing having changed, which nobody had measured. The comparison
                    // is over the block *pointers*, because a removal and an adoption in the same pass
                    // leave the size identical while the contents differ.
                    [[maybe_unused]] gr::trace::Scope syncScope{gr::trace::Event{.payload0 = gr::trace::saturate(localBlockList.size()), .kind = gr::trace::Kind::stateSync, .workerId = gr::trace::workerIdOf(runnerID)}};
                    if constexpr (gr::trace::kEnabled) {
                        std::uint64_t discarded = 0UL;
                        for (const SchedState& state : localStates) {
                            discarded += state.jobs.size;
                        }
                        syncScope.event().payload1 = gr::trace::saturate(discarded);
                        syncScope.event().flags    = (localBlockList.size() != traceListFingerprint.size() || !std::ranges::equal(localBlockList, traceListFingerprint, {}, [](const auto& b) { return b.get(); }, [](const void* p) { return p; })) ? gr::trace::flag::kListChanged : std::uint8_t{0U};
                        traceListFingerprint.clear();
                        traceListFingerprint.reserve(localBlockList.size());
                        for (const auto& block : localBlockList) {
                            traceListFingerprint.push_back(static_cast<const void*>(block.get()));
                        }
                    }

                    syncSchedStates(localBlockList, localStates, gr::trace::workerIdOf(runnerID));

                    // Re-order after the mutations: adoption appends to the end of the list, so
                    // without this a newly adopted block would run last whatever its priority. A no-op for `RoundRobinPolicy`, whose key is the position.
                    gr::scheduler::detail::applyStaticOrder<TPolicy>(localBlockList, localStates);

                    // N.B. `syncSchedStates()` assigns whole `SchedState`s, so this also discards
                    // every outstanding job and resets `lastRelease`. That is correct on an actual
                    // graph mutation, but it rides the house-keeping cadence and so fires even when
                    // nothing changed -- a known defect, recorded rather than papered over.
                    if constexpr (needsReleaseTracking(TPolicy::kPriorityClass)) {
                        buildReleaseStorage(localBlockList, localStates, localJobArena, localSuccessorArena, localReadyHeap);
                    }

                    std::ranges::for_each(localBlockList, &BlockModel::processScheduledMessages);
                    // Buffer housekeeping rides the same cadence as message handling. Light skips
                    // the scheduler-driven trigger entirely (intrinsic writer-pressure path still
                    // fires inside the buffer); Aggressive's post-consume hook is a follow-up.
                    if (house_keeping_policy.value != HouseKeepPolicy::Light) {
                        const HouseKeepPolicy             policy = house_keeping_policy.value;
                        const HouseKeepDepth              depth  = house_keeping_depth.value;
                        [[maybe_unused]] gr::trace::Scope houseKeepScope{gr::trace::Event{.payload0 = gr::trace::saturate(localBlockList.size()), .payload1 = static_cast<std::uint32_t>(std::to_underlying(policy)), .payload2 = static_cast<std::uint32_t>(std::to_underlying(depth)), .kind = gr::trace::Kind::houseKeeping, .workerId = gr::trace::workerIdOf(runnerID)}};
                        std::ranges::for_each(localBlockList, [policy, depth](auto& b) { b->houseKeeping(policy, depth); });
                    }
                }
                activeState = this->state();
                msgToCount++;
            } else {
                if (std::has_single_bit(process_stream_to_message_ratio.value)) {
                    msgToCount = (msgToCount + 1U) & (process_stream_to_message_ratio.value - 1);
                } else {
                    msgToCount = (msgToCount + 1U) % process_stream_to_message_ratio.value;
                }
            }

            if (activeState == RUNNING) {
                bool idleUntilAdoption = false;
                if (auto isWorking = WorkGuard{this}) {
                    // we must always clean up removed blocks before accessing localBlockList
                    cleanupRemovedBlocks(runnerID, localBlockList);
                    idleUntilAdoption = localBlockList.empty();
                    if (!idleUntilAdoption) {
                        ++sweepCount;
                        gr::work::Result result = traverseBlockListOnce(localBlockList, localStates, std::span<ReadyEntry>{localReadyHeap}, gr::trace::workerIdOf(runnerID));
                        if (result.status == work::Status::DONE) {
                            break; // nothing happened -> shutdown this worker
                        } else if (result.status == work::Status::ERROR) {
                            this->emitErrorMessageIfAny("LifecycleState (ERROR)", this->changeStateTo(ERROR));
                            break;
                        }
                    }
                }
                if (idleUntilAdoption) {
                    [[maybe_unused]] gr::trace::Scope idleScope{gr::trace::Event{.payload0 = std::to_underlying(gr::trace::IdleReason::awaitingAdoption), .kind = gr::trace::Kind::idle, .workerId = gr::trace::workerIdOf(runnerID)}};
                    std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
                    msgToCount = 0UZ;
                }
            } else if (activeState == PAUSED) {
                [[maybe_unused]] gr::trace::Scope idleScope{gr::trace::Event{.payload0 = std::to_underlying(gr::trace::IdleReason::paused), .kind = gr::trace::Kind::idle, .workerId = gr::trace::workerIdOf(runnerID)}};
                std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
                msgToCount = 0UZ;
            } else { // other states
                [[maybe_unused]] gr::trace::Scope idleScope{gr::trace::Event{.payload0 = std::to_underlying(gr::trace::IdleReason::otherState), .kind = gr::trace::Kind::idle, .workerId = gr::trace::workerIdOf(runnerID)}};
                std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
                msgToCount = 0UZ;
            }

            // optionally tracking progress and block if there is none
            if constexpr (executionPolicy() == ExecutionPolicy::singleThreadedBlocking) {
                auto progressAfter = progress->value();
                if (currentProgress == progressAfter) {
                    inactiveCycleCount++;
                } else {
                    inactiveCycleCount = 0UZ;
                }

                currentProgress = progressAfter;
                if (inactiveCycleCount > timeout_inactivity_count) {
                    // allow a scheduler process to wait on progress before retrying (N.B. intended to save CPU/battery power)
                    // N.B. a watchdog will periodically update the progress to check for non-responsive blocks.
                    [[maybe_unused]] gr::trace::Scope idleScope{gr::trace::Event{.payload0 = std::to_underlying(gr::trace::IdleReason::noProgress), .payload1 = gr::trace::saturate(inactiveCycleCount), .kind = gr::trace::Kind::idle, .workerId = gr::trace::workerIdOf(runnerID)}};
                    waitUntilChanged(*progress, currentProgress, timeout_ms);
                    msgToCount = 0UZ;
                }
            }
        } while (lifecycle::isActive(activeState));
    }

    void runWatchDog(std::shared_ptr<WatchdogStopContext> context, std::size_t timeOut_ms, std::size_t timeOut_count) {
        auto lock = context->lock();
        if (context->stopRequested()) {
            return;
        }

        auto thisName = gr::meta::shorten_type_name(this->unique_name);
        gr::thread_pool::thread::setThreadName(std::format("WatchDog-{}", thisName));

        std::size_t lastProgress = _graph->_progress->value();
        std::size_t nWarnings    = 0;
        do {
            context->sleepVariable().wait_for(lock, std::chrono::milliseconds(timeOut_ms));
            if (context->stopRequested()) { // scheduler exited or a new watchdog was started
                return;
            }

            // check and increase progress if there hasn't been none.
            std::size_t currentProgress = _graph->_progress->value();
            if ((_nRunningJobs->value() > 0UZ) && (currentProgress == lastProgress)) {
                nWarnings++;
                lastProgress = _graph->_progress->incrementAndGet(); // watchdog triggered manual update
                _graph->_progress->notify_all();
                if (nWarnings >= timeOut_count) {
                    std::println(stderr, "trigger watchdog update {} of {} in {}", nWarnings, timeOut_count, thisName);
                    // log or escalate (e.g., throw, abort, notify external watchdog)
                }
            } else {
                lastProgress = currentProgress;
                nWarnings    = 0UZ;
            }
        } while (_nRunningJobs->value() > 0UZ);
    }

    void stop() {
        using enum lifecycle::State;
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [this](auto& block) {
            if (block->blockCategory() == ScheduledBlockGroup) {
                auto* schedulerModel = detail::asSchedulerModel(*block);
                if (schedulerModel) {
                    schedulerModel->stop();
                } else {
                    this->emitErrorMessage("stop()", std::format("ScheduledBlockGroup is not a SchedulerModel {}", block->uniqueName()));
                }
            } else {
                this->emitErrorMessageIfAny("forEachBlock -> stop() -> LifecycleState", block->changeStateTo(REQUESTED_STOP));
                if (!block->isBlocking()) { // N.B. no other thread/constraint to consider before shutting down
                    this->emitErrorMessageIfAny("forEachBlock -> stop() -> LifecycleState", block->changeStateTo(STOPPED));
                }
            }
        });

        this->emitErrorMessageIfAny("stop() -> LifecycleState ->STOPPED", this->changeStateTo(STOPPED));
        if constexpr (requires(Derived& d) { d.customStop(); }) {
            static_cast<Derived*>(this)->customStop();
        }
    }

    void pause() {
        using enum lifecycle::State;
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [this](auto& block) {
            this->emitErrorMessageIfAny("pause() -> LifecycleState", block->changeStateTo(REQUESTED_PAUSE));
            if (!block->isBlocking()) { // N.B. no other thread/constraint to consider before shutting down
                this->emitErrorMessageIfAny("pause() -> LifecycleState", block->changeStateTo(PAUSED));
            }
        });
        this->emitErrorMessageIfAny("pause() -> LifecycleState", this->changeStateTo(PAUSED));
        if constexpr (requires(Derived& d) { d.customPause(); }) {
            static_cast<Derived*>(this)->customPause();
        }
    }

    void resume() {
        using enum lifecycle::State;
        {
            WorkQuiescenceGuard quiescence(this);
            auto                result = connectPendingEdges();
            if (!result) {
                this->emitErrorMessage("init()", "Failed to connect blocks in graph");
            }
        }
        graph::forEachBlock<TransparentBlockGroup>(*_graph, [this](auto& block) { this->emitErrorMessageIfAny("resume() -> LifecycleState", block->changeStateTo(RUNNING)); });
        if constexpr (requires(Derived& d) { d.customResume(); }) {
            static_cast<Derived*>(this)->customResume();
        }
    }

    void adoptBlock(const std::shared_ptr<BlockModel>& newBlock) {
        using enum lifecycle::State;
        if (const auto connectResult = _toChildMessagePort.connect(*newBlock->msgIn); !connectResult.has_value()) {
            this->emitErrorMessage("connectBlockMessagePorts()", std::format("Failed to connect scheduler input message port to child '{}'", newBlock->uniqueName()));
        }
        _fromChildMessagePort.materialiseDefaultBuffer(_graph->resources().dataResource(), _graph->resources().tagResource());
        auto toSchedulerBuffer = _fromChildMessagePort.buffer();
        newBlock->msgOut->setBuffer(toSchedulerBuffer.streamBuffer, toSchedulerBuffer.tagBuffer);

        if (!lifecycle::isActive(this->state())) {
            return;
        }

        const auto nBatches = _adoptionBlocks.size();
        if (nBatches == 0) {
            return;
        }
        std::lock_guard guard(_adoptionBlocksMutex);

        auto runnerIndex = std::hash<BlockModel*>{}(newBlock.get()) % nBatches;
        _adoptionBlocks[runnerIndex].push_back(newBlock);

        // start scheduler via the SchedulerWrapper::start() so it spins up a
        // thread, instead of potentially blocking this scheduler
        if (auto* schedulerModel = detail::asSchedulerModel(*newBlock)) {
            assert(newBlock->blockCategory() == ScheduledBlockGroup && "scheduler found with incorrect category");

            if (gr::lifecycle::isActive(newBlock->state())) {
                return;
            }

            schedulerModel->start();
            schedulerModel->blockUntilWorking();
            return;
        }

        switch (newBlock->state()) {
        case STOPPED:
        case IDLE: //
            this->emitErrorMessageIfAny("adoptBlock -> INITIALIZED", newBlock->changeStateTo(INITIALISED));
            this->emitErrorMessageIfAny("adoptBlock -> INITIALIZED", newBlock->changeStateTo(RUNNING));
            break;
        case INITIALISED: //
            this->emitErrorMessageIfAny("adoptBlock -> INITIALIZED", newBlock->changeStateTo(RUNNING));
            break;
        case RUNNING: break; // may occur when grouping or ungrouping blocks, as the blocks are moved to the parent while still in a running state
        default: this->emitErrorMessage("propertyCallbackEmplaceBlock", std::format("Unexpected block state during emplacement: {}", gr::meta::enumName(newBlock->state()).value_or("")));
        }
    }

    /*
     * Synchronously removes a blocks from execution order and adoption list,
     * and adds it to the moved block lists so that workers know to remove it
     * from their local job lists.
     *
     * Must be called within a WorkQuiescenceGuard while the scheduler is running.
     */
    void removeBlocksWhileRunning(std::span<const std::shared_ptr<BlockModel>> blocksToRemoveSpan) {
        assert(lifecycle::isActive(this->state()) && gr::atomic_ref(_workQuiescenceRequested).load_acquire() //
               && gr::atomic_ref(_nWorkersInWork).load_acquire() == 0);

        // we need to filter some items from this so it is easiest to create a copy, so at least avoid shared ptr copy and use raw ptrs
        auto blocksToRemove = blocksToRemoveSpan | std::views::transform(&std::shared_ptr<BlockModel>::get) | std::ranges::to<std::vector>();

        // Start by dealing with blocks that have not been adopted yet, they can be removed immediately
        {
            std::lock_guard guard(_adoptionBlocksMutex);
            const auto      toAddress   = [](const auto& ptr) { return std::to_address(ptr); };
            auto            toRemoveSet = blocksToRemove | std::views::transform(toAddress) | std::ranges::to<std::unordered_set>();
            for (std::vector<std::shared_ptr<BlockModel>>& adoptionList : _adoptionBlocks) {
                if (adoptionList.empty()) {
                    continue;
                }

                auto adoptionSet = adoptionList | std::views::transform(toAddress) | std::ranges::to<std::unordered_set>();
                std::erase_if(blocksToRemove, [&adoptionSet](const auto& block) { return adoptionSet.contains(std::to_address(block)); });
                std::erase_if(adoptionList, [&toRemoveSet](const auto& block) { return toRemoveSet.contains(std::to_address(block)); });
            }
        }

        std::lock_guard lock(_executionOrderMutex);

        // transfer blocks from the executionOrder to the movedBlocksList, so
        // the thread can observe that it needs to remove that from its
        // worklist after it wakes up
        const auto moveToMovedList = [&blocksToRemove](auto& workList, MovedBlockList& movedBlocksList) {
            auto iterator = workList.begin();
            while (iterator != workList.end()) {
                if (std::ranges::contains(blocksToRemove, (*iterator).get())) {
                    movedBlocksList.blocks.emplace_back(std::move(*iterator));
                    iterator = workList.erase(iterator);
                } else {
                    ++iterator;
                }
            }
        };

        auto&       executionOrder = *_executionOrder;
        std::size_t numThreads     = executionOrder.size();
        assert(_movedBlocks.size() == numThreads);
        for (std::size_t i = 0; i < numThreads; ++i) {
            std::vector<std::shared_ptr<BlockModel>>& workList = executionOrder.at(i);

            MovedBlockList& movedBlockList = _movedBlocks.at(i);
            std::lock_guard movedBlockGuard(*movedBlockList.mutex);

            moveToMovedList(workList, movedBlockList);
        }
    }

    /**
     * @brief Starts, stops and dumps a trace capture over the message channel.
     *
     * `command` is one of `start`, `stop`, `dump` or `status`. `start` takes an optional
     * `categories` mask and `stop` clears it; `dump` takes a `path` and writes the capture there.
     * Every reply carries the live mask and the record counts, so a caller that only wants to know
     * what is being captured sends `status`.
     *
     * A dump parks the workers first. Records are fixed-size but not written atomically, so a reader
     * walking a ring while its thread is still emitting can observe a torn record; with the workers
     * quiescent there is nothing to tear.
     */
    std::optional<Message> propertyCallbackTraceControl([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kTraceControl);
        auto&      messageData = message.data.value();
        const auto findOr      = [&messageData](std::string_view key) -> Value {
            auto it = messageData.find(key);
            return it != messageData.end() ? (*it).second : Value{};
        };

        const Value       commandValue = findOr(std::string_view{"command"});
        const std::string command(commandValue.value_or(std::string_view{"status"}));

        if constexpr (!gr::trace::kEnabled) {
            message.data = std::unexpected(Error{"trace control requested, but the trace layer was not compiled in: configure with -DGR4_ENABLE_TRACING=ON"});
            return message;
        } else {
            if (command == "start") {
                const Value      maskValue = findOr(std::string_view{"categories"});
                const gr::Size_t mask      = maskValue.value_or(gr::Size_t{gr::trace::kAllCategories});
                trace_categories.value     = mask;
                gr::trace::setCategories(mask); // routed through here, so starting a capture re-anchors its clock
            } else if (command == "stop") {
                trace_categories.value = 0U;
                gr::trace::setCategories(0U);
            } else if (command == "dump") {
                const Value       pathValue = findOr(std::string_view{"path"});
                const std::string path(pathValue.value_or(std::string_view{}));
                if (path.empty()) {
                    message.data = std::unexpected(Error{"trace dump requires a non-empty 'path'"});
                    return message;
                }

                // Parked, not merely asked: the dump reads rings that other threads own.
                const std::expected<std::size_t, gr::Error> written = [&] {
                    WorkQuiescenceGuard quiescence(this);
                    return gr::trace::dump(path);
                }();
                if (!written) {
                    message.data = std::unexpected(written.error());
                    return message;
                }
                messageData.insert_or_assign(std::string_view{"records"}, static_cast<gr::Size_t>(*written));
            } else if (command != "status") {
                message.data = std::unexpected(Error{std::format("unknown trace command '{}': expected start, stop, dump or status", command)});
                return message;
            }

            const gr::trace::RingStats stats = gr::trace::ringStats();
            messageData.insert_or_assign(std::string_view{"categories"}, gr::trace::categories());
            messageData.insert_or_assign(std::string_view{"recorded"}, static_cast<gr::Size_t>(stats.recorded));
            messageData.insert_or_assign(std::string_view{"lost"}, static_cast<gr::Size_t>(stats.lost));
            messageData.insert_or_assign(std::string_view{"rings"}, static_cast<gr::Size_t>(stats.rings));
            return message;
        }
    }

    std::optional<Message> propertyCallbackEmplaceBlock([[maybe_unused]] std::string_view propertyName, Message message) {
        using enum lifecycle::State;
        assert(propertyName == scheduler::property::kEmplaceBlock);
        using namespace std::string_literals;
        const auto& messageData = message.data.value();

        auto* targetGraph = findTargetSubGraph(messageData);
        if (targetGraph == nullptr) {
            message.data = std::unexpected(Error{std::format("No target graph for the message {}", message)});
            return message;
        }

        std::string  blockType;
        property_map blockProperties;

        if (auto yamlIt = messageData.find("yaml"); yamlIt != messageData.end()) {
            // YAML path: create block from a serialised block definition string
            const auto yamlStr = (*yamlIt).second.value_or(std::string_view{});
            if (yamlStr.empty()) {
                message.data = std::unexpected(Error{"yaml field is empty"s});
                return message;
            }
            auto parsed = pmt::yaml::deserialize(yamlStr);
            if (!parsed) {
                message.data = std::unexpected(Error{std::format("Could not parse yaml: {}", parsed.error().message)});
                return message;
            }

            if (auto idIt = parsed->find("id"); idIt != parsed->end()) {
                blockType = std::string((*idIt).second.value_or(std::string_view{}));
            }
            if (blockType.empty()) {
                message.data = std::unexpected(Error{"yaml block definition is missing id field"s});
                return message;
            }

            if (blockType == "SUBGRAPH") {
                // Wrap the single block definition so loadGraphFromMap can process it
                property_map  graphMap;
                Tensor<Value> blocksSeq;
                blocksSeq.push_back(Value(*parsed));
                graphMap.insert_or_assign(std::string_view{"blocks"}, std::move(blocksSeq));

                const std::size_t blocksBefore = targetGraph->blocks().size();
                if (auto loadResult = gr::detail::loadGraphFromMap(targetGraph->pluginLoader(), *targetGraph, std::move(graphMap)); !loadResult) {
                    message.data = std::unexpected(Error{std::format("Failed to create subgraph from yaml: {}", loadResult.error().message)});
                    return message;
                }

                const auto& blocks = targetGraph->blocks();
                if (blocks.size() <= blocksBefore) {
                    message.data = std::unexpected(Error{"No block was added from yaml"s});
                    return message;
                }

                for (std::size_t i = blocksBefore; i < blocks.size(); ++i) {
                    adoptBlock(blocks[i]);
                }

                auto replyData = serializeBlock(targetGraph->pluginLoader(), blocks[blocksBefore], BlockSerializationFlags::All);
                replyData.insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});
                this->emitMessage(scheduler::property::kBlockEmplaced, std::move(replyData));
                return {};
            }

            // Normal block from YAML: read parameters, stripping auto-generated system fields
            if (auto it = parsed->find("parameters"); it != parsed->end()) {
                const Value entry = (*it).second; // bind to lvalue; iter yields by value
                if (auto p = entry.get_if<property_map>()) {
                    blockProperties = *p;
                    blockProperties.erase("unique_name"); // auto-generated, not user-settable
                }
            }
        } else {
            // Non-YAML path: read type and properties directly from the message
            blockType = std::string(messageData.value_or<std::string_view>("type", std::string_view{}));
            if (blockType.empty()) {
                message.data = std::unexpected(Error{std::format("No type specified for the message {}", message)});
                return message;
            }
            if (auto it = messageData.find("properties"); it != messageData.end()) {
                const Value entry = (*it).second;
                if (auto result = entry.get_if<property_map>()) {
                    blockProperties = *result;
                }
            }
        }

        // For the YAML path, settings from the serialised block definition are applied
        // via loadParametersFromPropertyMap after emplacement
        const bool   isYamlPath   = messageData.contains("yaml");
        property_map yamlSettings = isYamlPath ? std::exchange(blockProperties, {}) : property_map{};

        auto emplaceResult = targetGraph->emplaceBlock(blockType, blockProperties);
        if (!emplaceResult) {
            message.data = std::unexpected(emplaceResult.error());
            return message;
        }
        auto newBlock = std::move(*emplaceResult);

        if (isYamlPath && !yamlSettings.empty()) {
            newBlock->settings().loadParametersFromPropertyMap(yamlSettings);
        }

        adoptBlock(newBlock);

        auto replyData = serializeBlock(targetGraph->pluginLoader(), newBlock, BlockSerializationFlags::All);
        replyData.insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});
        this->emitMessage(scheduler::property::kBlockEmplaced, std::move(replyData));

        // Message is sent as a reaction to emplaceBlock, no need for a separate one
        return {};
    }

    std::optional<Message> propertyCallbackRemoveBlock([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kRemoveBlock);
        using namespace std::string_literals;
        auto& messageData = message.data.value();
        // copy bytes into owning std::string — value_or<string_view>() aliases temp Value's storage
        // which dies at end of full expression.
        const auto uniqueName = std::string(messageData.value_or<std::string_view>("uniqueName", std::string_view{}));
        if (uniqueName.empty()) {
            message.data = std::unexpected(Error{std::format("No uniqueName in the message {}", message)});
            return message;
        }

        message.endpoint = scheduler::property::kBlockRemoved;

        auto* targetGraph = findTargetSubGraph(messageData);

        if (targetGraph == nullptr) {
            message.data = std::unexpected(Error{std::format("No target graph for the message {}", message)});
            return message;
        }

        messageData.insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});
        if (auto removedBlock = targetGraph->removeBlockByName(uniqueName); removedBlock.has_value()) {
            makeZombie(std::move(*removedBlock));
        } else {
            message.data = std::unexpected(removedBlock.error());
        }

        return {message};
    }

    std::optional<Message> propertyCallbackGroupBlocks([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kGroupBlocks);
        using namespace std::string_literals;
        auto& messageData = message.data.value();

        message.endpoint = scheduler::property::kBlocksGrouped;

        const std::string typeCopy(messageData.value_or<std::string_view>("type", std::string_view{}));
        if (typeCopy.empty()) {
            message.data = std::unexpected(Error{"No type specified"});
            return message;
        }

        std::vector<std::string> uniqueNames;
        if (auto it = messageData.find("uniqueNames"); it != messageData.end()) {
            const Value entry = (*it).second;
            for (const Value& name : entry.value_or(Tensor<Value>{})) {
                const auto nameView = name.value_or(std::string_view{});
                if (nameView.data() != nullptr) {
                    uniqueNames.emplace_back(nameView);
                }
            }
        }
        if (uniqueNames.empty()) {
            message.data = std::unexpected(Error{"No uniqueNames specified for the message {}"});
            return message;
        }

        auto* targetGraph = findTargetSubGraph(messageData);
        if (targetGraph == nullptr) {
            message.data = std::unexpected(Error{"No target graph specified"});
            return message;
        }

        messageData.insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});
        std::expected<std::shared_ptr<BlockModel>, Error> grouped;
        {
            WorkQuiescenceGuard quiescence(this);
            const auto          blocksToGroup = graph::findBlocks(*targetGraph, uniqueNames);
            if (!blocksToGroup) {
                message.data = std::unexpected(blocksToGroup.error());
                return message;
            }

            // create the subgraph and move blocks into it
            grouped = targetGraph->groupBlocks(*blocksToGroup, typeCopy);

            // if the blocks have been grouped into a new scheduler, we must
            // stop referencing these blocks forever as they are now
            // potentially being worked on by this subscheduler
            if (grouped.has_value() && grouped.value()->blockCategory() == block::Category::ScheduledBlockGroup) {
                removeBlocks(*blocksToGroup);
            }
        }

        if (grouped.has_value()) {
            adoptBlock(grouped.value());
            messageData.insert_or_assign(std::string_view{"uniqueName"}, std::string{grouped.value()->uniqueName()});
        } else {
            message.data = std::unexpected(grouped.error());
        }

        return message;
    }

    std::optional<Message> propertyCallbackUngroupBlocks([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kUngroupBlocks);
        using namespace std::string_literals;
        auto& messageData = message.data.value();

        message.endpoint = scheduler::property::kBlocksUngrouped;

        const std::string uniqueName(messageData.value_or<std::string_view>("uniqueName", std::string_view{}));
        if (uniqueName.empty()) {
            message.data = std::unexpected(Error{"No uniqueName specified"});
            return message;
        }

        auto* targetGraph = findTargetSubGraph(messageData);
        if (targetGraph == nullptr) {
            message.data = std::unexpected(Error{"No target graph specified"});
            return message;
        }
        messageData.insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});

        std::expected<std::shared_ptr<BlockModel>, Error> subGraphBlock = graph::findBlock(*targetGraph, std::string_view(uniqueName));
        if (!subGraphBlock.has_value()) {
            message.data = std::unexpected(subGraphBlock.error());
            return message;
        }

        const block::Category category = subGraphBlock.value()->blockCategory();
        if (category != block::Category::TransparentBlockGroup && category != block::Category::ScheduledBlockGroup) {
            message.data = std::unexpected(Error{std::format("block '{}' is not a sub-graph", uniqueName)});
            return message;
        }

        // if the subgraph is unmanaged, then we were already managing these blocks, this is relatively simple, just change the graph structure
        if (category == block::Category::TransparentBlockGroup) {
            {
                WorkQuiescenceGuard quiescence(this);
                if (auto result = targetGraph->ungroupBlocks(std::shared_ptr<BlockModel>(*subGraphBlock)); !result.has_value()) {
                    message.data = std::unexpected(result.error());
                    return message;
                }
                if (!connectPendingEdges()) {
                    message.data = std::unexpected(Error{std::format("failed to connect pending edges after ungrouping '{}'", uniqueName)});
                }
            }

            // we must now discard the subgraph block which is no longer in our graph. zombie cleanup only removes stopped blocks, but this
            // will not come to a stopped state on its own, so we have to stop it manually
            if ((*subGraphBlock)->state() == lifecycle::State::RUNNING || (*subGraphBlock)->state() == lifecycle::State::PAUSED) {
                this->emitErrorMessageIfAny("ungroupBlocks -> REQUESTED_STOP", (*subGraphBlock)->changeStateTo(lifecycle::State::REQUESTED_STOP));
                if (!(*subGraphBlock)->isBlocking()) {
                    this->emitErrorMessageIfAny("ungroupBlocks -> STOPPED", (*subGraphBlock)->changeStateTo(lifecycle::State::STOPPED));
                }
            }
            makeZombie(std::move((*subGraphBlock)));
            return message;
        }

        // the graph that is about to be ungrouped is a managed subgraph. we need to tell it to pause work, then remove its
        // blocks, and then we adopt its blocks, without changing their state.
        auto* schedulerModel = detail::asSchedulerModel(**subGraphBlock);
        if (schedulerModel == nullptr) {
            message.data = std::unexpected(Error{std::format("ScheduledBlockGroup is not a SchedulerModel {}", uniqueName)});
            return message;
        }
        assert(category == block::Category::ScheduledBlockGroup);

        gr::Graph* graphAboutToBeUngrouped = (*subGraphBlock)->graph();
        assert(graphAboutToBeUngrouped);
        auto ungroupedBlocks = graphAboutToBeUngrouped->blocks() | std::ranges::to<std::vector>();

        {
            WorkQuiescenceGuard thisQuiescence(this);
            // although we have requested work quiescence which prevents worker threads from modifying the graph, there is also the
            // SchedulerModel's runner thread that could potentially read/write to the graph/ports during start(). If that thread
            // has not been started then this function will not block at all.
            schedulerModel->blockUntilWorking();

            if (auto result = targetGraph->ungroupBlocks(*subGraphBlock); !result.has_value()) {
                message.data = std::unexpected(result.error());
                return message;
            }
            // some new edges had to be created, connecting ports directly instead of via exported ports.
            // we must connect those and, in the process, overwrite old port connections. These will
            // continue to exist and write to nothing until the next disconnectAllEdges + connectPendingEdges
            if (!connectPendingEdges()) {
                message.data = std::unexpected(Error{std::format("failed to connect pending edges after ungrouping '{}'", uniqueName)});
                // don't return because the subgraph is in an invalid state
            }

            // while the child is still not running, we tell it to remove all its blocks. when workers
            // come back online, they will not touch these blocks again
            schedulerModel->removeBlocks(ungroupedBlocks);
        }
        makeZombie(std::move(*subGraphBlock));

        // we must now run these blocks which have been extracted from the child graph while still running
        for (const std::shared_ptr<BlockModel>& block : ungroupedBlocks) {
            adoptBlock(block);
        }

        return message;
    }

    std::optional<Message> propertyCallbackRemoveEdge([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kRemoveEdge);
        using namespace std::string_literals;
        auto& messageData = message.data.value();
        // bind iter-yielded Values to lvalues — string_view aliases the Value's blob storage,
        // which dies at the end of the init expression otherwise (UAF).
        const auto findOr = [&messageData](std::string_view key) -> Value {
            auto it = messageData.find(key);
            return it != messageData.end() ? (*it).second : Value{};
        };
        const Value sourceBlockVal = findOr(gr::serialization_fields::EDGE_SOURCE_BLOCK);
        const Value sourcePortVal  = findOr(gr::serialization_fields::EDGE_SOURCE_PORT);
        const auto  sourceBlock    = sourceBlockVal.value_or(std::string_view{});
        const auto  sourcePort     = sourcePortVal.value_or(std::string_view{});
        if (sourceBlock.empty() || sourcePort.empty()) {
            message.data = std::unexpected(Error{std::format("No source definition for the message {}", message)});
            return message;
        }

        message.endpoint = scheduler::property::kEdgeRemoved;

        auto* targetGraph = findTargetSubGraph(messageData);

        if (targetGraph == nullptr) {
            message.data = std::unexpected(Error{std::format("No target graph for the message {}", message)});
            return message;
        }

        messageData.insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});
        {
            WorkQuiescenceGuard quiescence(this);
            if (auto result = targetGraph->removeEdgeBySourcePort(sourceBlock, sourcePort); !result.has_value()) {
                message.data = std::unexpected(result.error());
            }
        }

        return message;
    }

    std::optional<Message> propertyCallbackEmplaceEdge([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kEmplaceEdge);
        using namespace std::string_literals;
        auto& messageData = message.data.value();
        // bind iter-yielded Values to lvalues — string_view / get_if<>() pointers alias the
        // Value's storage which dies at the end of the init expression.
        const auto findOr = [&messageData](std::string_view key) -> Value {
            auto it = messageData.find(key);
            return it != messageData.end() ? (*it).second : Value{};
        };
        const Value sourceBlockVal      = findOr(gr::serialization_fields::EDGE_SOURCE_BLOCK);
        const Value sourcePortVal       = findOr(gr::serialization_fields::EDGE_SOURCE_PORT);
        const Value destinationBlockVal = findOr(gr::serialization_fields::EDGE_DESTINATION_BLOCK);
        const Value destinationPortVal  = findOr(gr::serialization_fields::EDGE_DESTINATION_PORT);
        const Value minBufferSizeVal    = findOr(gr::serialization_fields::EDGE_MIN_BUFFER_SIZE);
        const Value weightVal           = findOr(gr::serialization_fields::EDGE_WEIGHT);
        const Value edgeNameVal         = findOr(gr::serialization_fields::EDGE_NAME);

        const auto  sourceBlock      = sourceBlockVal.value_or(std::string_view{});
        const auto  sourcePort       = sourcePortVal.value_or(std::string_view{});
        const auto  destinationBlock = destinationBlockVal.value_or(std::string_view{});
        const auto  destinationPort  = destinationPortVal.value_or(std::string_view{});
        const auto* minBufferSize    = minBufferSizeVal.get_if<gr::Size_t>(); // raw ptr (checked_access_ptr terminates on null)
        const auto* weight           = weightVal.get_if<std::int32_t>();
        const auto  edgeName         = edgeNameVal.value_or(std::string_view{});

        if (sourceBlock.empty() || sourcePort.empty() || destinationBlock.empty() || destinationPort.empty() || minBufferSize == nullptr || weight == nullptr || edgeName.empty()) {
            message.data = std::unexpected(Error{std::format("Message is incomplete {}", message)});
            return message;
        }

        message.endpoint = scheduler::property::kEdgeEmplaced;

        auto* targetGraph = findTargetSubGraph(messageData);

        if (targetGraph == nullptr) {
            message.data = std::unexpected(Error{std::format("No target graph for the message {}", message)});
            return message;
        }

        messageData.insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});
        {
            WorkQuiescenceGuard quiescence(this);
            const std::size_t   effectiveMinBufferSize = (*minBufferSize == gr::undefined_Size) ? gr::undefined_size : static_cast<std::size_t>(*minBufferSize);
            if (auto result = targetGraph->emplaceEdge(sourceBlock, std::string(sourcePort), destinationBlock, std::string(destinationPort), effectiveMinBufferSize, *weight, edgeName); !result.has_value()) {
                message.data = std::unexpected(result.error());
            }
        }

        return message;
    }

    /*
      Zombie Tutorial:

      Blocks cannot be deleted unless stopped, but stopping may take time (asynchronous).
      We therefore move such blocks to the "zombie list" and disconnect them immediately from the graph,
      allowing them to stop and be deleted safely.

      Each worker thread periodically calls cleanupZombieBlocks(), which:
      - removes fully stopped zombies from the zombie list
      - erases corresponding entries from its own localBlockList
      - updates the shared _executionOrder to ensure zombies do not reappear on restart

      This mechanism supports safe dynamic block removal while the scheduler is running, without blocking execution.
    */
    void cleanupZombieBlocks(std::vector<std::shared_ptr<BlockModel>>& localBlockList) {
        using enum lifecycle::State;
        if (localBlockList.empty()) {
            return;
        }

        std::lock_guard guard(_zombieBlocksMutex);

        auto it = _zombieBlocks.begin();

        while (it != _zombieBlocks.end()) {
            const auto localBlockIt = std::ranges::find(localBlockList, *it);
            if (localBlockIt == localBlockList.end()) {
                // we only care about the blocks local to our thread.
                ++it;
                continue;
            }

            bool shouldDelete = false;

            switch ((*it)->state()) {
            case IDLE:
            case STOPPED:
            case INITIALISED: // block can be deleted immediately
                shouldDelete = true;
                break;
            case ERROR: // delete as well
                shouldDelete = true;
                break;
            case REQUESTED_STOP: // block will be deleted later
                break;
            case REQUESTED_PAUSE: // block will be deleted later
                // There's no transition from REQUESTED_PAUSE to REQUESTED_STOP
                // Will be moved to REQUESTED_STOP as soon as it's possible
                break;
            case PAUSED: // zombie was in REQUESTED_PAUSE and now finally in PAUSED. Can be stopped now.
                // Will be deleted in a next zombie maintenance period
                this->emitErrorMessageIfAny("cleanupZombieBlocks", (*it)->changeStateTo(REQUESTED_STOP));
                break;
            case RUNNING: assert(false && "Doesn't happen: zombie blocks are never running"); break;
            }

            if (shouldDelete) {
                localBlockList.erase(localBlockIt);

                std::shared_ptr<BlockModel> zombieRaw = *it;
                it                                    = _zombieBlocks.erase(it); // ~Block() runs here

                // We need to remove zombieRaw from jobLists as well, in case Scheduler ever goes to INITIALIZED again.
                std::lock_guard lock(_executionOrderMutex);
                for (auto& jobList : *this->_executionOrder) {
                    auto job_it = std::remove(jobList.begin(), jobList.end(), zombieRaw);
                    if (job_it != jobList.end()) {
                        jobList.erase(job_it, jobList.end());
                        break;
                    }
                }

            } else {
                ++it;
            }
        }
    }

    void adoptBlocks(std::size_t runnerID, std::vector<std::shared_ptr<BlockModel>>& localBlockList) {
        std::scoped_lock guard(_adoptionBlocksMutex, _executionOrderMutex);

        assert(_adoptionBlocks.size() > runnerID);
        auto& newBlocks = _adoptionBlocks[runnerID];
        if (newBlocks.empty()) {
            return;
        }

        // adopt both into our local blocklist and _executionOrder, because removeBlocksWhileRunning()
        // observes _executionOrder in order to understand what blocks a thread is working on
        assert(_executionOrder->size() > runnerID);
        auto& sharedJobList = (*_executionOrder)[runnerID];
        sharedJobList.insert(sharedJobList.end(), newBlocks.begin(), newBlocks.end());

        localBlockList.reserve(localBlockList.size() + newBlocks.size());
        localBlockList.insert(localBlockList.end(), newBlocks.begin(), newBlocks.end());
        newBlocks.clear();
    }

    /*
     * Removes blocks from a thread's local blocklist if they were removed
     * during workquiescence due to being grouped into a subgraph
     */
    void cleanupRemovedBlocks(std::size_t runnerID, std::vector<std::shared_ptr<BlockModel>>& localBlockList) {
        MovedBlockList& ourMovedBlockList = _movedBlocks[runnerID];
        std::lock_guard lockGuard(*ourMovedBlockList.mutex);

        auto ourMovedBlockListSet = ourMovedBlockList.blocks | std::ranges::to<std::unordered_set>();
        auto localBlockListSet    = localBlockList | std::ranges::to<std::unordered_set>();

        std::erase_if(ourMovedBlockList.blocks, //
            [&localBlockListSet](const auto& block) { return localBlockListSet.contains(block); });
        std::erase_if(localBlockList, //
            [&ourMovedBlockListSet](const auto& block) { return ourMovedBlockListSet.contains(block); });
    }

    /*
      Moves a block to the zombie list:

      - Requests stop if the block is still running or paused.
      - Removes the block from adoption lists (to handle edge cases such as Add Block → Remove Block).
      - Adds it to the zombie list.

      The block will be physically deleted by cleanupZombieBlocks() when it reaches a safe state.
    */
    void makeZombie(std::shared_ptr<BlockModel> block) {
        using enum lifecycle::State;
        if (block->state() == PAUSED || block->state() == RUNNING) {
            this->emitErrorMessageIfAny("makeZombie", block->changeStateTo(REQUESTED_STOP));
        }

        // maybe the block is not yet adopted, in which case there is no need to zombify it, just refuse to adopt it.
        // happens if the user issues quick successive add and then remove requests for the same block.
        {
            std::lock_guard guard(_adoptionBlocksMutex);
            for (std::vector<std::shared_ptr<BlockModel>>& adoptionList : _adoptionBlocks) {
                if (auto it = std::ranges::find(adoptionList, block); it != adoptionList.end()) {
                    adoptionList.erase(it);
                    return;
                }
            }
        }

        std::lock_guard guard(_zombieBlocksMutex);
        _zombieBlocks.push_back(std::move(block));
    }

    // Moves all blocks into the zombie list
    // Useful for bulk operations such as "set grc yaml" message
    void makeAllZombies() {
        using enum lifecycle::State;
        std::lock_guard guard(_zombieBlocksMutex);

        for (auto& block : this->_graph->blocks()) {
            switch (block->state()) {
            case RUNNING:
            case REQUESTED_PAUSE:
            case PAUSED: //
                this->emitErrorMessageIfAny("makeAllZombies", block->changeStateTo(REQUESTED_STOP));
                break;

            case INITIALISED: //
                this->emitErrorMessageIfAny("makeAllZombies", block->changeStateTo(STOPPED));
                break;
            case IDLE:
            case STOPPED:
            case ERROR:
            case REQUESTED_STOP:
                // Can go into the zombie list and deleted
                break;
            default:;
            }

            _zombieBlocks.push_back(std::move(block));
        }

        this->_graph->clear();
    }

    std::optional<Message> propertyCallbackGraphGRC([[maybe_unused]] std::string_view propertyName, Message message) {
        using enum lifecycle::State;
        assert(propertyName == scheduler::property::kGraphGRC);

        auto& pluginLoader = gr::globalPluginLoader();
        if (message.cmd == message::Command::Get) {
            message.data = property_map{{"value", gr::saveGrc(pluginLoader, *_graph)}};
        } else if (message.cmd == message::Command::Set) {
            const auto& messageData = message.data.value();
            const auto  yamlContent = std::string(messageData.value_or<std::string_view>("value", std::string_view{}));
            if (yamlContent.empty()) {
                message.data = std::unexpected(Error{std::format("Yaml content not found")});
            } else {
                auto newGraphResult = gr::loadGrc(pluginLoader, yamlContent);
                if (!newGraphResult) {
                    message.data = std::unexpected(Error{std::format("Error parsing YAML: {}", newGraphResult.error().message)});
                } else {
                    auto newGraph = std::move(*newGraphResult);

                    makeAllZombies();

                    const auto originalState = this->state();

                    // need to stop running scheduler before performing
                    // exchange, otherwise exchange will do state change
                    // operations expecting to be called from an external
                    // thread, but this may be from worker thread (hence
                    // waitDone(true) call)
                    if (lifecycle::isActive(originalState)) {
                        if (auto result = this->changeStateTo(REQUESTED_STOP); !result) {
                            auto msg = std::format("Failed to request stop: {}", result.error());
                            this->emitErrorMessage("propertyCallbackGraphGRC", msg);
                            message.data = std::unexpected(Error{msg});
                            return message;
                        }
                        waitDone(true); // wait for all *other* jobs to complete

                        if (auto result = this->changeStateTo(STOPPED); !result) {
                            auto msg = std::format("Failed to finish stopping scheduler: {}", result.error());
                            this->emitErrorMessage("propertyCallbackGraphGRC", msg);
                            message.data = std::unexpected(Error{msg});
                            return message;
                        }
                    }
                    assert(this->state() == STOPPED);

                    if (auto result = this->exchange(std::move(newGraph)); !result) {
                        auto msg = std::format("Failed to exchange graph: {}", result.error());
                        this->emitErrorMessage("propertyCallbackGraphGRC", msg);
                        message.data = std::unexpected(Error{msg});
                        return message;
                    }

                    message.data = property_map{{"originalSchedulerState", static_cast<int>(originalState)}};
                }
            }

        } else {
            message.data = std::unexpected(Error{std::format("Unexpected command type {}", message.cmd)});
        }

        return message;
    }

    std::optional<Message> propertyCallbackSchedulerInspect([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kSchedulerInspect);

        if (const bool yamlSerialize =
                [&] {
                    if (!message.data) {
                        return false;
                    }
                    if (const auto it = message.data->find("serialization_format"); it != message.data->cend()) {
                        return (*it).second.value_or(std::string_view{}) == "yaml";
                    }
                    return false;
                }();
            !yamlSerialize) {
            message.data = [&] {
                property_map result;
                result[std::pmr::string(serialization_fields::BLOCK_NAME)]        = std::string(this->name);
                result[std::pmr::string(serialization_fields::BLOCK_UNIQUE_NAME)] = std::string(this->unique_name);
                result[std::pmr::string(serialization_fields::BLOCK_CATEGORY)]    = std::string(gr::meta::enumName(blockCategory).value_or(""));

                // Requesting graph serialization
                property_map serializedChildren;
                auto         graphData = _graph->propertyCallbackGraphInspect(graph::property::kGraphInspect, {});
                if (!graphData.has_value()) {
                    return result;
                }
                serializedChildren[std::pmr::string(_graph->unique_name)] = graphData->data.value();

                result[std::pmr::string(serialization_fields::BLOCK_CHILDREN)] = std::move(serializedChildren);
                return result;
            }();
        } else {
            message.data = {{"yamlData", saveGrc(gr::globalPluginLoader(), *_graph)}};
        }

        message.endpoint = scheduler::property::kSchedulerInspected;
        return message;
    }

    std::optional<Message> propertyCallbackInspectBlock([[maybe_unused]] std::string_view propertyName, Message message) {
        auto result = _graph->propertyCallbackInspectBlock(propertyName, message);
        if (result) {
            result->serviceName = this->unique_name;
        }
        return result;
    }

    std::optional<Message> propertyCallbackReplaceBlock([[maybe_unused]] std::string_view propertyName, Message message) {
        assert(propertyName == scheduler::property::kReplaceBlock);
        using namespace std::string_literals;
        const auto& messageData = message.data.value();
        // copy into owning std::string — value_or<string_view>() aliases temp Value's storage.
        const auto uniqueName = std::string(messageData.value_or<std::string_view>("uniqueName", std::string_view{}));
        const auto type       = std::string(messageData.value_or<std::string_view>("type", std::string_view{}));
        if (uniqueName.empty() || type.empty()) {
            message.data = std::unexpected(Error{std::format("No uniqueName or type in the message {}", message)});
            return message;
        }
        const property_map properties = [&] {
            if (auto it = messageData.find("properties"); it != messageData.end()) {
                const Value entryVal = (*it).second;
                auto        result   = entryVal.get_if<property_map>();
                if (!result) {
                    return property_map{};
                }
                return result->owned(); // materialise the view-mode ValueMap into an owning copy
            }
            return property_map{};
        }();

        auto* targetGraph = findTargetSubGraph(messageData);

        if (targetGraph == nullptr) {
            message.data = std::unexpected(Error{std::format("No target graph for the message {}", message)});
            return message;
        }

        auto replaceResult = targetGraph->replaceBlock(uniqueName, type, properties);
        if (!replaceResult) {
            message.data = std::unexpected(replaceResult.error());
            return message;
        }
        auto& [oldBlock, newBlockRaw] = *replaceResult;
        makeZombie(std::move(oldBlock));

        std::optional<Message> result = gr::Message{};
        result->endpoint              = scheduler::property::kBlockReplaced;
        result->data                  = serializeBlock(gr::globalPluginLoader(), newBlockRaw, BlockSerializationFlags::All);

        result->data->insert_or_assign(std::string_view{"_targetGraph"}, std::string{targetGraph->unique_name.value()});
        result->data->insert_or_assign(std::string_view{"replacedBlockUniqueName"}, std::string{uniqueName});

        return result;
    }
};

template<ExecutionPolicy execution = ExecutionPolicy::singleThreaded, profiling::ProfilerLike TProfiler = profiling::null::Profiler, SchedulingPolicyLike TPolicy = RoundRobinPolicy>
struct Simple : SchedulerBase<Simple<execution, TProfiler, TPolicy>, execution, TProfiler, TPolicy> {
    using Description = Doc<R""(Simple loop based Scheduler, which iterates over all blocks in the order they have beein defined and emplaced definition in the graph.)"">;

    using SchedulerBase<Simple<execution, TProfiler, TPolicy>, execution, TProfiler, TPolicy>::SchedulerBase;

    void customInit() {
        [[maybe_unused]] const auto pe = this->_profilerHandler->startCompleteEvent("scheduler_simple.init");

        // generate job list
        const gr::Graph   flatGraph = graph::flatten(*this->_graph);
        const std::size_t nBlocks   = flatGraph.blocks().size();

        std::size_t n_batches = 1UZ;
        switch (this->executionPolicy()) {
        case ExecutionPolicy::singleThreaded:
        case ExecutionPolicy::singleThreadedBlocking:
        case ExecutionPolicy::externalStep: break; // single job list; the application drives step()
        case ExecutionPolicy::multiThreaded: n_batches = std::min(static_cast<std::size_t>(this->_pool->maxThreads()), nBlocks); break;
        default:;
        }

        std::lock_guard lock(this->_executionOrderMutex);
        std::lock_guard guard(this->_adoptionBlocksMutex);
        this->_adoptionBlocks.clear();
        this->_adoptionBlocks.resize(n_batches);
        this->_executionOrder->clear();
        this->_executionOrder->reserve(n_batches);
        for (std::size_t i = 0; i < n_batches; i++) {
            // create job-set for thread
            auto& job = this->_executionOrder->emplace_back(std::vector<std::shared_ptr<BlockModel>>());
            job.reserve(nBlocks / n_batches + 1);
            for (std::size_t j = i; j < nBlocks; j += n_batches) {
                job.push_back(flatGraph.blocks()[j]);
            }
        }
    }

    // repopulate _executionOrder when restarting the graph because our graph contents may have changed while stopped
    void customReset() { customInit(); }
};

namespace detail {
inline JobLists batchBlocks(const std::vector<std::shared_ptr<BlockModel>>& blocks, std::size_t n_batches) {
    JobLists result(n_batches);
    for (std::size_t batch = 0UZ; batch < n_batches; ++batch) {
        result[batch].reserve(blocks.size() / n_batches + 1UZ);
        for (std::size_t i = batch; i < blocks.size(); i += n_batches) {
            result[batch].push_back(blocks[i]);
        }
    }
    return result;
}

inline void printExecutionOrder(const std::vector<std::vector<std::shared_ptr<BlockModel>>>& executionOrder) {
    std::size_t batchIndex = 0;
    for (const auto& batch : executionOrder) {
        std::print("Batch #{}:\n", batchIndex++);
        for (const auto& block : batch) {
            std::print("  - {} ({})\n", block->name(), block->uniqueName());
        }
    }
}

} // namespace detail

template<ExecutionPolicy execution = ExecutionPolicy::singleThreaded, profiling::ProfilerLike TProfiler = profiling::null::Profiler, SchedulingPolicyLike TPolicy = RoundRobinPolicy>
struct BreadthFirst : SchedulerBase<BreadthFirst<execution, TProfiler, TPolicy>, execution, TProfiler, TPolicy> {
    using Description = Doc<R""(Breadth First Scheduler which traverses the graph starting from the source blocks in a breath first fashion
detecting cycles and blocks which can be reached from several source blocks.)"">;

    static_assert(execution == ExecutionPolicy::singleThreaded || execution == ExecutionPolicy::multiThreaded, "Unsupported execution policy");

    using SchedulerBase<BreadthFirst<execution, TProfiler, TPolicy>, execution, TProfiler, TPolicy>::SchedulerBase;

    void customInit() {
        /* implements Breadth-first search scheduling algorithm (https://en.wikipedia.org/wiki/Breadth-first_search)
         * 1. compute 'adjacencyList'
         * 2. determine all 'sourceBlocks' S (no incoming edges)
         * 3. initialise queue Q with S
         * 4. while Q not empty:
         *   - dequeue Block B
         *   - if B not visited:
         *     - mark visited
         *     - add B to result
         *   - for each outgoing edge from B:
         *     - if target not yet reached, enqueue target
         *
         * For more details see also:
         * [1] T. H. Cormen, C. E. Leiserson, R. L. Rivest, and C. Stein, "Introduction to Algorithms", 3rd ed., MIT Press, 2009, ch. 22.2.
         * [2] P. Morin, "Open Data Structures". [Online]. available at: https://opendatastructures.org/
         */
        using block_t                  = std::shared_ptr<BlockModel>;
        [[maybe_unused]] const auto pe = this->_profilerHandler->startCompleteEvent("breadth_first.init");

        gr::Graph                      flatGraph     = gr::graph::flatten(*this->_graph);
        const gr::graph::AdjacencyList adjacencyList = graph::computeAdjacencyList(flatGraph);
        const std::vector<block_t>     sourceBlocks  = graph::findSourceBlocks(adjacencyList);

        std::vector<block_t>        blockList;
        std::unordered_set<block_t> visited;
        std::queue<block_t>         queue;
        std::set<block_t>           reached;

        for (const auto& src : sourceBlocks) {
            if (reached.insert(src).second) {
                queue.push(src);
            }
        }

        while (!queue.empty()) {
            block_t current = queue.front();
            queue.pop();

            if (visited.insert(current).second) {
                blockList.push_back(current);
            }

            // enqueue outgoing neighbours, but only once
            if (adjacencyList.contains(current)) {
                for (const auto& edges : adjacencyList.at(current) | std::views::values) {
                    for (const auto* edge : edges) {
                        const auto& dst = edge->destinationBlock();
                        if (reached.insert(dst).second) {
                            queue.push(dst);
                        }
                    }
                }
            }
        }

        const std::size_t n_batches = (execution == ExecutionPolicy::multiThreaded) ? std::min(static_cast<std::size_t>(this->_pool->maxThreads()), blockList.size()) : 1UZ;

        std::lock_guard lock(this->_executionOrderMutex);
        std::lock_guard guard(this->_adoptionBlocksMutex);
        this->_adoptionBlocks.clear();
        this->_adoptionBlocks.resize(n_batches);
        *this->_executionOrder = detail::batchBlocks(blockList, n_batches);
    }

    // repopulate _executionOrder when restarting the graph because our graph contents may have changed while stopped
    void customReset() { customInit(); }
};

template<ExecutionPolicy execution = ExecutionPolicy::singleThreaded, profiling::ProfilerLike TProfiler = profiling::null::Profiler, SchedulingPolicyLike TPolicy = RoundRobinPolicy>
struct DepthFirst : SchedulerBase<DepthFirst<execution, TProfiler, TPolicy>, execution, TProfiler, TPolicy> {
    using Description = Doc<R""(Depth First Scheduler which traverses the graph starting from the source blocks in a depth-first manner.)"">;
    static_assert(execution == ExecutionPolicy::singleThreaded || execution == ExecutionPolicy::multiThreaded, "Unsupported execution policy");

    using SchedulerBase<DepthFirst<execution, TProfiler, TPolicy>, execution, TProfiler, TPolicy>::SchedulerBase;

    void customInit() {
        /**
         * implements Depth-first search scheduling algorithm (https://en.wikipedia.org/wiki/Depth-first_search)
         * 1. compute 'adjacencyList'
         * 2. determine all `sourceBlocks' S (no incoming edges)
         * 3. initialise visited set
         * 4. for each source s in S:
         *   - recursively visit(s)
         * 5. visit(Block B):
         *   - if B visited: return
         *   - mark B visited
         *   - add B to result
         *   - for each outgoing edge from B:
         *     - recursively visit(destination)
         *
         * For more details see also:
         * [1] T. H. Cormen, C. E. Leiserson, R. L. Rivest, and C. Stein, "Introduction to Algorithms", 3rd ed., MIT Press, 2009, ch. 22.2.
         * [2] P. Morin, "Open Data Structures". [Online]. available at: https://opendatastructures.org/
         */
        using block_t                  = std::shared_ptr<BlockModel>;
        [[maybe_unused]] const auto pe = this->_profilerHandler->startCompleteEvent("depth_first.init");

        gr::Graph                  flatGraph     = gr::graph::flatten(*this->_graph);
        const graph::AdjacencyList adjacencyList = graph::computeAdjacencyList(flatGraph);
        const std::vector<block_t> sourceBlocks  = graph::findSourceBlocks(adjacencyList);

        std::vector<block_t> blockList;
        std::set<block_t>    visited;

        auto dfs = [&](this auto&& self, const block_t& node) -> void {
            if (!visited.insert(node).second) {
                return; // already visited
            }
            blockList.push_back(node);

            if (adjacencyList.contains(node)) {
                for (const auto& edges : adjacencyList.at(node) | std::views::values) {
                    for (const auto* edge : edges) {
                        self(edge->destinationBlock());
                    }
                }
            }
        };

        for (const auto& src : sourceBlocks) {
            dfs(src);
        }

        const std::size_t n_batches = (execution == ExecutionPolicy::multiThreaded) ? std::min(static_cast<std::size_t>(this->_pool->maxThreads()), blockList.size()) : 1UZ;

        std::lock_guard lock(this->_executionOrderMutex);
        std::lock_guard guard(this->_adoptionBlocksMutex);
        this->_adoptionBlocks.clear();
        this->_adoptionBlocks.resize(n_batches);
        *this->_executionOrder = detail::batchBlocks(blockList, n_batches);
    }

    // repopulate _executionOrder when restarting the graph because our graph contents may have changed while stopped
    void customReset() { customInit(); }
};

} // namespace gr::scheduler

#endif // GNURADIO_SCHEDULER_HPP
