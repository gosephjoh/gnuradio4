#ifndef GNURADIO_TRACE_HPP
#define GNURADIO_TRACE_HPP

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include <gnuradio-4.0/AtomicRef.hpp>

/**
 * @brief Low-overhead trace markers for the scheduler worker loop, its policy routines and `work()`.
 *
 * A fixed-size POD record written into a per-thread ring, carrying enough metadata to reconstruct
 * execution times, response times and tardiness offline. Two gates govern it: the build flag
 * `GR_ENABLE_TRACING`, which discards every emit body, and a runtime category mask selecting which
 * marker groups are live.
 *
 * This header is deliberately dependency-free — it is included from `Scheduler.hpp` and, later,
 * from `Block.hpp`, so it must add nothing to their include graphs and must compile under
 * `-fno-rtti -fno-exceptions`. The ring, the interning registry and the file writer live in
 * `core/src/Trace.cpp`.
 */
namespace gr::trace {

#ifdef GR_ENABLE_TRACING
inline constexpr bool kEnabled = true;
#else
inline constexpr bool kEnabled = false;
#endif

/// What a record describes. The `Kind` is the discriminant for `Event`'s payload words and flag
/// bits: the same 32 bytes mean different things per kind, as a wire format does.
enum class Kind : std::uint8_t {
    workBegin,         /// entry to one productive `work()` invocation; an instant
    workEnd,           /// its exit; `durationNs` carries how long the call took
    workProbe,         /// aggregate of the unproductive ones, one per block per sweep
    workerStart,       /// worker thread entered its loop
    workerStop,        /// ... and left it
    sweep,             /// one `traverseBlockListOnce`
    messagePhase,      /// the message/house-keeping gate
    houseKeeping,      /// buffer reclaim
    stateSync,         /// `syncSchedStates` + `applyStaticOrder` + `buildReleaseStorage`
    adopt,             /// blocks adopted into a running worker
    zombieReap,        /// zombie cleanup
    quiescenceWait,    /// blocked on work quiescence
    idle,              /// worker slept rather than worked
    jobRelease,        /// a job was admitted
    jobReleaseDropped, /// ... or could not be, because the ring was full
    releaseScan,       /// a release-detection pass, backstop or successor walk
    select,            /// one selection decision
    selectEmpty,       /// nothing was released this pass
    selectionBoundHit, /// the per-pass selection bound was reached
    heapFallback,      /// the ready-heap reverted to the linear scan
    deadlineMiss,      /// completion later than the job's absolute deadline
    blockCounter,      /// per-block per-sweep aggregate
    workerCounter,     /// per-worker per-sweep aggregate
    blockStateChange,  /// a block left the schedule
    entityRetired,     /// an interned identity is no longer valid
    workExact,         /// intra-`work()`: one `workInternal` with exact sample counts
    workPhase          /// intra-`work()`: one phase within it
};

/// Runtime mask bits. A tracing-enabled binary ships with a mask of `0` and is turned on live, so
/// the cost in production is one relaxed load and a predictable branch per marker.
enum class Category : std::uint32_t {
    lifecycle     = 1U << 0, /// worker and block state transitions, adoption, quiescence
    counters      = 1U << 1, /// per-sweep aggregates
    work          = 1U << 2, /// per-invocation markers
    schedulerLoop = 1U << 3, /// sweep structure and its overheads
    release       = 1U << 4, /// job release, drop, detection scans
    select        = 1U << 5, /// selection decisions and their diagnostics
    deadline      = 1U << 6, /// misses and tardiness
    workPhases    = 1U << 7, /// the four-way split of one `work()` into its phases
    /// Exact per-invocation sample counts from inside `work()`. Appended rather than slotted next to
    /// `workPhases`, because `categoryMask` is written into the `.gr4trace` header: renumbering a bit
    /// would silently change what an already-written capture's mask means, with no version to signal it.
    workExact = 1U << 8
};

/// One past the last `Kind`, so a test can walk every enumerator. `categoryOf`'s own completeness is
/// guaranteed by `-Wswitch` rather than by this count.
inline constexpr std::size_t kKindCount = 27UZ;
static_assert(std::to_underlying(Kind::workPhase) + 1U == kKindCount, "a Kind was added or removed without updating kKindCount");

/**
 * Every `Kind` belongs to exactly one `Category`, so the category is a property of the record rather
 * than a second argument every call site has to supply and could get wrong. The mapping lives here,
 * once, instead of being restated at every emit site.
 *
 * A `switch` with no `default:` rather than a lookup table: `-Wswitch` is part of `-Wall` and
 * `-Werror` is on, so a `Kind` added without a category becomes a **compile error naming the
 * enumerator**. A table can only be guarded by a size assertion, which catches a changed count but
 * never a wrong mapping.
 */
[[nodiscard]] constexpr Category categoryOf(Kind kind) noexcept {
    switch (kind) {
    case Kind::workBegin:
    case Kind::workEnd:
    case Kind::workProbe: return Category::work;

    case Kind::workerStart:
    case Kind::workerStop:
    case Kind::adopt:
    case Kind::zombieReap:
    case Kind::blockStateChange:
    case Kind::entityRetired: return Category::lifecycle;

    case Kind::sweep:
    case Kind::messagePhase:
    case Kind::houseKeeping:
    case Kind::stateSync:
    case Kind::quiescenceWait:
    case Kind::idle: return Category::schedulerLoop;

    case Kind::jobRelease:
    case Kind::jobReleaseDropped:
    case Kind::releaseScan: return Category::release;

    case Kind::select:
    case Kind::selectEmpty:
    case Kind::selectionBoundHit:
    case Kind::heapFallback: return Category::select;

    case Kind::deadlineMiss: return Category::deadline;

    case Kind::blockCounter:
    case Kind::workerCounter: return Category::counters;

    case Kind::workExact: return Category::workExact;

    case Kind::workPhase: return Category::workPhases;
    }
    return Category::lifecycle; // unreachable for a declared Kind; -Wswitch guarantees the cases are complete
}

/// Folds categories into the mask `setCategories()` takes. `Category` is a scoped enum, so the bare
/// `|` that a plain flag enum would allow is unavailable, and this is the alternative that does not
/// require opening the type up to arbitrary arithmetic.
template<std::same_as<Category>... TCategories>
[[nodiscard]] constexpr std::uint32_t categoryMask(TCategories... categories) noexcept {
    return (std::uint32_t{0} | ... | std::to_underlying(categories));
}

/// Listed by hand, so adding a category means adding it here too — otherwise
/// `setCategories(kAllCategories)` silently stops meaning "all". `qa_Trace.cpp` asserts the popcount
/// and the mask value against the enumerator count, which is what turns that omission into a failure.
inline constexpr std::uint32_t kAllCategories = categoryMask(Category::lifecycle, Category::counters, Category::work, Category::schedulerLoop, //
    Category::release, Category::select, Category::deadline, Category::workPhases, Category::workExact);

/// Interned block or phase identity, assigned once per `BlockModel*` and cached in `SchedState`.
/// Narrow on purpose: the record has three payload words to spend and 65534 blocks is well beyond
/// anything GR4 has been benchmarked at.
using EntityId = std::uint16_t;

inline constexpr EntityId      kNoEntity    = 0U;     /// worker-scoped record, or interning failed
inline constexpr std::uint16_t kMaxEntities = 65534U; /// ids run [1, kMaxEntities]

/// The top two payload values are reserved, so a truncated count can never be mistaken for one.
inline constexpr std::uint32_t kUnsetDeadline = 0xFFFFFFFEU; /// `absoluteDeadline` was `time_point::max()`
inline constexpr std::uint32_t kSaturated     = 0xFFFFFFFFU; /// the real value exceeded the payload

/// `flags` is discriminated by `Kind`, exactly as the payload words are, so the same bit carries
/// different meaning in different records. Grouped below by the kinds that read them; no group
/// collides with itself, and no cross-group meaning is implied.
namespace flag {

/// Which selection loop produced a `workBegin`/`workEnd` pair. Occupies the low two bits, so a report
/// cannot silently compare a round-robin trace against an EDF one.
inline constexpr std::uint8_t kLoopKindMask = 0b0000'0011U;

inline constexpr std::uint8_t kIsSource     = 1U << 2; /// `Kind::workEnd`: performed_work is processedOut
inline constexpr std::uint8_t kJobBacked    = 1U << 3; /// `Kind::work*`: ran against a released job
inline constexpr std::uint8_t kBoundHit     = 1U << 0; /// `Kind::sweep`: the selection bound was reached
inline constexpr std::uint8_t kViaStep      = 1U << 1; /// `Kind::sweep`: `externalStep`, not a pool worker
inline constexpr std::uint8_t kDidAdopt     = 1U << 0; /// `Kind::messagePhase`
inline constexpr std::uint8_t kDidRemove    = 1U << 1; /// `Kind::messagePhase`
inline constexpr std::uint8_t kDidReap      = 1U << 2; /// `Kind::messagePhase`
inline constexpr std::uint8_t kDidHouseKeep = 1U << 3; /// `Kind::messagePhase`
inline constexpr std::uint8_t kDidStateSync = 1U << 4; /// `Kind::messagePhase`
inline constexpr std::uint8_t kListChanged  = 1U << 0; /// `Kind::stateSync`: the block list actually moved
/// `Kind::workerStop`: `payload2` holds a real on-core time. Distinguishes a platform with no
/// thread-CPU clock from a worker that consumed nothing -- a bare zero would read as total preemption.
inline constexpr std::uint8_t kThreadCpuValid   = 1U << 0;
inline constexpr std::uint8_t kQuiescenceDenied = 1U << 5; /// `Kind::messagePhase`: the work guard refused the pass

/// `Kind::jobRelease`. Clear means the per-sweep backstop released it; set means the event-driven
/// successor walk did. Bits [2,8) are reserved for the post-release queue depth.
inline constexpr std::uint8_t kViaSuccessorWalk = 1U << 0;
inline constexpr std::uint8_t kEosWaived        = 1U << 1; /// the batch floor was waived at end of stream

inline constexpr std::uint8_t kViaHeap           = 1U << 0; /// `Kind::select`: clear means the linear scan
inline constexpr std::uint8_t kStaleEntrySkipped = 1U << 1; /// `Kind::select`: a popped heap entry was stale
inline constexpr std::uint8_t kDeadlineMissed    = 1U << 0; /// `Kind::deadlineMiss`
inline constexpr std::uint8_t kDeadlineSuspect   = 1U << 1; /// ... but the deadline arithmetic overflowed

} // namespace flag

/// Why a worker slept rather than worked, in `Kind::idle`'s `payload0`.
enum class IdleReason : std::uint32_t {
    awaitingAdoption, /// the block list is empty and the worker is waiting to be given work
    paused,           /// the scheduler is PAUSED
    otherState,       /// neither RUNNING nor PAUSED
    noProgress        /// `singleThreadedBlocking` waiting on the progress counter
};

/// Which loop ran a `workBegin`/`workEnd` invocation, in `flags & flag::kLoopKindMask`.
enum class LoopKind : std::uint8_t { roundRobin = 0U, fixedPriority = 1U, jobDriven = 2U };

/// `Kind::workPhase`'s `payload0`: which stage of one `workInternal` the record covers. Together the
/// four partition the invocation, so their durations sum to no more than the enclosing `workExact`.
enum class Phase : std::uint32_t { computeSampleLimits = 0U, prepareStreams = 1U, dispatchProcessing = 2U, finaliseIO = 3U };

inline constexpr std::size_t kPhaseCount = 4UZ;
static_assert(std::to_underlying(Phase::finaliseIO) + 1U == kPhaseCount, "a Phase was added or removed without updating kPhaseCount");

/**
 * @brief One trace record: 32 bytes, trivially copyable, no indirection.
 *
 * Phase markers use the **complete-event** form — a start timestamp plus a duration, one record
 * rather than a begin/end pair. Records are appended at *end* time, so a ring is ordered by
 * completion rather than by start; the converter sorts by `startNs`.
 *
 * `work()` is the deliberate exception and emits a `workBegin`/`workEnd` **pair**. A complete event
 * would fold the scheduler overhead preceding an invocation into that invocation's duration, which
 * pollutes exactly the execution-time-against-samples regression this layer exists to feed; the
 * pair separates them, and the gap from one `workEnd` to the next `workBegin` *is* the scheduler
 * overhead, measured rather than inferred.
 *
 * `payload0..2` are named for their position rather than their meaning because the meaning is
 * given by `kind`. That is at odds with the project's usual naming rule and is the deliberate
 * exception a discriminated wire format earns: the twenty-six kinds want heterogeneous payloads,
 * and naming the fields for any one of them would mislead at the other twenty-five.
 */
struct Event {
    std::uint64_t startNs{};    /// `steady_clock`, absolute — the same domain as `Job::releaseTime`
    std::uint32_t durationNs{}; /// 0 for instantaneous kinds, `kSaturated` past 4.29 s
    std::uint32_t payload0{};
    std::uint32_t payload1{};
    std::uint32_t payload2{};
    EntityId      entity{};   /// block, worker phase, or `kNoEntity`
    Kind          kind{};     /// discriminates the payload words and the flag bits
    std::uint8_t  workerId{}; /// == `runnerID`
    std::int8_t   status{};   /// `work::Status`, or 0 where the kind has none
    std::uint8_t  flags{};
    std::uint16_t reserved{}; /// must be zero: the file format reads it back
};

static_assert(sizeof(Event) == 32UZ);
static_assert(std::has_single_bit(sizeof(Event))); /// `CircularBuffer` needs it, cf. `Profiler.hpp`
static_assert(alignof(Event) == 8UZ);
static_assert(std::is_trivially_copyable_v<Event>);
static_assert(std::is_standard_layout_v<Event>); /// so the writer may `offsetof` and `memcpy` it

/**
 * Narrows a 64-bit count into a payload word, saturating rather than truncating.
 *
 * Truncation is not a cosmetic problem here. `max_work_items` defaults to `SIZE_MAX` and
 * `kUnboundedBatch` is likewise `SIZE_MAX`, so a naive cast delivers `0xFFFFFFFF` — indistinguishable
 * from a real four-billion-sample count. Anything landing on a reserved sentinel is reported as
 * saturated instead, which costs the top two representable counts and buys an unambiguous file.
 */
[[nodiscard]] constexpr std::uint32_t saturate(std::uint64_t value) noexcept { return value >= std::uint64_t{kUnsetDeadline} ? kSaturated : static_cast<std::uint32_t>(value); }

/// A worker beyond what the 8-bit field can express. Distinct from every real id, so an overflowed
/// record is recognisable rather than silently attributed to somebody else.
inline constexpr std::uint8_t kWorkerOverflow = 255U;

/**
 * Narrows a runner id into `Event::workerId`, saturating onto `kWorkerOverflow`.
 *
 * A plain cast wraps: worker 256 becomes worker 0 and its records merge with the *busiest* worker's,
 * which is the one a reader is most likely to be studying. Saturating costs the single id 255 and
 * keeps every surviving attribution honest. Unreachable on today's hardware -- and invisible on the
 * day it is not, which is why it is a named function rather than a cast repeated at each marker.
 */
[[nodiscard]] constexpr std::uint8_t workerIdOf(std::size_t runnerId) noexcept { return runnerId >= std::size_t{kWorkerOverflow} ? kWorkerOverflow : static_cast<std::uint8_t>(runnerId); }

/**
 * @brief The marker clock, and why it is this one.
 *
 * `steady_clock`, not `high_resolution_clock` (which is wall-clock and NTP-steppable on libstdc++,
 * cf. `Profiler.hpp`) and not a raw `CLOCK_MONOTONIC` read.
 *
 * Two distinct properties are at stake, and it is worth keeping them apart because only one of them
 * can be checked at run time:
 *
 * - **the marker clock must be the clock the scheduler's deadlines live in**, or
 *   `completion - absoluteDeadline` is off by an unknown constant and every tardiness number is
 *   wrong. `Job::releaseTime` and `Job::absoluteDeadline` are `steady_clock::time_point`
 *   (`SchedulingPolicy.hpp`), so taking markers from the same clock makes this **structural**. It is
 *   asserted below at compile time, and no run-time check is needed or possible;
 * - **the marker clock should also be `CLOCK_MONOTONIC`**, so a trace lines up with `ftrace`,
 *   `perf` and BPF on one timeline. That is an implementation detail of the standard library —
 *   true on glibc and libc++ — and it *is* checkable, by `checkClockDomain()`.
 *
 * The two are easily run together, and are kept apart here: a failed domain check invalidates
 * **kernel correlation**, not tardiness.
 */
using TraceClock = std::chrono::steady_clock;

static_assert(std::is_same_v<TraceClock, std::chrono::steady_clock>, "markers must share the clock Job::absoluteDeadline is stamped in, or tardiness is meaningless");
static_assert(TraceClock::is_steady, "a marker clock that can step backwards makes durations negative");

/// Absolute nanoseconds since the clock's epoch. The `duration_cast` is a no-op wherever the clock
/// already ticks in nanoseconds (glibc, libc++), and correct where it does not.
[[nodiscard]] inline std::uint64_t now() noexcept { return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(TraceClock::now().time_since_epoch()).count()); }

/// Whether `TraceClock` was found to read the same counter as `CLOCK_MONOTONIC`.
enum class ClockDomain : std::uint8_t {
    unchecked,        /// no POSIX `clock_gettime` on this platform -- correlation unverified, not disproved
    matchesMonotonic, /// the two bracket each other; a kernel trace shares this timeline
    differs           /// they do not; merging with `ftrace`/`perf`/BPF would misalign silently
};

/**
 * @brief What one clock read costs, and where this process's timeline sits.
 *
 * Written into the `.gr4trace` header so a report can state execution times net of the
 * instrumentation that measured them, and so a converter can decide whether kernel correlation is
 * sound. Cheap to obtain and obtained once, so it is a value rather than a set of accessors.
 */
struct ClockInfo {
    std::uint64_t clockCostNs{};       /// minimum observed cost of one `now()`, over `kClockCalibrationSamples`
    std::uint64_t steadyAnchorNs{};    /// `now()` at calibration
    std::uint64_t monotonicAnchorNs{}; /// `CLOCK_MONOTONIC` at the same instant; 0 when `unchecked`
    ClockDomain   domain{ClockDomain::unchecked};
};

/// Enough pairs that the minimum is a real floor rather than one lucky sample, and few enough that
/// calibration stays under ~50 us. The **minimum** is the estimate, not the mean, because it is the
/// statistic least polluted by preemption and page faults.
inline constexpr std::size_t kClockCalibrationSamples = 1024UZ;

/// Measures the clock and re-tests the domain, replacing whatever `clockInfo()` held. Idempotent in
/// effect but not in value -- the anchors move to now, which is the point: the anchors belong next
/// to the records, so this is called when a capture *starts*, not when the process does.
ClockInfo calibrateClock() noexcept;

/// The stored calibration, computed on first use if `calibrateClock()` has not been called.
[[nodiscard]] const ClockInfo& clockInfo() noexcept;

/// Difference of two instants, saturating, and never negative — a clock read pair that comes back
/// out of order (a coarse clock, a chained timestamp reused across a boundary) yields 0 rather than
/// a four-billion-nanosecond duration from unsigned wrap-around.
[[nodiscard]] constexpr std::uint32_t durationOf(std::uint64_t startNs, std::uint64_t endNs) noexcept { return saturate(endNs > startNs ? endNs - startNs : std::uint64_t{0}); }

namespace detail {

/**
 * @brief One thread's overwriting ring. Owned by the registry, never by the thread.
 *
 * Pool workers exit before `waitDone()` returns, so a ring destroyed with its thread would take
 * unread records with it. The registry owns both this and the slab; the `thread_local` below is a
 * non-owning pointer and nothing runs at thread exit.
 */
struct Ring {
    Event*        slots    = nullptr;
    std::uint64_t mask     = 0UL; /// capacity - 1; capacity is a power of two, so the modulo is an AND
    std::uint64_t sequence  = 0UL; /// total pushes ever, not an index — the excess over capacity is the loss count
    std::uint64_t createdNs = 0UL; /// when construction finished: no span on this thread may start before it
};

extern std::uint32_t      gCategoryMask;
extern thread_local Ring* tRing;

/// Cold: allocates and registers this thread's ring. Out of line so the hot path carries a null
/// check rather than an allocator.
Ring* createThreadRing() noexcept;

[[nodiscard]] inline Ring* threadRing() noexcept { return tRing != nullptr ? tRing : createThreadRing(); }

/// When this thread's ring finished construction, if it has one. Building a ring costs about a
/// millisecond, and a span open at that moment would absorb it; comparing this against the thread's
/// earliest `startNs` checks that none did, without timing anything.
[[nodiscard]] inline std::optional<std::uint64_t> threadRingCreatedNs() noexcept { return tRing != nullptr ? std::optional{tRing->createdNs} : std::nullopt; }

/// The push itself, with **no category check**. Callers that have already decided to record use this;
/// everything else goes through `emit()`.
inline void append(const Event& event) noexcept {
    Ring* ring = threadRing();
    if (ring == nullptr) [[unlikely]] {
        return; // the slab could not be allocated; losing records beats failing the graph
    }
    ring->slots[ring->sequence & ring->mask] = event;
    ++ring->sequence;
}

} // namespace detail

/// One relaxed load and a predictable branch — the whole cost of a marker whose category is off.
[[nodiscard]] inline bool categoryEnabled(Category category) noexcept {
    if constexpr (!kEnabled) {
        return false;
    } else {
        return (gr::atomic_ref(detail::gCategoryMask).load_relaxed() & std::to_underlying(category)) != 0U;
    }
}

/// Builds this thread's ring now, if any category is live. Construction takes about a millisecond,
/// and whatever span is open when it happens absorbs it, so a caller about to read a span's start
/// calls this first. Under a zero mask it allocates nothing, which is what keeps a tracing-enabled
/// binary free until a capture is asked for.
inline void prepareThread() noexcept {
    if constexpr (kEnabled) {
        if (gr::atomic_ref(detail::gCategoryMask).load_relaxed() != 0U) {
            static_cast<void>(detail::threadRing());
        }
    }
}

/**
 * Appends one record to the calling thread's ring.
 *
 * The ring **overwrites**: it keeps the most recent `capacity` records rather than the first, which
 * is what the dominant workflow wants ("something missed a deadline, show me what led up to it") and
 * what `HistoryLoggerBackend` already does for log records. The push is consequently branchless —
 * there is no fullness test, because there is no full.
 *
 * Loss is therefore never counted here. It is *derived* at read time from `sequence` against the
 * capacity, because a drop in an overwriting ring is discovered by the reader, not by the
 * producer.
 */
inline void emit(Event event) noexcept {
    if constexpr (kEnabled) {
        if (!categoryEnabled(categoryOf(event.kind))) [[likely]] {
            return;
        }
        detail::append(event);
    }
}

/**
 * @brief RAII complete-event marker: stamps a start instant, emits one record when it goes away.
 *
 * A `class` rather than the project's usual `struct` because it carries an invariant worth
 * enforcing — exactly one record per scope, whether it ends by falling out of scope or by an early
 * `finish()`.
 *
 * `finish(endNs)` exists for timestamp chaining: where a caller already holds the end instant,
 * because it is also the *next* marker's start instant, it must not pay for a second clock read. At
 * a measured ~20 ns per read that is half the cost of a complete event.
 */
class Scope {
    Event _event;
    bool  _armed = false;

public:
    /// `event.startNs` is honoured where the caller set it (chaining) and taken now where it is 0.
    explicit Scope(Event event) noexcept : _event(event) {
        if constexpr (kEnabled) {
            if (categoryEnabled(categoryOf(_event.kind))) {
                if (_event.startNs == 0UL) {
                    // Before the start is read: the first scope a thread opens is the outermost one, so
                    // a ring built here lands before every span on the thread rather than inside one. A
                    // caller that supplied the start has read its clock already; building here would
                    // buy it nothing, so it is left to call `prepareThread()` first.
                    static_cast<void>(detail::threadRing());
                    _event.startNs = now();
                }
                _armed = true;
            }
        }
    }

    Scope(const Scope&)            = delete;
    Scope& operator=(const Scope&) = delete;
    Scope(Scope&&)                 = delete;
    Scope& operator=(Scope&&)      = delete;

    ~Scope() { finish(); }

    void finish(std::uint64_t endNs) noexcept {
        if constexpr (kEnabled) {
            if (!_armed) {
                return;
            }
            _armed            = false;
            _event.durationNs = durationOf(_event.startNs, endNs);

            // Deliberately **not** `emit()`. A scope that armed has already had its category
            // decision made, and re-checking the live mask here would let a capture stopped
            // mid-scope discard the enclosing record while the markers nested inside it -- emitted
            // while the mask was still live -- survive. A reader would then see an interval that
            // opened and never closed, which is the shape of a hang. The latch is the decision.
            detail::append(_event);
        }
    }

    /// Reads the clock only when there is a record to stamp, so a disabled scope costs nothing.
    void finish() noexcept {
        if (_armed) {
            finish(now());
        }
    }

    /// Lets a caller fill in counts discovered during the scope — samples processed, a status.
    [[nodiscard]] Event& event() noexcept { return _event; }
};

/// Reads one record. A plain function pointer, mirroring `gr::log::RecordConsumer`: allocation-free,
/// usable from a `-fno-exceptions` translation unit, and no `std::function` on a draining path.
using EventConsumer = void (*)(const Event&, void*) noexcept;

struct RingStats {
    std::uint64_t recorded{}; /// records still held across every ring
    std::uint64_t lost{};     /// records overwritten before anyone read them
    std::size_t   rings{};    /// threads that have emitted at least once
};

/// Default ring: 65536 records, 2 MB per thread. Affordable because a ring is allocated lazily on a
/// thread's first *enabled* emit — a tracing-enabled binary with a zero mask allocates nothing.
inline constexpr std::size_t kDefaultRingCapacity = 65536UZ;

/// The CPU this thread is running on, or -1 where the platform cannot say. Read once per worker at
/// start-up and never on the hot path: it is the thread-to-core mapping a report needs in order to
/// explain why two workers that ought to be independent are not.
/**
 * The calling thread's consumed CPU time in nanoseconds, or nothing where the platform cannot say.
 *
 * Distinct from `now()` in both meaning and cost. `now()` is wall-clock and reads from the vDSO in
 * ~25 ns; this is the time the thread actually spent **on a core**, and on Linux it is a real
 * syscall — hundreds of nanoseconds. It is therefore read exactly **twice per worker thread, ever**,
 * at the ends of the worker loop, and never on a marker path.
 *
 * Empty rather than zero when unavailable: a zero would be indistinguishable from a thread that
 * consumed no CPU, which is the one reading that would mislead a reader into seeing total preemption.
 */
[[nodiscard]] std::optional<std::uint64_t> threadCpuNow() noexcept;

[[nodiscard]] std::int32_t currentCpu() noexcept;

void                        setCategories(std::uint32_t mask) noexcept;
[[nodiscard]] std::uint32_t categories() noexcept;

/**
 * Largest ring a single thread may be given, in bytes.
 *
 * A ceiling, not a default -- the default stays at `kDefaultRingCapacity`, two megabytes. It exists
 * because the capacity setting is a 32-bit record count, so a caller can ask for 128 GiB by typing
 * one large number; the allocation then fails, every record from that thread is dropped for the
 * life of the process, and nothing says so. A capture that silently records nothing is the one
 * outcome this layer must never produce.
 *
 * Per *emitting thread*: a graph on eight workers can commit eight times this. That is why the
 * ceiling is generous rather than tight — it is a guard against a typo, not a memory budget.
 */
inline constexpr std::size_t kDefaultRingCapacityLimitBytes = 4UZ * 1024UZ * 1024UZ * 1024UZ;

/// Raises or lowers the ceiling above. Rounded down to a whole number of records, and never below
/// one record, so a ring always has somewhere to write.
void                      setRingCapacityLimitBytes(std::size_t bytes) noexcept;
[[nodiscard]] std::size_t ringCapacityLimitBytes() noexcept;

/// Takes effect for rings created after the call, so a thread that has already emitted keeps its
/// own. Ignored when `capacity` is 0. Returns the capacity actually adopted: the request rounded
/// **up** to a power of two, then clamped to the ceiling and rounded **down** to a power of two
/// again so the index mask stays valid. Compare it against what you asked for to detect a clamp --
/// the return value is the only way to find out, and ignoring it is how a capture ends up smaller
/// than intended.
[[nodiscard]] std::size_t setRingCapacity(std::size_t capacity) noexcept;
[[nodiscard]] std::size_t ringCapacity() noexcept;

/// Discards every record **and every identity**, without freeing any ring — a ring whose address a
/// live `thread_local` still holds must not be destroyed, and no thread can be made to drop that
/// pointer from here.
///
/// **Defined only while nothing is emitting**, the same precondition `dump()` carries. This rewinds
/// each ring's sequence counter, which emitters advance without synchronisation; calling it under a
/// running graph races them, and leaves slots holding records the counter now claims are unwritten.
/// Park the workers first.
///
/// Dropping identities invalidates any cached `EntityId`, `SchedState::entityId` included. That is
/// deliberate: `reset()` means "start a new capture", and a capture whose records referred to
/// identities from the previous one would be unreadable. Callers that cache ids must re-intern,
/// which `syncSchedStates` does on its own cadence anyway.
void reset() noexcept;

[[nodiscard]] RingStats ringStats() noexcept;

/**
 * Visits every retained record, oldest first **within** each ring, rings in creation order.
 *
 * Deliberately not merged by timestamp: a merge here would cost a sort on a path that may hold
 * megabytes, and the converter has to sort by `startNs` anyway because complete events are appended
 * at end time.
 *
 * Defined at quiescence. Each ring's `sequence` is snapshotted before its records are read, so a
 * concurrent emitter costs at most the oldest record of that ring rather than a corrupt walk — but
 * that is damage limitation, not a licence.
 */
std::size_t forEachEvent(EventConsumer consumer, void* user = nullptr) noexcept;

/**
 * @brief What a trace needs to know about a block, recorded once instead of in every record.
 *
 * Views, not owned strings: the caller passes `BlockModel::uniqueName()` and `typeName()` straight
 * through, and `intern()` copies what it keeps. Nothing here is required to outlive the call.
 */
struct EntityDescription {
    std::string_view uniqueName{};
    std::string_view typeName{};
    std::uint8_t     workerId{};
    std::uint16_t    nInputPorts{};
    std::uint16_t    nOutputPorts{};
};

/**
 * Assigns this key a stable `EntityId`, describing it on first sight only.
 *
 * Keyed on the address rather than on `uniqueName()` — matching `SchedulingAnalysis::perBlock`, and
 * cheaper than hashing a string on a path that runs per block per house-keeping cycle. The id is
 * invariant under `applyStaticOrder`'s permutation of the block list, which a position-derived id
 * would not be.
 *
 * **The description is stored on the first call and ignored afterwards**, which is what keeps the
 * steady state allocation-free: a caller that re-interns a known block every sync pays a hash lookup
 * and nothing else. The cost is that a block re-homed to another worker after adoption keeps its
 * original `workerId` in the metadata — recorded rather than solved, since no marker reads it yet.
 *
 * Returns `kNoEntity` when the key is null or the table is full, and a `kNoEntity` record is still a
 * valid record: it is worker-scoped rather than wrong.
 */
[[nodiscard]] EntityId intern(const void* key, const EntityDescription& description) noexcept;

/// The id this key already has, or `kNoEntity`. Never assigns one, so a marker on a path that must
/// not allocate can ask without consequence.
[[nodiscard]] EntityId internedId(const void* key) noexcept;

/**
 * Drops the key, keeps the identity, and emits one `Kind::entityRetired`.
 *
 * The description survives: records already in a ring still refer to this id and a reader has to be
 * able to resolve them. What goes is the *address* mapping, so that a later block allocated at the
 * same address is given a **new** id rather than inheriting a dead one's statistics.
 */
void retire(const void* key) noexcept;

using EntityConsumer = void (*)(EntityId, const EntityDescription&, void*) noexcept;

/// Visits every identity ever assigned, retired ones included, in assignment order. The views handed
/// to the consumer are valid for the duration of the call only.
std::size_t forEachEntity(EntityConsumer consumer, void* user = nullptr) noexcept;

} // namespace gr::trace

#endif // GNURADIO_TRACE_HPP
