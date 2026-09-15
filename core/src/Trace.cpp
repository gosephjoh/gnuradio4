#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#if !defined(EMBEDDED) && __has_include(<unistd.h>)
#include <unistd.h>
#define GR_TRACE_HAS_GETPID 1
#endif

#if !defined(EMBEDDED)
#if defined(__has_include)
#if __has_include(<time.h>)
#include <time.h>
#if defined(CLOCK_MONOTONIC)
#define GR_TRACE_HAS_CLOCK_GETTIME 1
#endif
#endif
#endif
#endif

namespace gr::trace {

namespace {

/// Cost of one `now()`, as the minimum over `kClockCalibrationSamples` back-to-back pairs.
///
/// Both readings feed the result, so neither call can be elided or merged: `TraceClock::now()` is
/// an opaque call as far as the optimiser is concerned, and the returned minimum keeps every sample
/// live.
///
/// A coarse clock can legitimately report 0 here, when two reads land in the same tick. That is an
/// honest answer and the header records it as such -- a trace taken on such a clock is
/// untrustworthy at `work()` granularity, and this number is how a report finds that out rather
/// than reporting confident nonsense.
[[nodiscard]] std::uint64_t measureClockCost() noexcept {
    std::uint64_t cost = std::numeric_limits<std::uint64_t>::max();
    for (std::size_t sample = 0UZ; sample < kClockCalibrationSamples; ++sample) {
        const std::uint64_t before = now();
        const std::uint64_t after  = now();
        cost                       = std::min(cost, after - before);
    }
    return cost;
}

#if defined(GR_TRACE_HAS_CLOCK_GETTIME)
/// `CLOCK_MONOTONIC` in nanoseconds; `false` when the call failed, which is not the same as zero.
[[nodiscard]] bool monotonicNow(std::uint64_t& outNs) noexcept {
    ::timespec ts{};
    if (::clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return false;
    }
    outNs = static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000UL + static_cast<std::uint64_t>(ts.tv_nsec);
    return true;
}
#endif

/**
 * Decides whether `TraceClock` reads the same counter as `CLOCK_MONOTONIC`, by bracketing.
 *
 * Sandwich one `CLOCK_MONOTONIC` read between two `TraceClock` reads. If the two are the same
 * counter then `before <= monotonic <= after` holds exactly, by monotonicity. If they are different
 * counters the middle reading sits outside the bracket by however much their epochs differ.
 *
 * The asymmetry is what makes this sound rather than heuristic: preemption between the reads can
 * only *widen* `[before, after]`, so a descheduled thread cannot manufacture a false `differs`. The
 * remaining risk runs the other way -- two genuinely distinct clocks whose offset is smaller than
 * one bracket -- which is why it repeats. `CLOCK_MONOTONIC_RAW`, the realistic near-miss, diverges
 * from `CLOCK_MONOTONIC` by NTP slew and fails these brackets comfortably.
 */
[[nodiscard]] ClockDomain checkClockDomain() noexcept {
#if defined(GR_TRACE_HAS_CLOCK_GETTIME)
    constexpr std::size_t kBrackets = 16UZ;
    for (std::size_t attempt = 0UZ; attempt < kBrackets; ++attempt) {
        std::uint64_t       monotonic = 0UL;
        const std::uint64_t before    = now();
        if (!monotonicNow(monotonic)) {
            return ClockDomain::unchecked; // the platform has the call but it failed: claim nothing
        }
        const std::uint64_t after = now();

        if (monotonic < before || monotonic > after) {
            return ClockDomain::differs;
        }
    }
    return ClockDomain::matchesMonotonic;
#else
    return ClockDomain::unchecked;
#endif
}

/// The calibration, replaced wholesale by `calibrateClock()`.
///
/// Unguarded on purpose. Calibration is a start-of-capture action and the readers are the file
/// writer and the report, neither of which runs while a capture is starting -- the same quiescence
/// precondition `dump()` carries. `clockInfo()`'s lazy path is serialised by
/// the function-local static's initialisation guard.
ClockInfo gClockInfo{};

} // namespace

ClockInfo calibrateClock() noexcept {
    ClockInfo info{};
    info.clockCostNs = measureClockCost();
    info.domain      = checkClockDomain();

    // The anchors are taken last and back to back, so they pin the two timelines to each other at
    // one instant. Taking them during the domain check instead would separate them by the whole
    // loop, and a converter aligning a kernel trace would inherit that gap as a constant skew.
    info.steadyAnchorNs = now();
#if defined(GR_TRACE_HAS_CLOCK_GETTIME)
    if (info.domain == ClockDomain::matchesMonotonic) {
        std::uint64_t monotonic = 0UL;
        if (monotonicNow(monotonic)) {
            info.monotonicAnchorNs = monotonic;
        }
    }
#endif

    gClockInfo = info;
    return info;
}

const ClockInfo& clockInfo() noexcept {
    [[maybe_unused]] static const bool calibratedOnce = [] {
        std::ignore = calibrateClock();
        return true;
    }();
    return gClockInfo;
}

namespace {

/// A ring and the slab it spans. Held by `unique_ptr` so the `Ring`'s address is stable: a thread's
/// `tRing` points straight at it and must survive the vector reallocating.
struct OwnedRing {
    std::unique_ptr<Event[]> slab;
    detail::Ring             ring;
};

std::mutex                              gRegistryMutex;
std::vector<std::unique_ptr<OwnedRing>> gRings;
std::size_t                             gRingCapacity = kDefaultRingCapacity;

/// One interned identity. Owns its strings: the views handed to `intern()` belong to a `BlockModel`
/// that may be destroyed long before the trace is written.
struct EntityEntry {
    std::string   uniqueName;
    std::string   typeName;
    std::uint8_t  workerId     = 0U;
    std::uint16_t nInputPorts  = 0U;
    std::uint16_t nOutputPorts = 0U;
};

/// Separate from `gRegistryMutex` on purpose, and not merely for contention. `createThreadRing()`
/// takes the registry lock, so `emit()` can reach it -- and `retire()` emits. One lock for both
/// would make that a self-deadlock the first time a thread retired an entity before it had ever
/// emitted.
std::mutex                                gEntityMutex;
std::unordered_map<const void*, EntityId> gKeyToId;
std::vector<EntityEntry>                  gEntities; /// index is `id - 1`; retired ids stay, keys do not

[[nodiscard]] std::size_t roundUpToPowerOfTwo(std::size_t value) noexcept { return value <= 1UZ ? 1UZ : std::bit_ceil(value); }

/// Records lost from one ring: every push beyond the capacity overwrote something.
[[nodiscard]] std::uint64_t lostFrom(const detail::Ring& ring) noexcept {
    const std::uint64_t capacity = ring.mask + 1UL;
    return ring.sequence > capacity ? ring.sequence - capacity : 0UL;
}

} // namespace

namespace detail {

std::uint32_t      gCategoryMask = 0U;
thread_local Ring* tRing         = nullptr;

Ring* createThreadRing() noexcept {
    const std::size_t capacity = roundUpToPowerOfTwo(gRingCapacity);

    // Nothrow `new` rather than `make_unique`, wrapped immediately: this is library code on a
    // `noexcept` path (CLAUDE.md section 5), and a 2 MB allocation is the realistic failure. Losing
    // a thread's records is the right response to a full heap; aborting the graph is not.
    std::unique_ptr<Event[]> slab{new (std::nothrow) Event[capacity]};
    if (slab == nullptr) {
        return nullptr;
    }

    auto owned = std::unique_ptr<OwnedRing>{new (std::nothrow) OwnedRing{}};
    if (owned == nullptr) {
        return nullptr;
    }
    owned->ring.slots = slab.get();
    owned->ring.mask  = static_cast<std::uint64_t>(capacity) - 1UL;
    owned->slab       = std::move(slab);

    Ring* ring = std::addressof(owned->ring);
    {
        const std::lock_guard lock(gRegistryMutex);
        gRings.push_back(std::move(owned));
    }
    tRing = ring;
    return ring;
}

} // namespace detail

void setCategories(std::uint32_t mask) noexcept {
    const std::uint32_t previous = gr::atomic_ref(detail::gCategoryMask).load_relaxed();
    gr::atomic_ref(detail::gCategoryMask).store_relaxed(mask);

    // A capture starts here, so the clock anchors belong here. Without this they would date from
    // whenever something first *read* `clockInfo()` -- at file-write time, most likely, which is the
    // wrong end of the trace to pin a kernel timeline against.
    if (previous == 0U && mask != 0U) {
        std::ignore = calibrateClock();
    }
}

std::uint32_t categories() noexcept { return gr::atomic_ref(detail::gCategoryMask).load_relaxed(); }

void setRingCapacity(std::size_t capacity) noexcept {
    if (capacity == 0UZ) {
        return;
    }
    const std::lock_guard lock(gRegistryMutex);
    gRingCapacity = roundUpToPowerOfTwo(capacity);
}

std::size_t ringCapacity() noexcept {
    const std::lock_guard lock(gRegistryMutex);
    return gRingCapacity;
}

void reset() noexcept {
    {
        const std::lock_guard lock(gRegistryMutex);
        for (const std::unique_ptr<OwnedRing>& owned : gRings) {
            owned->ring.sequence = 0UL;
        }
    }
    const std::lock_guard entityLock(gEntityMutex);
    gKeyToId.clear();
    gEntities.clear();
}

RingStats ringStats() noexcept {
    const std::lock_guard lock(gRegistryMutex);
    RingStats             stats{};
    stats.rings = gRings.size();
    for (const std::unique_ptr<OwnedRing>& owned : gRings) {
        const detail::Ring& ring     = owned->ring;
        const std::uint64_t capacity = ring.mask + 1UL;
        stats.recorded += std::min(ring.sequence, capacity);
        stats.lost += lostFrom(ring);
    }
    return stats;
}

std::size_t forEachEvent(EventConsumer consumer, void* user) noexcept {
    if (consumer == nullptr) {
        return 0UZ;
    }
    const std::lock_guard lock(gRegistryMutex);

    std::size_t visited = 0UZ;
    for (const std::unique_ptr<OwnedRing>& owned : gRings) {
        const detail::Ring& ring = owned->ring;

        // Snapshotted once, before anything is read. A concurrent emitter then costs at most this
        // ring's oldest record rather than an unbounded walk.
        const std::uint64_t sequence = ring.sequence;
        const std::uint64_t capacity = ring.mask + 1UL;
        const std::uint64_t oldest   = sequence > capacity ? sequence - capacity : 0UL;

        for (std::uint64_t index = oldest; index < sequence; ++index) {
            consumer(ring.slots[index & ring.mask], user);
            ++visited;
        }
    }
    return visited;
}

EntityId intern(const void* key, const EntityDescription& description) noexcept {
    if (key == nullptr) {
        return kNoEntity;
    }
    const std::lock_guard lock(gEntityMutex);

    if (const auto existing = gKeyToId.find(key); existing != gKeyToId.end()) {
        return existing->second; // known: a hash lookup and nothing else, so the steady state never allocates
    }
    if (gEntities.size() >= std::size_t{kMaxEntities}) {
        return kNoEntity;
    }

    gEntities.push_back(EntityEntry{
        .uniqueName   = std::string(description.uniqueName),
        .typeName     = std::string(description.typeName),
        .workerId     = description.workerId,
        .nInputPorts  = description.nInputPorts,
        .nOutputPorts = description.nOutputPorts,
    });
    const auto id = static_cast<EntityId>(gEntities.size());
    gKeyToId.emplace(key, id);
    return id;
}

EntityId internedId(const void* key) noexcept {
    if (key == nullptr) {
        return kNoEntity;
    }
    const std::lock_guard lock(gEntityMutex);
    const auto            existing = gKeyToId.find(key);
    return existing == gKeyToId.end() ? kNoEntity : existing->second;
}

void retire(const void* key) noexcept {
    EntityId id = kNoEntity;
    {
        const std::lock_guard lock(gEntityMutex);
        if (const auto existing = gKeyToId.find(key); existing != gKeyToId.end()) {
            id = existing->second;
            gKeyToId.erase(existing); // the entry stays; only the address mapping goes
        }
    }

    // Emitted outside the lock. `emit()` can reach `createThreadRing()`, which takes the *registry*
    // lock; holding an unrelated lock across that is how lock-order bugs start, and there is no
    // reason to.
    if (id != kNoEntity) {
        emit(Event{.startNs = now(), .entity = id, .kind = Kind::entityRetired});
    }
}

std::size_t forEachEntity(EntityConsumer consumer, void* user) noexcept {
    if (consumer == nullptr) {
        return 0UZ;
    }
    const std::lock_guard lock(gEntityMutex);
    for (std::size_t index = 0UZ; index < gEntities.size(); ++index) {
        const EntityEntry& entry = gEntities[index];
        consumer(static_cast<EntityId>(index + 1UZ), EntityDescription{.uniqueName = entry.uniqueName, .typeName = entry.typeName, .workerId = entry.workerId, .nInputPorts = entry.nInputPorts, .nOutputPorts = entry.nOutputPorts}, user);
    }
    return gEntities.size();
}

namespace {

/// Distinguishes this image's copy of the trace layer from any other. The address of a file-local
/// object is unique per loaded image, which is exactly the distinction `TraceHeader::imageId` has to
/// record: a statically-linked plugin carries its own rings, so two traces must be tellable apart.
const char gImageAnchor = '\0';

template<typename T>
void writeRaw(std::ostream& out, const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    out.write(reinterpret_cast<const char*>(std::addressof(value)), static_cast<std::streamsize>(sizeof(T)));
}

struct EventWriter {
    std::ostream* out   = nullptr;
    std::uint64_t count = 0UL;
};

void writeEvent(const Event& event, void* user) noexcept {
    auto* writer = static_cast<EventWriter*>(user);
    writer->out->write(reinterpret_cast<const char*>(std::addressof(event)), static_cast<std::streamsize>(sizeof(Event)));
    ++writer->count;
}

void writeEntity(EntityId id, const EntityDescription& description, void* user) noexcept {
    auto* out = static_cast<std::ostream*>(user);

    // Truncated rather than refused: a name long enough to overflow a u16 is a pathological block
    // name, and losing its tail beats losing the whole trace.
    const auto uniqueNameBytes = static_cast<std::uint16_t>(std::min(description.uniqueName.size(), std::size_t{0xFFFFU}));
    const auto typeNameBytes   = static_cast<std::uint16_t>(std::min(description.typeName.size(), std::size_t{0xFFFFU}));

    const EntityRecord record{.id = id, .workerId = description.workerId, .reserved = 0U, .nInputPorts = description.nInputPorts, .nOutputPorts = description.nOutputPorts, .uniqueNameBytes = uniqueNameBytes, .typeNameBytes = typeNameBytes};
    writeRaw(*out, record);
    out->write(description.uniqueName.data(), static_cast<std::streamsize>(uniqueNameBytes));
    out->write(description.typeName.data(), static_cast<std::streamsize>(typeNameBytes));
}

[[nodiscard]] std::uint64_t currentProcessId() noexcept {
#if defined(GR_TRACE_HAS_GETPID)
    return static_cast<std::uint64_t>(::getpid());
#else
    return 0UL;
#endif
}

} // namespace

std::expected<std::size_t, gr::Error> dump(std::string_view path) {
    const std::string filePath(path);
    std::ofstream     out(filePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        return std::unexpected(gr::Error(std::format("gr::trace::dump: cannot open '{}' for writing", filePath)));
    }

    const RingStats stats = ringStats();
    const ClockInfo clock = clockInfo();

    FileHeader header{};
    header.magic             = kFileMagic;
    header.formatVersion     = kFormatVersion;
    header.headerBytes       = static_cast<std::uint32_t>(sizeof(FileHeader));
    header.endianMarker      = kEndianMarker;
    header.eventBytes        = static_cast<std::uint32_t>(sizeof(Event));
    header.clockCostNs       = clock.clockCostNs;
    header.steadyAnchorNs    = clock.steadyAnchorNs;
    header.monotonicAnchorNs = clock.monotonicAnchorNs;
    header.clockDomain       = static_cast<std::uint32_t>(std::to_underlying(clock.domain));
    header.categoryMask      = categories();
    header.wallClockNs       = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    header.processId         = currentProcessId();
    header.imageId           = reinterpret_cast<std::uint64_t>(std::addressof(gImageAnchor));
    header.lostCount         = stats.lost;
    header.ringCount         = stats.rings;
    writeRaw(out, header);

    header.entityCount = forEachEntity(writeEntity, std::addressof(out));

    EventWriter writer{.out = std::addressof(out), .count = 0UL};
    std::ignore       = forEachEvent(writeEvent, std::addressof(writer));
    header.eventCount = writer.count;

    // The two counts are only known once written, so the header is patched rather than guessed.
    // Seeking back beats buffering the whole capture -- a full set of rings is megabytes, and a
    // dump that allocated them twice could fail on the memory the trace was measuring.
    out.seekp(0, std::ios::beg);
    writeRaw(out, header);
    out.flush();

    if (!out.good()) {
        return std::unexpected(gr::Error(std::format("gr::trace::dump: write failed for '{}' after {} records", filePath, header.eventCount)));
    }
    return static_cast<std::size_t>(header.eventCount);
}

} // namespace gr::trace
