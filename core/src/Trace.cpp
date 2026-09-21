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

#if !defined(EMBEDDED) && defined(__linux__) && __has_include(<sched.h>)
#include <sched.h>
#define GR_TRACE_HAS_SCHED_GETCPU 1
#endif

#if !defined(EMBEDDED)
#if defined(__has_include)
#if __has_include(<time.h>)
#include <time.h>
#if defined(CLOCK_MONOTONIC)
#define GR_TRACE_HAS_CLOCK_GETTIME 1
#endif
#if defined(CLOCK_THREAD_CPUTIME_ID)
#define GR_TRACE_HAS_THREAD_CPUTIME 1
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
std::size_t                             gRingCapacity      = kDefaultRingCapacity;
std::size_t                             gRingCapacityLimit = kDefaultRingCapacityLimitBytes;

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

/// The ceiling expressed in records, floored to a power of two. Rounding *down* here is what keeps
/// the index mask valid: a clamp to a non-power-of-two capacity would corrupt every subsequent
/// index. Never returns 0, so a ring always has somewhere to write.
[[nodiscard]] std::size_t capacityCeilingRecords(std::size_t limitBytes) noexcept {
    const std::size_t records = limitBytes / sizeof(Event);
    return records <= 1UZ ? 1UZ : std::bit_floor(records);
}

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
    // Read under the lock: `setRingCapacity()` writes it under the same one, and a thread creating
    // its ring while another changes the capacity would otherwise be a data race.
    std::size_t capacity = 0UZ;
    {
        const std::lock_guard lock(gRegistryMutex);
        capacity = gRingCapacity;
    }

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

std::optional<std::uint64_t> threadCpuNow() noexcept {
#if defined(GR_TRACE_HAS_THREAD_CPUTIME)
    ::timespec ts{};
    if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000UL + static_cast<std::uint64_t>(ts.tv_nsec);
#else
    return std::nullopt; // no per-thread CPU clock here; a report must say so rather than assume zero
#endif
}

std::int32_t currentCpu() noexcept {
#if defined(GR_TRACE_HAS_SCHED_GETCPU)
    return ::sched_getcpu();
#else
    return -1; // not knowable here; a report must say "unknown" rather than invent core 0
#endif
}

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

std::size_t setRingCapacity(std::size_t capacity) noexcept {
    const std::lock_guard lock(gRegistryMutex);
    if (capacity == 0UZ) {
        return gRingCapacity;
    }
    gRingCapacity = std::min(roundUpToPowerOfTwo(capacity), capacityCeilingRecords(gRingCapacityLimit));
    return gRingCapacity;
}

void setRingCapacityLimitBytes(std::size_t bytes) noexcept {
    const std::lock_guard lock(gRegistryMutex);
    gRingCapacityLimit = bytes;
    gRingCapacity      = std::min(gRingCapacity, capacityCeilingRecords(gRingCapacityLimit));
}

std::size_t ringCapacityLimitBytes() noexcept {
    const std::lock_guard lock(gRegistryMutex);
    return gRingCapacityLimit;
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

    EntityId displaced = kNoEntity;
    EntityId assigned  = kNoEntity;
    {
        const std::lock_guard lock(gEntityMutex);

        if (const auto existing = gKeyToId.find(key); existing != gKeyToId.end()) {
            // The address is known -- but is it still the same block? `unique_name` is
            // "{type}#{atomic counter}", unique per instance for the life of the process, so a
            // mismatch means this address was recycled by the allocator after the original block was
            // destroyed. Returning the stored id there would hand the new block the old one's
            // identity *and its name*, and every record it emitted would be attributed to a block
            // that no longer exists.
            //
            // Detected here rather than prevented by `retire()`, because prevention would depend on
            // every removal path remembering to call it -- `exchange()`, `removeBlocks()`, zombie
            // cleanup, scheduler destruction -- and missing one leaves the hazard silently. A string
            // comparison on the house-keeping path is the cheaper guarantee, and it allocates nothing.
            if (gEntities[existing->second - 1U].uniqueName == description.uniqueName) {
                return existing->second; // the same block: one hash lookup, one compare, no allocation
            }
            displaced = existing->second;
            gKeyToId.erase(existing);
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
        assigned = static_cast<EntityId>(gEntities.size());
        gKeyToId.emplace(key, assigned);
    }

    // Outside the lock: `emit()` can reach `createThreadRing()`, which takes the *registry* lock,
    // and holding an unrelated lock across that is how lock-order bugs start. The displaced identity keeps its description -- records already in a ring still
    // name it -- so this says when it stopped being reachable, exactly as `retire()` does.
    if (displaced != kNoEntity) {
        emit(Event{.startNs = now(), .entity = displaced, .kind = Kind::entityRetired});
    }
    return assigned;
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
/// object is unique per loaded image, which is exactly the distinction `FileHeader::imageId` has to
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

namespace {

/// Reads one trivially-copyable value, reporting a short read rather than leaving it half-filled.
template<typename T>
[[nodiscard]] bool readRaw(std::istream& in, T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    return static_cast<bool>(in.read(reinterpret_cast<char*>(std::addressof(value)), static_cast<std::streamsize>(sizeof(T))));
}

} // namespace

std::expected<Capture, gr::Error> load(std::string_view path) {
    const std::string filePath(path);
    std::ifstream     in(filePath, std::ios::binary | std::ios::ate);
    if (!in.is_open()) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: cannot open '{}' for reading", filePath)));
    }
    const auto fileBytes = static_cast<std::uint64_t>(in.tellg());
    in.seekg(0, std::ios::beg);

    Capture capture;
    if (fileBytes < sizeof(FileHeader) || !readRaw(in, capture.header)) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' is {} bytes, too short to hold a {}-byte header", filePath, fileBytes, sizeof(FileHeader))));
    }
    const FileHeader& header = capture.header;

    if (header.magic != kFileMagic) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' is not a .gr4trace file", filePath)));
    }
    if (header.endianMarker != kEndianMarker) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' was written on a machine of the other byte order; this reader refuses rather than reinterprets", filePath)));
    }
    if (header.formatVersion != kFormatVersion) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' is format version {}, this build knows {}", filePath, header.formatVersion, kFormatVersion)));
    }
    if (header.eventBytes != sizeof(Event)) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' has {}-byte records, this build's Event is {} -- the layout changed", filePath, header.eventBytes, sizeof(Event))));
    }
    if (header.headerBytes < sizeof(FileHeader)) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' declares a {}-byte header, shorter than the {} this build requires", filePath, header.headerBytes, sizeof(FileHeader))));
    }
    // Bounded from **both** sides. Only the lower bound was checked at first, and the subtraction
    // below is unsigned: a 5 KiB file declaring a 1 GiB header made `afterHeader` 1.8e19, walked
    // through the identity-count guard that exists to stop exactly this, and left an
    // attacker-chosen `eventCount` to be handed to `resize()`. A header cannot be larger than the
    // file that contains it, and saying so is the whole fix.
    if (header.headerBytes > fileBytes) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' declares a {}-byte header but is only {} bytes long", filePath, header.headerBytes, fileBytes)));
    }

    // A longer header is a *later* version that kept this one's prefix, which is what `headerBytes`
    // exists to make survivable. Skip the excess rather than misread the section after it.
    if (header.headerBytes > sizeof(FileHeader)) {
        in.seekg(static_cast<std::streamoff>(header.headerBytes), std::ios::beg);
    }

    // Checked before reserving anything. The counts come from the file, so a corrupt or hostile one
    // could otherwise ask for an allocation of arbitrary size before a single byte is validated.
    const std::uint64_t afterHeader = fileBytes - header.headerBytes;
    if (header.entityCount > afterHeader / sizeof(EntityRecord)) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' claims {} identities, more than its {} remaining bytes can hold", filePath, header.entityCount, afterHeader)));
    }

    capture.entities.reserve(static_cast<std::size_t>(header.entityCount));
    for (std::uint64_t index = 0UL; index < header.entityCount; ++index) {
        EntityRecord record{};
        if (!readRaw(in, record)) {
            return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' ends inside identity {} of {}", filePath, index, header.entityCount)));
        }
        LoadedEntity entity{.id = record.id, .workerId = record.workerId, .nInputPorts = record.nInputPorts, .nOutputPorts = record.nOutputPorts, .uniqueName = std::string(record.uniqueNameBytes, '\0'), .typeName = std::string(record.typeNameBytes, '\0')};
        if (!in.read(entity.uniqueName.data(), static_cast<std::streamsize>(record.uniqueNameBytes)) || !in.read(entity.typeName.data(), static_cast<std::streamsize>(record.typeNameBytes))) {
            return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' ends inside the names of identity {}", filePath, index)));
        }
        capture.entities.push_back(std::move(entity));
    }

    // `tellg()` returns -1 on a stream that has failed, which as an unsigned value is enormous and
    // makes the subtraction below wrap in the same way. Both operands are checked before either is
    // used, so no file offset arithmetic in this function can underflow.
    const auto position = in.tellg();
    if (position < 0 || static_cast<std::uint64_t>(position) > fileBytes) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' places its record section outside the file", filePath)));
    }
    const auto          eventSectionStart = static_cast<std::uint64_t>(position);
    const std::uint64_t remaining         = fileBytes - eventSectionStart;
    if (header.eventCount != remaining / sizeof(Event) || remaining % sizeof(Event) != 0UL) {
        // Exact, not "at least". The event section is the last thing in the file, so its length is
        // fully determined -- and a mismatch means the file was truncated or the header lies, either
        // of which makes every figure drawn from it suspect.
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' claims {} records but its last section is {} bytes, which is {} of them", filePath, header.eventCount, remaining, remaining / sizeof(Event))));
    }

    capture.events.resize(static_cast<std::size_t>(header.eventCount));
    if (header.eventCount > 0UL && !in.read(reinterpret_cast<char*>(capture.events.data()), static_cast<std::streamsize>(remaining))) {
        return std::unexpected(gr::Error(std::format("gr::trace::load: '{}' ends inside its record section", filePath)));
    }
    return capture;
}

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
