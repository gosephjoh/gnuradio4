#include <boost/ut.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <limits>
#include <new>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>

#include <unistd.h>

using namespace boost::ut;

namespace {

/// The on-disk layout, restated independently of the struct so that a field reorder fails here
/// rather than silently changing the `.gr4trace` format. Any change to this table is a format
/// change and must bump `formatVersion` in the file header.
struct FieldOffset {
    std::string_view name;
    std::size_t      offset;
    std::size_t      size;
};

constexpr std::array<FieldOffset, 11> kLayout{{
    {"startNs", 0UZ, 8UZ},
    {"durationNs", 8UZ, 4UZ},
    {"payload0", 12UZ, 4UZ},
    {"payload1", 16UZ, 4UZ},
    {"payload2", 20UZ, 4UZ},
    {"entity", 24UZ, 2UZ},
    {"kind", 26UZ, 1UZ},
    {"workerId", 27UZ, 1UZ},
    {"status", 28UZ, 1UZ},
    {"flags", 29UZ, 1UZ},
    {"reserved", 30UZ, 2UZ},
}};

/// Armed counter over ::operator new, borrowed from qa_Embedded.cpp -- scope-armed so boost::ut's
/// own reporting allocations do not poison the delta.
std::atomic<std::size_t> gNewCount{0UZ};
std::atomic<bool>        gNewArmed{false};

struct AllocationSentinel {
    std::size_t baseline;
    AllocationSentinel() noexcept : baseline(gNewCount.load(std::memory_order_relaxed)) { gNewArmed.store(true, std::memory_order_release); }
    ~AllocationSentinel() { gNewArmed.store(false, std::memory_order_release); }
    AllocationSentinel(const AllocationSentinel&)            = delete;
    AllocationSentinel& operator=(const AllocationSentinel&) = delete;

    [[nodiscard]] std::size_t delta() const noexcept { return gNewCount.load(std::memory_order_relaxed) - baseline; }
};

/// Collects into a vector through the plain-function-pointer consumer. `user` carries the sink, so
/// nothing here needs `std::function` or a capturing lambda.
void collectingConsumer(const gr::trace::Event& event, void* user) noexcept { static_cast<std::vector<gr::trace::Event>*>(user)->push_back(event); }

void countingConsumer(const gr::trace::Event&, void* user) noexcept { ++*static_cast<std::size_t*>(user); }

std::size_t gCounter = 0UZ;

[[nodiscard]] std::vector<gr::trace::Event> collect() {
    std::vector<gr::trace::Event> events;
    std::ignore = gr::trace::forEachEvent(collectingConsumer, &events);
    return events;
}

} // namespace

void* operator new(std::size_t size) {
    if (gNewArmed.load(std::memory_order_acquire)) {
        gNewCount.fetch_add(1UZ, std::memory_order_relaxed);
    }
    void* pointer = std::malloc(size == 0UZ ? 1UZ : size);
    if (pointer == nullptr) {
        throw std::bad_alloc{};
    }
    return pointer;
}

void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }

const boost::ut::suite<"Trace"> traceTests = [] {
    using namespace gr::trace;

    "the event record is a 32-byte trivially-copyable POD"_test = [] {
        expect(eq(sizeof(Event), 32UZ));
        expect(std::has_single_bit(sizeof(Event))) << "CircularBuffer requires a power-of-two element size";
        expect(eq(alignof(Event), 8UZ));
        expect(std::is_trivially_copyable_v<Event>);
        expect(std::is_standard_layout_v<Event>);
    };

    "a default-constructed event is all zeroes, so `reserved` is never garbage"_test = [] {
        const Event                          event{};
        std::array<std::byte, sizeof(Event)> bytes{};
        std::memcpy(bytes.data(), &event, sizeof(Event));
        expect(std::ranges::all_of(bytes, [](std::byte b) { return b == std::byte{0}; })) << "the writer memcpys the record whole; a non-zero pad byte would leak into the file";
    };

    "field offsets match the on-disk layout"_test = [] {
        expect(eq(offsetof(Event, startNs), kLayout[0].offset));
        expect(eq(offsetof(Event, durationNs), kLayout[1].offset));
        expect(eq(offsetof(Event, payload0), kLayout[2].offset));
        expect(eq(offsetof(Event, payload1), kLayout[3].offset));
        expect(eq(offsetof(Event, payload2), kLayout[4].offset));
        expect(eq(offsetof(Event, entity), kLayout[5].offset));
        expect(eq(offsetof(Event, kind), kLayout[6].offset));
        expect(eq(offsetof(Event, workerId), kLayout[7].offset));
        expect(eq(offsetof(Event, status), kLayout[8].offset));
        expect(eq(offsetof(Event, flags), kLayout[9].offset));
        expect(eq(offsetof(Event, reserved), kLayout[10].offset));

        std::size_t covered = 0UZ;
        for (const FieldOffset& field : kLayout) {
            covered += field.size;
        }
        expect(eq(covered, sizeof(Event))) << "the layout table leaves no padding unaccounted for";
    };

    "saturating narrowing never collides with a sentinel"_test = [] {
        expect(eq(saturate(0UL), 0U));
        expect(eq(saturate(1024UL), 1024U));
        expect(eq(saturate(std::uint64_t{kUnsetDeadline} - 1UL), kUnsetDeadline - 1U)) << "the largest representable count passes through";

        // The two that matter: an unbounded ceiling must not arrive looking like a real count.
        expect(eq(saturate(std::numeric_limits<std::uint64_t>::max()), kSaturated)) << "kUnboundedBatch / max_work_items default";
        expect(eq(saturate(std::uint64_t{0xFFFFFFFFUL}), kSaturated)) << "a real 4 G count is reported saturated, not as kSaturated-by-truncation";
        expect(eq(saturate(std::uint64_t{kUnsetDeadline}), kSaturated)) << "and never as the unset-deadline sentinel";
    };

    "the sentinels are distinct and sit at the top of the payload range"_test = [] {
        expect(neq(kUnsetDeadline, kSaturated));
        expect(eq(kSaturated, std::numeric_limits<std::uint32_t>::max()));
        expect(eq(kUnsetDeadline, kSaturated - 1U));
    };

    "category masks fold to exactly the defined bits"_test = [] {
        expect(eq(categoryMask(), 0U)) << "no categories is the shipped default";
        expect(eq(categoryMask(Category::work), 1U << 2));
        expect(eq(categoryMask(Category::work, Category::schedulerLoop), (1U << 2) | (1U << 3)));
        expect(eq(categoryMask(Category::work, Category::work), 1U << 2)) << "folding is idempotent";
        expect(eq(kAllCategories, 0xFFU)) << "eight categories, contiguous from bit 0";
        expect(eq(std::popcount(kAllCategories), 8)) << "every enumerator occupies a distinct bit";
    };

    "entity ids leave room for the no-entity sentinel"_test = [] {
        expect(eq(kNoEntity, EntityId{0}));
        expect(eq(std::uint32_t{kMaxEntities} + 1U, std::uint32_t{std::numeric_limits<EntityId>::max()})) << "ids run [1, kMaxEntities]; the top value stays free";
    };

    "loop kinds fit the two bits reserved for them"_test = [] {
        for (const LoopKind loopKind : {LoopKind::roundRobin, LoopKind::fixedPriority, LoopKind::jobDriven}) {
            expect(le(std::to_underlying(loopKind), flag::kLoopKindMask)) << "a loop kind must not spill into the neighbouring flag bits";
        }
        expect(eq(flag::kLoopKindMask & flag::kIsSource, 0)) << "`Kind::workEnd`'s own flags clear the loop-kind field";
        expect(eq(flag::kLoopKindMask & flag::kJobBacked, 0));
    };

    "flag bits do not collide within the kind that reads them"_test = [] {
        // The flags byte is discriminated by `Kind`, so bits legitimately repeat *across* groups.
        // What must never repeat is a bit within one group.
        const auto disjoint = [](std::initializer_list<std::uint8_t> bits) {
            std::uint8_t seen = 0U;
            for (const std::uint8_t bit : bits) {
                if ((seen & bit) != 0U) {
                    return false;
                }
                seen = static_cast<std::uint8_t>(seen | bit);
            }
            return true;
        };

        expect(disjoint({flag::kLoopKindMask, flag::kIsSource, flag::kJobBacked})) << "Kind::workEnd";
        expect(disjoint({flag::kBoundHit, flag::kViaStep})) << "Kind::sweep";
        expect(disjoint({flag::kDidAdopt, flag::kDidRemove, flag::kDidReap, flag::kDidHouseKeep, flag::kDidStateSync})) << "Kind::messagePhase";
        expect(disjoint({flag::kViaSuccessorWalk, flag::kEosWaived})) << "Kind::jobRelease";
        expect(disjoint({flag::kViaHeap, flag::kStaleEntrySkipped})) << "Kind::select";
        expect(disjoint({flag::kDeadlineMissed, flag::kDeadlineSuspect})) << "Kind::deadlineMiss";
    };

    "the marker clock is the clock the scheduler stamps deadlines in"_test = [] {
        // The property that makes tardiness meaningful is structural, not measured: `Job::releaseTime`
        // and `Job::absoluteDeadline` are `steady_clock::time_point`, and so is `TraceClock`. Asserted
        // here as a test too, because a future "let us use the TSC for speed" change would compile
        // fine and silently invalidate every tardiness number in every report.
        expect(constant<std::is_same_v<TraceClock, std::chrono::steady_clock>>);
        expect(constant<TraceClock::is_steady>);
    };

    "now() is non-decreasing and advances"_test = [] {
        const std::uint64_t first  = now();
        const std::uint64_t second = now();
        expect(ge(second, first)) << "a marker clock that steps backwards makes durations negative";

        // Advancing at all is a separate property from not going backwards: a stopped clock
        // satisfies the first and is useless. Spin rather than sleep (CLAUDE.md section 7).
        std::uint64_t later = now();
        for (std::size_t guard = 0UZ; guard < 1'000'000UZ && later == first; ++guard) {
            later = now();
        }
        expect(gt(later, first)) << "the clock never advanced over a million reads";
    };

    "calibration reports a plausible clock cost"_test = [] {
        const ClockInfo info = calibrateClock();

        // Asserted as a property, not a value: the number differs by machine and by clock source,
        // and pinning it would make this a flaky benchmark rather than a test.
        expect(lt(info.clockCostNs, 10'000UL)) << "a vDSO clock read costing over 10 us means it is not a vDSO read";
        expect(gt(info.steadyAnchorNs, 0UL)) << "the anchor is an absolute instant, not a delta";
    };

    "the clock domain is checked, and does not differ"_test = [] {
        const ClockInfo info = calibrateClock();

        // `unchecked` is acceptable -- a platform without `clock_gettime` proves nothing either way.
        // `differs` is a real finding: it means a kernel trace cannot be merged onto this timeline,
        // and the converter must refuse rather than misalign silently.
        expect(info.domain != ClockDomain::differs) << "steady_clock and CLOCK_MONOTONIC are different counters on this platform";

        if (info.domain == ClockDomain::matchesMonotonic) {
            expect(gt(info.monotonicAnchorNs, 0UL)) << "a matching domain must record its anchor pair";
            // The anchors are taken back to back, so they agree to within a clock read or two. This
            // is the property a converter relies on to place kernel events on our timeline.
            const std::uint64_t skew = info.steadyAnchorNs > info.monotonicAnchorNs ? info.steadyAnchorNs - info.monotonicAnchorNs : info.monotonicAnchorNs - info.steadyAnchorNs;
            expect(lt(skew, 1'000'000UL)) << "the anchor pair is separated by more than a millisecond, so it pins nothing";
        } else {
            expect(eq(info.monotonicAnchorNs, 0UL)) << "no anchor may be claimed when the domain was not established";
        }
    };

    "clockInfo() calibrates on first use and then reports what calibrateClock() stored"_test = [] {
        const ClockInfo lazy = clockInfo();
        expect(gt(lazy.steadyAnchorNs, 0UL)) << "first use must calibrate rather than return a zeroed struct";

        const ClockInfo forced = calibrateClock();
        expect(eq(clockInfo().steadyAnchorNs, forced.steadyAnchorNs)) << "an explicit calibration replaces the stored one";
        expect(ge(forced.steadyAnchorNs, lazy.steadyAnchorNs)) << "and its anchor is the later instant";
    };

    "every kind maps to exactly one category, and the table is complete"_test = [] {
        // The guard that matters: an enumerator added without a category entry would index past the
        // table. `kKindCount` is static_asserted against the last enumerator, and this walks it.
        std::uint32_t seen = 0U;
        for (std::size_t raw = 0UZ; raw < kKindCount; ++raw) {
            seen |= std::to_underlying(categoryOf(static_cast<Kind>(raw)));
        }
        expect(eq(seen, kAllCategories)) << "some category has no kind, or some kind has the wrong one";
        expect(eq(std::to_underlying(categoryOf(Kind::workEnd)), std::to_underlying(Category::work)));
        expect(eq(std::to_underlying(categoryOf(Kind::deadlineMiss)), std::to_underlying(Category::deadline)));
        expect(eq(std::to_underlying(categoryOf(Kind::heapFallback)), std::to_underlying(Category::select)));
    };

    "durations never wrap when the clock pair comes back out of order"_test = [] {
        expect(eq(durationOf(100UL, 350UL), 250U));
        expect(eq(durationOf(100UL, 100UL), 0U));
        expect(eq(durationOf(350UL, 100UL), 0U)) << "an inverted pair must yield 0, not four billion nanoseconds";
        expect(eq(durationOf(0UL, std::uint64_t{kUnsetDeadline} + 5UL), kSaturated));
    };

    if constexpr (!kEnabled) {
        "a compiled-out build records nothing at all"_test = [] {
            setCategories(kAllCategories);
            emit(Event{.kind = Kind::workEnd});
            expect(eq(forEachEvent(countingConsumer, &gCounter), 0UZ)) << "GR_ENABLE_TRACING is off; emit must be a no-op";
            expect(eq(ringStats().rings, 0UZ)) << "and no ring may be allocated";
        };

        "a compiled-out build still dumps a valid, empty file"_test = [] {
            // A tool pointed at a build with tracing off must get a well-formed file saying "nothing
            // was captured", not a missing file, an error, or a truncated one. Otherwise the tool has
            // to special-case a configuration it cannot detect from the outside.
            const std::filesystem::path file    = std::filesystem::temp_directory_path() / std::format("qa_Trace_off_{}.gr4trace", ::getpid());
            const auto                  written = dump(file.string());
            expect(written.has_value() >> fatal) << "tracing being compiled out is not a dump failure";
            expect(eq(written.value(), 0UZ));
            expect(eq(std::filesystem::file_size(file), sizeof(FileHeader))) << "a header and nothing else";

            std::ifstream in(file, std::ios::binary);
            FileHeader    header{};
            in.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
            expect(eq(std::string_view(header.magic.data(), 8UZ), std::string_view("GR4TRACE")));
            expect(eq(header.eventCount, 0UL));
            expect(eq(header.entityCount, 0UL));
            expect(eq(header.ringCount, 0UL));
            expect(eq(header.lostCount, 0UL)) << "nothing was captured, so nothing was lost -- the two must not be conflated";
            expect(eq(header.eventBytes, static_cast<std::uint32_t>(sizeof(Event)))) << "the layout must still be declared, so a reader can reject a mismatched build";

            // `load()` is compiled into this build too, and `gr4-trace` will be built from it. A
            // reader tested only where tracing is enabled is a reader untested for half the binaries
            // that contain it.
            const auto loaded = load(file.string());
            expect(loaded.has_value() >> fatal) << "the reader must work in a build that captures nothing";
            expect(eq(loaded->events.size(), 0UZ));
            expect(eq(loaded->header.eventBytes, static_cast<std::uint32_t>(sizeof(Event))));

            expect(!load((file.string() + ".does-not-exist")).has_value()) << "and must refuse a path that is not there";

            std::filesystem::remove(file);
        };
        return;
    }

    "a masked-off category emits nothing, an enabled one emits exactly one record"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        emit(Event{.kind = Kind::sweep});        // Category::schedulerLoop -- masked off
        emit(Event{.kind = Kind::deadlineMiss}); // Category::deadline      -- masked off
        expect(eq(collect().size(), 0UZ)) << "records of a disabled category must not reach the ring";

        emit(Event{.payload0 = 7U, .kind = Kind::workEnd});
        const std::vector<Event> recorded = collect();
        expect(eq(recorded.size(), 1UZ) >> fatal);
        expect(eq(recorded.front().payload0, 7U));
        expect(eq(std::to_underlying(recorded.front().kind), std::to_underlying(Kind::workEnd)));

        setCategories(0U);
    };

    "the ring overwrites, keeping the most recent records and counting what it dropped"_test = [] {
        constexpr std::size_t kCapacity = 8UZ;
        constexpr std::size_t kExcess   = 3UZ;

        reset();
        std::ignore = setRingCapacity(kCapacity);
        setCategories(categoryMask(Category::work));

        // A fresh thread so the ring is created at the capacity just set: an existing ring keeps its
        // own, which is the documented behaviour of setRingCapacity.
        std::thread emitter([] {
            for (std::uint32_t i = 0U; i < static_cast<std::uint32_t>(kCapacity + kExcess); ++i) {
                emit(Event{.payload0 = i, .kind = Kind::workEnd});
            }
        });
        emitter.join();

        const std::vector<Event> recorded = collect();
        expect(eq(recorded.size(), kCapacity)) << "a ring holds its capacity, no more";

        // The *last* capacity records, not the first -- this is the whole point of overwriting.
        for (std::size_t i = 0UZ; i < kCapacity; ++i) {
            expect(eq(recorded[i].payload0, static_cast<std::uint32_t>(kExcess + i))) << "records must be the most recent, oldest first";
        }

        const RingStats stats = ringStats();
        expect(eq(stats.recorded, std::uint64_t{kCapacity}));
        expect(eq(stats.lost, std::uint64_t{kExcess})) << "loss is derived from the sequence, not counted on the hot path";

        setCategories(0U);
        std::ignore = setRingCapacity(kDefaultRingCapacity);
    };

    "each thread gets its own ring and records never interleave"_test = [] {
        constexpr std::uint32_t kThreads   = 4U;
        constexpr std::uint32_t kPerThread = 64U;

        reset();
        std::ignore = setRingCapacity(1024UZ);
        setCategories(categoryMask(Category::work));

        std::vector<std::thread> emitters;
        for (std::uint32_t worker = 0U; worker < kThreads; ++worker) {
            emitters.emplace_back([worker] {
                for (std::uint32_t i = 0U; i < kPerThread; ++i) {
                    emit(Event{.payload0 = i, .kind = Kind::workEnd, .workerId = static_cast<std::uint8_t>(worker)});
                }
            });
        }
        for (std::thread& emitter : emitters) {
            emitter.join();
        }

        const std::vector<Event> recorded = collect();
        expect(eq(recorded.size(), std::size_t{kThreads} * kPerThread)) << "every record from every thread must survive";
        expect(eq(ringStats().lost, 0UL));

        // Per ring, oldest first: each worker's records must appear in its own emission order, and a
        // worker's counter must run 0..kPerThread-1 exactly once. Torn or shared rings break both.
        std::array<std::uint32_t, kThreads> nextExpected{};
        std::array<std::uint32_t, kThreads> counts{};
        for (const Event& event : recorded) {
            const std::uint8_t worker = event.workerId;
            expect(lt(worker, kThreads)) << "a record carries a worker id nobody emitted";
            expect(eq(event.payload0, nextExpected[worker])) << "records within a thread arrived out of order";
            ++nextExpected[worker];
            ++counts[worker];
        }
        for (const std::uint32_t count : counts) {
            expect(eq(count, kPerThread));
        }

        setCategories(0U);
        std::ignore = setRingCapacity(kDefaultRingCapacity);
    };

    "a capacity request is rounded up, and reported back"_test = [] {
        expect(eq(setRingCapacity(1000UZ), 1024UZ)) << "a non-power-of-two request rounds up to the next power of two";
        expect(eq(ringCapacity(), 1024UZ)) << "the return value is what was actually stored";
        expect(eq(setRingCapacity(1024UZ), 1024UZ)) << "an exact power of two is taken as given";
        expect(eq(setRingCapacity(0UZ), 1024UZ)) << "zero is ignored, and the standing capacity is reported unchanged";
        expect(eq(setRingCapacity(1UZ), 1UZ)) << "one record is the smallest ring, never zero";

        std::ignore = setRingCapacity(kDefaultRingCapacity);
    };

    "a capacity above the ceiling is clamped, and the caller can tell"_test = [] {
        expect(eq(ringCapacityLimitBytes(), kDefaultRingCapacityLimitBytes)) << "the shipped ceiling is what the header documents";

        constexpr std::size_t kCeilingRecords = kDefaultRingCapacityLimitBytes / sizeof(Event);
        constexpr std::size_t kTypo           = 1UZ << 40U; // a number a user could plausibly type by mistake

        const std::size_t adopted = setRingCapacity(kTypo);
        expect(lt(adopted, kTypo)) << "a request past the ceiling must not be granted";
        expect(eq(adopted, kCeilingRecords)) << "it is granted the ceiling instead";
        expect(eq(adopted & (adopted - 1UZ), 0UZ)) << "a clamped capacity stays a power of two, or the index mask is invalid";
        expect(eq(ringCapacity(), adopted));

        std::ignore = setRingCapacity(kDefaultRingCapacity);
    };

    "lowering the ceiling shrinks the standing capacity, and a new ring honours it"_test = [] {
        constexpr std::size_t   kCeilingRecords = 8UZ;
        constexpr std::uint32_t kEmitted        = 2U * static_cast<std::uint32_t>(kCeilingRecords);

        reset();
        std::ignore = setRingCapacity(4096UZ);
        setRingCapacityLimitBytes(kCeilingRecords * sizeof(Event));
        expect(eq(ringCapacity(), kCeilingRecords)) << "a standing capacity above a newly lowered ceiling comes down with it";

        // The bookkeeping above proves little on its own. What matters is that a ring *allocated*
        // after the clamp is the clamped size, so a thread emitting twice the ceiling must lose half.
        setCategories(categoryMask(Category::work));
        std::thread emitter([] {
            for (std::uint32_t i = 0U; i < kEmitted; ++i) {
                emit(Event{.payload0 = i, .kind = Kind::workEnd});
            }
        });
        emitter.join();

        expect(eq(collect().size(), kCeilingRecords)) << "the clamp reached the allocation, not just the bookkeeping";
        expect(eq(ringStats().lost, std::uint64_t{kCeilingRecords}));

        setCategories(0U);
        setRingCapacityLimitBytes(kDefaultRingCapacityLimitBytes);
        std::ignore = setRingCapacity(kDefaultRingCapacity);
        reset();
    };

    "a scope emits one complete record, however it ends"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        {
            Scope scope{Event{.startNs = 1'000UL, .kind = Kind::workEnd}};
            scope.event().payload1 = 42U; // counts discovered during the scope
            scope.finish(1'250UL);        // explicit end instant: the timestamp-chaining path
            scope.finish(9'999UL);        // a second finish must not emit a second record
        }
        std::vector<Event> recorded = collect();
        expect(eq(recorded.size(), 1UZ) >> fatal) << "exactly one record per scope";
        expect(eq(recorded.front().durationNs, 250U)) << "the explicit end instant must be used";
        expect(eq(recorded.front().payload1, 42U));

        reset();
        {
            Scope scope{Event{.kind = Kind::workEnd}};
            expect(gt(scope.event().startNs, 0UL)) << "an unset start is stamped at construction";
        } // emitted by the destructor
        recorded = collect();
        expect(eq(recorded.size(), 1UZ)) << "falling out of scope emits too";

        setCategories(0U);
    };

    "an armed scope finishes even if the capture is stopped underneath it"_test = [] {
        reset();
        setCategories(categoryMask(Category::schedulerLoop));

        // The markers nested inside a scope are emitted while the mask is live and survive a stop.
        // If the enclosing scope re-checked the mask on the way out it would be the one record
        // dropped, leaving an interval that opened and never closed -- the shape of a hang, invented
        // by the tracer rather than observed in the scheduler.
        {
            Scope scope{Event{.kind = Kind::sweep}};
            expect(neq(scope.event().startNs, 0UL)) << "the scope armed, so it stamped a start";
            setCategories(0U);
        }

        const std::vector<Event> recorded = collect();
        expect(eq(recorded.size(), 1UZ) >> fatal) << "an armed scope owes exactly one record, whatever the mask now says";
        expect(recorded.front().kind == Kind::sweep);

        reset();
    };

    "a scope that never armed stays silent even if the capture starts underneath it"_test = [] {
        reset();
        setCategories(0U);
        {
            Scope scope{Event{.kind = Kind::sweep}};
            setCategories(categoryMask(Category::schedulerLoop));
        }
        expect(eq(collect().size(), 0UZ)) << "a record with no start instant is worse than no record; the latch cuts both ways";

        setCategories(0U);
        reset();
    };

    "a worker id past the field saturates rather than wrapping onto another worker"_test = [] {
        // Widened to 32 bits purely so a failure prints a number rather than a control character.
        const auto id = [](std::size_t runner) { return std::uint32_t{workerIdOf(runner)}; };

        expect(eq(id(0UZ), 0U));
        expect(eq(id(254UZ), 254U)) << "the last id the field can express is passed through";
        expect(eq(id(255UZ), std::uint32_t{kWorkerOverflow}));
        expect(eq(id(256UZ), std::uint32_t{kWorkerOverflow})) << "a plain cast would make this worker 0 and merge it with the busiest one";
        expect(eq(id(std::numeric_limits<std::size_t>::max()), std::uint32_t{kWorkerOverflow}));

        // The property that matters is not the value but the collision: no overflowed worker may be
        // mistaken for a real one, and worker 0 is the one it would otherwise land on.
        for (const std::size_t runner : {255UZ, 256UZ, 1000UZ, 1UZ << 20U}) {
            expect(neq(id(runner), id(0UZ))) << "an overflowed id must never read as worker 0";
        }
    };

    "a scope whose category is off costs nothing and emits nothing"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));
        {
            Scope scope{Event{.kind = Kind::sweep}}; // schedulerLoop, masked off
            expect(eq(scope.event().startNs, 0UL)) << "a disarmed scope must not even read the clock";
        }
        expect(eq(collect().size(), 0UZ));
        setCategories(0U);
    };

    "turning tracing on calibrates the clock, so the anchors sit with the records"_test = [] {
        setCategories(0U);
        const std::uint64_t before = clockInfo().steadyAnchorNs;

        setCategories(categoryMask(Category::work)); // the off -> on transition
        expect(ge(clockInfo().steadyAnchorNs, before)) << "starting a capture must re-anchor the clock";

        const std::uint64_t anchored = clockInfo().steadyAnchorNs;
        setCategories(categoryMask(Category::work, Category::select)); // on -> on, not a new capture
        expect(eq(clockInfo().steadyAnchorNs, anchored)) << "widening a live capture must not move its anchors";

        setCategories(0U);
    };

    "an identity is stable per key and distinct between keys"_test = [] {
        reset();
        int alpha = 0;
        int beta  = 0;

        const EntityDescription alphaDescription{.uniqueName = "alpha", .typeName = "Src<float>", .workerId = 1U, .nInputPorts = 0U, .nOutputPorts = 1U};

        const EntityId first  = intern(&alpha, alphaDescription);
        const EntityId again  = intern(&alpha, alphaDescription);
        const EntityId second = intern(&beta, EntityDescription{.uniqueName = "beta", .typeName = "Sink<float>", .workerId = 1U, .nInputPorts = 1U, .nOutputPorts = 0U});

        expect(neq(first, kNoEntity)) << "ids run from 1; 0 is the worker-scoped sentinel";
        expect(eq(again, first)) << "the same block at the same address must always return the same id";
        expect(neq(second, first)) << "distinct keys must never collide";
        expect(eq(internedId(&alpha), first)) << "a lookup must not assign";

        int untouched = 0;
        expect(eq(internedId(&untouched), kNoEntity)) << "internedId must never assign an id";
        expect(eq(intern(nullptr, EntityDescription{}), kNoEntity));
    };

    "a recycled address is given a new identity, not the dead block's"_test = [] {
        // Found by qa_TraceScheduler, not by reading: destroying one graph and building another let
        // the allocator hand a new block a dead block's address, and interning returned the dead
        // block's id *and its name*. Every record the new block emitted would have been attributed
        // to a block that no longer existed.
        //
        // `unique_name` is "{type}#{atomic counter}", unique per instance for the life of the
        // process, so the mismatch is detectable at intern time -- which is a stronger guarantee than
        // relying on every removal path remembering to call retire().
        reset();
        setCategories(categoryMask(Category::lifecycle));

        int            address  = 0;
        const EntityId original = intern(&address, EntityDescription{.uniqueName = "Copy<float>#7", .typeName = "Copy<float>"});
        const EntityId recycled = intern(&address, EntityDescription{.uniqueName = "Copy<float>#42", .typeName = "Copy<float>"});

        expect(neq(recycled, original)) << "a different block at a recycled address must not inherit the old identity";
        expect(eq(internedId(&address), recycled)) << "the address now resolves to the live block";

        // The displaced identity stays readable -- records already in a ring still name it -- and its
        // departure is recorded, exactly as retire() does.
        std::size_t entities = 0UZ;
        std::ignore          = forEachEntity([](EntityId, const EntityDescription&, void* user) noexcept { ++*static_cast<std::size_t*>(user); }, &entities);
        expect(eq(entities, 2UZ)) << "the displaced identity must survive for the records that name it";

        const std::vector<Event> recorded = collect();
        expect(eq(recorded.size(), 1UZ) >> fatal) << "displacing an identity emits exactly one record";
        expect(eq(std::to_underlying(recorded.front().kind), std::to_underlying(Kind::entityRetired)));
        expect(eq(recorded.front().entity, original)) << "and it names the identity that went away";

        setCategories(0U);
    };

    "the description is kept from the first sight and not rewritten"_test = [] {
        // Only the *name* decides identity. Everything else in the description -- worker, port
        // counts -- is stored once and left alone, which is what keeps the house-keeping path
        // allocation-free: a block re-interned every re-sync must not copy two strings each time.
        reset();
        int                    block = 0;
        const std::string_view name  = "Copy<float>#3";
        std::ignore                  = intern(&block, EntityDescription{.uniqueName = name, .typeName = "First", .workerId = 3U, .nInputPorts = 2U, .nOutputPorts = 1U});
        std::ignore                  = intern(&block, EntityDescription{.uniqueName = name, .typeName = "Second", .workerId = 9U});

        std::vector<std::tuple<EntityId, std::string, std::uint8_t>> seen;
        std::ignore = forEachEntity(
            [](EntityId id, const EntityDescription& description, void* user) noexcept { //
                static_cast<std::vector<std::tuple<EntityId, std::string, std::uint8_t>>*>(user)->emplace_back(id, std::string(description.typeName), description.workerId);
            },
            &seen);

        expect(eq(seen.size(), 1UZ)) << "the same name at the same address is the same block";
        expect(eq(std::get<1>(seen.front()), std::string("First"))) << "re-interning must not rewrite the description";
        expect(eq(std::get<2>(seen.front()), std::uint8_t{3U})) << "a block re-homed after adoption keeps its original workerId -- recorded, not fixed";
    };

    "descriptions outlive the strings they were built from"_test = [] {
        reset();
        int block = 0;
        {
            const std::string transient = "gone-by-now";
            std::ignore                 = intern(&block, EntityDescription{.uniqueName = transient, .typeName = "T"});
        }
        std::string recovered;
        std::ignore = forEachEntity([](EntityId, const EntityDescription& description, void* user) noexcept { *static_cast<std::string*>(user) = std::string(description.uniqueName); }, &recovered);
        expect(eq(recovered, std::string("gone-by-now"))) << "intern must copy, not borrow -- a BlockModel dies long before the trace is written";
    };

    "retiring keeps the identity, drops the key, and records the fact"_test = [] {
        reset();
        setCategories(categoryMask(Category::lifecycle));

        int            block = 0;
        const EntityId id    = intern(&block, EntityDescription{.uniqueName = "doomed", .typeName = "T"});
        retire(&block);

        expect(eq(internedId(&block), kNoEntity)) << "the address mapping must be gone";

        const std::vector<Event> recorded = collect();
        expect(eq(recorded.size(), 1UZ) >> fatal) << "retiring emits exactly one record";
        expect(eq(std::to_underlying(recorded.front().kind), std::to_underlying(Kind::entityRetired)));
        expect(eq(recorded.front().entity, id)) << "the record names the identity that went away";

        // A block allocated at the same address must not inherit the dead one's statistics. This is
        // the whole bound on the pointer-reuse hazard.
        const EntityId reused = intern(&block, EntityDescription{.uniqueName = "successor", .typeName = "T"});
        expect(neq(reused, id)) << "a reused address must be given a new identity";

        // ... and the retired description survives, because records still refer to it.
        std::size_t count = 0UZ;
        std::ignore       = forEachEntity([](EntityId, const EntityDescription&, void* user) noexcept { ++*static_cast<std::size_t*>(user); }, &count);
        expect(eq(count, 2UZ)) << "a retired identity stays readable; only its key is released";

        retire(&block);
        setCategories(0U);
    };

    "retiring an unknown key is silent"_test = [] {
        reset();
        setCategories(categoryMask(Category::lifecycle));
        int stranger = 0;
        retire(&stranger);
        expect(eq(collect().size(), 0UZ)) << "no identity, no record";
        setCategories(0U);
    };

    "interning saturates rather than wrapping past kMaxEntities"_test = [] {
        reset();
        bool allAssigned = true;
        for (std::size_t i = 1UZ; i <= std::size_t{kMaxEntities}; ++i) {
            const auto key = reinterpret_cast<const void*>(i * sizeof(void*));
            allAssigned    = allAssigned && intern(key, EntityDescription{}) != kNoEntity;
        }
        expect(allAssigned) << "the table must hold kMaxEntities identities";
        const auto overflow = reinterpret_cast<const void*>((std::size_t{kMaxEntities} + 1UZ) * sizeof(void*));
        expect(eq(intern(overflow, EntityDescription{}), kNoEntity)) << "past the limit interning must fail, not wrap onto a live id";
        reset();
    };

    "the allocation sentinel actually counts — the guard against a test that passes for the wrong reason"_test = [] {
        // A broken sentinel would make the next case pass unconditionally -- it would report zero
        // allocations whether or not any happened. The only way to tell the two apart is to make
        // the sentinel see an allocation it must not miss, so one is made here deliberately.
        const AllocationSentinel sentinel;
        expect(eq(sentinel.delta(), 0UZ));

        auto* deliberate = new int(7); // NOLINT(cppcoreguidelines-owning-memory) -- the point is to allocate
        expect(gt(sentinel.delta(), 0UZ)) << "the sentinel does not see allocations, so the next test proves nothing";
        delete deliberate;
    };

    "the steady state does not allocate"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));

        int            block = 0;
        const EntityId id    = intern(&block, EntityDescription{.uniqueName = "warm", .typeName = "T"});
        emit(Event{.entity = id, .kind = Kind::workEnd}); // create this thread's ring before arming

        {
            // `intern()` is reached from `syncSchedStates`, which runs on the house-keeping cadence,
            // and `emit()` from `work()`. Neither may allocate once warm, or the trace layer becomes
            // a latency source in the loop it is measuring.
            const AllocationSentinel sentinel;
            for (std::uint32_t i = 0U; i < 1'000U; ++i) {
                emit(Event{.payload0 = i, .entity = id, .kind = Kind::workEnd});
            }
            std::ignore = intern(&block, EntityDescription{.uniqueName = "warm", .typeName = "T"});
            std::ignore = internedId(&block);
            std::ignore = categories();
            expect(eq(sentinel.delta(), 0UZ)) << "the warm path allocated";
        }

        setCategories(0U);
        reset();
    };

    "a dumped trace round-trips byte-exactly"_test = [] {
        reset();
        setCategories(categoryMask(Category::work, Category::lifecycle));
        std::ignore = setRingCapacity(64UZ);

        int            alpha = 0;
        int            beta  = 0;
        const EntityId one   = intern(&alpha, EntityDescription{.uniqueName = "src-0", .typeName = "Source<float>", .workerId = 2U, .nInputPorts = 0U, .nOutputPorts = 1U});
        const EntityId two   = intern(&beta, EntityDescription{.uniqueName = "sink-0", .typeName = "Sink<float>", .workerId = 2U, .nInputPorts = 1U, .nOutputPorts = 0U});

        std::vector<Event> expected;
        for (std::uint32_t i = 0U; i < 5U; ++i) {
            const Event event{.startNs = 1'000UL + i, .durationNs = 10U + i, .payload0 = i, .payload1 = 2U * i, .payload2 = 3U * i, .entity = (i % 2U == 0U) ? one : two, .kind = Kind::workEnd, .workerId = 2U, .status = -2, .flags = flag::kIsSource};
            emit(event);
            expected.push_back(event);
        }

        const std::filesystem::path file    = std::filesystem::temp_directory_path() / std::format("qa_Trace_{}.gr4trace", ::getpid());
        const auto                  written = dump(file.string());
        expect(written.has_value()) << "dump must succeed";
        expect(eq(written.value(), 5UZ));

        std::ifstream in(file, std::ios::binary);
        expect(in.is_open() >> fatal);

        FileHeader header{};
        in.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
        expect(eq(std::string_view(header.magic.data(), header.magic.size()), std::string_view("GR4TRACE")));
        expect(eq(header.formatVersion, kFormatVersion));
        expect(eq(header.headerBytes, static_cast<std::uint32_t>(sizeof(FileHeader))));
        expect(eq(header.endianMarker, kEndianMarker)) << "a byte-swapped reader must see 0x04030201 and refuse";
        expect(eq(header.eventBytes, static_cast<std::uint32_t>(sizeof(Event)))) << "a layout change must be detectable, not read as data";
        expect(eq(header.eventCount, 5UL));
        expect(eq(header.entityCount, 2UL));
        expect(eq(header.lostCount, 0UL));
        expect(gt(header.ringCount, 0UL));
        expect(eq(header.categoryMask, categoryMask(Category::work, Category::lifecycle))) << "the header records what was live, so a reader knows what is absent by choice";
        expect(gt(header.imageId, 0UL)) << "the producing image must be identifiable, so two traces from different images are not merged";
        expect(eq(header.clockDomain, static_cast<std::uint32_t>(std::to_underlying(clockInfo().domain))));

        // metadata section
        std::vector<std::pair<std::string, std::string>> names;
        for (std::uint64_t e = 0UL; e < header.entityCount; ++e) {
            EntityRecord record{};
            in.read(reinterpret_cast<char*>(&record), sizeof(EntityRecord));
            std::string uniqueName(record.uniqueNameBytes, '\0');
            std::string typeName(record.typeNameBytes, '\0');
            in.read(uniqueName.data(), record.uniqueNameBytes);
            in.read(typeName.data(), record.typeNameBytes);
            expect(eq(record.id, static_cast<EntityId>(e + 1UL))) << "identities are written in assignment order";
            names.emplace_back(std::move(uniqueName), std::move(typeName));
        }
        expect(eq(names.at(0).first, std::string("src-0")));
        expect(eq(names.at(0).second, std::string("Source<float>")));
        expect(eq(names.at(1).first, std::string("sink-0")));

        // event section, compared field by field against what was emitted
        for (std::uint64_t e = 0UL; e < header.eventCount; ++e) {
            Event event{};
            in.read(reinterpret_cast<char*>(&event), sizeof(Event));
            const Event& want = expected.at(static_cast<std::size_t>(e));
            expect(eq(event.startNs, want.startNs));
            expect(eq(event.durationNs, want.durationNs));
            expect(eq(event.payload0, want.payload0));
            expect(eq(event.payload1, want.payload1));
            expect(eq(event.payload2, want.payload2));
            expect(eq(event.entity, want.entity));
            expect(eq(std::to_underlying(event.kind), std::to_underlying(want.kind)));
            expect(eq(event.workerId, want.workerId));
            expect(eq(event.status, want.status));
            expect(eq(event.flags, want.flags));
        }
        expect(in.good() || in.eof()) << "the file must contain exactly what the header promised";
        expect(eq(std::filesystem::file_size(file), sizeof(FileHeader) + 2UL * sizeof(EntityRecord) + 5UL + 13UL + 6UL + 11UL + 5UL * sizeof(Event))) << "no padding, no slack: header + entities + names + events";

        std::filesystem::remove(file);
        setCategories(0U);
        std::ignore = setRingCapacity(kDefaultRingCapacity);
        reset();
    };

    "a trace that lost records says so in its header"_test = [] {
        reset();
        std::ignore = setRingCapacity(8UZ);
        setCategories(categoryMask(Category::work));

        std::thread emitter([] {
            for (std::uint32_t i = 0U; i < 20U; ++i) {
                emit(Event{.payload0 = i, .kind = Kind::workEnd});
            }
        });
        emitter.join();

        const std::filesystem::path file    = std::filesystem::temp_directory_path() / std::format("qa_Trace_lossy_{}.gr4trace", ::getpid());
        const auto                  written = dump(file.string());
        expect(written.has_value() >> fatal);
        expect(eq(written.value(), 8UZ));

        std::ifstream in(file, std::ios::binary);
        FileHeader    header{};
        in.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
        expect(eq(header.eventCount, 8UL));
        expect(eq(header.lostCount, 12UL)) << "a report consuming this trace must be able to say it is incomplete";

        std::filesystem::remove(file);
        setCategories(0U);
        std::ignore = setRingCapacity(kDefaultRingCapacity);
        reset();
    };

    "dumping to an unwritable path reports an error rather than throwing"_test = [] {
        const auto result = dump("/nonexistent-directory-for-qa/trace.gr4trace");
        expect(!result.has_value()) << "an unopenable path must be an error";
        expect(!result.error().message.empty());
    };

    "an empty capture dumps a valid, empty file"_test = [] {
        reset();
        const std::filesystem::path file    = std::filesystem::temp_directory_path() / std::format("qa_Trace_empty_{}.gr4trace", ::getpid());
        const auto                  written = dump(file.string());
        expect(written.has_value()) << "nothing traced is success, not failure -- the two answers differ";
        expect(eq(written.value(), 0UZ));
        expect(eq(std::filesystem::file_size(file), sizeof(FileHeader)));
        std::filesystem::remove(file);
    };

    "the image id is stable within a run, which is the only claim it makes"_test = [] {
        // Two dumps from one process must agree, because that is the comparison the field exists to
        // support. It deliberately says nothing across runs -- it is an address, and ASLR moves it --
        // so a reader that compares two files from two executions is reading noise.
        const std::filesystem::path first  = std::filesystem::temp_directory_path() / std::format("qa_Trace_image_a_{}.gr4trace", ::getpid());
        const std::filesystem::path second = std::filesystem::temp_directory_path() / std::format("qa_Trace_image_b_{}.gr4trace", ::getpid());
        expect(dump(first.string()).has_value() >> fatal);
        expect(dump(second.string()).has_value() >> fatal);

        const auto imageIdOf = [](const std::filesystem::path& path) {
            std::ifstream in(path, std::ios::binary);
            FileHeader    header{};
            in.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));
            return header.imageId;
        };
        expect(neq(imageIdOf(first), 0UL)) << "an unset image id would make every trace look like it came from the same place";
        expect(eq(imageIdOf(first), imageIdOf(second))) << "two dumps from one process describe one image";

        std::filesystem::remove(first);
        std::filesystem::remove(second);
    };

    "a capture survives a round trip through the file"_test = [] {
        reset();
        setCategories(categoryMask(Category::work));
        std::ignore = intern(reinterpret_cast<const void*>(0xB10C10UL), EntityDescription{.uniqueName = "roundtrip", .typeName = "gr::testing::Copy<float>", .workerId = 2U, .nInputPorts = 1U, .nOutputPorts = 1U});
        for (std::uint32_t i = 0U; i < 5U; ++i) {
            emit(Event{.startNs = 1000UL + i, .durationNs = 10U + i, .payload0 = i, .kind = Kind::workEnd, .workerId = 2U, .status = -1});
        }
        const std::vector<Event> captured = collect();

        const std::filesystem::path file = std::filesystem::temp_directory_path() / std::format("qa_Trace_load_{}.gr4trace", ::getpid());
        expect(dump(file.string()).has_value() >> fatal);

        const auto loaded = load(file.string());
        expect(loaded.has_value() >> fatal) << "a file this process just wrote must be readable by this process";
        expect(eq(loaded->events.size(), captured.size()) >> fatal) << "every record written must come back";
        for (std::size_t i = 0UZ; i < captured.size(); ++i) {
            expect(eq(loaded->events[i].startNs, captured[i].startNs));
            expect(eq(loaded->events[i].payload0, captured[i].payload0));
            expect(eq(std::to_underlying(loaded->events[i].kind), std::to_underlying(captured[i].kind)));
            expect(eq(loaded->events[i].status, captured[i].status)) << "status is what the fit filters on, so it must survive the file";
        }

        const auto entity = std::ranges::find_if(loaded->entities, [](const LoadedEntity& e) { return e.uniqueName == "roundtrip"; });
        expect((entity != loaded->entities.end()) >> fatal) << "the identity table must survive too, or records name nothing";
        expect(eq(entity->typeName, std::string("gr::testing::Copy<float>"))) << "including both names, which are length-prefixed rather than terminated";
        expect(eq(std::uint32_t{entity->workerId}, 2U));
        expect(eq(entity->nInputPorts, std::uint16_t{1U}));

        std::filesystem::remove(file);
        setCategories(0U);
        reset();
    };

    "an empty capture round-trips as an empty capture"_test = [] {
        reset();
        const std::filesystem::path file = std::filesystem::temp_directory_path() / std::format("qa_Trace_loadempty_{}.gr4trace", ::getpid());
        expect(dump(file.string()).has_value() >> fatal);

        const auto loaded = load(file.string());
        expect(loaded.has_value() >> fatal) << "nothing captured is a valid capture, not a corrupt one";
        expect(eq(loaded->events.size(), 0UZ));
        expect(eq(loaded->entities.size(), 0UZ));
        expect(eq(std::string_view(loaded->header.magic.data(), 8UZ), std::string_view("GR4TRACE")));

        std::filesystem::remove(file);
    };

    "a damaged file is refused, and the refusal names the damage"_test = [] {
        // Each of these is a rule the container was designed around; a reader that recovered from any
        // of them would be reinterpreting bytes whose meaning it cannot know.
        reset();
        setCategories(categoryMask(Category::work));
        emit(Event{.payload0 = 1U, .kind = Kind::workEnd});
        const std::filesystem::path good = std::filesystem::temp_directory_path() / std::format("qa_Trace_good_{}.gr4trace", ::getpid());
        expect(dump(good.string()).has_value() >> fatal);
        setCategories(0U);

        std::vector<char> original;
        {
            std::ifstream in(good, std::ios::binary);
            original.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }

        const auto writeDamaged = [&](const std::function<void(std::vector<char>&)>& damage) {
            std::vector<char> bytes = original;
            damage(bytes);
            const std::filesystem::path bad = std::filesystem::temp_directory_path() / std::format("qa_Trace_bad_{}.gr4trace", ::getpid());
            std::ofstream               out(bad, std::ios::binary | std::ios::trunc);
            out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            out.close();
            return bad;
        };
        const auto patch32 = [](std::vector<char>& bytes, std::size_t offset, std::uint32_t value) { std::memcpy(bytes.data() + offset, &value, sizeof(value)); };
        const auto patch64 = [](std::vector<char>& bytes, std::size_t offset, std::uint64_t value) { std::memcpy(bytes.data() + offset, &value, sizeof(value)); };

        struct Damage {
            const char*                             what;
            std::function<void(std::vector<char>&)> apply;
            const char*                             mustSay;
        };
        const std::vector<Damage> damages{
            {"wrong magic", [](std::vector<char>& b) { b[0] = 'X'; }, "not a .gr4trace"},
            {"other byte order", [&](std::vector<char>& b) { patch32(b, offsetof(FileHeader, endianMarker), 0x04030201U); }, "byte order"},
            {"unknown version", [&](std::vector<char>& b) { patch32(b, offsetof(FileHeader, formatVersion), 99U); }, "format version"},
            {"different Event size", [&](std::vector<char>& b) { patch32(b, offsetof(FileHeader, eventBytes), 64U); }, "layout changed"},
            {"header shorter than required", [&](std::vector<char>& b) { patch32(b, offsetof(FileHeader, headerBytes), 64U); }, "shorter than"},
            {"truncated mid-record", [](std::vector<char>& b) { b.resize(b.size() - 8UZ); }, "records"},
            {"impossible identity count", [&](std::vector<char>& b) { patch64(b, offsetof(FileHeader, entityCount), 1UL << 40U); }, "identities"},
            // A header larger than the file it is in. Only the lower bound was checked at first, and
            // the difference is computed on unsigned offsets: this made `fileBytes - headerBytes`
            // wrap to 1.8e19, walked past the identity-count guard written to stop exactly this, and
            // left an attacker-chosen record count to reach `resize()`.
            {"header larger than the file", [&](std::vector<char>& b) { patch32(b, offsetof(FileHeader, headerBytes), 1U << 30U); }, "only"},
            {"header larger than the file, with no identities to trip over", //
                [&](std::vector<char>& b) {
                    patch32(b, offsetof(FileHeader, headerBytes), 1U << 30U);
                    patch64(b, offsetof(FileHeader, entityCount), 0UL);
                    patch64(b, offsetof(FileHeader, eventCount), 1UL << 35U);
                },
                "only"},
        };

        for (const Damage& damage : damages) {
            const std::filesystem::path bad    = writeDamaged(damage.apply);
            const auto                  result = load(bad.string());
            expect(!result.has_value()) << std::format("a file with {} must be refused", damage.what);
            if (!result.has_value()) {
                expect(std::string(result.error().message).contains(damage.mustSay)) << std::format("the refusal for {} must name it, and said instead: {}", damage.what, result.error().message);
            }
            std::filesystem::remove(bad);
        }

        // The undamaged original must still load, or the loop above proves only that load() refuses.
        expect(load(good.string()).has_value()) << "the file the damages were derived from must itself be readable";
        std::filesystem::remove(good);
        reset();
    };

    "a longer header from a later version is skipped, not misread"_test = [] {
        // `headerBytes` exists precisely so a later version can grow the preamble without every older
        // reader silently parsing its extension as the identity table.
        reset();
        setCategories(categoryMask(Category::work));
        emit(Event{.payload0 = 42U, .kind = Kind::workEnd});
        const std::filesystem::path file = std::filesystem::temp_directory_path() / std::format("qa_Trace_future_{}.gr4trace", ::getpid());
        expect(dump(file.string()).has_value() >> fatal);
        setCategories(0U);

        std::vector<char> bytes;
        {
            std::ifstream in(file, std::ios::binary);
            bytes.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        constexpr std::uint32_t kPadding = 32U;
        const std::uint32_t     grown    = static_cast<std::uint32_t>(sizeof(FileHeader)) + kPadding;
        std::memcpy(bytes.data() + offsetof(FileHeader, headerBytes), &grown, sizeof(grown));
        bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(sizeof(FileHeader)), kPadding, '\0');
        {
            std::ofstream out(file, std::ios::binary | std::ios::trunc);
            out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        }

        const auto loaded = load(file.string());
        expect(loaded.has_value() >> fatal) << "a longer header is a later version, not a corrupt file: " << (loaded.has_value() ? std::string{} : std::string(loaded.error().message));
        expect(eq(loaded->events.size(), 1UZ)) << "and the records after it must still be found";
        expect(eq(loaded->events.front().payload0, 42U));

        std::filesystem::remove(file);
        reset();
    };

    "the build flag reaches the header"_test = [] {
#ifdef GR_ENABLE_TRACING
        expect(kEnabled) << "GR_ENABLE_TRACING is defined but gr::trace::kEnabled is false";
#else
        expect(!kEnabled) << "GR_ENABLE_TRACING is undefined but gr::trace::kEnabled is true";
#endif
    };
};

int main() { /* tests are statically registered as suites */ }
