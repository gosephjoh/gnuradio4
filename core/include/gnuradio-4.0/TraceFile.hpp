#ifndef GNURADIO_TRACEFILE_HPP
#define GNURADIO_TRACEFILE_HPP

#include <array>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <gnuradio-4.0/Logger.hpp>
#include <gnuradio-4.0/Trace.hpp>

/**
 * @brief The `.gr4trace` container: a fixed header, the interning table, then the packed records.
 *
 * Split from `Trace.hpp` deliberately. That header is included from `Scheduler.hpp` and, later,
 * `Block.hpp`, and must stay free of `<fstream>`, `gr::Error` and everything else a file writer
 * needs. Writing a trace is cold by definition — nothing on the marker path includes this.
 *
 * Native-endian by design: every GR4 target is little-endian, and byte-swapping
 * code that nothing exercises is code that will be wrong the first time it is needed. The header
 * declares what it was written as, and a reader that disagrees must refuse rather than reinterpret.
 */
namespace gr::trace {

inline constexpr std::array<char, 8> kFileMagic{{'G', 'R', '4', 'T', 'R', 'A', 'C', 'E'}};

/// Bumped by any change to `Event`'s layout, to `FileHeader`, or to `EntityRecord`. `qa_Trace`'s
/// golden layout table is the tripwire that makes such a change visible rather than silent.
inline constexpr std::uint32_t kFormatVersion = 1U;

/// Written natively; a reader whose byte order differs sees `0x04030201` and must give up.
inline constexpr std::uint32_t kEndianMarker = 0x01020304U;

/**
 * @brief Fixed 128-byte preamble.
 *
 * `headerBytes` and `formatVersion` are both present so that a later version which grows the header
 * is *detectable* rather than silently misparsed — a reader seeks by `headerBytes` and refuses a
 * `formatVersion` it does not know.
 */
struct FileHeader {
    std::array<char, 8> magic{};
    std::uint32_t       formatVersion{};
    std::uint32_t       headerBytes{};
    std::uint32_t       endianMarker{};
    std::uint32_t       eventBytes{}; /// `sizeof(Event)`, so a layout change cannot be read as data

    std::uint64_t clockCostNs{};       /// what one `now()` cost, so a report can net it out
    std::uint64_t steadyAnchorNs{};    /// the capture's `TraceClock` anchor
    std::uint64_t monotonicAnchorNs{}; /// its `CLOCK_MONOTONIC` twin; 0 unless the domain matched
    std::uint32_t clockDomain{};       /// `ClockDomain` — whether a kernel trace may be merged
    std::uint32_t categoryMask{};      /// what was live, so a reader knows what is *absent* by choice

    std::uint64_t wallClockNs{}; /// `system_clock` at dump, for human reference only
    std::uint64_t processId{};

    /**
     * Which copy of the trace layer produced this.
     *
     * GR4 links `gnuradio-core` **statically** into plugins, so a plugin carries its own category
     * mask, its own rings and its own interning table. Records emitted inside a
     * plugin cannot reach the host's dump — in fact they are never created, because the plugin's
     * mask is zero. This field is how two traces are known to have come from different images, so a
     * report cannot present a partial picture as a complete one.
     *
     * It distinguishes images; it does not name them. Naming would need `dladdr`, and a `libdl`
     * dependency in `gnuradio-core` is too much to pay for a diagnostic.
     *
     * **Meaningful only within one run.** The value is the address of a file-local object, so address
     * space layout randomisation gives the same image a different id on every execution. Two traces
     * captured in the same process may be compared; two captured in different runs may not, and an
     * equal pair across runs is coincidence rather than evidence.
     */
    std::uint64_t imageId{};

    std::uint64_t entityCount{};
    std::uint64_t eventCount{};
    std::uint64_t lostCount{}; /// records overwritten before the dump — a trace that lost data says so
    std::uint64_t ringCount{}; /// threads that emitted at least once

    std::array<std::uint64_t, 2> reserved{};
};

static_assert(sizeof(FileHeader) == 128UZ);
static_assert(std::is_trivially_copyable_v<FileHeader>);
static_assert(std::is_standard_layout_v<FileHeader>);

/// Precedes each entity's two names in the metadata section. Lengths rather than terminators, so a
/// name containing a NUL cannot truncate the table.
struct EntityRecord {
    EntityId      id{};
    std::uint8_t  workerId{};
    std::uint8_t  reserved{};
    std::uint16_t nInputPorts{};
    std::uint16_t nOutputPorts{};
    std::uint16_t uniqueNameBytes{};
    std::uint16_t typeNameBytes{};
};

static_assert(sizeof(EntityRecord) == 12UZ);
static_assert(std::is_trivially_copyable_v<EntityRecord>);
static_assert(std::is_standard_layout_v<EntityRecord>);

/**
 * Writes every retained record and every identity to `path`, returning how many records were
 * written.
 *
 * **Defined at quiescence.** A 32-byte store is not atomic, so a reader walking a
 * ring while its thread emits can observe a torn record; each ring's sequence is snapshotted first,
 * which bounds the damage to that ring's oldest record rather than the walk, but that is damage
 * limitation and not a licence.
 *
 * Zero records is a success, not an error: "nothing was traced" and "the file could not be opened"
 * are different answers and a caller will want to tell them apart — which is why this returns
 * `std::expected` rather than a count with a magic value, and why it does not throw: this is
 * library code, and the project's error handling is exception-free.
 */
[[nodiscard]] std::expected<std::size_t, gr::Error> dump(std::string_view path);

/// One identity as it was written, with its names owned rather than viewed: the block they came from
/// is long gone by the time anything reads the file.
struct LoadedEntity {
    EntityId      id{};
    std::uint8_t  workerId{};
    std::uint16_t nInputPorts{};
    std::uint16_t nOutputPorts{};
    std::string   uniqueName;
    std::string   typeName;
};

/// A capture, read back whole.
struct Capture {
    FileHeader                header{};
    std::vector<LoadedEntity> entities;
    std::vector<Event>        events;
};

/**
 * Reads a `.gr4trace` written by `dump()`.
 *
 * The reading counterpart the format always described but never had: until now the only reader was
 * whatever each test wrote inline, which is how a container acquires several subtly different
 * opinions about its own layout. One reader, shared by the report, the converter and the tests.
 *
 * **It refuses rather than reinterprets**, which is the rule the header was designed around. A
 * different byte order, an `Event` of a different size, a `formatVersion` it does not know, a header
 * shorter than it should be, a truncated section, or counts the file is too small to contain: each
 * is an error naming what was wrong, never a partial `Capture`. A half-read trace that looks whole is
 * the failure this format spent a `headerBytes` field and an endian marker to avoid.
 *
 * A header longer than this build's `FileHeader` is *not* an error — that is what `headerBytes` is
 * for. The extra is skipped, so a file from a later version whose `formatVersion` is still
 * recognised remains readable.
 */
[[nodiscard]] std::expected<Capture, gr::Error> load(std::string_view path);

} // namespace gr::trace

#endif // GNURADIO_TRACEFILE_HPP
