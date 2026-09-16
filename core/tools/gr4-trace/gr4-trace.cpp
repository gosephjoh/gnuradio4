#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <print>
#include <optional>
#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <gnuradio-4.0/TraceCatapult.hpp>
#include <gnuradio-4.0/TraceFile.hpp>
#include <gnuradio-4.0/TraceReport.hpp>

/**
 * `gr4-trace` — read a `.gr4trace` capture and say something useful about it.
 *
 * A front end, deliberately: every answer it gives comes from `load()`, `timingReport()` and
 * `catapultJson()`, all of which are tested directly. Putting the logic here instead would move it
 * somewhere no test can reach, which is how command-line tools come to be the least trustworthy part
 * of a codebase.
 *
 * It is built whether or not tracing is compiled in, because reading a capture and producing one are
 * unrelated: an engineer analysing a trace from a traced build is usually not running one.
 */
namespace {

using namespace gr::trace;

/// Keeps the distinguishing tail of a long block name. `gr::testing::ConstantSource<float32>#9`
/// truncated from the right becomes `gr::testing::ConstantSource<float3`, which loses both the type
/// argument and the instance number -- the two parts that tell one block from another.
[[nodiscard]] std::string elide(std::string_view name, std::size_t width) {
    if (name.size() <= width) {
        return std::string(name);
    }
    return std::string("\u2026") + std::string(name.substr(name.size() - (width - 1UZ)));
}

/// A number of nanoseconds a person can read at a glance.
[[nodiscard]] std::string humanNs(double nanoseconds) {
    if (nanoseconds < 1e3) {
        return std::format("{:.0f} ns", nanoseconds);
    }
    if (nanoseconds < 1e6) {
        return std::format("{:.2f} µs", nanoseconds / 1e3);
    }
    return std::format("{:.2f} ms", nanoseconds / 1e6);
}

template<typename T>
[[nodiscard]] T fieldOr(const gr::property_map& map, std::string_view key, T fallback) {
    const auto it = map.find(key);
    return it != map.end() ? (*it).second.value_or(std::move(fallback)) : fallback;
}

[[nodiscard]] gr::property_map nestedOr(const gr::property_map& map, std::string_view key) {
    const auto it = map.find(key);
    if (it == map.end()) {
        return {};
    }
    const gr::pmt::Value value = (*it).second;
    return value.value_or(gr::property_map{});
}

int usage() {
    std::println(stderr, "usage: gr4-trace <command> <capture.gr4trace> [output]");
    std::println(stderr, "");
    std::println(stderr, "  report    <capture>            per-block cost, jitter and marginal-cost fit");
    std::println(stderr, "  catapult  <capture> <out.json> [block ...] Chrome trace JSON, for ui.perfetto.dev");
    std::println(stderr, "            naming a source-to-sink chain adds latency flow arrows");
    std::println(stderr, "  summary   <capture>            header fields and a record census");
    return 2;
}

void printHeader(const Capture& capture) {
    std::println("{:<22} {}", "records", capture.events.size());
    std::println("{:<22} {}", "identities", capture.entities.size());
    if (capture.header.lostCount > 0UL) {
        // Said first and said plainly. Every figure below understates by an unknown amount, and a
        // reader who misses this will draw conclusions the capture cannot support.
        std::println("{:<22} {}  <-- records were evicted; everything below understates", "LOST", capture.header.lostCount);
    }
    std::println("{:<22} {}", "emitting threads", capture.header.ringCount);
    std::println("{:<22} 0x{:02x}", "categories live", capture.header.categoryMask);
    std::println("{:<22} {} ns", "clock read cost", capture.header.clockCostNs);
}

int commandSummary(const Capture& capture) {
    printHeader(capture);

    std::map<std::string_view, std::size_t> census;
    for (const Event& event : capture.events) {
        ++census[detail::kindName(event.kind)];
    }
    std::println("");
    std::println("{:<22} {:>9}", "record kind", "count");
    for (const auto& [kind, count] : census) {
        std::println("{:<22} {:>9}", kind, count);
    }
    return 0;
}

int commandReport(const Capture& capture) {
    printHeader(capture);

    const gr::property_map timing = timingReport(capture.events, capture.entities);
    const gr::property_map tardy  = report(capture.events, capture.header.lostCount);

    std::println("");
    std::println("{:<22} {}", "cost samples used", fieldOr<std::uint64_t>(timing, "invocations", 0UL));
    std::println("{:<22} {} unsuccessful, {} saturated, {} unattributed", "... and discarded", fieldOr<std::uint64_t>(timing, "rejected_status", 0UL), fieldOr<std::uint64_t>(timing, "rejected_saturated", 0UL), fieldOr<std::uint64_t>(timing, "rejected_no_entity", 0UL));

    const gr::property_map blocks = nestedOr(timing, "blocks");
    if (blocks.empty()) {
        std::println("");
        std::println("no per-block cost in this capture: it carries no successful work records");
    } else {
        std::println("");
        std::println("{:<38} {:>7} {:>11} {:>11} {:>11}  {}", "block", "calls", "average", "jitter", "worst", "marginal cost");
        for (const auto& entry : blocks) {
            const gr::pmt::Value   value = entry.second;
            const gr::property_map block = value.value_or(gr::property_map{});
            const std::string      fit   = fieldOr<std::string>(block, "fit", std::string("low-confidence"));

            // A refused fit prints the reason, never a number. The whole point of withholding the
            // slope is undone by a tool that prints something in its place.
            const std::string marginal = fit == "identifiable" //
                                             ? std::format("{:.3f} ns/item + {} per call", fieldOr<double>(block, "item_cost_ns", 0.0), humanNs(fieldOr<double>(block, "invocation_cost_ns", 0.0)))
                                             : std::format("not identifiable: {}", fieldOr<std::string>(block, "fit_reason", std::string("unknown")));

            std::println("{:<38} {:>7} {:>11} {:>11} {:>11}  {}", elide(entry.first, 38UZ), fieldOr<std::uint64_t>(block, "invocations", 0UL), humanNs(fieldOr<double>(block, "acet_ns", 0.0)), humanNs(fieldOr<double>(block, "jitter_ns", 0.0)), humanNs(static_cast<double>(fieldOr<std::uint64_t>(block, "wcet_ns", 0UL))), marginal);
        }
    }

    const std::uint64_t misses = fieldOr<std::uint64_t>(tardy, "deadline_misses", 0UL);
    std::println("");
    if (misses > 0UL) {
        std::println("{:<22} {} (worst {} late)", "deadline misses", misses, humanNs(static_cast<double>(fieldOr<std::uint64_t>(tardy, "tardiness_max_ns", 0UL))));
    } else {
        std::println("{:<22} none recorded", "deadline misses");
    }
    std::println("{:<22} {}", "response times", fieldOr<std::string>(tardy, "response_time", std::string("unavailable")));
    const std::string reason = fieldOr<std::string>(tardy, "response_time_reason", std::string{});
    if (!reason.empty()) {
        std::println("{:<22} {}", "", reason);
    }
    std::println("{:<22} {}", "live vs reconstructed", fieldOr<std::string>(tardy, "cross_check", std::string("unavailable")));
    return 0;
}

/// Resolves block names to interned ids, so a chain is given as names rather than ids a user would
/// have to look up first.
///
/// Matched as a **substring** of the unique name, because that name is type-derived -- a block the
/// user called "src" is recorded as `gr::testing::ConstantSource<float32>#16` -- so requiring the
/// whole thing would mean copying it out of a report first. An ambiguous or unknown name is an error
/// rather than a silently wrong chain: picking the first of two matches would produce a latency for
/// a path the user did not ask about.
[[nodiscard]] std::optional<std::vector<EntityId>> resolveChain(const Capture& capture, std::span<const std::string_view> names) {
    std::vector<EntityId> chain;
    for (const std::string_view name : names) {
        std::vector<const LoadedEntity*> matches;
        for (const LoadedEntity& entity : capture.entities) {
            if (entity.uniqueName.find(name) != std::string::npos) {
                matches.push_back(&entity);
            }
        }
        if (matches.empty()) {
            std::println(stderr, "gr4-trace: no block matching '{}' in this capture", name);
            return std::nullopt;
        }
        if (matches.size() > 1UZ) {
            std::println(stderr, "gr4-trace: '{}' matches {} blocks; name one of them more precisely:", name, matches.size());
            for (const LoadedEntity* entity : matches) {
                std::println(stderr, "  {}", entity->uniqueName);
            }
            return std::nullopt;
        }
        chain.push_back(matches.front()->id);
    }
    return chain;
}

int commandCatapult(const Capture& capture, const std::string& outputPath, std::span<const EntityId> chain) {
    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        std::println(stderr, "gr4-trace: cannot open '{}' for writing", outputPath);
        return 1;
    }
    const std::string json = catapultJson(capture, chain);
    if (!chain.empty()) {
        // Said out loud, because a chain that could not be reconstructed produces a timeline that
        // looks exactly like one that was never asked for.
        const ChainLatency latency = chainLatency(capture.events, chain);
        if (latency.computed) {
            std::println("latency over {} samples: min {} ns, median {} ns, max {} ns", latency.samples, latency.minNs, latency.medianNs, latency.maxNs);
        } else {
            std::println("no flow arrows: {}", latency.reason);
        }
    }
    out.write(json.data(), static_cast<std::streamsize>(json.size()));
    out.flush();
    if (!out.good()) {
        std::println(stderr, "gr4-trace: write failed for '{}'", outputPath);
        return 1;
    }
    std::println("{} events -> {} ({} bytes)", capture.events.size(), outputPath, json.size());
    if (capture.header.lostCount > 0UL) {
        std::println("note: {} records were evicted before this capture was written; the timeline has gaps", capture.header.lostCount);
    }
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    const std::vector<std::string_view> args(argv, argv + argc);
    if (args.size() < 3UZ) {
        return usage();
    }
    const std::string_view command = args[1];
    const std::string      path(args[2]);

    const auto capture = load(path);
    if (!capture.has_value()) {
        // The reader's refusals name what was wrong with the file; passing that through unchanged is
        // more use than a tool-level "could not read".
        std::println(stderr, "{}", capture.error().message);
        return 1;
    }

    if (command == "summary") {
        return commandSummary(*capture);
    }
    if (command == "report") {
        return commandReport(*capture);
    }
    if (command == "catapult") {
        if (args.size() < 4UZ) {
            std::println(stderr, "gr4-trace catapult needs an output path");
            return usage();
        }
        std::vector<EntityId> chain;
        if (args.size() > 4UZ) {
            const std::vector<std::string_view> names(args.begin() + 4, args.end());
            const auto                          resolved = resolveChain(*capture, names);
            if (!resolved.has_value()) {
                return 1;
            }
            chain = *resolved;
        }
        return commandCatapult(*capture, std::string(args[3]), chain);
    }
    std::println(stderr, "gr4-trace: unknown command '{}'", command);
    return usage();
}
