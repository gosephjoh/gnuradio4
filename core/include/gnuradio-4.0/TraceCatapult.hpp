#ifndef GNURADIO_TRACECATAPULT_HPP
#define GNURADIO_TRACECATAPULT_HPP

#include <algorithm>
#include <cstdint>
#include <format>
#include <map>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>
#include <gnuradio-4.0/TraceReport.hpp> // chainLinks, for the flow arrows

/**
 * @brief Converts a capture to Catapult (Chrome) trace JSON, which Perfetto ingests natively.
 *
 * The short path to a usable timeline: no new dependency, no protobuf, and `ui.perfetto.dev` opens
 * the result by drag and drop. The protobuf backend is a later milestone and buys tracks, flows and a
 * clock snapshot; none of that is worth having before the timeline exists at all.
 *
 * **Block slices nest inside the sweep that drove them.** They share the worker's lane rather than
 * taking one each, so a sweep visibly contains the work it dispatched -- which is the scheduling
 * story the capture exists to tell. Catapult requires slices on a lane to form a proper stack, so
 * this is only valid while every work interval lies within a sweep interval; measured on a real
 * capture it does, and a test asserts it rather than the converter defending against it at runtime.
 * If that ever stops being true the test says so, which is the better place to find out.
 */
namespace gr::trace {

namespace detail {

[[nodiscard]] inline std::string_view kindName(Kind kind) noexcept {
    switch (kind) {
    case Kind::workBegin: return "workBegin";
    case Kind::workEnd: return "work";
    case Kind::workProbe: return "workProbe";
    case Kind::workerStart: return "workerStart";
    case Kind::workerStop: return "workerStop";
    case Kind::sweep: return "sweep";
    case Kind::messagePhase: return "messagePhase";
    case Kind::houseKeeping: return "houseKeeping";
    case Kind::stateSync: return "stateSync";
    case Kind::adopt: return "adopt";
    case Kind::zombieReap: return "zombieReap";
    case Kind::quiescenceWait: return "quiescenceWait";
    case Kind::idle: return "idle";
    case Kind::jobRelease: return "jobRelease";
    case Kind::jobReleaseDropped: return "jobReleaseDropped";
    case Kind::releaseScan: return "releaseScan";
    case Kind::select: return "select";
    case Kind::selectEmpty: return "selectEmpty";
    case Kind::selectionBoundHit: return "selectionBoundHit";
    case Kind::heapFallback: return "heapFallback";
    case Kind::deadlineMiss: return "deadlineMiss";
    case Kind::blockCounter: return "blockCounter";
    case Kind::workerCounter: return "workerCounter";
    case Kind::blockStateChange: return "blockStateChange";
    case Kind::entityRetired: return "entityRetired";
    case Kind::workExact: return "workExact";
    case Kind::workPhase: return "workPhase";
    }
    // No `default:`, so `-Wswitch` makes a new `Kind` a compile error naming the enumerator rather
    // than a record that silently exports as "unknown" -- the same guard `categoryOf` uses.
    return "unknown";
}

/// JSON string escaping. Block type names carry `<`, `>` and `,`; a name containing a quote or a
/// backslash would otherwise produce a document that parses as something else, or not at all.
[[nodiscard]] inline std::string escape(std::string_view text) {
    std::string out;
    out.reserve(text.size() + 8UZ);
    for (const char character : text) {
        switch (character) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            if (static_cast<unsigned char>(character) < 0x20U) {
                out += std::format("\\u{:04x}", static_cast<unsigned int>(static_cast<unsigned char>(character)));
            } else {
                out += character;
            }
        }
    }
    return out;
}

/// Complete and instant records both carry these; a duration event adds `dur`.
[[nodiscard]] inline bool isComplete(Kind kind) noexcept {
    switch (kind) {
    case Kind::workEnd:
    case Kind::sweep:
    case Kind::messagePhase:
    case Kind::houseKeeping:
    case Kind::stateSync:
    case Kind::adopt:
    case Kind::zombieReap:
    case Kind::quiescenceWait:
    case Kind::idle:
    case Kind::releaseScan:
    case Kind::workExact:
    case Kind::workPhase: return true;
    default: return false;
    }
}

} // namespace detail

/**
 * Renders a capture as a Catapult JSON document.
 *
 * `ts` is **microseconds and rebased** to the capture's first record. Catapult's unit is microseconds
 * while `startNs` is `steady_clock` nanoseconds since an arbitrary epoch, so raw values put every
 * event some tens of thousands of years along the axis and the UI opens on empty space. Rebasing
 * first and dividing afterwards keeps nanosecond resolution: a `double` represents every integer up
 * to 2^53 exactly, which is a hundred days of nanoseconds.
 */
/**
 * `chain` is optional. Supplied, each sink invocation is joined to the source batch that produced the
 * samples it read by a Catapult **flow** — a start/finish pair sharing an `id`, which Perfetto draws
 * as an arrow between the two spans. That turns the latency from a number in a report into something
 * traceable in the UI.
 *
 * It has to be supplied because a capture carries no topology (`chainLinks`). A chain whose ratios
 * cannot be reconstructed produces **no arrows at all** rather than partial ones: a timeline showing
 * some links and not others reads as "these are the slow paths" rather than "this was refused".
 */
[[nodiscard]] inline std::string catapultJson(const Capture& capture, std::span<const EntityId> chain = {}, LatencyMode mode = LatencyMode::streamPosition) {
    std::map<EntityId, std::string> names;
    for (const LoadedEntity& entity : capture.entities) {
        names[entity.id] = entity.uniqueName;
    }

    const std::uint64_t origin = capture.events.empty() ? 0UL : std::ranges::min(capture.events | std::views::transform([](const Event& e) { return e.startNs; }));
    const auto          micros = [origin](std::uint64_t ns) { return static_cast<double>(ns - origin) / 1000.0; };
    const std::uint64_t pid    = capture.header.processId;

    // A `workEnd` span already covers its invocation -- `startNs` *is* the entry instant -- so a
    // `workBegin` beside it is a redundant tick at the head of every span. The one worth keeping is
    // the **unmatched** one: a `work()` that entered and never returned leaves nothing else behind,
    // and is the hang the begin/end pair exists to expose.
    std::set<std::pair<EntityId, std::uint64_t>> completed;
    for (const Event& event : capture.events) {
        if (event.kind == Kind::workEnd) {
            completed.emplace(event.entity, event.startNs);
        }
    }

    std::string out   = "{\"traceEvents\":[";
    bool        first = true;
    const auto  comma = [&] {
        if (!first) {
            out += ',';
        }
        first = false;
    };

    // Lane names first, so a viewer shows "pW0" rather than a bare number.
    std::vector<std::uint8_t> workers;
    for (const Event& event : capture.events) {
        if (std::ranges::find(workers, event.workerId) == workers.end()) {
            workers.push_back(event.workerId);
        }
    }
    for (const std::uint8_t worker : workers) {
        comma();
        out += std::format(R"({{"name":"thread_name","ph":"M","pid":{},"tid":{},"args":{{"name":"worker {}"}}}})", pid, worker, worker);
    }
    comma();
    out += std::format(R"({{"name":"process_name","ph":"M","pid":{},"tid":0,"args":{{"name":"gnuradio4 trace"}}}})", pid);

    for (const Event& event : capture.events) {
        if (event.kind == Kind::workBegin && completed.contains({event.entity, event.startNs})) {
            continue; // the span that follows says everything this would
        }
        const std::string label = event.entity != kNoEntity && names.contains(event.entity) //
                                      ? std::format("{} · {}", detail::kindName(event.kind), names.at(event.entity))
                                      : std::string(detail::kindName(event.kind));
        const std::string shown = event.kind == Kind::workBegin ? std::format("UNRETURNED {}", label) : label;

        // Every record carries its payloads: a timeline whose tooltips say nothing but the name is a
        // picture, not a trace. `args` is where the numbers this layer exists to record actually live.
        const std::string args = std::format(R"({{"p0":{},"p1":{},"p2":{},"entity":{},"worker":{},"status":{},"flags":{}}})", //
            event.payload0, event.payload1, event.payload2, event.entity, event.workerId, event.status, event.flags);

        comma();
        if (detail::isComplete(event.kind)) {
            out += std::format(R"({{"name":"{}","cat":"{}","ph":"X","ts":{:.3f},"dur":{:.3f},"pid":{},"tid":{},"args":{}}})", //
                detail::escape(shown), detail::kindName(event.kind), micros(event.startNs), static_cast<double>(event.durationNs) / 1000.0, pid, event.workerId, args);
        } else if (event.kind == Kind::workProbe) {
            out += std::format(R"({{"name":"{}","cat":"workProbe","ph":"C","ts":{:.3f},"pid":{},"tid":{},"args":{{"probes":{},"ns":{}}}}})", //
                detail::escape(shown), micros(event.startNs), pid, event.workerId, event.payload0, event.payload1);
        } else {
            out += std::format(R"({{"name":"{}","cat":"{}","ph":"i","ts":{:.3f},"pid":{},"tid":{},"s":"t","args":{}}})", //
                detail::escape(shown), detail::kindName(event.kind), micros(event.startNs), pid, event.workerId, args);
        }
    }

    // Flow arrows last: an `id` ties the "s" to its "f", and Perfetto binds each end to the span it
    // lands inside, so they must follow the spans they attach to.
    std::size_t arrows = 0UZ;
    if (!chain.empty()) {
        const ChainLinks matched = chainLinks(capture.events, chain, mode);
        for (const LatencyLink& link : matched.links) {
            const std::string flowName = std::format("latency {} ns", link.latencyNs);
            comma();
            out += std::format(R"({{"name":"{}","cat":"latency","ph":"s","id":{},"ts":{:.3f},"pid":{},"tid":{}}})", //
                detail::escape(flowName), arrows, micros(link.producedAt), pid, link.producerWorker);
            comma();
            out += std::format(R"({{"name":"{}","cat":"latency","ph":"f","bp":"e","id":{},"ts":{:.3f},"pid":{},"tid":{}}})", //
                detail::escape(flowName), arrows, micros(link.consumedAt), pid, link.consumerWorker);
            ++arrows;
        }
    }

    out += R"(],"displayTimeUnit":"ns")";
    out += std::format(R"(,"otherData":{{"lostRecords":"{}","categoryMask":"{}","clockCostNs":"{}","latencyFlows":"{}"}}}})", capture.header.lostCount, capture.header.categoryMask, capture.header.clockCostNs, arrows);
    return out;
}

} // namespace gr::trace

#endif // GNURADIO_TRACECATAPULT_HPP
