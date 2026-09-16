#ifndef GNURADIO_TRACEREPORT_HPP
#define GNURADIO_TRACEREPORT_HPP

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <span>
#include <string>
#include <vector>

#include <gnuradio-4.0/Tag.hpp> // property_map
#include <gnuradio-4.0/Trace.hpp>

namespace gr::trace {

/**
 * @brief Tardiness and response-time summary, computed from a capture rather than from a running
 * scheduler.
 *
 * Deliberately not part of `Trace.hpp`: that header may include nothing from `gr`, and this one needs
 * `property_map`. It is also the wrong layer -- emitting records must stay free of any opinion about
 * what they mean.
 *
 * Two sources of truth, and the difference between them is the point. A `deadlineMiss` record is
 * computed live against the job that actually ran. A *reconstruction* pairs executions with the
 * releases that admitted them, per block, in order. Where a capture carries both, the report computes
 * both and compares them: they can only disagree if a released job was discarded without running or
 * the ring dropped records, so a disagreement is evidence rather than noise.
 *
 * It **refuses rather than approximates**. A capture without release records can still count misses,
 * but it cannot say what any job waited, and a half-populated distribution that looks complete is the
 * failure this is written to avoid.
 */

namespace detail {

[[nodiscard]] inline std::uint64_t percentileOf(std::vector<std::uint64_t>& values, double fraction) {
    if (values.empty()) {
        return 0UL;
    }
    const std::size_t index = std::min(values.size() - 1UZ, static_cast<std::size_t>(fraction * static_cast<double>(values.size())));
    std::ranges::nth_element(values, values.begin() + static_cast<std::ptrdiff_t>(index));
    return values[index];
}

} // namespace detail

/**
 * `lostRecords` is what the capture reports having dropped -- `RingStats::lost` in memory, or the
 * header's count for a file. It is not inferred, because a reader cannot tell a short capture from a
 * truncated one, and every figure below is unreliable once anything was evicted.
 */
[[nodiscard]] inline property_map report(std::span<const Event> events, std::uint64_t lostRecords) {
    property_map out;
    out["records"] = events.size();
    out["lost"]    = lostRecords;

    std::vector<std::uint64_t> lateness;
    std::vector<std::uint64_t> liveResponse;
    std::uint64_t              suspect       = 0UL;
    std::uint64_t              saturatedLate = 0UL;
    std::uint64_t              liveMisses    = 0UL;
    // A deque, not a vector: releases are consumed from the front, and `erase(begin())` on a vector
    // is linear per pairing and quadratic per block -- invisible on a test capture, ruinous on a real
    // one.
    std::map<EntityId, std::deque<std::pair<std::uint64_t, std::uint32_t>>> released; // release instant, relative deadline
    std::vector<std::uint64_t>                                              reconstructedResponse;
    std::uint64_t                                                           reconstructedLate = 0UL;
    std::uint64_t                                                           executions        = 0UL;
    bool                                                                    sawRelease        = false;
    bool                                                                    sawWork           = false;

    for (const Event& event : events) {
        switch (event.kind) {
        case Kind::deadlineMiss:
            if ((event.flags & flag::kDeadlineSuspect) != 0U) {
                ++suspect;
                break;
            }
            // A saturated lateness means "at least 4.29 s, by an unknown margin". Counting it *and*
            // admitting it to the distribution is the worst of both: the maximum then reads as exactly
            // the sentinel and the median is dragged by a number that is not a measurement. It is
            // counted as a miss, and `tardiness_samples` says how many the distribution could include.
            ++liveMisses;
            if (event.payload0 == kSaturated) {
                ++saturatedLate;
                break;
            }
            lateness.push_back(event.payload0);
            liveResponse.push_back(event.payload1);
            break;
        case Kind::jobRelease:
            sawRelease = true;
            released[event.entity].emplace_back(event.startNs, event.payload1);
            break;
        case Kind::workEnd: {
            sawWork = true;
            if ((event.flags & flag::kJobBacked) == 0U) {
                break;
            }
            ++executions;
            auto queue = released.find(event.entity);
            if (queue == released.end() || queue->second.empty()) {
                break; // no release to pair with: counted as an execution, contributes no response time
            }
            const auto [releaseNs, relativeDeadlineNs] = queue->second.front();
            queue->second.pop_front();
            const std::uint64_t completion = event.startNs + event.durationNs;
            if (completion < releaseNs) {
                break; // clocks disagree; refuse the sample rather than record a negative wait
            }
            const std::uint64_t response = completion - releaseNs;
            reconstructedResponse.push_back(response);
            if (relativeDeadlineNs != kUnsetDeadline && relativeDeadlineNs != kSaturated && response > relativeDeadlineNs) {
                ++reconstructedLate;
            }
            break;
        }
        default: break;
        }
    }

    out["deadline_misses"]     = liveMisses;
    out["deadline_suspect"]    = suspect;
    out["tardiness_saturated"] = saturatedLate;
    out["tardiness_samples"]   = lateness.size(); // fewer than `deadline_misses` where any saturated
    if (!lateness.empty()) {
        out["tardiness_max_ns"]    = *std::ranges::max_element(lateness);
        out["tardiness_median_ns"] = detail::percentileOf(lateness, 0.5);
    }

    // The refusal, stated as a reason rather than as an absent key a caller might not notice.
    if (!sawRelease) {
        out["response_time"]        = std::string("unavailable");
        out["response_time_reason"] = std::string("the capture carries no release records, so no execution can be attributed to the release that admitted it");
    } else if (!sawWork) {
        out["response_time"]        = std::string("unavailable");
        out["response_time_reason"] = std::string("the capture carries no execution records, so nothing can be matched to a completion");
    } else {
        out["response_time"]          = std::string("reconstructed");
        out["response_time_samples"]  = reconstructedResponse.size();
        out["response_time_unpaired"] = executions - reconstructedResponse.size();
        if (!reconstructedResponse.empty()) {
            out["response_time_max_ns"]    = *std::ranges::max_element(reconstructedResponse);
            out["response_time_median_ns"] = detail::percentileOf(reconstructedResponse, 0.5);
        }
    }

    // Both methods present: they must agree, and a disagreement names a cause rather than splitting
    // the difference. Reconstruction alone cannot see a job discarded before it ran; the live record
    // cannot exist for a job that never ran. That asymmetry is exactly what the comparison detects.
    // Both methods ran, so both can be compared -- including when both found nothing. Agreement on
    // zero is the common healthy outcome and the one a reader most wants confirmed; calling it
    // "unavailable" would hide the check exactly when it succeeded.
    if (sawRelease && sawWork) {
        out["misses_live"]          = liveMisses;
        out["misses_reconstructed"] = reconstructedLate;
        const bool agree            = reconstructedLate == liveMisses;
        out["cross_check"]          = std::string(agree ? "agree" : "disagree");
        if (!agree) {
            out["cross_check_reason"] = std::string("live and reconstructed miss counts differ; a released job was discarded before running, or records were lost");
        }
    } else {
        out["cross_check"] = std::string("unavailable");
    }

    if (lostRecords > 0UL) {
        out["reliable"]        = false;
        out["reliable_reason"] = std::string("records were evicted before the capture was read, so releases may be missing and every figure here understates");
    } else {
        out["reliable"] = true;
    }
    return out;
}

} // namespace gr::trace

#endif // GNURADIO_TRACEREPORT_HPP
