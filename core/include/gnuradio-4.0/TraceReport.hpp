#ifndef GNURADIO_TRACEREPORT_HPP
#define GNURADIO_TRACEREPORT_HPP

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <span>
#include <string>
#include <vector>

#include <array>
#include <bit>
#include <cmath>
#include <limits>

#include <gnuradio-4.0/Tag.hpp> // property_map
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp> // LoadedEntity, for names
#include <gnuradio-4.0/WorkStatus.hpp>

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

/// `bit_width(0) = 0` through `bit_width(0xFFFFFFFD) = 32`. Sized for the whole payload range rather
/// than for a plausible one: `max_work_items` defaults to `SIZE_MAX`, so a block may legitimately
/// report billions of items in a single invocation, and a shorter table would fold every large batch
/// into its top bucket -- collapsing the very span the fit needs and turning a well-conditioned
/// measurement into a refused one, silently.
inline constexpr std::size_t kWorkBuckets = 33UZ;

/**
 * One log-spaced bucket of invocation costs.
 *
 * The **minimum** is the clean cost estimate: least polluted by preemption and page faults, which is
 * why RT §3.8.3 specifies it. It is also biased low, increasingly so as `count` falls -- the minimum
 * of three samples is a worse estimate of a floor than the minimum of three thousand -- so `count`
 * travels with it and a consumer can see which centroids are thinly supported. No bias correction is
 * attempted: that needs distributional assumptions this layer has no business making.
 *
 * Mean and M2 are Welford, updated in O(1) and never storing a sample.
 */
struct Bucket {
    std::uint64_t count   = 0UL;
    std::uint64_t workSum = 0UL; /// for the centroid: the fit regresses cost against this, not the bucket edge
    std::uint32_t minNs   = std::numeric_limits<std::uint32_t>::max();
    std::uint32_t maxNs   = 0U;
    double        mean    = 0.0;
    double        m2      = 0.0;

    void add(std::uint32_t durationNs, std::uint32_t performedWork) noexcept {
        ++count;
        workSum += performedWork;
        minNs = std::min(minNs, durationNs);
        maxNs = std::max(maxNs, durationNs);

        const double sample = static_cast<double>(durationNs);
        const double delta  = sample - mean;
        mean += delta / static_cast<double>(count);
        m2 += delta * (sample - mean);
    }

    [[nodiscard]] double stddev() const noexcept { return count > 1UL ? std::sqrt(m2 / static_cast<double>(count - 1UL)) : 0.0; }
    [[nodiscard]] double centroid() const noexcept { return count > 0UL ? static_cast<double>(workSum) / static_cast<double>(count) : 0.0; }
};

/// Per block: the buckets, plus the same statistics over every admitted invocation.
struct BlockTiming {
    std::array<Bucket, kWorkBuckets> buckets{};
    Bucket                           overall{};
    std::uint32_t                    workMin = std::numeric_limits<std::uint32_t>::max();
    std::uint32_t                    workMax = 0U;

    [[nodiscard]] std::size_t populatedBuckets() const noexcept {
        return static_cast<std::size_t>(std::ranges::count_if(buckets, [](const Bucket& b) { return b.count > 0UL; }));
    }
};

/**
 * Folds records into per-block timing, one record at a time.
 *
 * Deliberately a *fold*, not a loop over a span. In T3 the source is a capture already in memory; in
 * streaming mode it will be a drain thread with no span to iterate. Writing the update per record
 * means that change is a new caller rather than a new accumulator.
 *
 * **Two filters are correctness requirements, not refinements.** `computePerformedWork()` returns
 * **0 for any status other than OK**, so a `DONE` record reports no work for an invocation that may
 * have done a great deal; admitting it puts a `(0, large)` point into the regression, which is a
 * direct attack on the intercept. And a saturated payload or duration is a sentinel, not a
 * measurement -- reading one as a number is how a report invents data. Both rejections are counted,
 * so a capture that is mostly rejected says so rather than reporting confidently on a tenth of itself.
 */
struct TimingAccumulator {
    std::map<EntityId, BlockTiming> blocks;
    std::uint64_t                   admitted          = 0UL;
    std::uint64_t                   rejectedStatus    = 0UL;
    std::uint64_t                   rejectedSaturated = 0UL;
    std::uint64_t                   rejectedNoEntity  = 0UL;

    void fold(const Event& event) {
        if (event.kind != Kind::workEnd) {
            return; // not a cost sample at all; silently skipped rather than counted as a rejection
        }
        if (event.status != static_cast<std::int8_t>(std::to_underlying(gr::work::Status::OK))) {
            ++rejectedStatus;
            return;
        }
        if (event.payload1 == kSaturated || event.durationNs == kSaturated) {
            ++rejectedSaturated;
            return;
        }
        if (event.entity == kNoEntity) {
            ++rejectedNoEntity; // cannot be attributed, so cannot contribute to any block's cost
            return;
        }

        BlockTiming& timing = blocks[event.entity];
        const auto   index  = static_cast<std::size_t>(std::bit_width(event.payload1));
        timing.buckets[index].add(event.durationNs, event.payload1);
        timing.overall.add(event.durationNs, event.payload1);
        timing.workMin = std::min(timing.workMin, event.payload1);
        timing.workMax = std::max(timing.workMax, event.payload1);
        ++admitted;
    }
};

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
/// The name a report shows for a block, or a stable stand-in when the capture carries no table.
[[nodiscard]] inline std::string entityLabel(EntityId id, std::span<const LoadedEntity> entities) {
    const auto found = std::ranges::find_if(entities, [id](const LoadedEntity& e) { return e.id == id; });
    return found != entities.end() ? found->uniqueName : std::format("entity {}", id);
}

/**
 * Per-block execution cost: average, jitter and observed worst case.
 *
 * These need no regression and are **valid from a single batch size**, which matters because the
 * `(I_v, Δ_v)` fit is not (§3.5 of the design note). Refusing a slope must not suppress the
 * statistics that remain sound, so they are computed here and the fit is layered on separately.
 */
[[nodiscard]] inline property_map timingReport(std::span<const Event> events, std::span<const LoadedEntity> entities = {}) {
    TimingAccumulator accumulator;
    for (const Event& event : events) {
        accumulator.fold(event);
    }

    property_map out;
    out["invocations"]        = accumulator.admitted;
    out["rejected_status"]    = accumulator.rejectedStatus;
    out["rejected_saturated"] = accumulator.rejectedSaturated;
    out["rejected_no_entity"] = accumulator.rejectedNoEntity;
    out["blocks_seen"]        = accumulator.blocks.size();

    property_map perBlock;
    for (const auto& [entity, timing] : accumulator.blocks) {
        property_map block;
        block["invocations"]                    = timing.overall.count;
        block["acet_ns"]                        = timing.overall.mean;
        block["jitter_ns"]                      = timing.overall.stddev();
        block["wcet_ns"]                        = static_cast<std::uint64_t>(timing.overall.maxNs);
        block["fastest_ns"]                     = static_cast<std::uint64_t>(timing.overall.minNs);
        block["work_min"]                       = static_cast<std::uint64_t>(timing.workMin);
        block["work_max"]                       = static_cast<std::uint64_t>(timing.workMax);
        block["buckets_populated"]              = timing.populatedBuckets();
        perBlock[entityLabel(entity, entities)] = std::move(block);
    }
    out["blocks"] = std::move(perBlock);
    return out;
}

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
