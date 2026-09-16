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
#include <format>
#include <functional>
#include <limits>
#include <ranges>
#include <utility>

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
/**
 * @brief The marginal-cost fit, and the three gates that decide whether it may be reported at all.
 *
 * `cost ≈ I + Δ · work`: an invocation costs a fixed amount plus a per-item amount. The intercept is
 * framework overhead per call, the slope is the block's actual work per item, and the pair is what a
 * batch optimiser and a `wcet_estimate` both want.
 *
 * **The failure this exists to prevent.** A graph run at one batch size puts every sample in one
 * bucket, and a line through one point has whatever slope you care to give it. The fit feeds
 * admission and utilisation decisions, so a confident-looking slope from one batch size is invented
 * data entering a scheduling decision — not a cosmetic problem.
 *
 * So the verdict carries **no slope at all** when it is not identifiable, rather than a slope beside a
 * warning. A number present in a report will be read by something; the only reliable way to stop that
 * is for it not to be there.
 *
 * The regression uses **per-bucket minima against bucket centroids**, not every sample: the minimum
 * is the least preemption-polluted estimate of the true floor (RT §3.8.3), and the centroid is where
 * that bucket's work actually sat rather than where its edge is.
 */
struct Fit {
    bool        identifiable   = false;
    double      interceptNs    = 0.0; /// `I` — per-invocation cost independent of the work done
    double      slopeNsPerItem = 0.0; /// `Δ` — marginal cost per item
    double      residualRms    = 0.0; /// relative to the mean observed cost; the fit's own self-assessment
    std::size_t points         = 0UZ; /// populated buckets the fit had to work with
    double      spanRatio      = 0.0; /// largest centroid over smallest; a narrow span cannot pin a slope
    std::string reason;               /// why it is not identifiable, when it is not
};

/// A slope is reported only when all three hold. Each threshold answers a specific way of being
/// wrong, rather than being a general-purpose confidence number.
inline constexpr std::size_t kMinFitPoints   = 3UZ;  /// two points fit a line exactly, leaving no residual to disbelieve
inline constexpr double      kMinFitSpan     = 4.0;  /// over a narrow range the slope is swamped by the intercept's error
inline constexpr double      kMaxFitResidual = 0.25; /// a fit that does not describe its own inputs describes nothing

[[nodiscard]] inline Fit fitCost(const BlockTiming& timing) {
    Fit fit;

    std::vector<std::pair<double, double>> points; // centroid, floor cost
    for (const Bucket& bucket : timing.buckets) {
        if (bucket.count > 0UL) {
            points.emplace_back(bucket.centroid(), static_cast<double>(bucket.minNs));
        }
    }
    fit.points = points.size();

    if (fit.points < kMinFitPoints) {
        fit.reason = std::format("only {} populated work bucket(s); a slope needs at least {}, since two points fit a line exactly", fit.points, kMinFitPoints);
        return fit;
    }

    // Over **positive** centroids only. A zero-work invocation is a real and valuable observation --
    // it measures the intercept with no slope to subtract, and an all-asynchronous-input block
    // produces one by returning OK having consumed nothing -- but its centroid is zero, and a ratio
    // against zero is not a narrow span, it is no span at all. Letting it set `spanRatio` to zero
    // refused the entire block's fit on the strength of one sample. The point itself stays in the
    // regression below, where it belongs.
    std::vector<double> positiveCentroids;
    for (const auto& [centroid, cost] : points) {
        if (centroid > 0.0) {
            positiveCentroids.push_back(centroid);
        }
    }
    if (positiveCentroids.size() < 2UZ) {
        fit.reason = std::format("only {} bucket(s) with a non-zero work count; a slope needs at least two to span", positiveCentroids.size());
        return fit;
    }
    const auto [smallest, largest] = std::ranges::minmax(positiveCentroids);
    fit.spanRatio                  = largest / smallest;
    if (fit.spanRatio < kMinFitSpan) {
        fit.reason = std::format("work counts span only {:.2f}x, below the {:.0f}x a slope can be separated from the intercept over", fit.spanRatio, kMinFitSpan);
        return fit;
    }

    // Ordinary least squares over the bucket floors.
    const auto   n     = static_cast<double>(points.size());
    const double sumX  = std::ranges::fold_left(points | std::views::transform([](const auto& p) { return p.first; }), 0.0, std::plus{});
    const double sumY  = std::ranges::fold_left(points | std::views::transform([](const auto& p) { return p.second; }), 0.0, std::plus{});
    const double meanX = sumX / n;
    const double meanY = sumY / n;

    double covariance = 0.0;
    double varianceX  = 0.0;
    for (const auto& [x, y] : points) {
        covariance += (x - meanX) * (y - meanY);
        varianceX += (x - meanX) * (x - meanX);
    }
    if (varianceX <= 0.0) {
        fit.reason = "every populated bucket has the same centroid, so no slope exists to find";
        return fit;
    }

    fit.slopeNsPerItem = covariance / varianceX;
    fit.interceptNs    = meanY - fit.slopeNsPerItem * meanX;

    double squaredError = 0.0;
    for (const auto& [x, y] : points) {
        const double predicted = fit.interceptNs + fit.slopeNsPerItem * x;
        squaredError += (y - predicted) * (y - predicted);
    }
    fit.residualRms = meanY > 0.0 ? std::sqrt(squaredError / n) / meanY : 0.0;
    if (fit.residualRms > kMaxFitResidual) {
        fit.reason = std::format("the line misses its own inputs by {:.1f}% on average, above the {:.0f}% a fit is trusted within", 100.0 * fit.residualRms, 100.0 * kMaxFitResidual);
        return fit;
    }

    // A negative intercept or slope is arithmetically possible and physically meaningless: an
    // invocation cannot cost less than nothing, nor an item save time. It means the model does not
    // describe this block, which is a refusal rather than a number to clamp.
    if (fit.interceptNs < 0.0 || fit.slopeNsPerItem < 0.0) {
        fit.reason = std::format("the fit is physically impossible: intercept {:.1f} ns, slope {:.4f} ns/item", fit.interceptNs, fit.slopeNsPerItem);
        return fit;
    }

    fit.identifiable = true;
    return fit;
}

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
        block["invocations"]       = timing.overall.count;
        block["acet_ns"]           = timing.overall.mean;
        block["jitter_ns"]         = timing.overall.stddev();
        block["wcet_ns"]           = static_cast<std::uint64_t>(timing.overall.maxNs);
        block["fastest_ns"]        = static_cast<std::uint64_t>(timing.overall.minNs);
        block["work_min"]          = static_cast<std::uint64_t>(timing.workMin);
        block["work_max"]          = static_cast<std::uint64_t>(timing.workMax);
        block["buckets_populated"] = timing.populatedBuckets();

        // The slope and intercept keys are **absent** unless the fit is identifiable, rather than
        // present beside a low-confidence flag. A consumer reading a `property_map` will take a
        // number it finds; the only reliable way to stop it taking one that means nothing is for the
        // key not to exist. `wcet_estimate_ns` is emitted as an integer beside the float a caller
        // might want, because a seconds-valued float truncates -- the same hazard that silently
        // floors a sub-microsecond relative deadline to zero.
        const Fit fit           = fitCost(timing);
        block["fit"]            = std::string(fit.identifiable ? "identifiable" : "low-confidence");
        block["fit_points"]     = static_cast<std::uint64_t>(fit.points);
        block["fit_span_ratio"] = fit.spanRatio;
        if (fit.identifiable) {
            block["invocation_cost_ns"] = fit.interceptNs;
            block["item_cost_ns"]       = fit.slopeNsPerItem;
            block["fit_residual"]       = fit.residualRms;
            block["wcet_estimate_ns"]   = static_cast<std::uint64_t>(timing.overall.maxNs);
            block["wcet_estimate_s"]    = static_cast<double>(timing.overall.maxNs) * 1e-9;
        } else {
            block["fit_reason"] = fit.reason;
        }

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
