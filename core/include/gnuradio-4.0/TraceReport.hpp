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
#include <numeric>
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
/**
 * @brief Per-block produce/consume ratio, derived from what a capture observed.
 *
 * The acceptance criterion is scoped to graphs with *predictable* ratios, so the ratio is the gate
 * rather than an output: a block whose ratio is not constant across the capture is not predictable,
 * and a latency derived from the average of something that is not constant is a fabrication.
 *
 * Derived from observation rather than from `input_chunk_size : output_chunk_size`, which is a
 * declaration of intent. The declared value is not in a capture today -- `EntityRecord` carries
 * names, worker and port counts and nothing else -- so the cross-check the design asks for is not
 * available here; observation alone is the stricter of the two in any case, because a block can
 * declare one ratio and exhibit another.
 */
struct RatioEstimate {
    bool        stable      = false;
    double      outPerIn    = 0.0; /// for reporting only -- never for the coordinate walk, see below
    std::size_t invocations = 0UZ;
    std::string reason; /// why it was refused; empty when stable

    /// The same ratio as an exact fraction, reduced. The walk in `chainLinks` **must** use these
    /// rather than `outPerIn`: a sink coordinate normally lands exactly on a source batch boundary,
    /// and a double division by a non-dyadic ratio lands a fraction of a sample *below* it, which
    /// silently matches the previous batch and reports a latency a whole batch period out.
    std::uint64_t outTotal = 0UL;
    std::uint64_t inTotal  = 0UL;
};

/// Productive `workExact` records for one block, in the order they were emitted.
[[nodiscard]] inline std::vector<Event> exactRecordsFor(std::span<const Event> events, EntityId entity) {
    std::vector<Event> records;
    for (const Event& event : events) {
        if (event.kind == Kind::workExact && event.entity == entity && (event.payload0 != 0U || event.payload1 != 0U)) {
            records.push_back(event);
        }
    }
    std::ranges::stable_sort(records, {}, &Event::startNs);
    return records;
}

inline constexpr double kRatioTolerance = 1e-9; /// the counts are integers, so a stable ratio is exact

[[nodiscard]] inline RatioEstimate ratioOf(std::span<const Event> events, EntityId entity) {
    const std::vector<Event> records = exactRecordsFor(events, entity);
    RatioEstimate            estimate{.invocations = records.size()};
    if (records.empty()) {
        estimate.reason = "no productive block-side records";
        return estimate;
    }

    std::uint64_t totalIn  = 0UL;
    std::uint64_t totalOut = 0UL;
    for (const Event& record : records) {
        totalIn += record.payload0;
        totalOut += record.payload1;
        if (record.payload0 == kSaturated || record.payload1 == kSaturated) {
            estimate.reason = "a count saturated, so the ratio cannot be recovered";
            return estimate;
        }
    }
    if (totalIn == 0UL) {
        estimate.reason = "no input consumed: a source has no ratio to derive";
        return estimate;
    }

    // The **continuity check**, and the reason it is here rather than in the latency function: a
    // block with a stride consumes samples through `inputSkipBefore` that appear in no count field,
    // so its positions advance by more than it reports processing. Positions stay correct; counts
    // stop accounting for them. A consumer that accumulated counts instead of reading positions
    // would drift silently, so a capture showing the divergence is refused outright.
    for (std::size_t i = 1UZ; i < records.size(); ++i) {
        const std::uint32_t previous = records[i - 1UZ].payload2;
        const std::uint32_t current  = records[i].payload2;
        if (current < previous) {
            estimate.reason = "stream position went backwards: a 32-bit wrap or a discontinuity, and the two are indistinguishable";
            return estimate;
        }
        if (current - previous != records[i - 1UZ].payload0) {
            estimate.reason = "positions advance by more than the counts report: samples were consumed outside them, which is what a stride does";
            return estimate;
        }
    }

    const double ratio = static_cast<double>(totalOut) / static_cast<double>(totalIn);
    for (const Event& record : records) {
        if (record.payload0 == 0U) {
            estimate.reason = "an invocation produced output from no input, so no ratio describes it";
            return estimate;
        }
        const double perInvocation = static_cast<double>(record.payload1) / static_cast<double>(record.payload0);
        if (std::abs(perInvocation - ratio) > kRatioTolerance * std::max(1.0, ratio)) {
            estimate.reason = std::format("ratio varies across the capture ({:.6g} against {:.6g} overall)", perInvocation, ratio);
            return estimate;
        }
    }

    const std::uint64_t divisor = std::gcd(totalIn, totalOut);
    estimate.stable             = true;
    estimate.outPerIn           = ratio;
    estimate.outTotal           = divisor == 0UL ? totalOut : totalOut / divisor;
    estimate.inTotal            = divisor == 0UL ? totalIn : totalIn / divisor;
    return estimate;
}

/**
 * @brief Source-to-sink latency for one chain, by data provenance.
 *
 * The chain is supplied rather than discovered: **a capture carries no topology**. `EntityRecord`
 * has no edge list and no record kind describes one, so nothing in a `.gr4trace` file says which
 * block feeds which. Inferring it from names or port counts would be a guess dressed as a
 * measurement.
 *
 * Every hop must have a stable ratio, and the whole chain is refused if any hop does not. Partial
 * latency for the hops that happen to qualify would be a number whose meaning depends on which
 * blocks were excluded.
 */
/// One sink invocation matched to the source batch that produced the samples it read.
struct LatencyLink {
    std::uint64_t producedAt     = 0UL; /// when the source batch finished
    std::uint64_t consumedAt     = 0UL; /// when the sink invocation that read it finished
    std::uint64_t latencyNs      = 0UL;
    EntityId      producer       = kNoEntity;
    EntityId      consumer       = kNoEntity;
    std::uint8_t  producerWorker = 0U;
    std::uint8_t  consumerWorker = 0U;
};

/**
 * @brief How a sink invocation is matched to the source batch that fed it.
 *
 * `streamPosition` is the default and assumes nothing: it walks recorded stream coordinates back
 * through each intermediate block's observed ratio, and refuses any chain it cannot reconstruct
 * exactly (§ `ratioOf`).
 *
 * `invocationLockstep` assumes instead that **one invocation of a block causes exactly one
 * invocation of each of its successors**, so the n-th productive invocation of the source is the one
 * whose data the n-th productive invocation of the sink read. Flowgraphs are sometimes configured
 * that way deliberately, because it tightens analytical end-to-end latency bounds at no runtime
 * cost, and where that holds the latency is a subtraction between two records found by counting.
 *
 * Note this is **not** a claim about sample rates. A 4:1 decimator is in lockstep with its source
 * whenever each of its invocations consumes exactly one produced batch; the sample ratio is 4:1 and
 * the invocation ratio is 1:1. Naming it for the invocations is deliberate.
 *
 * What it buys: it reads no positions and needs no ratios, so it reconstructs chains the default
 * mode must decline — a block with a stride, an unstable ratio, or per-port rates that one position
 * field cannot describe. What it costs: the assumption is the user's, not the capture's, so it is
 * never the default and is checked as far as a capture allows (see `chainLinks`).
 */
enum class LatencyMode { streamPosition, invocationLockstep };

struct ChainLinks {
    bool                     computed = false;
    std::string              reason;
    std::vector<LatencyLink> links;
    /// Invocations the capture could not pair, and so could not time. The end it is missing differs
    /// by mode -- lockstep leaves a *tail* of source batches the sink had not yet read, the position
    /// walk leaves a *head* of sink invocations whose producer predates the capture -- but in both
    /// the actionable fact is the same: this many invocations are absent from the distribution.
    std::size_t unmatched = 0UZ;
};

struct ChainLatency {
    bool          computed = false;
    std::string   reason;
    std::uint64_t medianNs  = 0UL;
    std::uint64_t minNs     = 0UL;
    std::uint64_t maxNs     = 0UL;
    std::size_t   samples   = 0UZ;
    std::size_t   unmatched = 0UZ; /// invocations that could not be paired, and so are absent below
};

[[nodiscard]] inline ChainLinks chainLinks(std::span<const Event> events, std::span<const EntityId> chain, LatencyMode mode = LatencyMode::streamPosition) {
    ChainLinks result;
    if (chain.size() < 2UZ) {
        result.reason = "a chain needs at least a source and a sink";
        return result;
    }

    if (mode == LatencyMode::invocationLockstep) {
        const std::vector<Event> sourceInvocations = exactRecordsFor(events, chain.front());
        const std::vector<Event> sinkInvocations   = exactRecordsFor(events, chain.back());
        if (sourceInvocations.empty() || sinkInvocations.empty()) {
            result.reason = "the chain's ends left no productive records";
            return result;
        }

        // The assumption is the caller's, but it is not beyond checking. Under lockstep the sink
        // cannot run productively more often than the source did: it would be consuming batches that
        // were never produced. A capture showing that is describing a graph the caller has
        // misunderstood, and pairing anyway would yield confident nonsense.
        if (sinkInvocations.size() > sourceInvocations.size()) {
            result.reason = std::format("not in lockstep: the sink ran {} productive invocations against the source's {}", sinkInvocations.size(), sourceInvocations.size());
            return result;
        }

        // Only the ends matter. The intermediate blocks are what the assumption is *about*, so
        // reading their records would be assuming and verifying the same thing.
        for (std::size_t i = 0UZ; i < sinkInvocations.size(); ++i) {
            const std::uint64_t producedAt = sourceInvocations[i].startNs + sourceInvocations[i].durationNs;
            const std::uint64_t consumedAt = sinkInvocations[i].startNs + sinkInvocations[i].durationNs;
            if (consumedAt < producedAt) {
                result.reason = std::format("not in lockstep: sink invocation {} completed before the source invocation it would be paired with", i);
                return result;
            }
            result.links.push_back(LatencyLink{.producedAt = producedAt,
                .consumedAt                                = consumedAt,
                .latencyNs                                 = consumedAt - producedAt, //
                .producer                                  = sourceInvocations[i].entity,
                .consumer                                  = sinkInvocations[i].entity, //
                .producerWorker                            = sourceInvocations[i].workerId,
                .consumerWorker                            = sinkInvocations[i].workerId});
        }
        // A tail is expected rather than wrong: a capture that stops while batches are in flight
        // leaves source invocations with no sink invocation yet. Reported, not hidden.
        result.unmatched = sourceInvocations.size() - sinkInvocations.size();
        result.computed  = true;
        return result;
    }

    // Every intermediate hop's ratio, composed into one exact fraction. Walking a sink coordinate
    // back to the source means multiplying by `in/out` at each hop, and the product of those is a
    // single rational -- so it is accumulated as one, reduced at each step to keep it small.
    //
    // The **intermediate** blocks only: `chain.front()` is the source, whose ratio describes nothing
    // upstream of it, and `chain.back()` is the sink, whose output nobody downstream reads.
    std::uint64_t inFactor  = 1UL; // numerator of the composed in/out
    std::uint64_t outFactor = 1UL;
    for (std::size_t hop = 1UZ; hop + 1UZ < chain.size(); ++hop) {
        const RatioEstimate estimate = ratioOf(events, chain[hop]);
        if (!estimate.stable) {
            result.reason = std::format("entity {} has no stable ratio: {}", chain[hop], estimate.reason);
            return result;
        }
        if (estimate.outTotal == 0UL) {
            result.reason = std::format("entity {} consumes without producing, so the chain does not carry data through", chain[hop]);
            return result;
        }
        // Checked *before* multiplying, not after: a product that has already wrapped tells you
        // nothing about what it should have been.
        const std::uint64_t ceiling = std::numeric_limits<std::uint64_t>::max();
        if (estimate.inTotal > ceiling / inFactor || estimate.outTotal > ceiling / outFactor) {
            result.reason = "the chain's composed ratio overflows an exact fraction";
            return result;
        }
        inFactor *= estimate.inTotal;
        outFactor *= estimate.outTotal;
        const std::uint64_t divisor = std::gcd(inFactor, outFactor);
        if (divisor > 1UL) {
            inFactor /= divisor;
            outFactor /= divisor;
        }
        // Positions are 32-bit, so `position * inFactor` must stay inside 64 bits for the walk below
        // to be exact. A chain whose composed numerator threatens that is refused rather than walked
        // in floating point, which is the thing this fraction exists to avoid.
        if (inFactor > (ceiling >> 32U)) {
            result.reason = "the chain's composed ratio is too large to walk exactly";
            return result;
        }
    }

    const std::vector<Event> sourceRecords = exactRecordsFor(events, chain.front());
    const std::vector<Event> sinkRecords   = exactRecordsFor(events, chain.back());
    if (sourceRecords.empty() || sinkRecords.empty()) {
        result.reason = "the chain's ends left no productive records";
        return result;
    }

    // The search below is a binary one, which needs the source's positions ordered. They are, for any
    // capture worth reconstructing -- a source's output position advances with time -- so a sequence
    // that is not ordered has wrapped or is discontinuous, and that is refused for the same reason
    // `ratioOf` refuses it: a wrap and a gap are indistinguishable in 32 bits.
    //
    // Linear search here would be O(sinks x sources). The ring holds 65 536 records per thread by
    // default and its ceiling is configurable to 4 GiB, so that product reaches the billions on a
    // capture the layer is explicitly built to take.
    for (std::size_t i = 1UZ; i < sourceRecords.size(); ++i) {
        if (sourceRecords[i].payload2 < sourceRecords[i - 1UZ].payload2) {
            result.reason = "the source's stream positions do not advance: a 32-bit wrap or a discontinuity";
            return result;
        }
    }

    for (const Event& sinkRecord : sinkRecords) {
        // The sink's recorded position is its *input* coordinate, which is the preceding block's
        // output coordinate. Dividing by each intermediate block's ratio walks that coordinate back
        // to the source's output coordinate: a block producing `r` per input means output position
        // `p` came from input position `p / r`.
        //
        // The **intermediate** blocks only. The sink's own ratio describes what it would emit, which
        // no one downstream reads, and the source's describes nothing upstream of it.
        // Exact, in integers. `inFactor / outFactor` is the composed in-per-out of every block
        // between the ends, so this is the source output coordinate the sink's first sample came
        // from. Floating point here is what put the answer a whole batch out.
        const std::uint64_t coordinate = (static_cast<std::uint64_t>(sinkRecord.payload2) * inFactor) / outFactor;

        // The last batch that began at or before this coordinate is the only one that can contain
        // it, because the batches are contiguous and ordered.
        const auto after = std::ranges::partition_point(sourceRecords, [coordinate](const Event& candidate) { return static_cast<std::uint64_t>(candidate.payload2) <= coordinate; });
        if (after == sourceRecords.begin()) {
            ++result.unmatched; // produced before the capture opened; not an error, but not hidden either
            continue;
        }
        const Event&        candidate = *std::prev(after);
        const std::uint64_t begin     = candidate.payload2;
        if (coordinate >= begin + candidate.payload1) {
            ++result.unmatched; // falls in a gap between batches, so nothing in this capture produced it
            continue;
        }
        const Event*        producer   = &candidate;
        const std::uint64_t producedAt = producer->startNs + producer->durationNs;
        const std::uint64_t consumedAt = sinkRecord.startNs + sinkRecord.durationNs;
        if (consumedAt < producedAt) {
            result.reason = "a sample was consumed before it was produced: the capture's clock or its positions are inconsistent";
            return result;
        }
        result.links.push_back(LatencyLink{.producedAt = producedAt,
            .consumedAt                                = consumedAt,
            .latencyNs                                 = consumedAt - producedAt, //
            .producer                                  = producer->entity,
            .consumer                                  = sinkRecord.entity,
            .producerWorker                            = producer->workerId,
            .consumerWorker                            = sinkRecord.workerId});
    }

    if (result.links.empty()) {
        result.reason = "no sink invocation could be matched to a source batch within the capture";
        return result;
    }
    result.computed = true;
    return result;
}

/**
 * @brief Source-to-sink latency for one chain, by data provenance.
 *
 * The chain is supplied rather than discovered: **a capture carries no topology**. `EntityRecord`
 * has no edge list and no record kind describes one, so nothing in a `.gr4trace` file says which
 * block feeds which. Inferring it from names or port counts would be a guess dressed as a
 * measurement.
 *
 * Every hop must have a stable ratio, and the whole chain is refused if any hop does not. Partial
 * latency for the hops that happen to qualify would be a number whose meaning depends on which
 * blocks were excluded.
 */
[[nodiscard]] inline ChainLatency chainLatency(std::span<const Event> events, std::span<const EntityId> chain, LatencyMode mode = LatencyMode::streamPosition) {
    const ChainLinks matched = chainLinks(events, chain, mode);
    ChainLatency     latency{.computed = matched.computed, .reason = matched.reason};
    if (!matched.computed) {
        return latency;
    }
    std::vector<std::uint64_t> values;
    values.reserve(matched.links.size());
    for (const LatencyLink& link : matched.links) {
        values.push_back(link.latencyNs);
    }
    std::ranges::sort(values);
    latency.samples   = values.size();
    latency.unmatched = matched.unmatched;
    latency.minNs     = values.front();
    latency.maxNs     = values.back();
    latency.medianNs  = values[values.size() / 2UZ];
    return latency;
}

/**
 * Whether an entity is a fused group rather than a single block.
 *
 * A `Merge<>` unit is one `BlockModel` and therefore one identity with one set of markers; its
 * internal stages are invisible by construction. That is correct -- the merged group is the
 * schedulable grain -- but a report that prints the group under a block's heading invites exactly
 * the wrong conclusion about where the time went.
 *
 * Keyed on the recorded type name because nothing else in a capture distinguishes the two: the trace
 * layer sees a `BlockModel`, and fusion is a compile-time property of the type behind it. `Merge` and
 * `FeedbackMerge` are aliases for `gr::MergeByIndex` and `gr::FeedbackMergeByIndex`, and an alias
 * does not survive into a type name, so the framework spelling is what a capture actually holds.
 */
[[nodiscard]] inline bool isFusedGroup(std::string_view typeName) noexcept { return typeName.find("MergeByIndex<") != std::string_view::npos; }

[[nodiscard]] inline std::string entityTypeName(EntityId id, std::span<const LoadedEntity> entities) {
    const auto found = std::ranges::find_if(entities, [id](const LoadedEntity& e) { return e.id == id; });
    return found != entities.end() ? found->typeName : std::string{};
}

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

        // Which side of the boundary the counts came from, always stated. The two disagree for a
        // resampling block -- `performed_work` is one number under an affine map, `(in, out)` is two
        // exact ones -- so a reader comparing two reports has no way to know they used the same rule
        // unless each says so.
        const RatioEstimate ratio    = ratioOf(events, entity);
        const bool          hasExact = ratio.invocations > 0UZ;
        block["sample_source"]       = std::string(hasExact ? "block-side" : "scheduler-side");
        if (hasExact) {
            block["exact_invocations"] = static_cast<std::uint64_t>(ratio.invocations);
            block["ratio_stable"]      = ratio.stable;
            if (ratio.stable) {
                block["ratio_out_per_in"] = ratio.outPerIn;
            } else {
                block["ratio_reason"] = ratio.reason;
            }
        }

        // Named as what it is. A fused group's internal stages left no markers, so a reader must not
        // take these numbers for one block's.
        const std::string typeName = entityTypeName(entity, entities);
        block["grain"]             = std::string(isFusedGroup(typeName) ? "fused-group" : "block");
        if (isFusedGroup(typeName)) {
            block["grain_note"] = std::string("stages inside a fused group emit no markers; these figures cover the group as a whole");
        }

        perBlock[entityLabel(entity, entities)] = std::move(block);
    }
    out["blocks"] = std::move(perBlock);
    return out;
}

/**
 * @brief How one worker thread spent its life, decomposed.
 *
 * Answers "is this flowgraph limited by its blocks or by its scheduler?" — the time a worker spent
 * inside block `work()` code, against the time it was alive.
 *
 * **Terms** are disjoint spans of the lifetime that sum to it: `messagePhaseNs`, `sweepNs`, `idleNs`
 * and the `unaccountedNs` residual, with block execution nested inside the sweep. **Metrics** are
 * ratios between terms and are never added to anything.
 *
 * `occupancy` is block time over the whole life; `utilisation` excludes time the worker deliberately
 * waited, so a starved worker reads as 0 % occupied but can still be fully utilised over the little
 * time it was trying to work. Both are wall-clock fractions. `utilisation` is **not** block time
 * over CPU time: that mixes a wall numerator with a CPU denominator and exceeds 1 as soon as the
 * worker is preempted mid-`work()`, which is precisely when a reader most needs it to be sound.
 */
struct WorkerUtilisation {
    std::uint8_t workerId = 0U;
    bool         computed = false;
    std::string  reason; /// why not, when `!computed`; carries the offending numbers

    std::uint64_t lifetimeNs        = 0UL;
    std::uint64_t blockProductiveNs = 0UL;
    std::uint64_t blockProbeNs      = 0UL;
    std::uint64_t sweepNs           = 0UL;
    std::uint64_t messagePhaseNs    = 0UL;
    std::uint64_t idleNs            = 0UL;
    std::uint64_t unaccountedNs     = 0UL;

    /// On-core time, from the worker's own `CLOCK_THREAD_CPUTIME_ID` delta. Absent rather than zero
    /// where the platform has no such clock: a zero would read as total preemption.
    bool          hasThreadCpuTime  = false;
    std::uint64_t threadCpuNs       = 0UL;
    std::uint64_t preemptionNs      = 0UL;   /// off-core involuntarily: lifetime - cpu - idle, clamped
    bool          preemptionClamped = false; /// the clamp fired, so an assumption behind it bent

    double occupancy      = 0.0;   /// (productive + probe) / lifetime
    double utilisation    = 0.0;   /// (productive + probe) / (lifetime - idle); 0 when never active
    bool   hasUtilisation = false; /// false when the worker only ever idled, so the ratio is undefined

    [[nodiscard]] std::uint64_t blockNs() const noexcept { return blockProductiveNs + blockProbeNs; }
};

namespace detail {

/// Sums the durations of one kind for one worker, reporting whether any of them had saturated. A
/// saturated duration makes the sum a *lower bound*, which cannot be divided into a percentage.
struct DurationSum {
    std::uint64_t total     = 0UL;
    bool          saturated = false;
    std::size_t   count     = 0UZ;
};

[[nodiscard]] inline DurationSum sumDurations(std::span<const Event> events, std::uint8_t worker, Kind kind) {
    DurationSum sum;
    for (const Event& event : events) {
        if (event.kind != kind || event.workerId != worker) {
            continue;
        }
        ++sum.count;
        sum.saturated = sum.saturated || event.durationNs == kSaturated;
        sum.total += event.durationNs;
    }
    return sum;
}

} // namespace detail

/// Microseconds, because the payload word is 32 bits and nanoseconds would overflow it after 4.295 s.
inline constexpr std::uint64_t kThreadCpuScaleNs = 1000UL;

/**
 * One entry per worker that emitted anything, whether or not its figures could be computed.
 *
 * Refuses **per worker** rather than per capture: one thread with a mangled lifetime says nothing
 * about the others, and a partial answer is worth having so long as it is never a partial *number*.
 */
[[nodiscard]] inline std::vector<WorkerUtilisation> workerUtilisation(std::span<const Event> events, std::uint64_t lostRecords) {
    std::vector<std::uint8_t> workers;
    bool                      sawViaStep = false;
    for (const Event& event : events) {
        if (std::ranges::find(workers, event.workerId) == workers.end()) {
            workers.push_back(event.workerId);
        }
        sawViaStep = sawViaStep || (event.kind == Kind::sweep && (event.flags & flag::kViaStep) != 0U);
    }
    std::ranges::sort(workers);

    std::vector<WorkerUtilisation> out;
    out.reserve(workers.size());
    for (const std::uint8_t worker : workers) {
        WorkerUtilisation w{.workerId = worker};

        // Before anything is summed. With a wrapped ring the terms cover a truncated window while the
        // lifetime spans the whole run, so every ratio understates by an unknowable amount -- and the
        // interval-clipping problem disappears entirely once this is refused rather than warned about.
        if (lostRecords > 0UL) {
            w.reason = std::format("{} records were lost, so the sums cover a shorter window than the lifetime they would be divided by", lostRecords);
            out.push_back(std::move(w));
            continue;
        }
        if (worker == kWorkerOverflow) {
            w.reason = std::format("worker id {} is the saturation bucket into which every thread past {} folds, so it names no single thread", kWorkerOverflow, kWorkerOverflow - 1U);
            out.push_back(std::move(w));
            continue;
        }

        const Event* started = nullptr;
        const Event* stopped = nullptr;
        std::size_t  nStarts = 0UZ;
        std::size_t  nStops  = 0UZ;
        for (const Event& event : events) {
            if (event.workerId != worker) {
                continue;
            }
            if (event.kind == Kind::workerStart) {
                started = &event;
                ++nStarts;
            } else if (event.kind == Kind::workerStop) {
                stopped = &event;
                ++nStops;
            }
        }

        if (started == nullptr || stopped == nullptr) {
            w.reason = sawViaStep ? std::string("the capture was taken under ExecutionPolicy::externalStep, which has no worker loop: step() runs on the caller's thread, so there is no worker whose lifetime this could be a fraction of") : std::string("no workerStart/workerStop pair for this worker -- enable Category::lifecycle, or the capture was written while the worker was still running");
            out.push_back(std::move(w));
            continue;
        }
        if (nStarts != 1UZ || nStops != 1UZ) {
            w.reason = std::format("expected exactly one workerStart and one workerStop, found {} and {}", nStarts, nStops);
            out.push_back(std::move(w));
            continue;
        }
        if (stopped->startNs <= started->startNs) {
            w.reason = std::format("workerStop at {} ns does not follow workerStart at {} ns", stopped->startNs, started->startNs);
            out.push_back(std::move(w));
            continue;
        }

        w.lifetimeNs = stopped->startNs - started->startNs;

        const detail::DurationSum sweep    = detail::sumDurations(events, worker, Kind::sweep);
        const detail::DurationSum message  = detail::sumDurations(events, worker, Kind::messagePhase);
        const detail::DurationSum idle     = detail::sumDurations(events, worker, Kind::idle);
        const detail::DurationSum produced = detail::sumDurations(events, worker, Kind::workEnd);

        std::uint64_t probeNs        = 0UL;
        bool          probeSaturated = false;
        for (const Event& event : events) {
            if (event.kind == Kind::workProbe && event.workerId == worker) {
                probeSaturated = probeSaturated || event.payload1 == kSaturated;
                probeNs += event.payload1;
            }
        }

        // A saturated term is a lower bound, not a value. It can never cause a *false* containment
        // failure -- it only ever makes a sum smaller -- but every percentage derived from it
        // understates, so it is caught here by name rather than indirectly by an invariant that would
        // then report the wrong cause.
        if (sweep.saturated || message.saturated || idle.saturated || produced.saturated || probeSaturated) {
            w.reason = "an interval exceeded the 4.295 s the duration field can hold, so at least one term is a lower bound rather than a value";
            out.push_back(std::move(w));
            continue;
        }

        w.sweepNs           = sweep.total;
        w.messagePhaseNs    = message.total;
        w.idleNs            = idle.total;
        w.blockProductiveNs = produced.total;
        w.blockProbeNs      = probeNs;

        // Containment, exact. The markers are structurally nested -- a workEnd lies wholly inside the
        // sweep that produced it, invocations within a worker are sequential, and messagePhase/sweep/
        // idle occupy successive regions of one loop iteration -- so there is no skew to absorb and a
        // violation by one nanosecond is a genuine fault rather than noise.
        if (w.blockNs() > w.sweepNs) {
            w.reason = std::format("block execution ({} ns productive + {} ns probing) exceeds the {} ns of sweeps that must contain it", w.blockProductiveNs, w.blockProbeNs, w.sweepNs);
            out.push_back(std::move(w));
            continue;
        }
        const std::uint64_t accounted = w.messagePhaseNs + w.sweepNs + w.idleNs;
        if (accounted > w.lifetimeNs) {
            w.reason = std::format("sweep {} ns + messages {} ns + idle {} ns = {} ns exceeds the {} ns lifetime that must contain them", w.sweepNs, w.messagePhaseNs, w.idleNs, accounted, w.lifetimeNs);
            out.push_back(std::move(w));
            continue;
        }
        w.unaccountedNs = w.lifetimeNs - accounted;

        if ((stopped->flags & flag::kThreadCpuValid) != 0U) {
            // The same rule the interval terms follow: a clipped reading is a lower bound, not a
            // value. Believing it would report the gap between the true on-core time and the clipped
            // one as preemption that never happened. Reachable without the lost-records refusal
            // intervening -- a long run with only `lifecycle` live emits two records per worker, so
            // the ring never wraps and the 71-minute ceiling is what gives way first.
            if (stopped->payload2 == kSaturated) {
                w.reason = std::format("the on-core time exceeded the {} minutes the field can hold, so it is a lower bound rather than a value", (std::uint64_t{kSaturated} * kThreadCpuScaleNs) / 60'000'000'000UL);
                out.push_back(std::move(w));
                continue;
            }
            w.hasThreadCpuTime = true;
            w.threadCpuNs      = static_cast<std::uint64_t>(stopped->payload2) * kThreadCpuScaleNs;
            if (w.threadCpuNs > w.lifetimeNs) {
                w.reason = std::format("on-core time {} ns exceeds the {} ns the worker was alive, which no single thread can do", w.threadCpuNs, w.lifetimeNs);
                out.push_back(std::move(w));
                continue;
            }
            // Idling is a genuine blocking wait, so it consumes no CPU and is subtracted out to leave
            // *involuntary* time off-core. The clamp is still needed -- not every idle reason is
            // guaranteed to block, and the two clocks are read microseconds apart -- and when it fires
            // it is recorded rather than hidden, because it means one of those assumptions bent.
            const std::uint64_t offCore = w.lifetimeNs - w.threadCpuNs;
            w.preemptionClamped         = offCore < w.idleNs;
            w.preemptionNs              = w.preemptionClamped ? 0UL : offCore - w.idleNs;
        }

        w.occupancy = static_cast<double>(w.blockNs()) / static_cast<double>(w.lifetimeNs);
        if (w.lifetimeNs > w.idleNs) {
            w.hasUtilisation = true;
            w.utilisation    = static_cast<double>(w.blockNs()) / static_cast<double>(w.lifetimeNs - w.idleNs);
        }
        // else: the worker only ever idled. Occupancy of 0 % is true and meaningful; "share of active
        // time" has none, and reporting it as 0 % would present an idle worker as a busy one that
        // achieved nothing.

        w.computed = true;
        out.push_back(std::move(w));
    }
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
