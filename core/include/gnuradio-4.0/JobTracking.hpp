#ifndef GNURADIO_JOB_TRACKING_HPP
#define GNURADIO_JOB_TRACKING_HPP

#include <gnuradio-4.0/BlockModel.hpp>
#include <gnuradio-4.0/Port.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace gr::scheduler {

/// per-block job period in nanoseconds, read from BlockModel::metaInformation(); overrides the rate-derived value
inline constexpr std::string_view kPeriodKey = "gr:period_ns";

/// per-block job size in work units, read from BlockModel::metaInformation(); overrides the chunk-derived value
inline constexpr std::string_view kBatchSizeKey = "gr:batch_size";

/// optional lower/upper bounds for the fixed batch selected for a block
inline constexpr std::string_view kMinBatchSizeKey = "gr:min_batch_size";
inline constexpr std::string_view kMaxBatchSizeKey = "gr:max_batch_size";

/**
 * @brief Best-effort observation of job releases and completions for a dataflow block.
 *
 * A block is modelled as a recurring real-time task whose job is the processing of one fixed batch of
 * `batchSize` work units. Work units are counted in whatever unit work::Result::performed_work reports for that
 * block — consumed input samples for a normal block or sink, produced output samples for a source — so the
 * cumulative count needs no access to port internals.
 *
 *   job k          := work units [k * batchSize, (k + 1) * batchSize)
 *   release of k   := the first observation at which enough data (and output room) exists to run job k
 *   completion of k:= the first observation at which the cumulative work count has passed (k + 1) * batchSize
 *   deadline of k  := an absolute instant fixed at release and stored with the job. The scheduler decides it:
 *                     releaseTime(k) + D for a source, D being the block's own relative deadline (implicit
 *                     D == period); for every other block the earliest deadline carried by the producer jobs
 *                     whose output job k consumes (see ProductionLog), and never later than the block's own
 *                     releaseTime(k) + D. A sample's deadline therefore travels with it through the graph, and
 *                     a miss at a sink is an end-to-end miss rather than a per-hop one.
 *
 * "Best effort" is meant literally and is the central caveat: GR4 gives no callback when data arrives, so both
 * instants are observed at scheduler poll boundaries rather than when they actually occur. Every recorded
 * release and completion time is therefore an upper bound on the true one, and both errors are bounded by the
 * dispatch pass duration. Measured response times are consequently pessimistic, and a released job that is
 * never selected is indistinguishable from one released later.
 *
 * The value of anchoring to release rather than to dispatch is that a block passed over by the scheduler keeps
 * its deadline instead of having it pushed forward, which is what makes earliest-deadline-first ordering mean
 * anything on an unannotated graph.
 */
template<typename TClock>
struct JobState {
    using TimePoint = typename TClock::time_point;
    using Duration  = std::chrono::nanoseconds;

    /// distinct (release instant, deadline) groups that can be outstanding at once; the dispatch path must not
    /// allocate, and a block further behind than this is already a diagnosed failure rather than a case worth
    /// tracking exactly
    static constexpr std::size_t kMaxPendingReleases = 32UZ;
    /// response-time histogram: bucket b counts completed jobs whose response lies in [2^b, 2^(b+1)) microseconds;
    /// bucket 0 also takes everything below 2 us and the last bucket everything above 2^23 us (~8 s). Always on,
    /// one increment per completed job, never allocates: the counter-only alternative to a full trace.
    static constexpr std::size_t kHistogramBuckets = 24UZ;

    struct PendingRelease {
        TimePoint     releaseTime{};
        TimePoint     deadline{}; // absolute; every job in the group is judged and ordered against it
        std::uint64_t nJobs = 0UZ;
    };
    std::array<PendingRelease, kMaxPendingReleases> pending{};
    std::size_t                                     oldest   = 0UZ; // ring index of the earliest outstanding release
    std::size_t                                     nEntries = 0UZ;

    std::uint64_t nWorkUnits        = 0UZ; // cumulative performed_work
    std::uint64_t nReleased         = 0UZ;
    std::uint64_t nCompleted        = 0UZ;
    std::uint64_t nCancelled        = 0UZ; // observed releases withdrawn when the block reports DONE
    std::uint64_t nWithdrawn        = 0UZ; // premature releases taken back because the block could not actually run
    std::uint64_t nMissed           = 0UZ;
    std::uint64_t nReleaseOverflows = 0UZ; // releases merged into the newest entry because the ring was full
    Duration      maxResponseTime{};
    Duration      totalResponseTime{}; // over completed jobs; mean response = totalResponseTime / nCompleted
    Duration      maxLateness{};
    std::array<std::uint64_t, kHistogramBuckets> responseHistogram{};

    [[nodiscard]] bool hasPendingJob() const noexcept { return nEntries > 0UZ; }

    [[nodiscard]] std::uint64_t nPendingJobs() const noexcept { return nReleased - nCompleted - nCancelled; }

    /// the group holding the oldest outstanding job, i.e. the one whose deadline governs the block's urgency
    [[nodiscard]] const PendingRelease* oldestPending() const noexcept { return nEntries > 0UZ ? std::addressof(pending[oldest]) : nullptr; }

    /// release instant of the oldest outstanding job
    [[nodiscard]] std::optional<TimePoint> oldestReleaseTime() const noexcept {
        if (nEntries == 0UZ) {
            return std::nullopt;
        }
        return pending[oldest].releaseTime;
    }

    /// absolute deadline of the oldest outstanding job; deadlines are non-decreasing in job order, so this is
    /// also the earliest one outstanding
    [[nodiscard]] std::optional<TimePoint> earliestDeadline() const noexcept {
        if (nEntries == 0UZ) {
            return std::nullopt;
        }
        return pending[oldest].deadline;
    }

    /// index of the first work unit of the next job to be released
    [[nodiscard]] std::uint64_t nextReleaseUnit(std::size_t batchSize) const noexcept { return nReleased * static_cast<std::uint64_t>(batchSize); }

    /// jobs whose data has arrived (and whose output room exists) but that have not been recorded as released
    [[nodiscard]] std::uint64_t releasable(std::size_t availableWorkUnits, std::size_t batchSize) const noexcept {
        if (batchSize == 0UZ) {
            return 0UZ;
        }
        const std::uint64_t nReleasable = (nWorkUnits + static_cast<std::uint64_t>(availableWorkUnits)) / static_cast<std::uint64_t>(batchSize);
        return nReleasable > nReleased ? nReleasable - nReleased : 0UZ;
    }

    /// records `nJobs` further jobs as released at `releaseTime`, each with the absolute deadline `deadline`
    void release(std::uint64_t nJobs, TimePoint releaseTime, TimePoint deadline) {
        if (nJobs == 0UZ) {
            return;
        }
        pushReleases(nJobs, releaseTime, deadline);
        nReleased += nJobs;
    }

    /// releases every job whose data has arrived, all at `now` and with the block's own deadline `now + relativeDeadline`
    void observeReleases(std::size_t availableWorkUnits, std::size_t batchSize, TimePoint now, Duration relativeDeadline) {
        release(releasable(availableWorkUnits, batchSize), now, now + relativeDeadline);
    }

    /// credits one work() call and retires every job it finished, counting a miss for each that finished after its
    /// deadline. `onRetired(nJobs, deadline)` is invoked once per retired group, oldest first, so a caller can
    /// pass the retired jobs' deadlines on (e.g. to a ProductionLog).
    template<typename TOnRetired>
    void observeCompletion(std::size_t performedWork, std::size_t batchSize, TimePoint now, TOnRetired&& onRetired) {
        nWorkUnits += static_cast<std::uint64_t>(performedWork);
        if (batchSize == 0UZ) {
            return;
        }
        const std::uint64_t nFinished   = nWorkUnits / static_cast<std::uint64_t>(batchSize);
        const std::uint64_t nCreditable = std::min(nFinished, nReleased);
        if (nCreditable <= nCompleted) {
            return;
        }
        // a job cannot complete before it has been observed as released; releases are recorded first on every
        // pass, so this only clamps the fallback sweep path where the two observations coincide
        popCompletions(nCreditable - nCompleted, now, onRetired);
        nCompleted = nCreditable;
    }

    void observeCompletion(std::size_t performedWork, std::size_t batchSize, TimePoint now) {
        observeCompletion(performedWork, batchSize, now, [](std::uint64_t, TimePoint) {});
    }

    void clear() noexcept { *this = JobState{}; }

    void cancelPending() noexcept {
        nCancelled += nPendingJobs();
        oldest   = 0UZ;
        nEntries = 0UZ;
    }

    /// takes back every outstanding release as if it had never been observed. Cancelling is not enough for a
    /// premature release: the job would stay counted in nReleased, observeReleases would refuse to release it
    /// again, and a block whose releases are synthesised (sources, occupancy-invisible blocks) could then never
    /// become eligible again. Withdrawing rolls nReleased back so the next observation re-releases the job at
    /// its own, later instant.
    void withdrawPendingReleases() noexcept {
        std::uint64_t outstanding = 0UZ;
        for (std::size_t entry = 0UZ; entry < nEntries; ++entry) {
            outstanding += pending[(oldest + entry) % kMaxPendingReleases].nJobs;
        }
        nReleased -= outstanding;
        nWithdrawn += outstanding;
        oldest   = 0UZ;
        nEntries = 0UZ;
    }

    [[nodiscard]] static constexpr std::size_t histogramBucket(Duration response) noexcept {
        const auto us = static_cast<std::uint64_t>(std::max<typename Duration::rep>(0, response.count()) / 1000);
        if (us < 2UZ) {
            return 0UZ;
        }
        return std::min(kHistogramBuckets - 1UZ, static_cast<std::size_t>(std::bit_width(us)) - 1UZ);
    }

private:
    void pushReleases(std::uint64_t nJobs, TimePoint releaseTime, TimePoint deadline) {
        if (nEntries > 0UZ) {
            PendingRelease& newest = pending[(oldest + nEntries - 1UZ) % kMaxPendingReleases];
            if (newest.releaseTime == releaseTime && newest.deadline == deadline) {
                newest.nJobs += nJobs;
                return;
            }
            if (nEntries == kMaxPendingReleases) {
                // keep the oldest entry exact so the governing deadline stays correct, and attribute the
                // overflow to the newest known group; its deadline can only move earlier, i.e. pessimistic
                newest.nJobs += nJobs;
                newest.deadline = std::min(newest.deadline, deadline);
                nReleaseOverflows += nJobs;
                return;
            }
        }
        pending[(oldest + nEntries) % kMaxPendingReleases] = PendingRelease{.releaseTime = releaseTime, .deadline = deadline, .nJobs = nJobs};
        nEntries++;
    }

    template<typename TOnRetired>
    void popCompletions(std::uint64_t nJobs, TimePoint now, TOnRetired&& onRetired) {
        while (nJobs > 0UZ && nEntries > 0UZ) {
            PendingRelease&     entry   = pending[oldest];
            const std::uint64_t retired = std::min(nJobs, entry.nJobs);

            const Duration response = std::chrono::duration_cast<Duration>(now - entry.releaseTime);
            maxResponseTime         = std::max(maxResponseTime, response);
            totalResponseTime += response * static_cast<typename Duration::rep>(retired);
            responseHistogram[histogramBucket(response)] += retired;
            if (now > entry.deadline) {
                nMissed += retired;
                maxLateness = std::max(maxLateness, std::chrono::duration_cast<Duration>(now - entry.deadline));
            }
            onRetired(retired, entry.deadline);

            entry.nJobs -= retired;
            nJobs -= retired;
            if (entry.nJobs == 0UZ) {
                oldest = (oldest + 1UZ) % kMaxPendingReleases;
                nEntries--;
            }
        }
    }
};

/**
 * @brief The deadlines carried by a block's released output, indexed by cumulative output unit.
 *
 * Every release group appends one record: the cumulative output unit the group's jobs reach once run, and the
 * jobs' absolute deadline. A consumer releasing a job that starts at input unit u asks `cover(u)` for the
 * record containing u — its deadline is the earliest one over the whole job, because deadlines are non-decreasing
 * in unit order — and that is how a deadline inherits along an edge. Output units are the producer's work units
 * scaled by its resampling ratio, which is exactly what the consumer counts as input units.
 *
 * The ring is fixed-size and never allocates on the dispatch path. Records every in-worker consumer has finished
 * with are retired on the next publish; if a consumer lags further than the ring holds, the two oldest records
 * are merged and keep the earlier deadline, which can only make an inherited deadline pessimistic (`nMerged`).
 * Withdrawn releases truncate the log so a re-release can restate its units.
 */
template<typename TClock>
struct ProductionLog {
    using TimePoint = typename TClock::time_point;

    static constexpr std::size_t kMaxRecords = 64UZ;

    struct Record {
        std::uint64_t endUnit = 0UZ; // units [previous record's endUnit, endUnit) carry `deadline`
        TimePoint     deadline{};
    };
    struct Cover {
        TimePoint     deadline{};
        std::uint64_t validUntil = 0UZ; // every unit in [queried unit, validUntil) carries the same deadline
    };

    std::array<Record, kMaxRecords> records{};
    std::size_t                     oldest    = 0UZ;
    std::size_t                     nRecords  = 0UZ;
    std::uint64_t                   firstUnit = 0UZ; // where records[oldest] begins
    std::uint64_t                   endUnit   = 0UZ; // cumulative units covered by the log
    std::uint64_t                   nMerged   = 0UZ; // overflow merges; each one makes some inherited deadline pessimistic

    /// extends the log up to cumulative unit `end` with `deadline`; records at or below `retireBefore` are dropped first
    void publishUntil(std::uint64_t end, TimePoint deadline, std::uint64_t retireBefore) {
        retire(retireBefore);
        if (end <= endUnit) {
            return;
        }
        endUnit = end;
        if (nRecords > 0UZ) {
            Record& newest = at(nRecords - 1UZ);
            if (newest.deadline == deadline) {
                newest.endUnit = end;
                return;
            }
            if (nRecords == kMaxRecords) {
                Record& second  = at(1UZ);
                second.deadline = std::min(at(0UZ).deadline, second.deadline);
                oldest          = (oldest + 1UZ) % kMaxRecords; // `second` now spans from firstUnit
                nRecords--;
                nMerged++;
            }
        }
        at(nRecords) = Record{.endUnit = end, .deadline = deadline};
        nRecords++;
    }

    /// the record containing `unit`, or nullopt when the log does not (or no longer) cover it
    [[nodiscard]] std::optional<Cover> cover(std::uint64_t unit) const noexcept {
        if (unit < firstUnit || unit >= endUnit) {
            return std::nullopt;
        }
        for (std::size_t index = 0UZ; index < nRecords; ++index) {
            const Record& record = records[(oldest + index) % kMaxRecords];
            if (unit < record.endUnit) {
                return Cover{.deadline = record.deadline, .validUntil = record.endUnit};
            }
        }
        return std::nullopt;
    }

    /// forgets everything at or beyond cumulative unit `newEnd` (a withdrawn release)
    void truncate(std::uint64_t newEnd) noexcept {
        if (newEnd >= endUnit) {
            return;
        }
        while (nRecords > 0UZ && startOf(nRecords - 1UZ) >= newEnd) {
            nRecords--;
        }
        if (nRecords > 0UZ) {
            at(nRecords - 1UZ).endUnit = std::min(at(nRecords - 1UZ).endUnit, newEnd);
        } else {
            firstUnit = std::min(firstUnit, newEnd);
        }
        endUnit = newEnd;
    }

    void retire(std::uint64_t before) noexcept {
        while (nRecords > 0UZ && at(0UZ).endUnit <= before) {
            firstUnit = at(0UZ).endUnit;
            oldest    = (oldest + 1UZ) % kMaxRecords;
            nRecords--;
        }
    }

    void clear() noexcept { *this = ProductionLog{}; }

private:
    [[nodiscard]] Record&       at(std::size_t index) noexcept { return records[(oldest + index) % kMaxRecords]; }
    [[nodiscard]] const Record& at(std::size_t index) const noexcept { return records[(oldest + index) % kMaxRecords]; }
    [[nodiscard]] std::uint64_t startOf(std::size_t index) const noexcept { return index == 0UZ ? firstUnit : at(index - 1UZ).endUnit; }
};

/// static per-block job parameters, resolved once when the schedule is formed
struct JobParameters {
    std::size_t              batchSize           = 1UZ; // fixed work units per job
    std::size_t              minBatchSize        = 1UZ;
    std::size_t              maxBatchSize        = std::numeric_limits<std::size_t>::max();
    std::chrono::nanoseconds period{};                   // minimum inter-arrival time of jobs
    bool                     periodFromSampleRate = false;
    bool                     periodFromFallback   = false; // neither annotated nor rate-derived: not a real inter-arrival bound
    bool                     batchDefaulted       = false; // no gr:batch_size: the scheduler's work quantum (or buffer) chose the batch
    bool                     tracked              = true;  // false when port occupancy cannot describe readiness
    bool                     source               = false; // source releases are observed one job at a time
};

namespace detail {

[[nodiscard]] inline std::size_t minAvailableOnConnectedSyncPorts(std::span<const std::size_t> available, std::span<const gr::port::BitMask> types) {
    using enum gr::port::BitMask;
    std::size_t  smallest = std::numeric_limits<std::size_t>::max();
    const size_t nPorts   = std::min(available.size(), types.size());
    for (std::size_t port = 0UZ; port < nPorts; ++port) {
        if (!gr::port::any(types[port], Connected) || !gr::port::any(types[port], Synchronous) || !gr::port::any(types[port], Stream)) {
            continue;
        }
        smallest = std::min(smallest, available[port]);
    }
    return smallest;
}

[[nodiscard]] inline std::optional<float> declaredSampleRate(std::vector<gr::PortMetaInfo> metaInfos) {
    // PortMetaInfo::sample_rate defaults to 1 Hz, so an undeclared rate is indistinguishable from a genuine
    // 1 Hz one; no realistic flowgraph runs at 1 Hz, and treating the default as "undeclared" is the only
    // reading that lets an unannotated graph fall back rather than silently claim a one-second period
    for (const gr::PortMetaInfo& info : metaInfos) {
        const float rate = info.sample_rate;
        if (rate > 0.f && rate != 1.f) {
            return rate;
        }
    }
    return std::nullopt;
}

[[nodiscard]] inline std::uint64_t readUnsigned(const property_map& map, std::string_view key) {
    if (const auto it = map.find(key); it != map.end()) {
        const auto       entry = (*it).second;
        return entry.value_or<std::uint64_t>(0UZ);
    }
    return 0UZ;
}

} // namespace detail

/**
 * Work units currently available to the block, expressed in the same unit as work::Result::performed_work.
 *
 * A job needs both input data and somewhere to put its output, so the two constraints are combined: output
 * room is converted into input units through the block's resampling ratio, which is input_chunk_size to
 * output_chunk_size. Sources declare no input ports and report performed_work in output samples, so for them
 * the output room *is* the work unit count.
 */
[[nodiscard]] inline std::size_t availableWorkUnits(BlockModel& block) {
    const std::size_t fromInput  = detail::minAvailableOnConnectedSyncPorts(block.availableInputSamples(true), block.blockInputTypes());
    const std::size_t outputRoom = detail::minAvailableOnConnectedSyncPorts(block.availableOutputSamples(true), block.blockOutputTypes());
    if (outputRoom == std::numeric_limits<std::size_t>::max()) {
        return fromInput; // sink: nothing downstream can throttle it
    }

    const gr::Ratio ratio = block.resamplingRatio();
    if (ratio.numerator <= 0 || ratio.denominator <= 0) {
        return std::min(fromInput, outputRoom);
    }
    const std::size_t fromOutput = (outputRoom * static_cast<std::size_t>(ratio.numerator)) / static_cast<std::size_t>(ratio.denominator);
    return std::min(fromInput, fromOutput);
}

/**
 * Resolves the job size and period once, at schedule-formation time.
 *
 * The minimum defaults to the block's own chunking — input_chunk_size for a normal block, widened to whatever
 * minimum its ports insist on. `gr:min_batch_size` can raise that floor and `gr:max_batch_size` can cap it.
 * `gr:batch_size` selects the fixed value within those bounds; without it, `defaultBatch` -- the scheduler's work
 * quantum, or the smallest buffer the block touches when that quantum is unbounded -- is used, and only when
 * neither is known does the batch fall to the minimum. One sample per work() call would otherwise be the job
 * size of every unannotated block, which is pure dispatch overhead. Contradictory bounds are normalised by
 * raising the maximum to the minimum, because running below a port's indivisible chunk would never be valid.
 *
 * The period is derived from the fixed batch and declared sample rate where the graph provides one, which is
 * the only place a real-time period can honestly come from in a dataflow graph. A rate propagated from an
 * upstream source can fill an otherwise undeclared port rate. Absent either, the period falls back to the
 * caller's default; `periodFromSampleRate` records when a declared or propagated rate was used.
 */
[[nodiscard]] inline JobParameters deriveJobParameters(BlockModel& block, std::chrono::nanoseconds fallbackPeriod, std::optional<float> propagatedRate = std::nullopt, std::size_t defaultBatch = 0UZ) {
    const property_map& meta = block.metaInformation();
    JobParameters       parameters;

    parameters.tracked = !block.hasAsyncInputPorts() && !block.hasAsyncOutputPorts();

    const gr::Ratio ratio    = block.resamplingRatio();
    const bool      isSource = block.blockInputTypes().empty();
    parameters.source        = isSource;
    const auto chunkSize     = static_cast<std::size_t>(std::max(1, isSource ? ratio.denominator : ratio.numerator));

    const std::span<const std::size_t> minimums    = isSource ? block.minOutputRequirements() : block.minInputRequirements();
    const auto                         portMinimum = minimums.empty() ? 1UZ : *std::ranges::max_element(minimums);

    const std::size_t intrinsicMinimum = std::max({1UZ, chunkSize, portMinimum});
    const std::uint64_t annotatedMin   = detail::readUnsigned(meta, kMinBatchSizeKey);
    const std::uint64_t annotatedMax   = detail::readUnsigned(meta, kMaxBatchSizeKey);
    const std::uint64_t annotatedBatch = detail::readUnsigned(meta, kBatchSizeKey);

    parameters.minBatchSize = annotatedMin > 0UZ ? std::max(intrinsicMinimum, static_cast<std::size_t>(annotatedMin)) : intrinsicMinimum;
    parameters.maxBatchSize = annotatedMax > 0UZ ? std::max(parameters.minBatchSize, static_cast<std::size_t>(annotatedMax)) : std::numeric_limits<std::size_t>::max();
    const std::size_t preferred = annotatedBatch > 0UZ ? static_cast<std::size_t>(annotatedBatch) : defaultBatch > 0UZ ? defaultBatch : parameters.minBatchSize;
    parameters.batchSize        = std::clamp(preferred, parameters.minBatchSize, parameters.maxBatchSize);
    parameters.batchDefaulted   = annotatedBatch == 0UZ;

    if (const std::uint64_t annotatedPeriod = detail::readUnsigned(meta, kPeriodKey); annotatedPeriod > 0UZ) {
        parameters.period = std::chrono::nanoseconds(static_cast<std::int64_t>(annotatedPeriod));
        return parameters;
    }

    std::optional<float> rate = detail::declaredSampleRate(isSource ? block.outputMetaInfos(true) : block.inputMetaInfos(true));
    if (!rate.has_value()) {
        rate = propagatedRate;
    }
    if (rate.has_value()) {
        const double seconds = static_cast<double>(parameters.batchSize) / static_cast<double>(*rate);
        parameters.period    = std::chrono::nanoseconds(static_cast<std::int64_t>(seconds * 1e9));
        if (parameters.period > std::chrono::nanoseconds::zero()) {
            parameters.periodFromSampleRate = true;
            return parameters;
        }
    }

    parameters.period             = fallbackPeriod;
    parameters.periodFromFallback = true;
    return parameters;
}

} // namespace gr::scheduler

#endif // GNURADIO_JOB_TRACKING_HPP
