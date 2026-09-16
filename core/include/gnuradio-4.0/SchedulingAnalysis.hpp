#ifndef GNURADIO_SCHEDULINGANALYSIS_HPP
#define GNURADIO_SCHEDULINGANALYSIS_HPP

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <format>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gnuradio-4.0/BlockModel.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp> // kUnboundedBatch

namespace gr::scheduler {

/// How a derived scheduling attribute came to hold its value.
enum class AttributeOrigin : std::uint8_t {
    unknown,         /// no value could be established
    userSet,         /// explicitly set by the user; the derivation must not overwrite it
    derivedFromRate, /// propagated from a source's sample rate through the graph
    configured,      /// inherited from an explicit configuration value, e.g. the scheduler's max_work_items
    assumedBatch     /// depends on the batch operating point the strategy assumed
};

[[nodiscard]] constexpr std::string_view toString(AttributeOrigin origin) noexcept {
    switch (origin) {
    case AttributeOrigin::userSet: return "user_set";
    case AttributeOrigin::derivedFromRate: return "derived_from_rate";
    case AttributeOrigin::configured: return "configured";
    case AttributeOrigin::assumedBatch: return "assumed_batch";
    case AttributeOrigin::unknown: break;
    }
    return "unknown";
}

/// Which scheduling attributes the user set explicitly, as opposed to leaving at their default.
/// Supplied by the caller because the underlying signal (`settings().autoUpdateParameters()`
/// membership) requires non-const access and must be sampled before any write-back.
struct UserSetAttributes {
    bool priority = false;
    bool period   = false;
    bool deadline = false;
    bool wcet     = false;
};

/// Per-block result: the attribute values plus how each was obtained.
struct DerivedAttributes {
    std::int32_t priority         = 0;
    float        period           = 0.f; // [s], 0 = unset
    float        relativeDeadline = 0.f; // [s], 0 = unset
    float        wcetEstimate     = 0.f; // [s], never derived -- see SchedulingAnalysis docs

    AttributeOrigin priorityOrigin = AttributeOrigin::unknown;
    AttributeOrigin periodOrigin   = AttributeOrigin::unknown;
    AttributeOrigin deadlineOrigin = AttributeOrigin::unknown;
    AttributeOrigin wcetOrigin     = AttributeOrigin::unknown;

    /// The source this block's rate descends from. Periods are anchored against *this* source's
    /// `sample_rate`, not a single graph-wide one, so independent chains keep independent
    /// timebases and one chain's rate cannot rescale another's.
    const BlockModel* originSource = nullptr;

    /// Rate of the stream this block processes, relative to its source's -- the propagated
    /// primitive. It depends only on resampling ratios and strides, never on batch sizes, which is
    /// what lets blocks run at different batches without disturbing one another.
    double relativeSampleRate = 1.0;

    double      relativeRate = 1.0; /// invocations per invocation of the component's source; derived from the above
    std::size_t nominalBatch = 1UZ; /// batch the strategy assumed when sizing period/WCET

    /// The batch interval. `batchFloor` is the release threshold, `executionCeiling` what the
    /// worker requests -- `kUnboundedBatch` when nothing bounds it, which is what the scheduler
    /// passes today. `nominalBatch` is the modelling operating point and falls back to the
    /// strategy's ceiling where `executionCeiling` is unbounded; the two must not be conflated.
    std::size_t     batchFloor       = 1UZ;
    std::size_t     executionCeiling = kUnboundedBatch;
    AttributeOrigin batchOrigin      = AttributeOrigin::unknown;
};

struct SchedulingAnalysis {
    std::unordered_map<const BlockModel*, DerivedAttributes> perBlock;
    std::vector<std::string>                                 diagnostics;

    [[nodiscard]] const DerivedAttributes* find(const BlockModel& block) const noexcept {
        const auto it = perBlock.find(std::addressof(block));
        return it == perBlock.end() ? nullptr : std::addressof(it->second);
    }
};

namespace detail {

[[nodiscard]] inline std::size_t settingAsSize(const BlockModel& block, const std::string& key, std::size_t fallback) {
    const std::optional<Value> value = block.settings().get(key);
    if (!value.has_value()) {
        return fallback;
    }
    if (const auto* asSize = value->get_if<gr::Size_t>()) {
        return static_cast<std::size_t>(*asSize);
    }
    if (const auto* asStd = value->get_if<std::size_t>()) {
        return *asStd;
    }
    return fallback;
}

/// Largest `min_samples` over the block's synchronous ports, and at least one resampling chunk:
/// `computeSampleLimits()` gates release on a whole chunk via `ensureMinimalDecimation`, so for a
/// resampling block the real threshold is `input_chunk_size` even when every `min_samples` is 1.
[[nodiscard]] inline std::size_t releaseThreshold(BlockModel& block) {
    std::size_t threshold = 1UZ;

    const auto visitSynchronous = [&threshold](std::size_t nPorts, auto&& accessor, auto&& toInputSamples) {
        for (std::size_t i = 0UZ; i < nPorts; ++i) {
            auto port = accessor(i);
            if (port.has_value() && port.value()->isSynchronous()) {
                threshold = std::max(threshold, toInputSamples(port.value()->min_samples));
            }
        }
    };

    // As in portCeiling(): an output port's threshold is in *output* samples and must be converted
    // before it can be compared with an input-sample quantity.
    const std::size_t inChunk  = std::max(settingAsSize(block, "input_chunk_size", 1UZ), 1UZ);
    const std::size_t outChunk = std::max(settingAsSize(block, "output_chunk_size", 1UZ), 1UZ);

    visitSynchronous(block.dynamicInputPortsSize(), [&block](std::size_t i) { return block.dynamicInputPort(i); }, [](std::size_t n) { return n; });
    visitSynchronous(block.dynamicOutputPortsSize(), [&block](std::size_t i) { return block.dynamicOutputPort(i); }, [inChunk, outChunk](std::size_t n) { return n > kUnboundedBatch / inChunk ? kUnboundedBatch : n * inChunk / outChunk; });

    return std::max(threshold, inChunk);
}

/// Smallest `max_samples` over the block's synchronous ports, or `kUnboundedBatch` if none binds.
///
/// N.B. these are the *type-erased* copies held by `DynamicPort`, snapshotted once when
/// `initDynamicPorts()` first runs and never refreshed. A bound applied to the typed port after
/// that point is invisible here even though `computeSampleLimits()` honours it.
[[nodiscard]] inline std::size_t portCeiling(BlockModel& block) {
    std::size_t ceiling = kUnboundedBatch;

    const auto visitSynchronous = [&ceiling](std::size_t nPorts, auto&& accessor, auto&& toInputSamples) {
        for (std::size_t i = 0UZ; i < nPorts; ++i) {
            auto port = accessor(i);
            if (port.has_value() && port.value()->isSynchronous() && port.value()->max_samples != kUnboundedBatch) {
                ceiling = std::min(ceiling, toInputSamples(port.value()->max_samples));
            }
        }
    };

    // Everything here is expressed in *input* samples, the unit `requestedWork` is measured in.
    // An output port's bound is in output samples, so a resampling block needs it converted --
    // for an 11:5 decimator a 41-sample output bound is an 88-sample input bound, not a 41.
    const std::size_t inChunk  = std::max(settingAsSize(block, "input_chunk_size", 1UZ), 1UZ);
    const std::size_t outChunk = std::max(settingAsSize(block, "output_chunk_size", 1UZ), 1UZ);

    visitSynchronous(block.dynamicInputPortsSize(), [&block](std::size_t i) { return block.dynamicInputPort(i); }, [](std::size_t n) { return n; });
    visitSynchronous(block.dynamicOutputPortsSize(), [&block](std::size_t i) { return block.dynamicOutputPort(i); }, [inChunk, outChunk](std::size_t n) { return n > kUnboundedBatch / std::max(inChunk, 1UZ) ? kUnboundedBatch : n * inChunk / outChunk; });

    return ceiling;
}

/// GR4 treats a stride equal to the chunk (or zero) as back-to-back, i.e. disabled.
[[nodiscard]] inline bool strideActive(const BlockModel& block) {
    const std::size_t stride = settingAsSize(block, "stride", 0UZ);
    return stride != 0UZ && stride != settingAsSize(block, "input_chunk_size", 1UZ);
}

/// Samples the stream advances per invocation -- `stride` where it is active, otherwise the
/// window itself. This is the *consumption* quantum, which sets invocation rate and downstream
/// sample rate; the *work* quantum, which execution cost scales with, is the window. The two
/// coincide only while stride is inactive.
[[nodiscard]] inline std::size_t effectiveAdvance(const BlockModel& block) {
    const std::size_t chunk = std::max(settingAsSize(block, "input_chunk_size", 1UZ), 1UZ);
    return strideActive(block) ? std::max(settingAsSize(block, "stride", chunk), 1UZ) : chunk;
}

[[nodiscard]] inline bool isResampling(const BlockModel& block) { return settingAsSize(block, "input_chunk_size", 1UZ) != 1UZ || settingAsSize(block, "output_chunk_size", 1UZ) != 1UZ; }

/// The one place the port-gating rule lives: which ports count, and how their per-port quantities
/// combine. Everything that reasons about a block's input gate goes through it, because two
/// implementations of this rule agreeing on the combination and diverging on the membership is
/// exactly how the capacity bound came to be computed over the wrong ports.
///
/// A port counts when it is **connected** and is a **stream** port. Message ports are excluded
/// deliberately: `Port::kIsSynch` is "synchronous unless marked `Async`", so a message port reports
/// itself synchronous and an included one would land in the minimum below and shrink whatever
/// quantity is being combined.
///
/// The survivors combine exactly as `RateAccumulator` combines rates: the slowest synchronous port,
/// the fastest asynchronous one, and the faster of the two where both are present.
///
/// `std::nullopt` means no port counted, which the caller interprets -- over an input span it
/// identifies a source, over an output span a block that can make no progress at all.
template<typename TPortEntry>
[[nodiscard]] inline std::optional<std::size_t> combineGating(std::size_t nPorts, TPortEntry&& entry) {
    std::size_t syncMin  = gr::undefined_size;
    std::size_t asyncMax = 0UZ;
    bool        hasSync  = false;
    bool        hasAsync = false;

    for (std::size_t i = 0UZ; i < nPorts; ++i) {
        const std::optional<std::pair<port::BitMask, std::size_t>> current = entry(i);
        if (!current.has_value()) {
            continue;
        }
        const auto [mask, value] = *current;
        if (!port::isConnected(mask) || !port::isStream(mask)) {
            continue; // an unconnected port gates nothing; a message port is not a data gate at all
        }
        if (port::isSynchronous(mask)) {
            syncMin = std::min(syncMin, value);
            hasSync = true;
        } else {
            asyncMax = std::max(asyncMax, value);
            hasAsync = true;
        }
    }

    if (hasSync && hasAsync) {
        return std::max(syncMin, asyncMax);
    }
    if (hasSync) {
        return syncMin;
    }
    if (hasAsync) {
        return asyncMax;
    }
    return std::nullopt;
}

/// Per-port occupancy, as the stream port caches report it. The stream check inside `combineGating`
/// is a no-op here -- these masks already come from a stream-only cache -- which is the point: the
/// rule is stated once and each caller is checked against it rather than trusted to match it.
[[nodiscard]] inline std::optional<std::size_t> gatingAvailability(std::span<const std::size_t> available, std::span<const port::BitMask> types) {
    return combineGating(std::min(available.size(), types.size()), [available, types](std::size_t i) { return std::optional{std::pair{types[i], available[i]}}; });
}

} // namespace detail

/// Non-destructive answer to "would this block make progress?", obtained without invoking `work()`.
///
/// `available` is the gating quantity, in input samples -- or, for a block with no connected input,
/// in output samples: a source is bounded by the space it can write into, which is the only thing
/// that limits the work it can do.
struct Readiness {
    std::size_t available = 0UZ;
    bool        runnable  = false;
};

/// N.B. forces a re-read of the port caches: an upstream publish leaves the consumer's
/// `_dirtyAvailable` set, so a cached read would report pre-publish occupancy.
[[nodiscard]] inline Readiness inputReadiness(BlockModel& block, std::size_t threshold) {
    const std::span<const std::size_t>   inputAvailable = block.availableInputSamples(true);
    const std::span<const port::BitMask> inputTypes     = block.blockInputTypes();

    std::optional<std::size_t> gating = detail::gatingAvailability(inputAvailable, inputTypes);
    if (!gating.has_value()) {
        const std::span<const std::size_t>   outputAvailable = block.availableOutputSamples(true);
        const std::span<const port::BitMask> outputTypes     = block.blockOutputTypes();
        gating                                               = detail::gatingAvailability(outputAvailable, outputTypes);
    }
    if (!gating.has_value()) {
        return {};
    }
    return {.available = *gating, .runnable = *gating >= std::max(threshold, 1UZ)};
}

/// Ceiling of the gating availability: the same rule `inputReadiness()` applies to occupancy,
/// applied instead to buffer *capacities*. A bound computed over any other set of ports could be
/// violated -- or, worse, silently under-sized -- by construction.
///
/// As with readiness, a block with no counting input falls back to its output side.
[[nodiscard]] inline std::size_t gatingCapacity(BlockModel& block) {
    const auto inputEntry = [&block](std::size_t i) -> std::optional<std::pair<port::BitMask, std::size_t>> {
        auto port = block.dynamicInputPort(i);
        if (!port.has_value()) {
            return std::nullopt;
        }
        return std::pair{port.value()->portMaskInfo(), port.value()->bufferSize()};
    };
    const auto outputEntry = [&block](std::size_t i) -> std::optional<std::pair<port::BitMask, std::size_t>> {
        auto port = block.dynamicOutputPort(i);
        if (!port.has_value()) {
            return std::nullopt;
        }
        return std::pair{port.value()->portMaskInfo(), port.value()->bufferSize()};
    };

    if (const std::optional<std::size_t> inputs = detail::combineGating(block.dynamicInputPortsSize(), inputEntry); inputs.has_value()) {
        return *inputs;
    }
    if (const std::optional<std::size_t> outputs = detail::combineGating(block.dynamicOutputPortsSize(), outputEntry); outputs.has_value()) {
        return *outputs;
    }
    return 0UZ;
}

/// How many jobs of one block can be outstanding at once. Every released job holds at least
/// `batchFloor` samples and the committed total cannot exceed the gating buffer, so the quotient
/// bounds the ring -- which is what lets the whole job arena be sized during setup.
///
/// `cap` clamps that bound. The derived value is a true maximum but a wildly pessimistic one: with
/// 65536-sample buffers and a floor of one it reserves 65536 jobs (1.5 MB) for a block that will
/// realistically hold a handful. Clamping trades an unreachable guarantee for the memory, and
/// changes what a full ring means -- see `SchedState::overruns`. `kUnboundedBatch` disables it.
[[nodiscard]] inline std::size_t maxOutstandingJobs(BlockModel& block, std::size_t batchFloor, std::size_t cap = kUnboundedBatch) {
    const std::size_t floor = std::max(batchFloor, 1UZ);
    return std::clamp(gatingCapacity(block) / floor, 1UZ, std::max(cap, 1UZ));
}

/// `available - assigned`, saturating at zero. Availability can legitimately fall below what
/// outstanding jobs already hold -- under the asynchronous `max` rule the gating port may change
/// between detection points -- and unsigned wrap-around there would open the data gate wide instead
/// of closing it. Saturating fails closed: the gate stays shut until the outstanding jobs drain.
[[nodiscard]] constexpr std::size_t unassignedSamples(std::size_t available, std::size_t assigned) noexcept { return available > assigned ? available - assigned : 0UZ; }

/// Releases one job for `state` when both gates are open, and does nothing otherwise.
///
/// Sporadic, not gridded: `lastRelease` advances only on an actual release, so `period` reads as a
/// minimum separation between consecutive releases. `now` is the detection instant and becomes the
/// job's release time -- no nominal instant is ever invented, because a release is *defined* as the
/// detection of eligibility and a nominal one asserts an eligibility that may never have occurred.
/// Largest relative deadline whose conversion to `steady_clock::duration` stays inside the integer
/// range. Beyond it `duration_cast` performs an out-of-range `double` -> `int64` conversion, which is
/// undefined behaviour rather than wrap-around -- so the check has to precede the conversion, and a
/// deadline computed past this point cannot be tested after the fact because there is no defined
/// "after". This detects the condition; clamping it would change which block the scheduler runs and
/// belongs to the scheduling thrust, not to instrumentation.
inline constexpr double kMaxRepresentableDeadlineSeconds = static_cast<double>(std::numeric_limits<std::chrono::steady_clock::rep>::max()) * static_cast<double>(std::chrono::steady_clock::period::num) / static_cast<double>(std::chrono::steady_clock::period::den);

/// `traceFlags` carries what only the caller knows: which detection path this is. Clear means the
/// per-sweep backstop, `gr::trace::flag::kViaSuccessorWalk` the event-driven walk. Defaulted because
/// the backstop is the neutral answer and the unit tests that drive this function directly are not
/// testing the detection path; the two scheduler call sites both pass it explicitly.
inline void releaseIfEligible(BlockModel& block, SchedState& state, std::chrono::steady_clock::time_point now, [[maybe_unused]] std::uint8_t traceFlags = 0U) {
    if (state.finished) {
        return;
    }
    if (block.state() != lifecycle::State::RUNNING) {
        // A block that stopped itself -- a source reaching `n_samples_max`, say -- will never be
        // released again, so it can never return `DONE` and nothing else would ever mark it
        // finished. Recording it here is what lets a worker conclude the graph is over; without it
        // the pass stays unfinished for ever, waiting on a block that has already left.
        if (lifecycle::isShuttingDown(block.state()) || block.state() == lifecycle::State::ERROR) {
            state.finished = true;
        }
        return; // otherwise merely paused: it may yet resume, and would only accumulate jobs meanwhile
    }

    // End of stream makes the batch floor unsatisfiable: no more data is coming, so waiting for it
    // is waiting for ever, and the block would never run, never report DONE and never let its worker
    // conclude the graph had finished. Only checked when the gate is about to shut, so it costs
    // nothing while data is flowing.
    //
    // Only with an empty ring: a shut gate can equally mean the samples are committed to outstanding
    // jobs rather than absent, and waiving then would commit them twice.
    const Readiness   readiness  = inputReadiness(block, state.batchFloor);
    const std::size_t unassigned = unassignedSamples(readiness.available, state.assignedSamples);
    const bool        draining   = unassigned < std::max(state.batchFloor, 1UZ) && state.jobs.empty() && block.inputStreamEnded();

    if (!draining && state.periodSeconds > 0.0) { // a zero period imposes no temporal gate
        const std::chrono::duration<double> elapsed = now - state.lastRelease;
        if (elapsed.count() < state.periodSeconds) {
            return; // ... and a terminating block does not wait out a period to run its last job
        }
    }

    if (unassigned < std::max(state.batchFloor, 1UZ) && !draining) {
        return;
    }

    // At least one, so a drain job can run on an empty-but-ended port and observe the end-of-stream
    // tag; `work(1)` there reports DONE.
    const std::size_t batch = std::min(std::max(unassigned, 1UZ), state.batchCeiling);
    if (batch == 0UZ || batch == kUnboundedBatch || batch == gr::undefined_size) {
        // Defence in depth. `combineGating()` only ever folds in *connected* ports, so availability
        // cannot come back as `undefined_size` and this cannot currently fire -- but `work(SIZE_MAX)`
        // is not a batch, and the guard is one comparison.
        return;
    }

    const double deadlineSeconds = state.relativeDeadlineSeconds > 0.0 ? state.relativeDeadlineSeconds : state.periodSeconds;

    // An unset deadline sorts *last*, never first: a zero absolute deadline would look infinitely
    // urgent and starve everything else.
    const std::chrono::steady_clock::time_point deadline = deadlineSeconds > 0.0 //
                                                               ? now + std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::duration<double>(deadlineSeconds))
                                                               : std::chrono::steady_clock::time_point::max();

    if (!state.jobs.push(Job{.batch = batch, .releaseTime = now, .absoluteDeadline = deadline})) {
        ++state.overruns;
        if constexpr (gr::trace::kEnabled) {
            if (gr::trace::categoryEnabled(gr::trace::Category::release)) {
                // The samples stay unassigned and are offered again later, so this is backlog rather
                // than an error -- and it is the only place the `max_outstanding_jobs` clamp becomes
                // visible, which is what makes its default profileable at all.
                gr::trace::emit(gr::trace::Event{.startNs = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count()), //
                    .payload0                             = gr::trace::saturate(batch),
                    .payload1                             = gr::trace::saturate(state.jobs.capacity()),
                    .payload2                             = gr::trace::saturate(state.overruns), //
                    .entity                               = state.entityId,
                    .kind                                 = gr::trace::Kind::jobReleaseDropped,
                    .workerId                             = state.workerId,
                    .flags                                = traceFlags});
            }
        }
        return;
    }
    state.assignedSamples += batch;
    state.lastRelease = now;

    if constexpr (gr::trace::kEnabled) {
        if (gr::trace::categoryEnabled(gr::trace::Category::release)) {
            // `startNs` *is* the release instant, in the same clock domain as every other marker and
            // as `Job::releaseTime` itself -- which is what lets a report subtract a completion from
            // it without a conversion step that could disagree.
            const bool suspectDeadline = deadlineSeconds > kMaxRepresentableDeadlineSeconds;

            // A suspect deadline is reported as saturated rather than as its own flag: `jobRelease`
            // has no spare bit (0 is the detection path, 1 the waiver, [2,8) the queue depth), and
            // `kSaturated` already means "the real value exceeded the payload", which is exactly the
            // case. The miss site recomputes the suspicion from `SchedState`, so nothing has to be
            // carried through `Job` to reach it.
            const std::uint32_t relativeDeadlineNs = suspectDeadline ? gr::trace::kSaturated                                      //
                                                     : deadlineSeconds > 0.0                                                      //
                                                         ? gr::trace::saturate(static_cast<std::uint64_t>(deadlineSeconds * 1e9)) //
                                                         : gr::trace::kUnsetDeadline;

            // Saturating at 63 rather than masking: the queue depth shares a byte with two flags and
            // gets six bits, while `max_outstanding_jobs` defaults to 64 -- so masking would wrap a
            // full queue to 0 and report "empty" for "completely full". Read together with
            // `jobReleaseDropped`, which carries the true capacity, 63 is unambiguous.
            const std::uint8_t packedDepth = static_cast<std::uint8_t>(std::min(state.jobs.size, 63UZ) << 2U);

            gr::trace::emit(gr::trace::Event{.startNs = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count()), //
                .payload0                             = gr::trace::saturate(batch),
                .payload1                             = relativeDeadlineNs,
                .payload2                             = gr::trace::saturate(unassigned), //
                .entity                               = state.entityId,
                .kind                                 = gr::trace::Kind::jobRelease,
                .workerId                             = state.workerId, //
                .flags                                = static_cast<std::uint8_t>(traceFlags | (draining ? gr::trace::flag::kEosWaived : 0U) | packedDepth)});
        }
    }
}

/// Retires the job at the head of the queue, returning its whole assignment to the unassigned pool.
/// The job's actual consumption is deliberately not consulted: `work()` may legitimately process
/// less than it was asked for, and carrying a remainder would make ring entries mutable.
inline void retireFrontJob(SchedState& state) {
    if (state.jobs.empty()) {
        return;
    }
    const std::size_t batch = state.jobs.front().batch;
    state.assignedSamples   = state.assignedSamples > batch ? state.assignedSamples - batch : 0UZ;
    state.jobs.pop();
}

/// A period is only as sound as the batch it was computed at, so its provenance follows the
/// batch's: configuration where a value was configured, `derivedFromRate` where the rate model
/// alone settled it, and `assumedBatch` where a stand-in was needed. Note `userSet` does **not**
/// carry over -- on a period that tag means "the user set `period`", not "the user set the batch".
[[nodiscard]] constexpr AttributeOrigin periodOriginFor(AttributeOrigin batchOrigin) noexcept {
    switch (batchOrigin) {
    case AttributeOrigin::derivedFromRate: return AttributeOrigin::derivedFromRate;
    case AttributeOrigin::userSet:
    case AttributeOrigin::configured: return AttributeOrigin::configured;
    case AttributeOrigin::assumedBatch:
    case AttributeOrigin::unknown: break;
    }
    return AttributeOrigin::assumedBatch;
}

/**
 * @brief The batch interval a block runs at, and where the number came from.
 *
 * `executionCeiling` and `nominalBatch` answer different questions and coincide only when
 * something actually bounds the batch. The executor needs what to *request*; the derivation needs
 * an operating point to compute a period at. Where nothing bounds the batch the former stays
 * `kUnboundedBatch` -- exactly what the scheduler passes today -- while the latter falls back to
 * the strategy's ceiling, because "unbounded" is not a usable period input.
 */
struct BatchResolution {
    std::size_t     floor            = 1UZ;
    std::size_t     executionCeiling = kUnboundedBatch;
    std::size_t     nominalBatch     = 1UZ;
    AttributeOrigin origin           = AttributeOrigin::unknown;
};

/**
 * @brief Resolves a block's batch interval from its own setting, the scheduler's, and its ports.
 *
 * Precedence, first match wins:
 *  1. the block's `max_batch_size`, when non-zero    -> `userSet`
 *  2. `schedulerCeiling`, when it binds              -> `configured`
 *  3. the smallest synchronous `max_samples`         -> `derivedFromRate`
 *  4. otherwise unbounded; `nominalBatch` then falls back to `strategyCeiling` -> `assumedBatch`
 *
 * A `BatchStrategy` that chooses its own per-block batch simply returns without calling this --
 * the strategy is the outer layer, and this is the helper the default one is built from. Any
 * strategy that bypasses it takes on the obligation of honouring rule 1, since a user's explicit
 * `max_batch_size` must never be overridden by a derived value.
 *
 * Both results are clamped up to the release threshold (a ceiling below it cannot take effect --
 * `computeResampling()` clamps `requestedWork` up to `minSync`) and floored to a whole number of
 * resampling chunks, mirroring the arithmetic the block itself performs.
 */
[[nodiscard]] inline BatchResolution resolveBatch(BlockModel& block, std::size_t schedulerCeiling, std::size_t strategyCeiling) {
    const std::size_t threshold = detail::releaseThreshold(block);
    const std::size_t chunk     = std::max(detail::settingAsSize(block, "input_chunk_size", 1UZ), 1UZ);

    const auto applyLimits = [threshold, chunk](std::size_t batch) {
        const std::size_t clamped = std::max(batch, threshold);
        const std::size_t floored = (clamped / chunk) * chunk; // whole chunks only, as computeResampling() does
        return std::max(floored, threshold);
    };

    BatchResolution resolution{.floor = threshold};

    // A strided resampling block runs exactly one chunk per invocation whatever it is asked for:
    // computeResampling() caps it, and clamps a smaller request back up. The ceiling is inert, so
    // report the window the block will actually run rather than a number that suggests otherwise.
    if (detail::strideActive(block) && detail::isResampling(block)) {
        return BatchResolution{.floor = std::max(threshold, chunk), .executionCeiling = chunk, .nominalBatch = chunk, .origin = AttributeOrigin::derivedFromRate};
    }

    // `executionCeiling` carries only what the block does not already know. It enforces its own
    // port bounds inside computeSampleLimits(), so restating them here is at best redundant and at
    // worst wrong -- an input-sample ceiling derived from an output bound changes what the block
    // runs. Only an explicit block-level or scheduler-level ceiling is new information.
    if (const std::size_t blockCeiling = detail::settingAsSize(block, "max_batch_size", 0UZ); blockCeiling != 0UZ) {
        resolution.executionCeiling = applyLimits(blockCeiling);
        resolution.origin           = AttributeOrigin::userSet;
    } else if (schedulerCeiling != kUnboundedBatch) {
        resolution.executionCeiling = applyLimits(schedulerCeiling);
        resolution.origin           = AttributeOrigin::configured;
    } else {
        resolution.executionCeiling = kUnboundedBatch; // nothing explicit: request as much as the scheduler does today
    }

    const std::size_t ports = detail::portCeiling(block);
    if (resolution.executionCeiling != kUnboundedBatch) {
        // The block clamps `requestedWork` to its own port bounds, so the batch it *runs* is the
        // smaller of the two. Reporting the ceiling alone would overstate `nominalBatch` and make
        // the derived period correspondingly too long.
        if (ports != kUnboundedBatch && ports < resolution.executionCeiling) {
            resolution.nominalBatch = applyLimits(ports);
            resolution.origin       = AttributeOrigin::derivedFromRate; // the port bound is what binds, not the request
        } else {
            resolution.nominalBatch = resolution.executionCeiling;
        }
    } else if (ports != kUnboundedBatch) {
        // Port bounds are sound *modelling* input even though they must not be executed against.
        resolution.nominalBatch = applyLimits(ports);
        resolution.origin       = AttributeOrigin::derivedFromRate;
    } else if (detail::strideActive(block)) {
        // A 1:1 block with an active stride runs the pre-2024 contract: its window is whatever the
        // port bound and the scheduler allow, so with nothing bounding it the window is a scheduler
        // artefact and no honest static value exists. `unknown` withholds the period -- decision 5,
        // row 3.
        resolution.nominalBatch = applyLimits(strategyCeiling);
        resolution.origin       = AttributeOrigin::unknown;
    } else {
        resolution.nominalBatch = applyLimits(strategyCeiling);
        resolution.origin       = AttributeOrigin::assumedBatch;
    }
    return resolution;
}

/**
 * @brief Chooses the batch interval the derivation assumes, and the ceiling the worker requests.
 *
 * Runtime-polymorphic on purpose: the analysis runs once per (re)start, never in the work loop,
 * so dispatch cost is irrelevant and the strategy stays selectable at run time.
 *
 * The strategy is the **outer** layer: `resolveBatch()` is the helper the shipped strategy is
 * built from, not a gate around it. A strategy that computes its own batch (a future
 * optimiser-fed one) must still honour a user's explicit `max_batch_size`.
 */
struct BatchStrategy {
    BatchStrategy()                                = default;
    BatchStrategy(const BatchStrategy&)            = delete;
    BatchStrategy& operator=(const BatchStrategy&) = delete;
    virtual ~BatchStrategy()                       = default;

    [[nodiscard]] virtual std::string_view name() const noexcept                                          = 0;
    [[nodiscard]] virtual BatchResolution  resolve(BlockModel& block, std::size_t schedulerCeiling) const = 0;
};

/**
 * @brief Assumes each block runs at whatever bounds its per-invocation batch.
 *
 * Delegates entirely to `resolveBatch()`, supplying `batchCeiling` as the modelling stand-in for
 * a block nothing bounds. That stand-in reaches `nominalBatch` only -- never the worker -- because
 * an unbounded block genuinely does process everything available, and substituting a finite number
 * there would cap every graph.
 *
 * Keying on the ceiling rather than `min_samples` reflects GR4's normal behaviour of consuming
 * whatever is available: the release threshold would put most blocks at a single sample, yielding
 * per-sample periods and utilisations far above 1 for ordinary graphs.
 */
struct NominalBatchStrategy : BatchStrategy {
    std::size_t batchCeiling = 4096UZ; /// modelling stand-in for a block whose ports are all unbounded

    [[nodiscard]] std::string_view name() const noexcept override { return "NominalBatch"; }

    [[nodiscard]] BatchResolution resolve(BlockModel& block, std::size_t schedulerCeiling) const override { return resolveBatch(block, schedulerCeiling, batchCeiling); }
};

namespace detail {

[[nodiscard]] inline double settingAsDouble(const BlockModel& block, const std::string& key, double fallback) {
    const std::optional<Value> value = block.settings().get(key);
    if (!value.has_value()) {
        return fallback;
    }
    if (const auto* asFloat = value->get_if<float>()) {
        return static_cast<double>(*asFloat);
    }
    if (const auto* asDouble = value->get_if<double>()) {
        return *asDouble;
    }
    if (const auto* asSize = value->get_if<gr::Size_t>()) {
        return static_cast<double>(*asSize);
    }
    if (const auto* asInt32 = value->get_if<std::int32_t>()) { // e.g. sched_priority
        return static_cast<double>(*asInt32);
    }
    if (const auto* asInt64 = value->get_if<std::int64_t>()) {
        return static_cast<double>(*asInt64);
    }
    return fallback;
}

[[nodiscard]] inline bool hasSampleRate(const BlockModel& block) { return block.settings().get(std::string(gr::tag::SAMPLE_RATE.shortKey())).has_value(); }

} // namespace detail

/**
 * @brief Derives per-block scheduling attributes from graph topology and resampling ratios.
 *
 * Side-effect free: it reads the graph and returns a result. `graph` is taken by mutable
 * reference only because port metadata (`min_samples`/`max_samples`) is reachable solely through
 * non-const accessors; nothing is written.
 *
 * What it derives:
 * - **relative rates** — always, by propagating `output_chunk_size / input_chunk_size` along
 *   edges in breadth-first order from the source blocks;
 * - **periods** — only where a source supplies an anchor (`sample_rate`); a graph without one
 *   yields unset periods and a diagnostic, never a fabricated value;
 * - **relative deadlines** — implicit (equal to the period) unless the user set one;
 * - **priorities** — rate-monotonic (shorter period wins) where periods are known.
 *
 * `wcet_estimate` is never derived: it comes from measurement or the user, or stays unknown.
 * User-set attributes are preserved exactly as supplied.
 */
template<typename TUserSetLookup>
requires std::invocable<const TUserSetLookup&, const BlockModel&>
[[nodiscard]] SchedulingAnalysis deriveSchedulingAttributes(gr::Graph& graph, const BatchStrategy& strategy, const TUserSetLookup& userSet, std::size_t schedulerCeiling = kUnboundedBatch) {
    SchedulingAnalysis analysis;

    const gr::graph::AdjacencyList                     adjacency = gr::graph::computeAdjacencyList(graph);
    const std::vector<std::shared_ptr<gr::BlockModel>> sources   = gr::graph::findSourceBlocks(adjacency);

    for (const std::shared_ptr<BlockModel>& block : graph.blocks()) {
        const BatchResolution batch = strategy.resolve(*block, schedulerCeiling);

        DerivedAttributes attributes;
        attributes.batchFloor       = batch.floor;
        attributes.executionCeiling = batch.executionCeiling;
        attributes.nominalBatch     = batch.nominalBatch;
        attributes.batchOrigin      = batch.origin;
        analysis.perBlock.emplace(block.get(), attributes);
    }

    if (sources.empty() && !graph.blocks().empty()) {
        analysis.diagnostics.emplace_back("no source block found (every block has an incoming edge); relative rates left at 1");
    }

    // Relative-rate relaxation from the sources.
    //
    // How a block combines several predecessors is decided by the *destination port's* waiting
    // semantics, not by policy: a **synchronous** port makes the block wait for all its inputs, so
    // its rate is that of the **slowest** contributor; an **asynchronous** port lets it run when
    // any input has data, so the **fastest** wins (Block.hpp:1482 and :1484 respectively).
    //
    // A block is therefore re-enqueued whenever its effective rate *changes* -- not merely when it
    // rises. The min-rule lowers rates, and a lowering must reach everything downstream exactly as
    // a raising one must; a visit-once sweep, or a rise-only guard, leaves successors holding a
    // stale rate on diamond topologies. `relaxationLimit` keeps gain cycles finite.
    struct RateAccumulator {
        double            syncMin     = std::numeric_limits<double>::infinity();
        double            asyncMax    = 0.0;
        bool              hasSync     = false;
        bool              hasAsync    = false;
        const BlockModel* syncOrigin  = nullptr;
        const BlockModel* asyncOrigin = nullptr;

        /// The two groups are combined with **OR**: `workInternal()` proceeds when the synchronous
        /// ports are satisfied *or* any asynchronous port has data
        /// (Block.hpp:2062). A block holding both kinds is therefore invoked at whichever of the
        /// two triggers fires more often -- the *slowest* synchronous input, or the *fastest*
        /// asynchronous one, whichever is faster.
        [[nodiscard]] double rate() const noexcept {
            if (hasSync && hasAsync) {
                return std::max(syncMin, asyncMax);
            }
            return hasSync ? syncMin : (hasAsync ? asyncMax : 1.0);
        }

        [[nodiscard]] const BlockModel* origin() const noexcept {
            if (hasSync && hasAsync) {
                return syncMin >= asyncMax ? syncOrigin : asyncOrigin;
            }
            return hasSync ? syncOrigin : asyncOrigin;
        }
    };

    std::unordered_map<const BlockModel*, RateAccumulator> accumulator;
    std::unordered_set<const BlockModel*>                  reached;
    std::unordered_set<const BlockModel*>                  conflicting; // report each contested block once
    std::unordered_set<const BlockModel*>                  sourceSet;
    std::queue<std::shared_ptr<BlockModel>>                pending;
    for (const std::shared_ptr<BlockModel>& source : sources) {
        reached.insert(source.get());
        sourceSet.insert(source.get());
        if (const auto it = analysis.perBlock.find(source.get()); it != analysis.perBlock.end()) {
            it->second.originSource = source.get();
        }
        pending.push(source);
    }

    const std::size_t nBlocks         = graph.blocks().size();
    const std::size_t relaxationLimit = nBlocks * nBlocks + nBlocks + 1UZ;
    std::size_t       relaxations     = 0UZ;

    while (!pending.empty() && relaxations < relaxationLimit) {
        const std::shared_ptr<BlockModel> current = pending.front();
        pending.pop();

        const auto currentIt = analysis.perBlock.find(current.get());
        if (currentIt == analysis.perBlock.end() || !adjacency.contains(current)) {
            continue;
        }
        // Rate of the stream `current` emits. A source has no input stream, so its own relative
        // rate *is* what it emits; every other block scales its input rate by its own resampling
        // ratio over its own advance -- the upstream block's numbers, never the downstream one's.
        const double emitted = sourceSet.contains(current.get()) //
                                   ? currentIt->second.relativeSampleRate
                                   : currentIt->second.relativeSampleRate * detail::settingAsDouble(*current, "output_chunk_size", 1.0) / static_cast<double>(detail::effectiveAdvance(*current));

        for (const auto& edges : adjacency.at(current) | std::views::values) {
            for (const gr::Edge* edge : edges) {
                const std::shared_ptr<BlockModel>& next   = edge->destinationBlock();
                const auto                         nextIt = analysis.perBlock.find(next.get());
                if (nextIt == analysis.perBlock.end()) {
                    continue;
                }

                // An unresolvable port is treated as synchronous: the conservative reading, and the
                // overwhelmingly common one.
                auto       destinationPort = next->dynamicInputPort(edge->destinationPortDefinition());
                const bool isSynchronous   = !destinationPort.has_value() || destinationPort.value()->isSynchronous();

                RateAccumulator& accumulated = accumulator[next.get()];
                const double     before      = accumulated.rate();
                const bool       hadAny      = accumulated.hasSync || accumulated.hasAsync;

                if (isSynchronous) {
                    if (accumulated.hasSync && accumulated.syncMin != emitted && conflicting.insert(next.get()).second) {
                        // Synchronous inputs arriving at different rates means the faster branch
                        // must pile up without bound -- GR4 has neither sample discard nor
                        // lookahead -- so this is far more likely a wiring error than an intent.
                        analysis.diagnostics.emplace_back(std::format("'{}' has synchronous inputs of differing rates ({} vs {}); it can only run at the slower, so the faster branch will back up -- check the graph", next->name(), accumulated.syncMin, emitted));
                    }
                    if (!accumulated.hasSync || emitted < accumulated.syncMin) {
                        accumulated.syncMin    = emitted;
                        accumulated.syncOrigin = currentIt->second.originSource;
                    }
                    accumulated.hasSync = true;
                } else {
                    if (!accumulated.hasAsync || emitted > accumulated.asyncMax) {
                        accumulated.asyncMax    = emitted;
                        accumulated.asyncOrigin = currentIt->second.originSource;
                    }
                    accumulated.hasAsync = true;
                }

                reached.insert(next.get());
                if (hadAny && accumulated.rate() == before) {
                    continue; // nothing changed here, so nothing downstream can change either
                }
                nextIt->second.relativeSampleRate = accumulated.rate();
                nextIt->second.originSource       = accumulated.origin(); // the anchor travels with the rate
                pending.push(next);
                ++relaxations;
            }
        }
    }
    if (relaxations >= relaxationLimit) {
        analysis.diagnostics.emplace_back("relative-rate propagation did not converge (a cycle appears to amplify its rate); rates are truncated");
    }

    for (const std::shared_ptr<BlockModel>& block : graph.blocks()) {
        // A container block (a flattened subgraph's wrapper) carries no stream of its own -- its
        // children were hoisted alongside it and are analysed individually -- so reporting it as
        // "unreachable" is noise, not information. Its own rate and period stay inert.
        const bool isContainer = block->blockCategory() != gr::block::Category::NormalBlock;
        if (!isContainer && !reached.contains(block.get())) {
            analysis.diagnostics.emplace_back(std::format("'{}' is not reachable from any source; its rate is assumed equal to the source rate", block->name()));
        }
    }

    // The invocation rate is now a *derived* quantity: a block processing `nominalBatch` samples of
    // a stream at `relativeSampleRate` fires proportionally to rate/batch. Expressed relative to a
    // reference source, so it stays meaningful on graphs with no absolute anchor at all.
    const auto consumedPerInvocation = [&analysis](BlockModel& block) {
        // What the *stream* advances per invocation, which is what sets how often the block fires.
        // Only stride separates this from the batch the block processes.
        return detail::strideActive(block) ? static_cast<double>(detail::effectiveAdvance(block)) : static_cast<double>(analysis.perBlock.at(std::addressof(block)).nominalBatch);
    };

    // N.B. `sources` comes from the adjacency list, which may name blocks that are not entries of
    // `graph.blocks()` -- so this must not be an unguarded `perBlock.at()`.
    // Normalised against the block's *own* origin, so `relativeRate` stays "invocations per
    // invocation of my source" on a graph with several independent chains.
    for (const std::shared_ptr<BlockModel>& block : graph.blocks()) {
        DerivedAttributes& attributes = analysis.perBlock.at(block.get());
        const double       advance    = consumedPerInvocation(*block);

        double referenceAdvance = 1.0;
        if (attributes.originSource != nullptr && analysis.perBlock.contains(attributes.originSource)) {
            referenceAdvance = consumedPerInvocation(const_cast<BlockModel&>(*attributes.originSource));
        }
        attributes.relativeRate = advance > 0.0 ? attributes.relativeSampleRate * referenceAdvance / advance : attributes.relativeSampleRate;
    }

    // Anchor to seconds *per origin*, not once for the whole graph. Two independent chains at
    // different sample rates have two timebases, and forcing them onto one silently rescales every
    // period in the losing chain -- by 48x for a 1 kHz chain measured against a 48 kHz anchor. It
    // was also decided by block *name*, since findSourceBlocks() sorts by name, so renaming a block
    // could change every derived period in the graph.
    std::unordered_map<const BlockModel*, double> anchorOf;
    std::size_t                                   anchoredSources = 0UZ;
    for (const std::shared_ptr<BlockModel>& source : sources) {
        const double rate = detail::hasSampleRate(*source) ? detail::settingAsDouble(*source, std::string(gr::tag::SAMPLE_RATE.shortKey()), 0.0) : 0.0;
        anchorOf.emplace(source.get(), rate > 0.0 ? rate : 0.0);
        if (rate > 0.0) {
            ++anchoredSources;
        }
    }
    if (anchoredSources == 0UZ && !sources.empty()) {
        analysis.diagnostics.emplace_back("no source declares a positive 'sample_rate'; relative rates derived but periods left unset");
    } else {
        for (const std::shared_ptr<BlockModel>& source : sources) {
            if (anchorOf.at(source.get()) <= 0.0) {
                analysis.diagnostics.emplace_back(std::format("source '{}' declares no positive 'sample_rate'; the blocks it feeds get no period", source->name()));
            }
        }
    }

    const auto anchorFor = [&anchorOf](const DerivedAttributes& attributes) {
        const auto it = attributes.originSource == nullptr ? anchorOf.end() : anchorOf.find(attributes.originSource);
        return it == anchorOf.end() ? 0.0 : it->second;
    };

    for (const std::shared_ptr<BlockModel>& block : graph.blocks()) {
        DerivedAttributes&      attributes = analysis.perBlock.at(block.get());
        const UserSetAttributes explicitly = userSet(*block);

        if (explicitly.period) {
            attributes.period       = static_cast<float>(detail::settingAsDouble(*block, "period", 0.0));
            attributes.periodOrigin = AttributeOrigin::userSet;
        } else if (attributes.batchOrigin == AttributeOrigin::unknown) {
            // decision 5, row 3: a 1:1 block with an active stride and nothing bounding its window
            analysis.diagnostics.emplace_back(std::format("'{}' has an active stride but no bounded window; its batch is scheduler-dependent and its period is left unset", block->name()));
        } else if (const double anchorSampleRate = anchorFor(attributes); anchorSampleRate > 0.0 && attributes.relativeSampleRate > 0.0) {
            // A block advancing `consumedPerInvocation` samples through a stream arriving at
            // `anchorSampleRate * relativeSampleRate` samples/s releases every advance/rate
            // seconds. N.B. the *advance*, not the batch it processes: under an active stride an
            // overlapping block fires more often than its window suggests, and a skipping one less
            // often. Execution cost still scales with the window -- the two quanta.
            const double arrivalRate = anchorSampleRate * attributes.relativeSampleRate;
            attributes.period        = static_cast<float>(consumedPerInvocation(*block) / arrivalRate);
            attributes.periodOrigin  = periodOriginFor(attributes.batchOrigin); // the period is only as sound as the batch it assumes
        }

        if (explicitly.deadline) {
            attributes.relativeDeadline = static_cast<float>(detail::settingAsDouble(*block, "relative_deadline", 0.0));
            attributes.deadlineOrigin   = AttributeOrigin::userSet;
        } else if (attributes.period > 0.f) {
            attributes.relativeDeadline = attributes.period; // implicit deadline
            attributes.deadlineOrigin   = attributes.periodOrigin;
        }

        if (explicitly.wcet) {
            attributes.wcetEstimate = static_cast<float>(detail::settingAsDouble(*block, "wcet_estimate", 0.0));
            attributes.wcetOrigin   = AttributeOrigin::userSet;
        } // otherwise left unknown: never derived from the graph
    }

    // rate-monotonic priority: shorter period -> larger value; user-set priorities are untouched
    std::vector<const BlockModel*> byPeriod;
    for (const std::shared_ptr<BlockModel>& block : graph.blocks()) {
        if (!userSet(*block).priority && analysis.perBlock.at(block.get()).period > 0.f) {
            byPeriod.push_back(block.get());
        }
    }
    std::ranges::sort(byPeriod, [&](const BlockModel* lhs, const BlockModel* rhs) {
        const float lhsPeriod = analysis.perBlock.at(lhs).period;
        const float rhsPeriod = analysis.perBlock.at(rhs).period;
        return lhsPeriod != rhsPeriod ? lhsPeriod < rhsPeriod : lhs->uniqueName() < rhs->uniqueName();
    });
    std::int32_t rank = static_cast<std::int32_t>(byPeriod.size());
    for (const BlockModel* block : byPeriod) {
        DerivedAttributes& attributes = analysis.perBlock.at(block);
        attributes.priority           = rank--;
        attributes.priorityOrigin     = AttributeOrigin::derivedFromRate;
    }

    for (const std::shared_ptr<BlockModel>& block : graph.blocks()) {
        if (userSet(*block).priority) {
            DerivedAttributes& attributes = analysis.perBlock.at(block.get());
            attributes.priority           = static_cast<std::int32_t>(detail::settingAsDouble(*block, "sched_priority", 0.0));
            attributes.priorityOrigin     = AttributeOrigin::userSet;
        }
    }

    return analysis;
}

/// Samples which scheduling attributes were explicitly set, from auto-update membership.
/// Must be called before any derived value is written back, since `settings().set()` also
/// prunes auto-update and would make derived values indistinguishable from user-set ones.
[[nodiscard]] inline UserSetAttributes userSetFromSettings(BlockModel& block) {
    const auto& autoUpdate = block.settings().autoUpdateParameters();
    const auto  wasSet     = [&autoUpdate](std::string_view key) { return !autoUpdate.contains(std::string(key)); };
    return UserSetAttributes{.priority = wasSet("sched_priority"), .period = wasSet("period"), .deadline = wasSet("relative_deadline"), .wcet = wasSet("wcet_estimate")};
}

} // namespace gr::scheduler

#endif // GNURADIO_SCHEDULINGANALYSIS_HPP
