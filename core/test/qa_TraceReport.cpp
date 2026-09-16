#include <boost/ut.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceCatapult.hpp>
#include <gnuradio-4.0/TraceFile.hpp>
#include <gnuradio-4.0/TraceReport.hpp>

using namespace boost::ut;
using namespace gr::trace;

/**
 * The timing report, tested on **synthetic** records rather than on a running graph.
 *
 * That is the point rather than a shortcut. A report derived from a real capture can only be checked
 * against itself -- the numbers look plausible, and plausible is exactly what a wrong report looks
 * like. Records built from a known cost model have a known answer, so the arithmetic can be wrong and
 * be caught. The scheduling suites already prove the records themselves are produced correctly; this
 * file proves what is computed from them.
 */
namespace {

/// `ValueMap::at()` indexes by position, so a field is read through `find`.
template<typename T>
[[nodiscard]] T fieldOr(const gr::property_map& map, std::string_view key, T fallback) {
    const auto it = map.find(key);
    return it != map.end() ? (*it).second.value_or(std::move(fallback)) : fallback;
}

[[nodiscard]] gr::property_map blockOf(const gr::property_map& report, std::string_view name) {
    const auto blocks = report.find("blocks");
    if (blocks == report.end()) {
        return {};
    }
    // A nested map must be bound to a `Value` before it can be read: `find()` yields a view, and the
    // view type has no map overload -- the diagnostic for getting this wrong points at Value.hpp
    // rather than here, which is worth a comment.
    const gr::pmt::Value   blocksValue = (*blocks).second;
    const gr::property_map nested      = blocksValue.value_or(gr::property_map{});
    const auto             block       = nested.find(name);
    if (block == nested.end()) {
        return {};
    }
    const gr::pmt::Value blockValue = (*block).second;
    return blockValue.value_or(gr::property_map{});
}

/// One productive invocation: `performed_work` items costing `durationNs`.
[[nodiscard]] Event invocation(EntityId entity, std::uint32_t performedWork, std::uint32_t durationNs, std::int8_t status = 0) { return Event{.durationNs = durationNs, .payload0 = performedWork, .payload1 = performedWork, .entity = entity, .kind = Kind::workEnd, .status = status}; }

/// Records from an exact cost model, so the report has a known answer to be checked against.
[[nodiscard]] std::vector<Event> fromCostModel(EntityId entity, std::uint64_t interceptNs, double slopeNsPerItem, const std::vector<std::uint32_t>& batches, std::size_t repeats = 8UZ) {
    std::vector<Event> events;
    for (const std::uint32_t batch : batches) {
        for (std::size_t r = 0UZ; r < repeats; ++r) {
            // A floor that rises with the repeat index, so the per-bucket *minimum* is the exact model
            // value and everything above it is the noise a real measurement would carry.
            const auto cost = static_cast<std::uint32_t>(static_cast<double>(interceptNs) + slopeNsPerItem * static_cast<double>(batch)) + static_cast<std::uint32_t>(r);
            events.push_back(invocation(entity, batch, cost));
        }
    }
    return events;
}

} // namespace

const boost::ut::suite<"TraceReport"> reportTests = [] {
    "a record lands in the bucket its work count belongs to"_test = [] {
        // Powers of two by `bit_width`: 0 is its own bucket, 1 is bucket 1, 2-3 bucket 2, 4-7 bucket 3.
        TimingAccumulator accumulator;
        for (const std::uint32_t work : {0U, 1U, 2U, 3U, 4U, 7U, 8U}) {
            accumulator.fold(invocation(1U, work, 100U));
        }
        const BlockTiming& timing = accumulator.blocks.at(1U);

        expect(eq(timing.buckets[0].count, 1UL)) << "zero work is its own bucket, not folded into the first";
        expect(eq(timing.buckets[1].count, 1UL)) << "a single item";
        expect(eq(timing.buckets[2].count, 2UL)) << "2 and 3 share a bucket";
        expect(eq(timing.buckets[3].count, 2UL)) << "4 and 7 share one; 8 does not, being the next power of two";
        expect(eq(timing.buckets[4].count, 1UL));
        expect(eq(timing.populatedBuckets(), 5UZ));
    };

    "the top of the payload range has a bucket of its own"_test = [] {
        // The table is sized for what `saturate()` can produce, not for what a plausible graph does.
        // A shorter one would fold every large batch together and collapse the span a fit needs.
        TimingAccumulator accumulator;
        accumulator.fold(invocation(1U, 0xFFFFFFFDU, 5000U));
        const BlockTiming& timing = accumulator.blocks.at(1U);

        expect(eq(timing.buckets[kWorkBuckets - 1UZ].count, 1UL)) << "the largest representable work count must have somewhere to go";
        expect(eq(timing.populatedBuckets(), 1UZ));
    };

    "a saturated payload or duration is never treated as a measurement"_test = [] {
        TimingAccumulator accumulator;
        accumulator.fold(invocation(1U, kSaturated, 100U));
        accumulator.fold(invocation(1U, 64U, kSaturated));
        accumulator.fold(invocation(1U, 64U, 100U));

        expect(eq(accumulator.admitted, 1UL)) << "only the real invocation counts";
        expect(eq(accumulator.rejectedSaturated, 2UL)) << "and both sentinels are rejected, not silently dropped";
        expect(eq(accumulator.blocks.at(1U).overall.maxNs, 100U)) << "a saturated duration admitted here would become the worst case";
    };

    "only successful invocations contribute, because the others report no work"_test = [] {
        // `computePerformedWork()` returns zero for any status but OK, so a DONE record says a block
        // did nothing during an invocation that may have done a great deal. Admitting it puts a
        // (0 work, real cost) point into the data, which is an attack on the intercept specifically.
        TimingAccumulator accumulator;
        accumulator.fold(invocation(1U, 0U, 9000U, -1)); // DONE
        accumulator.fold(invocation(1U, 0U, 9000U, -2)); // INSUFFICIENT_INPUT_ITEMS
        accumulator.fold(invocation(1U, 512U, 600U, 0)); // OK

        expect(eq(accumulator.admitted, 1UL));
        expect(eq(accumulator.rejectedStatus, 2UL));
        expect(eq(accumulator.blocks.at(1U).overall.count, 1UL)) << "one sample, from the one invocation that reported its work honestly";
        expect(eq(accumulator.blocks.at(1U).overall.maxNs, 600U)) << "a 9000 ns DONE record admitted here would be reported as the worst case";
    };

    "a record naming no block cannot be attributed to one"_test = [] {
        TimingAccumulator accumulator;
        accumulator.fold(invocation(kNoEntity, 64U, 100U));
        expect(eq(accumulator.admitted, 0UL));
        expect(eq(accumulator.rejectedNoEntity, 1UL));
        expect(accumulator.blocks.empty()) << "an unattributable cost belongs to no block, not to a placeholder one";
    };

    "average, jitter and worst case are computed over the invocations"_test = [] {
        TimingAccumulator accumulator;
        for (const std::uint32_t cost : {100U, 200U, 300U, 400U}) {
            accumulator.fold(invocation(1U, 64U, cost));
        }
        const Bucket& overall = accumulator.blocks.at(1U).overall;

        expect(eq(overall.count, 4UL));
        expect(std::abs(overall.mean - 250.0) < 1e-9) << "Welford must agree with the arithmetic mean";
        expect(std::abs(overall.stddev() - 129.0994448736) < 1e-6) << "and with the sample standard deviation";
        expect(eq(overall.maxNs, 400U)) << "the observed worst case is the largest seen, not a percentile";
        expect(eq(overall.minNs, 100U)) << "and the floor is the smallest, which is what the fit will use";
    };

    "the statistics survive a single batch size, where a fit could not"_test = [] {
        // The counterpart to refusing a slope: ACET, jitter and WCET need no regression and remain
        // valid from one batch size. The likeliest over-correction is to throw out the whole report.
        const std::vector<Event> events = fromCostModel(1U, 500UL, 2.0, {512U});
        const gr::property_map   block  = blockOf(timingReport(events), "entity 1");

        expect(gt(fieldOr<std::uint64_t>(block, "invocations", 0UL), 0UL) >> fatal) << "the summary must be produced at all";
        expect(eq(fieldOr<std::uint64_t>(block, "buckets_populated", 0UL), 1UL)) << "one batch size is one bucket, which is exactly the ill-conditioned case";
        expect(gt(fieldOr<double>(block, "acet_ns", 0.0), 0.0)) << "and the average is still a number worth having";
        expect(ge(fieldOr<std::uint64_t>(block, "wcet_ns", 0UL), static_cast<std::uint64_t>(fieldOr<double>(block, "acet_ns", 0.0)))) << "the worst case cannot be below the average";
    };

    "the fit recovers a cost model it was never told"_test = [] {
        // The gate for this milestone. Until a fit is shown to recover a *known* answer, every number
        // downstream of it is unverified arithmetic that happens to look reasonable. The per-bucket
        // minimum is the exact model value by construction here, so the recovery should be tight.
        constexpr std::uint64_t kIntercept = 800UL;
        constexpr double        kSlope     = 3.0;

        TimingAccumulator accumulator;
        for (const Event& event : fromCostModel(1U, kIntercept, kSlope, {64U, 256U, 1024U, 4096U, 16384U})) {
            accumulator.fold(event);
        }
        const Fit fit = fitCost(accumulator.blocks.at(1U));

        expect(fit.identifiable >> fatal) << "five well-separated batch sizes are as identifiable as this gets: " << fit.reason;
        expect(lt(std::abs(fit.slopeNsPerItem - kSlope), 0.05)) << std::format("slope {:.4f} should be {:.4f} ns per item", fit.slopeNsPerItem, kSlope);
        expect(lt(std::abs(fit.interceptNs - static_cast<double>(kIntercept)), 50.0)) << std::format("intercept {:.1f} should be {} ns", fit.interceptNs, kIntercept);
        expect(lt(fit.residualRms, kMaxFitResidual)) << "and the line must describe the points it was fitted to";
    };

    "a single batch size yields no slope, not a confident one"_test = [] {
        // The defect the gates exist to prevent: one bucket, and a line through one point has any
        // slope you care to give it. This feeds admission decisions, so a plausible number here is
        // invented data entering a scheduling decision.
        TimingAccumulator accumulator;
        for (const Event& event : fromCostModel(1U, 800UL, 3.0, {512U})) {
            accumulator.fold(event);
        }
        const Fit fit = fitCost(accumulator.blocks.at(1U));

        expect(!fit.identifiable) << "one work bucket cannot identify a slope";
        expect(fit.reason.contains("bucket")) << "and the refusal must say which gate stopped it";

        // The keys must be *absent*, not present-and-flagged: a consumer will read a number it finds.
        const gr::property_map block = blockOf(timingReport(fromCostModel(1U, 800UL, 3.0, {512U})), "entity 1");
        expect(eq(fieldOr<std::string>(block, "fit", std::string{}), std::string("low-confidence")) >> fatal);
        expect(block.find("item_cost_ns") == block.end()) << "no slope key may exist when no slope was identified";
        expect(block.find("invocation_cost_ns") == block.end()) << "nor an intercept, which is the same line";
        expect(block.find("wcet_estimate_ns") == block.end()) << "nor an estimate derived from a fit that was refused";
    };

    "two batch sizes are not enough, because two points always fit exactly"_test = [] {
        TimingAccumulator accumulator;
        for (const Event& event : fromCostModel(1U, 800UL, 3.0, {64U, 4096U})) {
            accumulator.fold(event);
        }
        const Fit fit = fitCost(accumulator.blocks.at(1U));

        expect(!fit.identifiable) << "a line through two points has no residual, so nothing can disagree with it";
        expect(eq(fit.points, 2UZ));
        expect(fit.reason.contains("at least")) << "the refusal must name the threshold, not merely refuse";
    };

    "a narrow span of work counts cannot separate a slope from an intercept"_test = [] {
        // Reaching this gate at all takes care, and that is itself informative. Buckets are powers of
        // two, so three *distinct* ones normally span at least 4x and the point-count gate would have
        // passed them anyway. The span gate binds independently only when the centroids sit at
        // opposite ends of their buckets -- here 127 at the top of one and 257 at the bottom of the
        // one two along, a span of 2.02x across three buckets.
        TimingAccumulator accumulator;
        for (const Event& event : fromCostModel(1U, 800UL, 3.0, {127U, 128U, 257U})) {
            accumulator.fold(event);
        }
        const Fit fit = fitCost(accumulator.blocks.at(1U));

        expect(ge(fit.points, 3UZ) >> fatal) << "the point-count gate must not be what stops this one";
        expect(!fit.identifiable) << "a span this narrow cannot pin a slope";
        expect(fit.reason.contains("span")) << "and the refusal must identify the span as the reason";
        expect(lt(fit.spanRatio, kMinFitSpan));
    };

    "a fit that does not describe its own inputs is refused"_test = [] {
        // Costs unrelated to the work done: a straight line through them is arithmetically available
        // and physically meaningless, and the residual is what notices.
        TimingAccumulator                                          accumulator;
        const std::vector<std::pair<std::uint32_t, std::uint32_t>> wild{{64U, 9000U}, {512U, 200U}, {4096U, 7000U}, {32768U, 300U}};
        for (const auto& [work, cost] : wild) {
            for (std::size_t r = 0UZ; r < 4UZ; ++r) {
                accumulator.fold(invocation(1U, work, cost));
            }
        }
        const Fit fit = fitCost(accumulator.blocks.at(1U));

        expect(!fit.identifiable) << "noise is not a cost model";
        expect(fit.reason.contains("misses its own inputs") or fit.reason.contains("impossible")) << std::format("the refusal must name the residual or the impossibility, and said: {}", fit.reason);
    };

    "a plausible-looking line that misses its points is refused on the residual alone"_test = [] {
        // Deliberately constructed so the *other* gates pass: four buckets spanning 512x, and a fit
        // whose intercept (+5232 ns) and slope (+0.46 ns/item) both come out positive, leaving only
        // the residual (29 %) to notice that the line does not describe the points. Getting here took
        // arithmetic rather than intuition -- the obvious "wild costs" data produces a *negative*
        // intercept, so the impossible-fit gate fires first and the residual gate is never reached. Without a case like this the residual gate can be deleted
        // with every test still green -- the impossible-fit gate catches the ill-conditioned data
        // first, and the coverage looks complete while resting on one gate doing two jobs.
        TimingAccumulator accumulator;
        for (const auto& [work, cost] : std::vector<std::pair<std::uint32_t, std::uint32_t>>{{64U, 8000U}, {512U, 1000U}, {4096U, 9000U}, {32768U, 20000U}}) {
            for (std::size_t r = 0UZ; r < 4UZ; ++r) {
                accumulator.fold(invocation(1U, work, cost));
            }
        }
        const Fit fit = fitCost(accumulator.blocks.at(1U));

        expect(ge(fit.points, kMinFitPoints) >> fatal) << "the point-count gate must not be what stops this";
        expect(ge(fit.spanRatio, kMinFitSpan) >> fatal) << "nor the span gate";
        expect(!fit.identifiable) << "a line missing its own points by tens of percent describes nothing";
        expect(fit.reason.contains("misses its own inputs")) << std::format("and the residual must be the stated reason; it said: {}", fit.reason);
    };

    "outliers do not move the fit, which is why the floor is used and not the average"_test = [] {
        // RT specifies the per-bucket minimum because preemption and page faults inflate costs upward
        // and never downward, so the floor is the clean signal. Verified rather than assumed: the same
        // model, with a handful of ten-fold outliers in the largest bucket only. A fit built on bucket
        // means would swing its slope badly; one built on floors should not move at all.
        constexpr std::uint64_t          kIntercept = 800UL;
        constexpr double                 kSlope     = 3.0;
        const std::vector<std::uint32_t> batches{64U, 256U, 1024U, 4096U, 16384U};

        TimingAccumulator clean;
        for (const Event& event : fromCostModel(1U, kIntercept, kSlope, batches)) {
            clean.fold(event);
        }
        const Fit cleanFit = fitCost(clean.blocks.at(1U));
        expect(cleanFit.identifiable >> fatal);

        TimingAccumulator disturbed;
        for (const Event& event : fromCostModel(1U, kIntercept, kSlope, batches)) {
            disturbed.fold(event);
        }
        for (std::size_t r = 0UZ; r < 6UZ; ++r) {
            const auto stalled = static_cast<std::uint32_t>(10.0 * (static_cast<double>(kIntercept) + kSlope * 16384.0));
            disturbed.fold(invocation(1U, 16384U, stalled)); // the worker was preempted mid-invocation
        }
        const Fit disturbedFit = fitCost(disturbed.blocks.at(1U));

        expect(disturbedFit.identifiable >> fatal) << "outliers must not make a good measurement unusable: " << disturbedFit.reason;
        expect(lt(std::abs(disturbedFit.slopeNsPerItem - cleanFit.slopeNsPerItem), 1e-9)) << "the floor is unchanged by anything above it, so the slope must be identical";
        expect(lt(std::abs(disturbedFit.slopeNsPerItem - kSlope), 0.05)) << "and must still be the model's slope";

        // The outliers are not discarded -- they are what the observed worst case is *for*.
        expect(gt(disturbed.blocks.at(1U).overall.maxNs, clean.blocks.at(1U).overall.maxNs)) << "a stall belongs in the worst case even though it is kept out of the fit";
    };

    "a physically impossible fit is refused rather than clamped"_test = [] {
        // Cost falling with work: a negative slope fits, and means an item saves time. The model does
        // not describe this block, which is a refusal rather than a number to clamp to zero.
        TimingAccumulator accumulator;
        for (const auto& [work, cost] : std::vector<std::pair<std::uint32_t, std::uint32_t>>{{64U, 4000U}, {512U, 3000U}, {4096U, 2000U}, {32768U, 1000U}}) {
            for (std::size_t r = 0UZ; r < 4UZ; ++r) {
                accumulator.fold(invocation(1U, work, cost));
            }
        }
        const Fit fit = fitCost(accumulator.blocks.at(1U));

        expect(!fit.identifiable) << "an item cannot save time";
        expect(fit.reason.contains("impossible")) << std::format("and the refusal must say so, rather than reporting a clamped zero; it said: {}", fit.reason);
    };

    "refusing a slope does not suppress the statistics that remain valid"_test = [] {
        // The likeliest over-correction to the gates above: throwing out the whole report when only
        // the regression was ill-conditioned. ACET, jitter and worst case need no fit.
        const gr::property_map block = blockOf(timingReport(fromCostModel(1U, 800UL, 3.0, {512U})), "entity 1");

        expect(eq(fieldOr<std::string>(block, "fit", std::string{}), std::string("low-confidence")) >> fatal);
        expect(gt(fieldOr<std::uint64_t>(block, "invocations", 0UL), 0UL)) << "the invocations were still counted";
        expect(gt(fieldOr<double>(block, "acet_ns", 0.0), 0.0)) << "the average is still an average";
        expect(gt(fieldOr<std::uint64_t>(block, "wcet_ns", 0UL), 0UL)) << "and the observed worst case is still what was observed";
    };

    "a block is named from the capture's identity table when it has one"_test = [] {
        const std::vector<Event>        events{invocation(7U, 64U, 100U)};
        const std::vector<LoadedEntity> entities{LoadedEntity{.id = 7U, .workerId = 0U, .nInputPorts = 1U, .nOutputPorts = 1U, .uniqueName = "fir_filter", .typeName = "gr::blocks::Fir<float>"}};

        expect(gt(fieldOr<std::uint64_t>(blockOf(timingReport(events, entities), "fir_filter"), "invocations", 0UL), 0UL)) << "a named capture must report the name";
        expect(gt(fieldOr<std::uint64_t>(blockOf(timingReport(events), "entity 7"), "invocations", 0UL), 0UL)) << "and one without a table must still be readable, by id";
    };

    "every record kind survives the conversion, under its own name and phase"_test = [] {
        // The failure this guards is a converter that drops or mislabels a field and still renders:
        // the timeline looks fine, and whatever went wrong is simply absent without anyone being told.
        //
        // Names and phases are spelled out **here** rather than obtained from the converter. An
        // earlier version built the names by calling `kindName()`, which made the check tautological
        // -- a mutation swapping two names swapped the expectation with it and passed. A test that
        // asks the code under test what the right answer is has no opinion of its own.
        //
        // The phase matters as much as the name: a record exported as an instant instead of a
        // duration loses its duration outright, and the timeline shows a tick where a span belongs.
        struct Expected {
            Kind             kind;
            std::string_view name;
            char             phase; // 'X' complete, 'i' instant, 'C' counter
        };
        const std::vector<Expected> expected{{Kind::workBegin, "workBegin", 'i'}, {Kind::workEnd, "work", 'X'}, {Kind::workProbe, "workProbe", 'C'}, {Kind::workerStart, "workerStart", 'i'}, {Kind::workerStop, "workerStop", 'i'}, {Kind::sweep, "sweep", 'X'}, {Kind::messagePhase, "messagePhase", 'X'}, {Kind::houseKeeping, "houseKeeping", 'X'}, {Kind::stateSync, "stateSync", 'X'}, {Kind::adopt, "adopt", 'X'}, {Kind::zombieReap, "zombieReap", 'X'}, {Kind::quiescenceWait, "quiescenceWait", 'X'}, {Kind::idle, "idle", 'X'}, {Kind::jobRelease, "jobRelease", 'i'}, {Kind::jobReleaseDropped, "jobReleaseDropped", 'i'}, {Kind::releaseScan, "releaseScan", 'X'}, {Kind::select, "select", 'i'}, {Kind::selectEmpty, "selectEmpty", 'i'}, {Kind::selectionBoundHit, "selectionBoundHit", 'i'}, {Kind::heapFallback, "heapFallback", 'i'}, {Kind::deadlineMiss, "deadlineMiss", 'i'}, {Kind::blockCounter, "blockCounter", 'i'}, {Kind::workerCounter, "workerCounter", 'i'}, {Kind::blockStateChange, "blockStateChange", 'i'}, {Kind::entityRetired, "entityRetired", 'i'}, {Kind::workExact, "workExact", 'X'}, {Kind::workPhase, "workPhase", 'X'}};

        // Every enumerator must appear, so adding one without extending the converter fails here
        // rather than producing a record that exports as "unknown".
        expect(eq(expected.size(), static_cast<std::size_t>(kKindCount))) << "the table must cover every Kind the layer declares";

        Capture capture;
        capture.header.processId = 4242UL;
        capture.entities.push_back(LoadedEntity{.id = 1U, .workerId = 0U, .nInputPorts = 1U, .nOutputPorts = 1U, .uniqueName = "mid", .typeName = "gr::testing::Copy<float>"});
        std::uint64_t instant = 1'000'000UL;
        for (const Expected& item : expected) {
            capture.events.push_back(Event{.startNs = instant, .durationNs = 500U, .payload0 = 1U, .payload1 = 2U, .payload2 = 3U, .entity = 1U, .kind = item.kind, .workerId = 0U});
            instant += 1000UL;
        }

        const std::string json = catapultJson(capture);
        for (const Expected& item : expected) {
            const std::string needle = std::format("\"cat\":\"{}\",\"ph\":\"{}\"", item.name, item.phase);
            expect(json.contains(needle)) << std::format("{} must export as phase {}, and did not", item.name, item.phase);
        }

        // A duration record without its duration is the silent half of that failure.
        for (const Expected& item : expected) {
            if (item.phase == 'X') {
                expect(json.contains(std::format("\"cat\":\"{}\",\"ph\":\"X\",\"ts\":", item.name))) << item.name;
            }
        }
        expect(eq(std::ranges::count(json, '{'), std::ranges::count(json, '}'))) << "braces must balance, or the document is not JSON at all";
    };

    "timestamps are rebased, so a capture opens where its records are"_test = [] {
        // `startNs` is steady_clock since an arbitrary epoch. Emitted raw, every event lands tens of
        // thousands of years along the axis and the viewer opens on empty space.
        Capture capture;
        capture.events.push_back(Event{.startNs = 5'000'000'000'000UL, .durationNs = 1000U, .entity = kNoEntity, .kind = Kind::sweep});
        capture.events.push_back(Event{.startNs = 5'000'000'002'000UL, .durationNs = 1000U, .entity = kNoEntity, .kind = Kind::sweep});

        const std::string json = catapultJson(capture);
        expect(json.contains("\"ts\":0.000")) << "the first record must sit at the origin";
        expect(json.contains("\"ts\":2.000")) << "and the second two microseconds along, not five thousand seconds";
        expect(!json.contains("5000000")) << "no raw epoch value may survive into the output";
    };

    "a name carrying JSON metacharacters cannot break the document"_test = [] {
        // Block type names are full C++ template spellings, and a name containing a quote or a
        // backslash would otherwise end the JSON string early and produce a document that parses as
        // something else -- or not at all.
        const std::string odd = std::string("od\"d") + '\\' + "name" + '\n' + '\t';

        Capture capture;
        capture.entities.push_back(LoadedEntity{.id = 1U, .uniqueName = odd, .typeName = "gr::Block<T>"});
        capture.events.push_back(Event{.startNs = 1000UL, .durationNs = 10U, .entity = 1U, .kind = Kind::workEnd});

        const std::string json = catapultJson(capture);
        expect(json.contains(std::string("od") + '\\' + '"' + "d" + '\\' + '\\' + "name")) << "quote and backslash must be escaped, not passed through";
        expect(json.contains("\\n")) << "a newline must be escaped rather than ending the line";
        expect(json.contains("\\t")) << "and a tab likewise";
        expect(!json.contains(odd)) << "the raw, unescaped name must not appear anywhere in the output";
        expect(eq(std::ranges::count(json, '{'), std::ranges::count(json, '}'))) << "and the document must still balance";
    };

    "an empty capture converts to an empty but valid document"_test = [] {
        const std::string json = catapultJson(Capture{});
        expect(json.starts_with("{\"traceEvents\":[")) << "a capture with nothing in it is still a document";
        expect(json.ends_with("}"));
        expect(eq(std::ranges::count(json, '{'), std::ranges::count(json, '}')));
    };

    "the report counts what it rejected, so a mostly-rejected capture says so"_test = [] {
        std::vector<Event> events;
        for (std::size_t i = 0UZ; i < 9UZ; ++i) {
            events.push_back(invocation(1U, 0U, 400U, -1)); // DONE: nine of them
        }
        events.push_back(invocation(1U, 64U, 100U));

        const gr::property_map summary = timingReport(events);
        expect(eq(fieldOr<std::uint64_t>(summary, "invocations", 0UL), 1UL));
        expect(eq(fieldOr<std::uint64_t>(summary, "rejected_status", 0UL), 9UL)) << "a report drawn from a tenth of its input must be able to say which tenth";
    };
};

int main() { /* tests are statically registered as suites */ }
