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

    "a block is named from the capture's identity table when it has one"_test = [] {
        const std::vector<Event>        events{invocation(7U, 64U, 100U)};
        const std::vector<LoadedEntity> entities{LoadedEntity{.id = 7U, .workerId = 0U, .nInputPorts = 1U, .nOutputPorts = 1U, .uniqueName = "fir_filter", .typeName = "gr::blocks::Fir<float>"}};

        expect(gt(fieldOr<std::uint64_t>(blockOf(timingReport(events, entities), "fir_filter"), "invocations", 0UL), 0UL)) << "a named capture must report the name";
        expect(gt(fieldOr<std::uint64_t>(blockOf(timingReport(events), "entity 7"), "invocations", 0UL), 0UL)) << "and one without a table must still be readable, by id";
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
