#include <boost/ut.hpp>

#include <cstdlib>
#include <format>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <set>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gnuradio-4.0/Block.hpp>
#include <gnuradio-4.0/BlockMerging.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceCatapult.hpp>
#include <gnuradio-4.0/TraceReport.hpp>

#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;

/**
 * Block-side trace coverage: the markers that live inside `Block::workInternal` rather than at the
 * scheduler boundary.
 *
 * Kept apart from `qa_TraceScheduler.cpp` because the question is different. That suite asks whether
 * the scheduler attributes and emits correctly; this one asks whether a marker *inside* a block
 * reports the block's own view of an invocation -- exact `processedIn`/`processedOut`, which
 * `performed_work` collapses into one number and cannot recover for a resampling block.
 */
namespace {

using TestScheduler = gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::externalStep>;

/// 4:1 decimator. The whole point of the block-side markers is that `performed_work` cannot describe
/// this invocation: it reports `processedIn` alone, and nothing at the boundary recovers the 4×
/// smaller output.
template<typename T>
struct Decimator : public gr::Block<Decimator<T>, gr::Resampling<>> {
    gr::PortIn<T>  in{};
    gr::PortOut<T> out{};

    GR_MAKE_REFLECTABLE(Decimator, in, out);

    gr::work::Status processBulk(std::span<const T>& input, std::span<T>& output) noexcept {
        for (std::size_t i = 0UZ; i < output.size(); ++i) {
            output[i] = input[i * 4UZ];
        }
        return gr::work::Status::OK;
    }
};

/// Writes the capture to `$GR4_TRACE_ARTEFACT_DIR/<name>.gr4trace` when that variable is set, and
/// does nothing otherwise, so the suite stays hermetic and writes nothing in an ordinary run.
void exportTimeline([[maybe_unused]] std::string_view name) {
    if constexpr (gr::trace::kEnabled) {
        const char* directory = std::getenv("GR4_TRACE_ARTEFACT_DIR");
        if (directory == nullptr || name.empty()) {
            return;
        }
        std::ignore = gr::trace::dump(std::format("{}/{}.gr4trace", directory, name));
    }
}

/// Fixed 10-sample chunks with a trailing `processEpilogue`, so a stream whose length is not a
/// multiple of ten leaves a remainder that only the epilogue path moves.
template<typename T>
struct EpilogueBlock : gr::Block<EpilogueBlock<T>, gr::Resampling<10UZ, 10UZ, true>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(EpilogueBlock, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) noexcept {
        std::copy_n(input.begin(), std::min(input.size(), output.size()), output.begin());
        return gr::work::Status::OK;
    }

    gr::work::Status processEpilogue(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) noexcept {
        const auto n = std::min(input.size(), output.size());
        std::copy_n(input.begin(), n, output.begin());
        output.publish(n);
        return gr::work::Status::OK;
    }
};

void collectingConsumer(const gr::trace::Event& event, void* user) noexcept { static_cast<std::vector<gr::trace::Event>*>(user)->push_back(event); }

[[nodiscard]] std::vector<gr::trace::Event> collect() {
    std::vector<gr::trace::Event> events;
    std::ignore = gr::trace::forEachEvent(collectingConsumer, &events);
    return events;
}

[[nodiscard]] std::vector<gr::trace::Event> ofKind(const std::vector<gr::trace::Event>& events, gr::trace::Kind kind) {
    std::vector<gr::trace::Event> matching;
    std::ranges::copy_if(events, std::back_inserter(matching), [kind](const gr::trace::Event& event) { return event.kind == kind; });
    return matching;
}

[[nodiscard]] std::vector<gr::trace::Event> forEntity(const std::vector<gr::trace::Event>& events, gr::trace::EntityId entity) {
    std::vector<gr::trace::Event> matching;
    std::ranges::copy_if(events, std::back_inserter(matching), [entity](const gr::trace::Event& event) { return event.entity == entity; });
    return matching;
}

/// Runs `ConstantSource -> Decimator -> NullSink` to completion under `step()`, so nothing in this
/// suite depends on thread timing. Returns the decimator's interned id alongside the capture.
struct DecimatingRun {
    std::vector<gr::trace::Event> events;
    gr::trace::EntityId           decimator = gr::trace::kNoEntity;
    gr::trace::EntityId           source    = gr::trace::kNoEntity;
    gr::trace::EntityId           sink      = gr::trace::kNoEntity;

    /// `batchCap` is not decoration: left unset, the whole stream moves in a *single* `work()` call
    /// and there is no second record to compare a position against. Capping it is what makes the
    /// stream coordinate observable at all.
    std::string artefact; /// non-empty exports the capture when GR4_TRACE_ARTEFACT_DIR is set

    void run(std::uint32_t categories, std::size_t nSamples = 4096UZ, gr::Size_t batchCap = 512U) {
        gr::trace::reset();
        gr::trace::setCategories(categories);

        gr::Graph graph;
        auto&     src = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", static_cast<gr::Size_t>(nSamples)}, {"max_batch_size", batchCap}});
        auto&     dec = graph.emplaceBlock<Decimator<float>>({{"name", std::string("dec")}, {"input_chunk_size", gr::Size_t{4U}}, {"output_chunk_size", gr::Size_t{1U}}, {"max_batch_size", batchCap}});
        auto&     snk = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore   = graph.connect<"out", "in">(src, dec);
        std::ignore   = graph.connect<"out", "in">(dec, snk);

        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

        for (const auto& block : scheduler.graph().blocks()) {
            const gr::trace::EntityId id = gr::trace::internedId(static_cast<const void*>(block.get()));
            if (block->uniqueName().find("dec") != std::string_view::npos || block->name() == "dec") {
                decimator = id;
            }
            if (block->name() == "src") {
                source = id;
            }
            if (block->name() == "snk") {
                sink = id;
            }
        }

        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 256UZ; ++pass) {
            std::ignore = scheduler.step();
        }
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        events = collect();
        exportTimeline(artefact);
        gr::trace::setCategories(0U);
    }
};

/// One hand-built `workExact` record. The refusal paths cannot be reached from a healthy graph --
/// a real chain has a stable ratio and positions that match its counts -- so they are driven from
/// constructed records, the same way `qa_TraceReport.cpp` drives its fit gates.
/// One scheduler-side invocation, so `timingReport` has something to fold.
[[nodiscard]] gr::trace::Event invocationOf(gr::trace::EntityId entity, std::uint32_t performedWork, std::uint32_t durationNs) { return gr::trace::Event{.durationNs = durationNs, .payload0 = performedWork, .payload1 = performedWork, .entity = entity, .kind = gr::trace::Kind::workEnd}; }

[[nodiscard]] gr::trace::Event exactRecord(gr::trace::EntityId entity, std::uint32_t processedIn, std::uint32_t processedOut, std::uint32_t position, std::uint64_t startNs = 0UL) { return gr::trace::Event{.startNs = startNs, .durationNs = 100U, .payload0 = processedIn, .payload1 = processedOut, .payload2 = position, .entity = entity, .kind = gr::trace::Kind::workExact}; }

} // namespace

const boost::ut::suite<"TraceBlock"> traceBlockTests = [] {
    using namespace gr::trace;

    if constexpr (!kEnabled) {
        "a compiled-out build emits no block-side records"_test = [] {
            DecimatingRun run;
            run.run(kAllCategories);
            expect(eq(ofKind(run.events, Kind::workExact).size(), 0UZ)) << "the `if constexpr` in workInternal must leave nothing behind";
        };
        return;
    }

    "a resampling block's exact counts differ from performed_work"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::work, Category::workExact));
        expect(neq(run.decimator, kNoEntity) >> fatal) << "the decimator must be interned before anything can be attributed to it";

        const std::vector<Event> exact = forEntity(ofKind(run.events, Kind::workExact), run.decimator);
        expect(gt(exact.size(), 0UZ) >> fatal) << "a 4:1 decimator that ran must leave workExact records";

        std::size_t productive = 0UZ;
        for (const Event& event : exact) {
            if (event.payload0 == 0U && event.payload1 == 0U) {
                continue; // an early exit anchors no data
            }
            ++productive;
            expect(eq(event.payload0, event.payload1 * 4U)) << "processedIn must be exactly 4x processedOut for a 4:1 decimator";
            expect(neq(event.payload0, event.payload1)) << "the two counts must not be the same number -- that is the whole reason this marker exists";
        }
        expect(gt(productive, 0UZ) >> fatal) << "every invocation exited early: the run did no work";

        // And the scheduler-side number is the *input* count for a non-source, so it cannot describe
        // the output at all. This is the disagreement §3.6 settles, asserted rather than assumed.
        const std::vector<Event> boundary = forEntity(ofKind(run.events, Kind::workEnd), run.decimator);
        expect(gt(boundary.size(), 0UZ) >> fatal);
        std::size_t boundaryTotal = 0UZ;
        for (const Event& event : boundary) {
            boundaryTotal += event.payload1; // performedWork
        }
        std::size_t exactIn  = 0UZ;
        std::size_t exactOut = 0UZ;
        for (const Event& event : exact) {
            exactIn += event.payload0;
            exactOut += event.payload1;
        }
        expect(eq(boundaryTotal, exactIn)) << "performed_work for a non-source is processedIn";
        expect(neq(boundaryTotal, exactOut)) << "and therefore says nothing about what was produced";
    };

    "every block-side record names a block"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));

        const std::vector<Event> exact = ofKind(run.events, Kind::workExact);
        expect(gt(exact.size(), 0UZ) >> fatal);
        for (const Event& event : exact) {
            expect(neq(event.entity, kNoEntity)) << "an unattributed record is useless: the identity is pushed down by syncSchedStates";
        }
        // More than one block must appear, or the push-down could be writing one id everywhere.
        std::set<EntityId> named;
        for (const Event& event : exact) {
            named.insert(event.entity);
        }
        expect(gt(named.size(), 1UZ)) << "each block must receive its own id, not a shared one";
    };

    "a productive invocation emits exactly one workExact"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::work, Category::workExact));

        const std::vector<Event> begins = forEntity(ofKind(run.events, Kind::workBegin), run.decimator);
        const std::vector<Event> ends   = forEntity(ofKind(run.events, Kind::workEnd), run.decimator);
        const std::vector<Event> exact  = forEntity(ofKind(run.events, Kind::workExact), run.decimator);
        expect(gt(begins.size(), 0UZ) >> fatal);

        // One block-side record per invocation, exactly. `workBegin` is emitted *before* the call, so
        // it cannot be progress-filtered and covers every invocation too -- which makes it the honest
        // thing to compare against.
        expect(eq(exact.size(), begins.size())) << "the RAII scope must record every invocation, early exits included";

        // `workEnd` is filtered on INSUFFICIENT_INPUT_ITEMS / INSUFFICIENT_OUTPUT_ITEMS only -- those
        // are aggregated into `workProbe` instead. Every other exit, `DONE` included, still records.
        // So the identity that must hold is a conservation law, not an inequality: every invocation
        // leaves either a `workEnd` or a probe behind it.
        const std::vector<Event> probes     = forEntity(ofKind(run.events, Kind::workProbe), run.decimator);
        std::size_t              probedRuns = 0UZ;
        for (const Event& event : probes) {
            probedRuns += event.payload0; // probeCount, aggregated per block per sweep
        }
        expect(eq(ends.size() + probedRuns, exact.size())) << "every invocation must leave exactly one block-side record and one boundary outcome";
    };

    "an invocation that produced nothing still says why"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));

        const std::vector<Event> exact = forEntity(ofKind(run.events, Kind::workExact), run.decimator);
        expect(gt(exact.size(), 0UZ) >> fatal);

        // Every exit fills the payload through the `on_scope_exit`, so a record with no counts must
        // still carry the status that explains it. A scope finished early -- before the fill lands --
        // emits a record that claims OK while having done nothing, which reads as a successful
        // zero-sample invocation rather than as a block that had already stopped.
        std::size_t barren = 0UZ;
        for (const Event& event : exact) {
            if (event.payload0 != 0U || event.payload1 != 0U) {
                continue;
            }
            ++barren;
            expect(neq(event.status, static_cast<std::int8_t>(gr::work::Status::OK))) << "a zero-count record claiming OK is indistinguishable from a real empty invocation";
        }
        expect(gt(barren, 0UZ) >> fatal) << "the early-exit paths must be exercised, or this asserts nothing";
        expect(eq(barren + 8UZ, exact.size())) << "4096 samples at a 512 cap is 8 productive invocations; the rest exited early";
    };

    "a source records its output position, a non-source its input"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));
        expect(neq(run.source, kNoEntity) >> fatal);

        const std::vector<Event> sourceRecords = forEntity(ofKind(run.events, Kind::workExact), run.source);
        expect(gt(sourceRecords.size(), 0UZ) >> fatal);

        // A source has no input port, so an input-only implementation would leave every position at
        // zero and the chain's first link unanchored.
        bool sawAdvancingPosition = false;
        for (const Event& event : sourceRecords) {
            if (event.payload2 > 0U) {
                sawAdvancingPosition = true;
            }
        }
        expect(sawAdvancingPosition) << "a source must anchor on its output position; input-only would pin it at 0";

        const std::vector<Event> decRecords = forEntity(ofKind(run.events, Kind::workExact), run.decimator);
        expect(gt(decRecords.size(), 0UZ) >> fatal);
        bool decAdvances = false;
        for (const Event& event : decRecords) {
            if (event.payload2 > 0U) {
                decAdvances = true;
            }
        }
        expect(decAdvances) << "a non-source must anchor on its input position";
    };

    "positions advance monotonically within one uninterrupted run"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));

        const std::vector<Event> decRecords = forEntity(ofKind(run.events, Kind::workExact), run.decimator);
        std::uint32_t            previous   = 0U;
        bool                     first      = true;
        std::size_t              advances   = 0UZ;
        for (const Event& event : decRecords) {
            if (event.payload0 == 0U) {
                continue;
            }
            if (!first) {
                expect(ge(event.payload2, previous)) << "within a capture far shorter than the 2^32 wrap, positions must not go backwards";
                if (event.payload2 > previous) {
                    ++advances;
                }
            }
            previous = event.payload2;
            first    = false;
        }
        expect(gt(advances, 0UZ)) << "a position that never advances is a constant, not a stream coordinate";
    };

    "each position is the running sum of what the block already consumed"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));

        // The known-answer check. "Positions advance" is satisfied by any increasing sequence,
        // including one that is uniformly wrong -- an off-by-one survives it untouched. The position
        // a record carries must equal exactly the total consumed before it, which pins the value
        // rather than its trend.
        const std::vector<Event> decRecords = forEntity(ofKind(run.events, Kind::workExact), run.decimator);
        std::size_t              expected   = 0UZ;
        std::size_t              checked    = 0UZ;
        for (const Event& event : decRecords) {
            if (event.payload0 == 0U && event.payload1 == 0U) {
                continue;
            }
            expect(eq(static_cast<std::size_t>(event.payload2), expected)) << "invocation must start where the previous one stopped consuming";
            expected += event.payload0;
            ++checked;
        }
        expect(gt(checked, 1UZ) >> fatal) << "one productive invocation cannot demonstrate a running sum";
        expect(eq(expected, 4096UZ)) << "and the sum must land on the whole stream";
    };

    "the exact counts a block reports sum to what the sink received"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact), 4096UZ, 512U);

        const std::vector<Event> decRecords = forEntity(ofKind(run.events, Kind::workExact), run.decimator);
        std::size_t              totalIn    = 0UZ;
        std::size_t              totalOut   = 0UZ;
        for (const Event& event : decRecords) {
            totalIn += event.payload0;
            totalOut += event.payload1;
        }
        // The construction is known: 4096 samples in at 4:1. Anything else means the counts are
        // being read from the wrong variables.
        expect(eq(totalIn, 4096UZ)) << "the decimator must consume every sample the source produced";
        expect(eq(totalOut, 1024UZ)) << "and produce exactly a quarter of them";
    };

    "workExact costs nothing when its category is off"_test = [] {
        DecimatingRun withExact;
        withExact.run(categoryMask(Category::work, Category::workExact));
        DecimatingRun withoutExact;
        withoutExact.run(categoryMask(Category::work));

        expect(eq(ofKind(withoutExact.events, Kind::workExact).size(), 0UZ)) << "the category gate must precede the clock read, not follow it";
        expect(gt(ofKind(withExact.events, Kind::workExact).size(), 0UZ)) << "and must let records through when it is on";

        // The scheduler-side records must be unaffected either way: turning the microscope on must
        // not change what the boundary reports.
        expect(eq(ofKind(withExact.events, Kind::workEnd).size(), ofKind(withoutExact.events, Kind::workEnd).size())) << "enabling workExact must not perturb the scheduler-side capture";
    };

    "the four phases are contained by the invocation they partition"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact, Category::workPhases));

        // Grouped per invocation, not summed across the capture. An aggregate comparison is too weak:
        // a scope that wrongly spans its neighbours double-counts their time, and the surplus can
        // still hide under the total of every other invocation in the run.
        //
        // Records are appended at *completion*, so one entity's stream reads as the phases of an
        // invocation followed by the `workExact` that encloses them. Walking until each `workExact`
        // is therefore the grouping, and it holds however the phases are ordered among themselves.
        const std::vector<Event> stream = forEntity(run.events, run.decimator);
        expect(gt(stream.size(), 0UZ) >> fatal);

        std::set<std::uint32_t> seen;
        std::uint64_t           pendingNs    = 0UL;
        std::size_t             pendingCount = 0UZ;
        std::size_t             nFullGroups  = 0UZ;
        std::size_t             nGrouped     = 0UZ;
        std::uint64_t           totalPhaseNs = 0UL;
        for (const Event& event : stream) {
            if (event.kind == Kind::workPhase) {
                seen.insert(event.payload0);
                expect(lt(event.payload0, static_cast<std::uint32_t>(kPhaseCount))) << "a phase id outside the enum means the payload word is being read as something else";
                pendingNs += event.durationNs;
                totalPhaseNs += event.durationNs;
                ++pendingCount;
                continue;
            }
            if (event.kind != Kind::workExact) {
                continue;
            }
            if (pendingCount > 0UZ) {
                expect(le(pendingNs, static_cast<std::uint64_t>(event.durationNs))) << "the phases of one invocation must fit inside it -- a larger sum means a scope spans more than the call it names";
                // An invocation contributes either all four phases or just the first: the zero-work
                // exit is taken *after* computeSampleLimits and before the other three, so a group of
                // one is a real shape and any other size means a scope leaked across a boundary.
                expect((pendingCount == kPhaseCount) || (pendingCount == 1UZ)) << "an invocation must contribute four phases or the one that precedes its early exit";
                nFullGroups += (pendingCount == kPhaseCount) ? 1UZ : 0UZ;
                ++nGrouped;
            }
            pendingNs    = 0UL;
            pendingCount = 0UZ;
        }
        expect(eq(seen.size(), kPhaseCount)) << "all four stages must be represented, or the split does not partition the invocation";
        expect(gt(nFullGroups, 1UZ) >> fatal) << "fewer than two complete invocations cannot demonstrate containment";
        expect(eq(nFullGroups, 8UZ)) << "4096 samples at a 512 cap is eight productive invocations";
        expect(gt(totalPhaseNs, 0UL)) << "phases that all measure zero are not measuring anything";
    };

    "the phase split is opt-in on top of the exact counts"_test = [] {
        DecimatingRun exactOnly;
        exactOnly.run(categoryMask(Category::workExact));
        DecimatingRun both;
        both.run(categoryMask(Category::workExact, Category::workPhases));

        expect(eq(ofKind(exactOnly.events, Kind::workPhase).size(), 0UZ)) << "workExact alone must not pay for the four phase scopes";
        expect(gt(ofKind(both.events, Kind::workPhase).size(), 0UZ));
        expect(eq(ofKind(exactOnly.events, Kind::workExact).size(), ofKind(both.events, Kind::workExact).size())) << "turning the phases on must not change how many invocations are recorded";
    };

    "phases alone are self-delimiting, without the exact counts to group them"_test = [] {
        // `workPhases` is independent of `workExact`, so a capture may carry phases and no invocation
        // records to bracket them. That is still groupable: `computeSampleLimits` is emitted exactly
        // once per invocation that gets past `checkLifecycle`, with no early exit between the two, so
        // it marks the start of every instrumented invocation. Asserted because the alternative --
        // phases that cannot be attributed to an invocation at all -- would make the category useless
        // on its own, and nothing else in the suite would notice.
        DecimatingRun run;
        run.run(categoryMask(Category::workPhases));

        const std::vector<Event> phases = forEntity(ofKind(run.events, Kind::workPhase), run.decimator);
        expect(gt(phases.size(), 0UZ) >> fatal) << "the phase category must stand on its own";
        expect(eq(ofKind(run.events, Kind::workExact).size(), 0UZ)) << "and must not drag the exact counts in with it";

        std::size_t starts   = 0UZ;
        std::size_t inGroup  = 0UZ;
        std::size_t complete = 0UZ;
        for (const Event& event : phases) {
            if (event.payload0 == std::to_underlying(Phase::computeSampleLimits)) {
                if (inGroup == kPhaseCount) {
                    ++complete;
                }
                ++starts;
                inGroup = 0UZ;
            }
            ++inGroup;
        }
        if (inGroup == kPhaseCount) {
            ++complete;
        }
        expect(eq(starts, phases.size() - complete * (kPhaseCount - 1UZ))) << "every group must begin with computeSampleLimits";
        expect(eq(complete, 8UZ)) << "the eight productive invocations must each be recoverable as a group of four";
    };

    "a stable ratio is derived, an unstable one refused"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));

        const RatioEstimate decimator = ratioOf(run.events, run.decimator);
        expect(decimator.stable >> fatal) << "a 4:1 decimator driven at a fixed batch has a constant ratio: " << decimator.reason;
        expect(eq(decimator.outPerIn, 0.25)) << "and it is exactly a quarter, not approximately";

        // A source consumes nothing, so no ratio describes it. Refused with a reason rather than
        // reported as zero, which a consumer would read as "produces nothing".
        const RatioEstimate source = ratioOf(run.events, run.source);
        expect(!source.stable) << "a source has no input to form a ratio against";
        expect(!source.reason.empty()) << "a refusal must say why";

        const RatioEstimate absent = ratioOf(run.events, EntityId{9999U});
        expect(!absent.stable);
        expect(!absent.reason.empty());
    };

    "the report names which side supplied the counts"_test = [] {
        DecimatingRun withExact;
        withExact.run(categoryMask(Category::work, Category::workExact));
        const gr::property_map exactReport = timingReport(withExact.events);

        DecimatingRun boundaryOnly;
        boundaryOnly.run(categoryMask(Category::work));
        const gr::property_map boundaryReport = timingReport(boundaryOnly.events);

        // A nested map must be bound to a `Value` before it can be read: `find()` yields a view whose
        // type has no map overload.
        const auto sourcesIn = [](const gr::property_map& reportMap) {
            std::set<std::string> found;
            const auto            blocksIt = reportMap.find("blocks");
            if (blocksIt == reportMap.end()) {
                return found;
            }
            const gr::pmt::Value   blocksValue = (*blocksIt).second;
            const gr::property_map blocks      = blocksValue.value_or(gr::property_map{});
            for (const auto& entry : blocks) {
                const gr::pmt::Value   blockValue = entry.second;
                const gr::property_map block      = blockValue.value_or(gr::property_map{});
                const auto             it         = block.find("sample_source");
                if (it == block.end()) {
                    continue;
                }
                found.insert((*it).second.value_or(std::string{}));
            }
            return found;
        };

        const std::set<std::string> withBlockSide = sourcesIn(exactReport);
        expect(withBlockSide.contains("block-side") >> fatal) << "a capture carrying exact counts must say it used them";

        const std::set<std::string> withoutBlockSide = sourcesIn(boundaryReport);
        expect(!withoutBlockSide.contains("block-side")) << "a capture with no exact counts must not claim to have used them";
        expect(withoutBlockSide.contains("scheduler-side")) << "and must name the side it fell back to";
    };

    "end-to-end latency matches a chain whose answer is known"_test = [] {
        DecimatingRun run;
        run.artefact = "t4-latency-chain";
        run.run(categoryMask(Category::work, Category::workExact, Category::workPhases));

        const std::vector<EntityId> chain{run.source, run.decimator, run.sink};
        const ChainLatency          latency = chainLatency(run.events, chain);
        expect(gt(latency.samples, 1UZ)) << "one matched sample cannot demonstrate a distribution";
        expect(le(latency.minNs, latency.medianNs));
        expect(le(latency.medianNs, latency.maxNs));

        // The known answer, computed independently of the function under test. The chain is
        // fixed-rate with a 512-sample cap, so source batch k covers output positions [512k, 512k+512)
        // and produces sink input positions [128k, 128k+128) -- batch k pairs with batch k. Pairing
        // the n-th records directly and comparing the resulting distribution is what pins the
        // arithmetic; a bound like "no longer than the capture" is satisfied by a mapping that walks
        // the wrong ratio entirely, which is how this was got wrong the first time.
        const std::vector<Event> srcRecords  = exactRecordsFor(run.events, run.source);
        const std::vector<Event> sinkRecords = exactRecordsFor(run.events, run.sink);
        expect(eq(srcRecords.size(), sinkRecords.size()) >> fatal) << "a fixed-rate chain runs its ends the same number of times";

        std::vector<std::uint64_t> expected;
        for (std::size_t i = 0UZ; i < srcRecords.size(); ++i) {
            expect(eq(static_cast<std::uint64_t>(srcRecords[i].payload2), static_cast<std::uint64_t>(sinkRecords[i].payload2) * 4UL)) << "batch k's source position must be four times its sink position at 4:1";
            const std::uint64_t producedAt = srcRecords[i].startNs + srcRecords[i].durationNs;
            const std::uint64_t consumedAt = sinkRecords[i].startNs + sinkRecords[i].durationNs;
            expect(ge(consumedAt, producedAt) >> fatal);
            expected.push_back(consumedAt - producedAt);
        }
        std::ranges::sort(expected);
        expect(eq(latency.samples, expected.size())) << "every sink batch must be matched to the source batch that fed it";
        expect(eq(latency.minNs, expected.front())) << "the shortest latency must be the one the pairing gives";
        expect(eq(latency.maxNs, expected.back()));
        expect(eq(latency.medianNs, expected[expected.size() / 2UZ]));
    };

    "lockstep and the position walk agree where both apply"_test = [] {
        // The strongest check available for the lockstep option: on a chain that satisfies both
        // assumptions, two independent routes to one number must produce the same number. Same shape
        // as the live-versus-reconstructed cross-check T2 used, and for the same reason -- a
        // disagreement is evidence, not noise.
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));

        const std::vector<EntityId> chain{run.source, run.decimator, run.sink};
        const ChainLatency          byPosition = chainLatency(run.events, chain, LatencyMode::streamPosition);
        const ChainLatency          byLockstep = chainLatency(run.events, chain, LatencyMode::invocationLockstep);
        expect(byPosition.computed >> fatal) << byPosition.reason;
        expect(byLockstep.computed >> fatal) << byLockstep.reason;

        expect(eq(byPosition.samples, byLockstep.samples)) << "both routes must match the same number of batches";
        expect(eq(byPosition.minNs, byLockstep.minNs)) << "and produce the same distribution";
        expect(eq(byPosition.medianNs, byLockstep.medianNs));
        expect(eq(byPosition.maxNs, byLockstep.maxNs));
        expect(eq(byLockstep.unmatched, 0UZ)) << "nothing was left in flight in this capture";
        expect(eq(byPosition.unmatched, 0UZ)) << "and nothing predates it either -- both modes must account for every invocation";
    };

    "the position walk reports what it could not pair"_test = [] {
        // The two modes miss opposite ends -- lockstep leaves a tail the sink had not reached, the
        // position walk a head whose producer predates the capture -- but neither may drop the count
        // silently. A distribution that is quietly short understates whatever it was measuring.
        const std::vector<Event> lateSource{
            exactRecord(EntityId{1U}, 0U, 100U, 500U, 3000UL),   // the source record starts at 500
            exactRecord(EntityId{3U}, 100U, 100U, 400U, 1000UL), // this sink read data produced earlier
            exactRecord(EntityId{3U}, 100U, 100U, 500U, 4000UL), // this one is inside the capture
        };
        const std::vector<EntityId> chain{EntityId{1U}, EntityId{3U}};
        const ChainLatency          latency = chainLatency(lateSource, chain);
        expect(latency.computed >> fatal) << latency.reason;
        expect(eq(latency.samples, 1UZ)) << "only the sink invocation with a producer in the capture can be timed";
        expect(eq(latency.unmatched, 1UZ)) << "the one whose producer predates the capture must be counted, not dropped";
    };

    "the position walk is the default; lockstep must be asked for"_test = [] {
        // An assumption the caller supplies must never be one they get by accident.
        const std::vector<Event> strided{
            exactRecord(EntityId{1U}, 0U, 100U, 0U, 1000UL),   //
            exactRecord(EntityId{1U}, 0U, 100U, 100U, 3000UL), //
            exactRecord(EntityId{2U}, 100U, 100U, 0U, 1500UL), // positions outrun the counts
            exactRecord(EntityId{2U}, 100U, 100U, 150U, 3500UL),
            exactRecord(EntityId{3U}, 100U, 100U, 0U, 2000UL), //
            exactRecord(EntityId{3U}, 100U, 100U, 100U, 4000UL),
        };
        const std::vector<EntityId> chain{EntityId{1U}, EntityId{2U}, EntityId{3U}};

        const ChainLatency byDefault = chainLatency(strided, chain);
        expect(!byDefault.computed) << "the default must still refuse a chain it cannot reconstruct exactly";
        expect(byDefault.reason.find("stride") != std::string::npos) << byDefault.reason;

        // ... and the whole point of the option: lockstep reads no positions, so it reconstructs the
        // chain the position walk has to decline.
        const ChainLatency byLockstep = chainLatency(strided, chain, LatencyMode::invocationLockstep);
        expect(byLockstep.computed >> fatal) << "lockstep needs no positions, so a stride cannot stop it: " << byLockstep.reason;
        expect(eq(byLockstep.samples, 2UZ));
        expect(eq(byLockstep.minNs, 1000UL)) << "invocation 0: produced at 1100, consumed at 2100";
        expect(eq(byLockstep.maxNs, 1000UL)) << "invocation 1: produced at 3100, consumed at 4100";
    };

    "lockstep refuses a capture that contradicts the assumption"_test = [] {
        // The sink cannot productively run more often than the source did -- it would be consuming
        // batches nobody produced. A capture showing that describes a graph the caller has
        // misunderstood, and pairing anyway would be confident nonsense.
        const std::vector<Event> sinkRunsMore{
            exactRecord(EntityId{1U}, 0U, 100U, 0U, 1000UL), //
            exactRecord(EntityId{3U}, 50U, 50U, 0U, 2000UL), //
            exactRecord(EntityId{3U}, 50U, 50U, 50U, 3000UL),
        };
        const std::vector<EntityId> chain{EntityId{1U}, EntityId{3U}};
        const ChainLatency          refused = chainLatency(sinkRunsMore, chain, LatencyMode::invocationLockstep);
        expect(!refused.computed) << "two sink invocations against one source invocation is not lockstep";
        // The *specific* reason, not merely a refusal. Dropping the count check makes the pairing loop
        // read past the source records, and whatever it finds there can trip the ordering check
        // instead -- a refusal with a different cause, which a looser assertion accepts happily.
        expect(refused.reason.find("the sink ran 2") != std::string::npos) << "the refusal must name the count mismatch it found: " << refused.reason;
        expect(refused.reason.find("source's 1") != std::string::npos) << refused.reason;
    };

    "lockstep reports what the capture ended before delivering"_test = [] {
        // A capture that stops with batches in flight leaves source invocations with no sink
        // invocation yet. That is expected rather than wrong, so it is reported rather than hidden --
        // a silently shorter distribution would understate the tail.
        const std::vector<Event> inFlight{
            exactRecord(EntityId{1U}, 0U, 100U, 0U, 1000UL),   //
            exactRecord(EntityId{1U}, 0U, 100U, 100U, 2000UL), //
            exactRecord(EntityId{1U}, 0U, 100U, 200U, 3000UL), //
            exactRecord(EntityId{3U}, 100U, 100U, 0U, 1500UL),
        };
        const std::vector<EntityId> chain{EntityId{1U}, EntityId{3U}};
        const ChainLatency          latency = chainLatency(inFlight, chain, LatencyMode::invocationLockstep);
        expect(latency.computed >> fatal) << latency.reason;
        expect(eq(latency.samples, 1UZ)) << "only the batch that reached the sink can be timed";
        expect(eq(latency.unmatched, 2UZ)) << "and the two still in flight must be counted, not dropped";
    };

    "a chain is refused rather than averaged when it cannot be reconstructed"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));

        const std::vector<EntityId> tooShort{run.source};
        expect(!chainLatency(run.events, tooShort).computed) << "a single block is not a chain";
        expect(!chainLatency(run.events, tooShort).reason.empty());

        // A block that left no records cannot anchor a hop, and the refusal must name it rather than
        // quietly dropping that hop and reporting the rest.
        const std::vector<EntityId> unknownMiddle{run.source, EntityId{9999U}, run.sink};
        const ChainLatency          refused = chainLatency(run.events, unknownMiddle);
        expect(!refused.computed) << "a hop with no records must not be skipped over";
        expect(!refused.reason.empty()) << "and the refusal must say which";
    };

    "a stride is detected as positions outrunning the counts"_test = [] {
        // A block with a stride consumes samples through `inputSkipBefore` that appear in no count
        // field. Its positions stay correct; the counts stop accounting for them. Nothing in the
        // record says "stride", so the only evidence is the divergence -- and a consumer that
        // accumulated counts rather than reading positions would drift silently past it.
        const std::vector<Event> strided{
            exactRecord(EntityId{1U}, 100U, 100U, 0U),   //
            exactRecord(EntityId{1U}, 100U, 100U, 150U), // 50 samples consumed and discarded
            exactRecord(EntityId{1U}, 100U, 100U, 300U),
        };
        const RatioEstimate estimate = ratioOf(strided, EntityId{1U});
        expect(!estimate.stable) << "positions advancing by 150 against a count of 100 must be refused";
        expect(estimate.reason.find("stride") != std::string::npos) << "and the refusal must name what causes it: " << estimate.reason;

        const std::vector<Event> contiguous{
            exactRecord(EntityId{1U}, 100U, 100U, 0U),   //
            exactRecord(EntityId{1U}, 100U, 100U, 100U), //
            exactRecord(EntityId{1U}, 100U, 100U, 200U),
        };
        expect(ratioOf(contiguous, EntityId{1U}).stable) << "the same records without the gap must be accepted";
    };

    "an unstable ratio is refused rather than averaged"_test = [] {
        // Two invocations at 1:1 and one at 2:1 average to something no invocation exhibited. The
        // acceptance criterion is scoped to predictable ratios, so this is out of scope by
        // construction -- and reporting the mean would be the failure the scoping exists to prevent.
        const std::vector<Event> varying{
            exactRecord(EntityId{2U}, 100U, 100U, 0U),  //
            exactRecord(EntityId{2U}, 100U, 50U, 100U), //
            exactRecord(EntityId{2U}, 100U, 100U, 200U),
        };
        const RatioEstimate estimate = ratioOf(varying, EntityId{2U});
        expect(!estimate.stable) << "a ratio that changes between invocations is not a ratio";
        expect(estimate.reason.find("varies") != std::string::npos) << estimate.reason;

        const std::vector<Event> steady{
            exactRecord(EntityId{2U}, 100U, 50U, 0U),   //
            exactRecord(EntityId{2U}, 100U, 50U, 100U), //
            exactRecord(EntityId{2U}, 100U, 50U, 200U),
        };
        const RatioEstimate ok = ratioOf(steady, EntityId{2U});
        expect(ok.stable >> fatal) << ok.reason;
        expect(eq(ok.outPerIn, 0.5));
    };

    "a position that goes backwards is a discontinuity, not a negative latency"_test = [] {
        // The payload holds the low 32 bits of a 64-bit position, so it wraps -- about 23 h at
        // 50 kHz. A wrap and a genuine discontinuity are indistinguishable in one field, so the
        // reconstruction refuses rather than choosing an interpretation.
        const std::vector<Event> wrapped{
            exactRecord(EntityId{3U}, 100U, 100U, 0xFFFFFF00U), //
            exactRecord(EntityId{3U}, 100U, 100U, 0x00000064U),
        };
        const RatioEstimate estimate = ratioOf(wrapped, EntityId{3U});
        expect(!estimate.stable) << "a backwards step must not be read as a huge forward jump";
        expect(estimate.reason.find("backwards") != std::string::npos) << estimate.reason;
    };

    "a non-dyadic ratio maps to the right batch, not the one before it"_test = [] {
        // 14:9. A sink coordinate normally lands *exactly* on a source batch boundary, and in double
        // precision 9 / (9.0/14.0) is 13.999999999999998 rather than 14 -- which matches the previous
        // batch and reports a latency a whole batch period out. A 4:1 chain never shows this because
        // 0.25 is dyadic and the division is exact, which is why the real-graph test could not catch
        // it. The walk is done in integers for this reason.
        const std::vector<Event> events{
            exactRecord(EntityId{1U}, 0U, 14U, 0U, 1000UL),  // source batches of 14
            exactRecord(EntityId{1U}, 0U, 14U, 14U, 2000UL), //
            exactRecord(EntityId{1U}, 0U, 14U, 28U, 3000UL), //
            exactRecord(EntityId{2U}, 14U, 9U, 0U, 1200UL),  // 14:9 middle
            exactRecord(EntityId{2U}, 14U, 9U, 14U, 2200UL), //
            exactRecord(EntityId{2U}, 14U, 9U, 28U, 3200UL), //
            exactRecord(EntityId{3U}, 9U, 9U, 0U, 1500UL),   // sink consumes 9 at a time
            exactRecord(EntityId{3U}, 9U, 9U, 9U, 2500UL),   //
            exactRecord(EntityId{3U}, 9U, 9U, 18U, 3500UL),
        };
        const std::vector<EntityId> chain{EntityId{1U}, EntityId{2U}, EntityId{3U}};

        const RatioEstimate middle = ratioOf(events, EntityId{2U});
        expect(middle.stable >> fatal) << middle.reason;
        expect(eq(middle.outTotal, std::uint64_t{9U})) << "the ratio must also be carried as an exact fraction";
        expect(eq(middle.inTotal, std::uint64_t{14U}));

        const ChainLinks links = chainLinks(events, chain);
        expect(links.computed >> fatal) << links.reason;
        expect(eq(links.links.size(), 3UZ) >> fatal) << "every sink batch has a producer inside this capture";

        // Every record has durationNs 100, so the answers are exact and computable by hand: sink
        // batch k reads source batch k, produced at 1000k+1100 and consumed at 1000k+1600.
        for (std::size_t k = 0UZ; k < links.links.size(); ++k) {
            expect(eq(links.links[k].producedAt, 1000UL * k + 1100UL)) << "sink batch " << k << " must map to source batch " << k;
            expect(eq(links.links[k].latencyNs, 500UL)) << "a batch matched one too early reports a latency a whole period out";
        }
    };

    "the trailing epilogue batch is recorded, not lost"_test = [] {
        // The epilogue consumes real samples and then returns DONE. Leaving its counts at zero would
        // reproduce, in the marker built to fix it, exactly the blindness the acceptance criterion
        // blames `computePerformedWork()` for: a final batch that is processed and never reported.
        //
        // 4096 is a multiple of ten, so the remainder has to be made deliberately: 4095 samples
        // through a fixed-10-chunk block leaves five for the epilogue.
        gr::trace::reset();
        gr::trace::setCategories(categoryMask(Category::workExact));

        gr::Graph graph;
        auto&     src = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{4095U}}});
        auto&     mid = graph.emplaceBlock<EpilogueBlock<float>>({{"name", std::string("mid")}});
        auto&     snk = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
        std::ignore   = graph.connect<"out", "in">(src, mid);
        std::ignore   = graph.connect<"out", "in">(mid, snk);

        TestScheduler scheduler;
        expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);
        EntityId middle = kNoEntity;
        for (const auto& block : scheduler.graph().blocks()) {
            if (block->name() == "mid") {
                middle = gr::trace::internedId(static_cast<const void*>(block.get()));
            }
        }
        expect(neq(middle, kNoEntity) >> fatal);
        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 256UZ; ++pass) {
            std::ignore = scheduler.step();
        }
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        const std::vector<Event> events = collect();
        gr::trace::setCategories(0U);

        std::uint64_t consumed    = 0UL;
        std::uint64_t produced    = 0UL;
        std::uint64_t epilogueIn  = 0UL;
        std::uint64_t epilogueOut = 0UL;
        for (const Event& event : events) {
            if (event.kind != Kind::workExact || event.entity != middle) {
                continue;
            }
            consumed += event.payload0;
            produced += event.payload1;
            if (event.status == static_cast<std::int8_t>(gr::work::Status::DONE) && event.payload0 > 0U) {
                epilogueIn += event.payload0;
                epilogueOut += event.payload1;
            }
        }
        // 4095 through a fixed-10-chunk block is 409 full chunks and a remainder of five that only
        // the epilogue moves, so the remainder is the whole point of the arithmetic below.
        expect(eq(epilogueIn, 5UL)) << "the DONE invocation that ran the epilogue must report what it consumed";
        expect(eq(epilogueOut, 5UL)) << "and what it produced -- a 10:10 block publishes what it took";
        expect(eq(consumed, 4095UL)) << "every sample the block consumed must appear in some record, remainder included";
        expect(eq(produced, 4095UL)) << "and every sample it produced";
    };

    "a chain with an unstable hop is refused, not averaged"_test = [] {
        // The middle block's ratio varies, so no coordinate maps through it. The whole chain is
        // refused: a latency for the hops that happen to qualify is a number whose meaning depends
        // on which blocks were silently excluded.
        std::vector<Event> events{
            exactRecord(EntityId{1U}, 0U, 400U, 0U, 1000UL),   //
            exactRecord(EntityId{2U}, 400U, 100U, 0U, 2000UL), //
            exactRecord(EntityId{2U}, 400U, 200U, 400U, 3000UL),
            exactRecord(EntityId{3U}, 100U, 100U, 0U, 4000UL),
        };
        const std::vector<EntityId> chain{EntityId{1U}, EntityId{2U}, EntityId{3U}};
        const ChainLatency          refused = chainLatency(events, chain);
        expect(!refused.computed) << "an unstable middle hop must refuse the chain";
        expect(refused.reason.find("stable") != std::string::npos) << refused.reason;
    };

    "a chain becomes flow arrows a viewer can follow"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::work, Category::workExact));

        Capture capture;
        capture.events = run.events;

        const std::string without = catapultJson(capture);
        expect(without.find(R"("ph":"s")") == std::string::npos) << "no chain means no arrows: the converter must not invent a topology";
        expect(without.find(R"("latencyFlows":"0")") != std::string::npos) << "and must say it emitted none";

        const std::vector<EntityId> chain{run.source, run.decimator, run.sink};
        const std::string           with = catapultJson(capture, chain);
        expect(with.find(R"("ph":"s")") != std::string::npos >> fatal) << "a reconstructable chain must produce flow starts";
        expect(with.find(R"("ph":"f")") != std::string::npos) << "and the finishes that close them";
        expect(with.find(R"("cat":"latency")") != std::string::npos);

        // Every arrow is a start and a finish sharing an id, so the two counts must agree -- a
        // dangling start is an arrow Perfetto silently drops.
        const auto occurrences = [](const std::string& text, std::string_view needle) {
            std::size_t count = 0UZ;
            for (std::size_t at = text.find(needle); at != std::string::npos; at = text.find(needle, at + needle.size())) {
                ++count;
            }
            return count;
        };
        const std::size_t starts   = occurrences(with, R"("ph":"s")");
        const std::size_t finishes = occurrences(with, R"("ph":"f")");
        expect(eq(starts, finishes)) << "every flow start must have a finish";

        const ChainLatency latency = chainLatency(run.events, chain);
        expect(latency.computed >> fatal);
        expect(eq(starts, latency.samples)) << "one arrow per matched sample, so the picture and the report agree";
        expect(with.find(std::format(R"("latencyFlows":"{}")", latency.samples)) != std::string::npos) << "and the count is stated in the file";
    };

    "a refused chain draws no arrows rather than some"_test = [] {
        // Half a picture is worse than none: arrows for the hops that happened to reconstruct read
        // as "these are the slow paths" rather than as "this chain was refused".
        Capture capture;
        capture.events = std::vector<Event>{
            exactRecord(EntityId{1U}, 0U, 400U, 0U, 1000UL),   //
            exactRecord(EntityId{2U}, 400U, 100U, 0U, 2000UL), //
            exactRecord(EntityId{2U}, 400U, 200U, 400U, 3000UL),
            exactRecord(EntityId{3U}, 100U, 100U, 0U, 4000UL),
        };
        const std::vector<EntityId> chain{EntityId{1U}, EntityId{2U}, EntityId{3U}};
        const std::string           json = catapultJson(capture, chain);
        expect(json.find(R"("ph":"s")") == std::string::npos) << "an unstable middle hop must produce no arrows at all";
        expect(json.find(R"("latencyFlows":"0")") != std::string::npos);
    };

    "the timeline draws an invocation and its exact counts on one lane"_test = [] {
        // The user-visible half of the fix, and a hop the record-level check does not cover: the
        // records agreeing about the worker is one thing, the converter actually mapping that onto
        // the viewer's thread id is another. Before the fix a block's workExact span was drawn on
        // worker 0's track while the workEnd span enclosing it sat on the track that really ran it --
        // one invocation, two lanes, for a marker whose whole purpose is to nest inside the other.
        DecimatingRun run;
        run.run(categoryMask(Category::work, Category::workExact));

        Capture capture;
        capture.events         = run.events;
        const std::string json = catapultJson(capture);

        const auto tidsFor = [&json](std::string_view category) {
            std::set<std::string> tids;
            for (std::size_t at = json.find(std::format(R"("cat":"{}")", category)); at != std::string::npos; at = json.find(std::format(R"("cat":"{}")", category), at + 1UZ)) {
                const std::size_t key = json.find(R"("tid":)", at);
                if (key == std::string::npos) {
                    continue;
                }
                const std::size_t start = key + 6UZ;
                tids.insert(json.substr(start, json.find_first_of(",}", start) - start));
            }
            return tids;
        };

        const std::set<std::string> boundaryTids = tidsFor("work");
        const std::set<std::string> blockTids    = tidsFor("workExact");
        expect(gt(boundaryTids.size(), 0UZ) >> fatal) << "the boundary spans must reach the timeline";
        expect(gt(blockTids.size(), 0UZ) >> fatal) << "and so must the block-side spans";
        expect(boundaryTids == blockTids) << "both must be drawn on the same lanes, or one invocation appears as two on different threads";
    };

    "a block run outside any scheduler reports no worker and no identity"_test = [] {
        // The block-side markers take their worker from what the scheduler pushed down, so a block
        // nobody scheduled has nothing to report. Worker 0 and `kNoEntity` are the honest answers
        // there -- there is no worker and no interned identity -- and the defaults must say exactly
        // that rather than some other number that would read as a real thread.
        reset();
        setCategories(categoryMask(Category::workExact));

        gr::testing::Copy<float> loose;
        std::ignore = loose.work(64UZ); // never adopted, never re-synced, no setTraceContext call

        const std::vector<Event> events = ofKind(collect(), Kind::workExact);
        setCategories(0U);

        expect(gt(events.size(), 0UZ) >> fatal) << "an invocation outside a scheduler must still record itself";
        for (const Event& event : events) {
            expect(eq(event.workerId, std::uint8_t{0U})) << "no scheduler means no worker, which is 0 -- not an arbitrary default";
            expect(eq(event.entity, kNoEntity)) << "and no interned identity";
        }
    };

    "a fused group is named as a group, not as a block"_test = [] {
        // A Merge<> unit is one BlockModel, so it interns once and its internal stages emit nothing.
        // The figures are the group's, and a report that labelled them a block's would invite exactly
        // the wrong conclusion about where the time went.
        expect(isFusedGroup("gr::MergeByIndex<gr::testing::Copy<float>, 0, gr::testing::Copy<float>, 0>"));
        expect(isFusedGroup("gr::FeedbackMergeByIndex<A, 0, B, 0>")) << "the feedback form is a fused group too";
        expect(!isFusedGroup("gr::testing::Copy<float32>")) << "an ordinary block is not a group";
        expect(!isFusedGroup("")) << "an entity with no recorded type name is not a group by default";
        // The match has to be the framework's type, not the word. A user block whose own name happens
        // to contain "Merge" is an ordinary block, and labelling it a group would tell a reader its
        // internals are invisible when they were never fused in the first place.
        expect(!isFusedGroup("acme::MergeSortFilter<float>")) << "a user type containing the word is still one block";
        expect(!isFusedGroup("acme::Merge<float>")) << "and so is one that shares the alias name without being the framework type";

        // The one input that actually matters: the type name a real merged unit records. Everything
        // above is a string literal written by hand, which cannot tell us that `MergeByIndex` is what
        // `typeName()` truly produces -- only building one can.
        using Fused = gr::Merge<gr::testing::Copy<float>, "out", gr::testing::Copy<float>, "in">;
        gr::trace::reset();
        gr::trace::setCategories(categoryMask(Category::workExact));
        {
            gr::Graph graph;
            auto&     src   = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{256U}}});
            auto&     fused = graph.emplaceBlock<Fused>({{"name", std::string("fused")}});
            auto&     snk   = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
            std::ignore     = graph.connect<"out", "in">(src, fused);
            std::ignore     = graph.connect<"out", "in">(fused, snk);

            TestScheduler scheduler;
            expect(scheduler.exchange(std::move(graph)).has_value() >> fatal);
            expect(scheduler.changeStateTo(gr::lifecycle::State::INITIALISED).has_value() >> fatal);

            // Found by type rather than by the name given above, because a merged unit **ignores the
            // `name` setting**: it reports its own type as its name. Worth knowing before trying to
            // pick one out of a capture.
            std::string recordedTypeName;
            std::string recordedUniqueName;
            std::string recordedName;
            for (const auto& block : scheduler.graph().blocks()) {
                if (std::string(block->typeName()).find("MergeByIndex") != std::string::npos) {
                    recordedTypeName   = std::string(block->typeName());
                    recordedUniqueName = std::string(block->uniqueName());
                    recordedName       = std::string(block->name());
                }
            }
            expect(!recordedTypeName.empty() >> fatal) << "the merged unit must appear in the block list";
            expect(isFusedGroup(recordedTypeName)) << "a real Merge<> must be recognised from what typeName() actually produces: " << recordedTypeName;
            expect(eq(recordedName, recordedTypeName)) << "a merged unit reports its type as its name, so the `name` setting given to emplaceBlock does not stick";
            // The unique name is spelled differently again -- no `gr::`, ports as `:0` -- and it is
            // what `gr4-trace` matches a chain against, so the two must not be assumed identical.
            expect(recordedUniqueName.find("MergeByIndex<") != std::string::npos) << "the unique name must still be recognisable: " << recordedUniqueName;
            expect(neq(recordedUniqueName, recordedTypeName)) << "unique name and type name use different spellings for the same unit";
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
        }
        gr::trace::setCategories(0U);

        // And the key reaches the report, keyed off what a capture actually records.
        const std::vector<Event>        events{exactRecord(EntityId{1U}, 100U, 100U, 0U), invocationOf(EntityId{1U}, 100U, 500U)};
        const std::vector<LoadedEntity> fused{LoadedEntity{.id = EntityId{1U}, .uniqueName = "merged#1", .typeName = "gr::MergeByIndex<A, 0, B, 0>"}};
        const gr::property_map          reportMap = timingReport(events, fused);

        const auto blocksIt = reportMap.find("blocks");
        expect((blocksIt != reportMap.end()) >> fatal);
        const gr::pmt::Value   blocksValue = (*blocksIt).second;
        const gr::property_map blocks      = blocksValue.value_or(gr::property_map{});
        const auto             blockIt     = blocks.find("merged#1");
        expect((blockIt != blocks.end()) >> fatal) << "the group must appear under its own name";
        const gr::pmt::Value   blockValue = (*blockIt).second;
        const gr::property_map block      = blockValue.value_or(gr::property_map{});
        const auto             grainIt    = block.find("grain");
        expect((grainIt != block.end()) >> fatal) << "every entry must say what grain it describes";
        expect(eq((*grainIt).second.value_or(std::string{}), std::string("fused-group")));
    };

    "the block-side markers do not change what a block does"_test = [] {
        // The gate that matters most in this milestone: `Block.hpp` is instantiated per block *type*
        // and `workInternal` is the innermost frame in the framework, so a perturbation here reaches
        // every block ever written. Asserted on the counts and statuses a run produces -- never on
        // record order, which is not a property the block controls.
        const auto runOnce = [](std::uint32_t categories) {
            gr::trace::reset();
            gr::trace::setCategories(categories);

            gr::Graph graph;
            auto&     src = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"name", std::string("src")}, {"n_samples_max", gr::Size_t{8192U}}, {"max_batch_size", gr::Size_t{512U}}});
            auto&     dec = graph.emplaceBlock<Decimator<float>>({{"name", std::string("dec")}, {"input_chunk_size", gr::Size_t{4U}}, {"output_chunk_size", gr::Size_t{1U}}, {"max_batch_size", gr::Size_t{512U}}});
            auto&     snk = graph.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("snk")}});
            std::ignore   = graph.connect<"out", "in">(src, dec);
            std::ignore   = graph.connect<"out", "in">(dec, snk);

            TestScheduler scheduler;
            std::ignore = scheduler.exchange(std::move(graph));
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::INITIALISED);
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::RUNNING);

            std::vector<std::pair<std::size_t, int>> results;
            for (std::size_t pass = 0UZ; pass < 128UZ; ++pass) {
                const gr::work::Result result = scheduler.step();
                results.emplace_back(result.performed_work, static_cast<int>(result.status));
            }
            gr::trace::setCategories(0U);
            std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);
            return results;
        };

        const auto untraced = runOnce(0U);
        const auto traced   = runOnce(kAllCategories);
        expect(eq(untraced.size(), traced.size()) >> fatal);
        expect(std::ranges::equal(untraced, traced)) << "the block-side markers changed the work::Result sequence -- the layer is not behaviour-neutral inside Block.hpp";
    };

    "enabling workExact does not enable the phase split"_test = [] {
        DecimatingRun run;
        run.run(categoryMask(Category::workExact));
        expect(eq(ofKind(run.events, Kind::workPhase).size(), 0UZ)) << "the two categories are separate so the criterion costs one scope, not five";
    };
};

int main() { /* tests are run by the boost::ut suite registration above */ }
