#include <boost/ut.hpp>

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
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/Trace.hpp>

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

    /// `batchCap` is not decoration: left unset, the whole stream moves in a *single* `work()` call
    /// and there is no second record to compare a position against. Capping it is what makes the
    /// stream coordinate observable at all.
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
        }

        expect(scheduler.changeStateTo(gr::lifecycle::State::RUNNING).has_value() >> fatal);
        for (std::size_t pass = 0UZ; pass < 256UZ; ++pass) {
            std::ignore = scheduler.step();
        }
        std::ignore = scheduler.changeStateTo(gr::lifecycle::State::STOPPED);

        events = collect();
        gr::trace::setCategories(0U);
    }
};

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
        const std::vector<Event> probes    = forEntity(ofKind(run.events, Kind::workProbe), run.decimator);
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
