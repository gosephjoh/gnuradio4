#include <boost/ut.hpp>

#include <algorithm>
#include <string>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::scheduler;

namespace {

/// A source that declares a sample rate. Most GR4 blocks (including `testing::ConstantSource`)
/// do not -- `sample_rate` is a tag/opt-in convention, not a `Block` base field -- which is
/// precisely why the derivation treats a missing anchor as a normal outcome.
template<typename T>
struct RateSource : gr::Block<RateSource<T>> {
    gr::PortOut<T> out;

    gr::Annotated<float, "sample_rate", gr::Unit<"Hz">> sample_rate = 0.f;

    GR_MAKE_REFLECTABLE(RateSource, out, sample_rate);

    [[nodiscard]] constexpr T processOne() const noexcept { return T{}; }
};

/// A decimator: consumes `kDecim` samples per invocation, produces one.
template<typename T, gr::Size_t kDecim>
struct Decimate : gr::Block<Decimate<T, kDecim>, gr::Resampling<kDecim, 1UZ>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(Decimate, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) const noexcept {
        const std::size_t nOut = std::min(output.size(), input.size() / kDecim);
        for (std::size_t i = 0UZ; i < nOut; ++i) {
            output[i] = input[i * kDecim];
        }
        output.publish(nOut);
        std::ignore = input.consume(nOut * kDecim);
        return gr::work::Status::OK;
    }
};

/// A sliding-window block: processes `kWindow` samples per invocation, advances `kStride`.
/// `kStride < kWindow` overlaps, `> kWindow` skips.
template<typename T, gr::Size_t kWindow, gr::Size_t kStride>
struct StridedWindow : gr::Block<StridedWindow<T, kWindow, kStride>, gr::Resampling<kWindow, 1UZ>, gr::Stride<kStride>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(StridedWindow, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) const noexcept {
        const std::size_t nOut = std::min(output.size(), input.size() / kWindow);
        output.publish(nOut);
        std::ignore = input.consume(nOut * kWindow);
        return gr::work::Status::OK;
    }
};

/// A 1:1 block with an active stride -- the pre-2024 contract, where the window comes from the
/// port bound rather than from `input_chunk_size`. See STRIDE_SEMANTICS.md §2.2.
template<typename T, gr::Size_t kStride>
struct StridedPassThrough : gr::Block<StridedPassThrough<T, kStride>, gr::Stride<kStride>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(StridedPassThrough, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) const noexcept {
        const std::size_t n = std::min(input.size(), output.size());
        output.publish(n);
        std::ignore = input.consume(n);
        return gr::work::Status::OK;
    }
};

/// Two *asynchronous* inputs -- the block runs when either has data, not when both do.
template<typename T>
struct Join2Async : gr::Block<Join2Async<T>> {
    gr::PortIn<T, gr::Async> in0;
    gr::PortIn<T, gr::Async> in1;
    gr::PortOut<T>           out;

    GR_MAKE_REFLECTABLE(Join2Async, in0, in1, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& a, gr::InputSpanLike auto& b, gr::OutputSpanLike auto& output) noexcept {
        const std::size_t n = std::min({a.size(), b.size(), output.size()});
        output.publish(n);
        std::ignore = a.consume(n);
        std::ignore = b.consume(n);
        return gr::work::Status::OK;
    }
};

/// One synchronous and one asynchronous input. `workInternal()` proceeds when the synchronous
/// group is satisfied *or* any asynchronous port has data, so such a block runs at whichever
/// trigger fires more often.
template<typename T>
struct MixedJoin : gr::Block<MixedJoin<T>> {
    gr::PortIn<T>            inSync;
    gr::PortIn<T, gr::Async> inAsync;
    gr::PortOut<T>           out;

    GR_MAKE_REFLECTABLE(MixedJoin, inSync, inAsync, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& sync, gr::InputSpanLike auto& async, gr::OutputSpanLike auto& output) noexcept {
        const std::size_t n = std::min(sync.size(), output.size());
        output.publish(n);
        std::ignore = sync.consume(n);
        std::ignore = async.consume(async.size());
        return gr::work::Status::OK;
    }
};

/// Records the largest input span it is ever handed, so a test can observe the batch the scheduler
/// actually requested rather than inferring it from the derived numbers.
template<typename T>
struct BatchProbe : gr::Block<BatchProbe<T>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(BatchProbe, in, out);

    std::size_t maxSeen = 0UZ;

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) noexcept {
        const std::size_t n = std::min(input.size(), output.size());
        maxSeen             = std::max(maxSeen, n);
        output.publish(n);
        std::ignore = input.consume(n);
        return gr::work::Status::OK;
    }
};

/// 1:kInterp interpolator -- the mirror of `Decimate`, and the case no test covered.
template<typename T, gr::Size_t kInterp>
struct Interpolate : gr::Block<Interpolate<T, kInterp>, gr::Resampling<1UZ, kInterp>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(Interpolate, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) const noexcept {
        const std::size_t nIn = std::min(input.size(), output.size() / kInterp);
        for (std::size_t i = 0UZ; i < nIn * kInterp; ++i) {
            output[i] = input[i / kInterp];
        }
        output.publish(nIn * kInterp);
        std::ignore = input.consume(nIn);
        return gr::work::Status::OK;
    }
};

/// Two synchronous inputs, one output -- the join point of a diamond.
template<typename T>
struct Join2 : gr::Block<Join2<T>> {
    gr::PortIn<T>  in0;
    gr::PortIn<T>  in1;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(Join2, in0, in1, out);

    [[nodiscard]] constexpr T processOne(T a, T b) const noexcept { return a + b; }
};

/// every attribute left at its default, i.e. nothing user-set
UserSetAttributes noneUserSet(const gr::BlockModel&) { return {}; }

NominalBatchStrategy defaultStrategy{};

/// TSource -> Copy -> NullSink, all 1:1
template<typename TSource>
struct Chain {
    gr::Graph       graph;
    gr::BlockModel* src  = nullptr;
    gr::BlockModel* mid  = nullptr;
    gr::BlockModel* sink = nullptr;

    explicit Chain(gr::property_map sourceSettings = {}) {
        auto& s = graph.template emplaceBlock<TSource>(std::move(sourceSettings));
        auto& m = graph.template emplaceBlock<gr::testing::Copy<float>>();
        auto& k = graph.template emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.template connect<"out", "in">(s, m).has_value());
        expect(graph.template connect<"out", "in">(m, k).has_value());

        const auto blocks = graph.blocks();
        src               = blocks[0].get();
        mid               = blocks[1].get();
        sink              = blocks[2].get();
    }
};

using AnchoredChain   = Chain<RateSource<float>>;
using UnanchoredChain = Chain<gr::testing::ConstantSource<float>>;

gr::property_map atRate(float hz) { return {{"sample_rate", hz}}; }

} // namespace

const boost::ut::suite<"SchedulingAnalysis"> schedulingAnalysisTests = [] {
    "every block gets an entry and a nominal batch"_test = [] {
        UnanchoredChain g;
        const auto      analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        expect(eq(analysis.perBlock.size(), 3UZ));
        for (const gr::BlockModel* block : {g.src, g.mid, g.sink}) {
            const auto* attributes = analysis.find(*block);
            expect(attributes != nullptr) << "every block must be analysed";
            expect(attributes->nominalBatch >= 1UZ) << "nominal batch must be positive";
        }
    };

    "the nominal batch is not the release threshold"_test = [] {
        // regression guard for the decision that superseded 'nominal = c_min': min_samples
        // defaults to 1, so keying on it would give per-sample periods and absurd utilisation.
        UnanchoredChain g;
        const auto      analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);
        expect(analysis.find(*g.mid)->nominalBatch > 1UZ) << "nominal batch must exceed the 1-sample release threshold";
    };

    "a uniform 1:1 chain has a uniform rate"_test = [] {
        UnanchoredChain g;
        const auto      analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        expect(eq(analysis.find(*g.src)->relativeRate, 1.0));
        expect(eq(analysis.find(*g.mid)->relativeRate, 1.0));
        expect(eq(analysis.find(*g.sink)->relativeRate, 1.0));
    };

    "a source without a sample_rate field yields no period"_test = [] {
        UnanchoredChain g;
        const auto      analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        for (const gr::BlockModel* block : {g.src, g.mid, g.sink}) {
            const auto* attributes = analysis.find(*block);
            expect(eq(attributes->period, 0.f)) << "a missing anchor must not fabricate a period";
            expect(attributes->periodOrigin == AttributeOrigin::unknown);
            expect(eq(attributes->relativeDeadline, 0.f));
        }
        expect(!analysis.diagnostics.empty()) << "the missing anchor must be reported, not silent";
    };

    "a declared but zero sample_rate is also treated as no anchor"_test = [] {
        AnchoredChain g; // RateSource defaults sample_rate to 0
        const auto    analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        expect(eq(analysis.find(*g.mid)->period, 0.f));
        expect(!analysis.diagnostics.empty());
    };

    "with an anchor, periods are derived and marked as batch-dependent"_test = [] {
        AnchoredChain g{atRate(1000.f)};
        const auto    analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        const auto* mid = analysis.find(*g.mid);
        expect(mid->period > 0.f) << "an anchored graph must yield a period";
        expect(mid->periodOrigin == AttributeOrigin::assumedBatch) << "the period depends on the assumed batch, and must say so";

        // period = nominalBatch / (sampleRate * relativeRate)
        const float expected = static_cast<float>(static_cast<double>(mid->nominalBatch) / 1000.0);
        expect(approx(mid->period, expected, 1e-6f));
    };

    "an implicit deadline equals the period"_test = [] {
        AnchoredChain g{atRate(1000.f)};
        const auto    analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        const auto* mid = analysis.find(*g.mid);
        expect(eq(mid->relativeDeadline, mid->period));
        expect(mid->deadlineOrigin == mid->periodOrigin);
    };

    "wcet is never derived"_test = [] {
        AnchoredChain g{atRate(1000.f)};
        const auto    analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        for (const gr::BlockModel* block : {g.src, g.mid, g.sink}) {
            const auto* attributes = analysis.find(*block);
            expect(eq(attributes->wcetEstimate, 0.f)) << "wcet must never be guessed from the graph";
            expect(attributes->wcetOrigin == AttributeOrigin::unknown);
        }
    };

    "rate-monotonic priorities are assigned where periods are known"_test = [] {
        AnchoredChain g{atRate(1000.f)};
        const auto    analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        for (const gr::BlockModel* block : {g.src, g.mid, g.sink}) {
            const auto* attributes = analysis.find(*block);
            expect(attributes->priority > 0) << "an anchored block must receive a priority";
            expect(attributes->priorityOrigin == AttributeOrigin::derivedFromRate);
        }
    };

    "an 8:1 decimator reduces the rate downstream of itself"_test = [] {
        // the coverage gap that mattered: every other topology here is 1:1, so a unit error in
        // the rate/period relationship would be invisible.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dec  = graph.emplaceBlock<Decimate<float, 8U>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, dec).has_value());
        expect(graph.connect<"out", "in">(dec, sink).has_value());

        const auto blocks   = graph.blocks();
        const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);

        const auto* srcAttr  = analysis.find(*blocks[0]);
        const auto* decAttr  = analysis.find(*blocks[1]);
        const auto* sinkAttr = analysis.find(*blocks[2]);

        // The 8x reduction appears *downstream* of the decimator, not at it. The decimator consumes
        // `nominalBatch` samples of the full-rate stream per invocation, exactly as its source
        // produces `nominalBatch` per invocation, so the two run equally often; it is the sink,
        // fed at an eighth of the rate, that runs less. Asserting 1/8 *at* the decimator was the
        // pre-M1d formula's error -- see DEVLOG_M1 §14.5 and §14.9 F3.
        expect(approx(decAttr->relativeSampleRate, 1.0, 1e-9)) << "the decimator still sees the full-rate stream on its input";
        expect(approx(sinkAttr->relativeSampleRate, 1.0 / 8.0, 1e-9)) << "the 8:1 reduction applies to what the decimator emits";
        expect(approx(decAttr->relativeRate, 1.0, 1e-9)) << "consuming a whole batch of the full-rate stream, it invokes as often as its source";
        expect(approx(sinkAttr->relativeRate, 1.0 / 8.0, 1e-9)) << "the reduced rate must propagate downstream of the decimator";
        expect(srcAttr->relativeRate > sinkAttr->relativeRate) << "the source must run more often than the block behind the decimator";
    };

    "a slower block gets a longer period and a lower priority"_test = [] {
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dec  = graph.emplaceBlock<Decimate<float, 8U>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, dec).has_value());
        expect(graph.connect<"out", "in">(dec, sink).has_value());

        const auto blocks   = graph.blocks();
        const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);

        const auto* srcAttr  = analysis.find(*blocks[0]);
        const auto* sinkAttr = analysis.find(*blocks[2]);

        expect(sinkAttr->period > srcAttr->period) << "a block running less often must have the longer period";
        expect(srcAttr->priority > sinkAttr->priority) << "rate-monotonic: the faster block must outrank the slower one";
    };

    "a rate correction propagates past the block that was corrected"_test = [] {
        // Diamond with a *short fast* branch and a *long slow* one. The join is reached at the high
        // rate and processed -- pushing that rate onto its successor -- before the slow branch
        // arrives and lowers it. A visit-once sweep leaves the tail holding the stale rate for
        // good, and so does any guard that only re-propagates on an *increase*.
        //
        // N.B. this used to assert the join took the *faster* branch. `Join2`'s inputs are
        // synchronous, so the block cannot run until both are satisfied and its rate is the
        // slower one -- see §14.15. The test's purpose is unchanged: it guards re-propagation past
        // an already-processed block, now in the lowering direction.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     hop1 = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     hop2 = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     slow = graph.emplaceBlock<Decimate<float, 8U>>(); // long branch: 3 hops, and slow
        auto&     join = graph.emplaceBlock<Join2<float>>();
        auto&     tail = graph.emplaceBlock<gr::testing::Copy<float>>();

        expect(graph.connect<"out", "in0">(src, join).has_value()); // short branch: 1 hop, full rate
        expect(graph.connect<"out", "in">(src, hop1).has_value());
        expect(graph.connect<"out", "in">(hop1, hop2).has_value());
        expect(graph.connect<"out", "in">(hop2, slow).has_value());
        expect(graph.connect<"out", "in1">(slow, join).has_value());
        expect(graph.connect<"out", "in">(join, tail).has_value());

        const auto  blocks   = graph.blocks();
        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* joinAttr = analysis.find(*blocks[4]);
        const auto* tailAttr = analysis.find(*blocks[5]);

        expect(approx(joinAttr->relativeSampleRate, 1.0 / 8.0, 1e-9)) << "synchronous inputs: the join runs at the slower of the two";
        expect(approx(tailAttr->relativeSampleRate, 1.0 / 8.0, 1e-9)) << "and the lowering must reach the block downstream of the join";

        const bool reportedConflict = std::ranges::any_of(analysis.diagnostics, [](const std::string& d) { return d.find("differing rates") != std::string::npos; });
        expect(reportedConflict) << "a contested rate must be reported, not silently resolved";
    };

    "an asynchronous join runs at the rate of its fastest input"_test = [] {
        // The other half of §14.15: an async port lets the block run when *any* input has data
        // (Block.hpp:1484), so the fastest contributor sets its rate -- the opposite of the
        // synchronous case above, from the same propagation pass.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     slow = graph.emplaceBlock<Decimate<float, 8U>>();
        auto&     join = graph.emplaceBlock<Join2Async<float>>();

        expect(graph.connect<"out", "in0">(src, join).has_value());
        expect(graph.connect<"out", "in">(src, slow).has_value());
        expect(graph.connect<"out", "in1">(slow, join).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* joinAttr = analysis.find(*graph.blocks()[2]);

        expect(approx(joinAttr->relativeSampleRate, 1.0, 1e-9)) << "asynchronous inputs: the join runs as often as its fastest input";
    };

    "a block with both port kinds runs at whichever trigger fires more often"_test = [] {
        // The sync and async groups are OR-ed (Block.hpp:2062), so a block holding both is invoked
        // when *either* fires. Its rate is therefore the max of the slowest sync input and the
        // fastest async one -- not the sync rate alone. Getting this wrong understates the
        // invocation rate, which overstates the period and *understates* utilisation: the unsafe
        // direction, since the block then looks less loaded than it is.
        gr::Graph graph;
        auto&     src   = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     slow  = graph.emplaceBlock<Decimate<float, 8U>>();
        auto&     mixed = graph.emplaceBlock<MixedJoin<float>>();

        expect(graph.connect<"out", "in">(src, slow).has_value());
        expect(graph.connect<"out", "inSync">(slow, mixed).has_value()); // slow, synchronous
        expect(graph.connect<"out", "inAsync">(src, mixed).has_value()); // fast, asynchronous

        const auto  analysis  = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* mixedAttr = analysis.find(*graph.blocks()[2]);

        expect(approx(mixedAttr->relativeSampleRate, 1.0, 1e-9)) << "the fast async trigger fires more often than the slow sync one, so it sets the rate";
    };

    "a slow async input does not drag down a faster sync group"_test = [] {
        // The converse, so the rule is not satisfied by simply preferring async.
        gr::Graph graph;
        auto&     src   = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     slow  = graph.emplaceBlock<Decimate<float, 8U>>();
        auto&     mixed = graph.emplaceBlock<MixedJoin<float>>();

        expect(graph.connect<"out", "in">(src, slow).has_value());
        expect(graph.connect<"out", "inSync">(src, mixed).has_value());   // fast, synchronous
        expect(graph.connect<"out", "inAsync">(slow, mixed).has_value()); // slow, asynchronous

        const auto  analysis  = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* mixedAttr = analysis.find(*graph.blocks()[2]);

        expect(approx(mixedAttr->relativeSampleRate, 1.0, 1e-9)) << "the sync group fires more often here, so it sets the rate";
    };

    "mismatched synchronous rates are reported as a likely wiring error"_test = [] {
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     slow = graph.emplaceBlock<Decimate<float, 8U>>();
        auto&     join = graph.emplaceBlock<Join2<float>>();

        expect(graph.connect<"out", "in0">(src, join).has_value());
        expect(graph.connect<"out", "in">(src, slow).has_value());
        expect(graph.connect<"out", "in1">(slow, join).has_value());

        const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);

        expect(std::ranges::any_of(analysis.diagnostics, [](const std::string& d) { return d.contains("back up"); })) << "the faster branch piles up without bound: GR4 has neither discard nor lookahead, so this warrants a warning";
    };

    "a shorter period outranks a longer one"_test = [] {
        AnchoredChain g{atRate(1000.f)};
        const auto    analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        const auto* mid  = analysis.find(*g.mid);
        const auto* sink = analysis.find(*g.sink);
        if (mid->period < sink->period) {
            expect(mid->priority > sink->priority) << "rate-monotonic: shorter period must win";
        } else if (sink->period < mid->period) {
            expect(sink->priority > mid->priority) << "rate-monotonic: shorter period must win";
        }
    };

    "no priorities are invented without periods"_test = [] {
        UnanchoredChain g;
        const auto      analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        for (const gr::BlockModel* block : {g.src, g.mid, g.sink}) {
            const auto* attributes = analysis.find(*block);
            expect(eq(attributes->priority, 0));
            expect(attributes->priorityOrigin == AttributeOrigin::unknown);
        }
    };

    "user-set attributes are preserved and marked"_test = [] {
        AnchoredChain g{atRate(1000.f)};
        std::ignore = g.mid->settings().set({{"sched_priority", std::int32_t{77}}, {"period", 0.25f}});
        std::ignore = g.mid->settings().activateContext();
        std::ignore = g.mid->settings().applyStagedParameters();

        const auto userSet = [&](const gr::BlockModel& block) { //
            return std::addressof(block) == g.mid ? UserSetAttributes{.priority = true, .period = true, .deadline = false, .wcet = false} : UserSetAttributes{};
        };

        const auto  analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, userSet);
        const auto* mid      = analysis.find(*g.mid);

        expect(eq(mid->priority, std::int32_t{77})) << "a user priority must survive derivation";
        expect(mid->priorityOrigin == AttributeOrigin::userSet);
        expect(approx(mid->period, 0.25f, 1e-6f)) << "a user period must survive derivation";
        expect(mid->periodOrigin == AttributeOrigin::userSet);
    };

    "userSetFromSettings detects an explicitly set attribute"_test = [] {
        UnanchoredChain g;
        expect(!userSetFromSettings(*g.mid).priority) << "an untouched attribute must not read as user-set";

        std::ignore = g.mid->settings().set({{"sched_priority", std::int32_t{5}}});
        std::ignore = g.mid->settings().activateContext();
        std::ignore = g.mid->settings().applyStagedParameters();
        expect(userSetFromSettings(*g.mid).priority) << "an explicitly set attribute must read as user-set";
    };

    "a feedback loop terminates and every block is analysed"_test = [] {
        gr::Graph graph;
        auto&     src = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     a   = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     b   = graph.emplaceBlock<gr::testing::Copy<float>>();
        expect(graph.connect<"out", "in">(src, a).has_value());
        expect(graph.connect<"out", "in">(a, b).has_value());
        std::ignore = graph.connect<"out", "in">(b, a); // close the cycle

        const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet); // must not hang
        expect(eq(analysis.perBlock.size(), 3UZ)) << "a cyclic graph must still analyse every block";
    };

    "an empty graph analyses cleanly"_test = [] {
        gr::Graph  graph;
        const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        expect(analysis.perBlock.empty());
    };

    "a scheduler populates the analysis at init()"_test = [] {
        AnchoredChain   g{atRate(1000.f)};
        gr::BlockModel* mid = g.mid;

        gr::scheduler::Simple<> sched;
        expect(sched.schedulingAnalysis().perBlock.empty()) << "nothing is derived before initialisation";

        expect(sched.exchange(std::move(g.graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

        const auto& analysis = sched.schedulingAnalysis();
        expect(eq(analysis.perBlock.size(), 3UZ)) << "init() must analyse every block in the graph";

        const auto* attributes = analysis.find(*mid);
        expect(attributes != nullptr) << "the analysis must key on the same blocks the scheduler runs";
        expect(attributes->period > 0.f) << "an anchored graph must yield periods through the scheduler too";
        expect(attributes->periodOrigin == AttributeOrigin::assumedBatch);

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "re-deriving does not freeze previously derived values"_test = [] {
        // guards the re-derivation hazard: derived values are kept in the scheduler, never
        // written back through settings(), so a second pass still sees them as derivable.
        AnchoredChain   g{atRate(1000.f)};
        gr::BlockModel* mid = g.mid;

        gr::scheduler::Simple<> sched;
        expect(sched.exchange(std::move(g.graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        const AttributeOrigin firstOrigin = sched.schedulingAnalysis().find(*mid)->periodOrigin;

        sched.refreshSchedulingAnalysis();
        const AttributeOrigin secondOrigin = sched.schedulingAnalysis().find(*mid)->periodOrigin;

        expect(firstOrigin == AttributeOrigin::assumedBatch);
        expect(secondOrigin == AttributeOrigin::assumedBatch) << "a re-derived value must not turn into a 'user_set' one";

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a graph containing a sub-scheduler is analysed without hanging"_test = [] {
        // Pays down debt D2 (§10.1): every other graph in this suite is flat, so `flatten()`'s
        // actual purpose went unexercised -- which is how M1d-3 shipped an unguarded
        // `perBlock.at()` on a block named by the adjacency list but absent from `graph.blocks()`.
        // It threw inside init(), the scheduler never reached RUNNING, and qa_ManagedSubGraph and
        // qa_SchedulerMessages hung for minutes. A flat-graph suite cannot see any of that.
        using SubScheduler = gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded>;

        gr::Graph inner;
        auto&     innerSrc  = inner.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     innerSink = inner.emplaceBlock<gr::testing::NullSink<float>>();
        expect(inner.connect<"out", "in">(innerSrc, innerSink).has_value());

        auto subScheduler = std::make_shared<gr::SchedulerWrapper<SubScheduler>>();
        subScheduler->setGraph(std::move(inner));

        gr::Graph outer;
        std::ignore = outer.emplaceBlock<RateSource<float>>(atRate(2000.f));
        std::ignore = outer.addBlock(std::static_pointer_cast<gr::BlockModel>(subScheduler));

        // Flatten first, exactly as SchedulerBase::refreshSchedulingAnalysis() does. This is what
        // makes the test bite: flatten() copies a nested block's *edges* into the flat graph while
        // leaving the nested blocks themselves out, so the adjacency list names blocks that are
        // absent from `blocks()` -- and any unguarded `perBlock.at()` on one of them throws.
        gr::Graph  flat     = gr::graph::flatten(outer);
        const auto analysis = deriveSchedulingAttributes(flat, defaultStrategy, noneUserSet);

        expect(eq(analysis.perBlock.size(), flat.blocks().size())) << "every block of the flattened graph must be analysed";
        for (const auto& block : flat.blocks()) {
            expect(analysis.find(*block) != nullptr) << "the pass must complete rather than throw on a nested graph";
        }
    };

    "a decimator's own period matches its source's"_test = [] {
        // The M1d-3 guard. Under the pre-M1d formula the decimator's period came out 8x too long,
        // because an *invocation*-rate ratio was multiplied by a graph-wide batch. It consumes a
        // whole batch of the full-rate stream, exactly as the source produces one, so the two
        // periods are equal. Reverting the formula makes this test, and only this test, fail.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dec  = graph.emplaceBlock<Decimate<float, 8U>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, dec).has_value());
        expect(graph.connect<"out", "in">(dec, sink).has_value());

        const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet, 512UZ);
        const auto blocks   = graph.blocks();

        const auto* srcAttr  = analysis.find(*blocks[0]);
        const auto* decAttr  = analysis.find(*blocks[1]);
        const auto* sinkAttr = analysis.find(*blocks[2]);

        expect(approx(decAttr->period, srcAttr->period, 1e-9f)) << "the decimator advances through the full-rate stream at the same pace as its source";
        expect(approx(sinkAttr->period, 8.f * srcAttr->period, 1e-9f)) << "the eightfold slowdown belongs to the block fed by the decimator";
    };

    "an overlapping stride makes a block fire more often than its window suggests"_test = [] {
        // 1024-sample window advancing 512: the block does window-sized work twice as often as
        // 1024/S would suggest, and emits one value per window, so downstream sees S/512 -- not
        // S/1024. Using the window for either number is the classic overlap error.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1024.f));
        auto&     fft  = graph.emplaceBlock<StridedWindow<float, 1024U, 512U>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, fft).has_value());
        expect(graph.connect<"out", "in">(fft, sink).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto  blocks   = graph.blocks();
        const auto* fftAttr  = analysis.find(*blocks[1]);
        const auto* sinkAttr = analysis.find(*blocks[2]);

        expect(eq(fftAttr->nominalBatch, 1024UZ)) << "the work quantum is the window";
        expect(approx(fftAttr->period, 0.5f, 1e-6f)) << "but it fires every 512/1024 s -- the advance, not the window";
        expect(approx(sinkAttr->relativeSampleRate, 1.0 / 512.0, 1e-12)) << "one value per 512 consumed samples, not per 1024 processed";
    };

    "a skipping stride makes a block fire less often"_test = [] {
        gr::Graph graph;
        auto&     src = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dut = graph.emplaceBlock<StridedWindow<float, 100U, 250U>>();
        expect(graph.connect<"out", "in">(src, dut).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* dutAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(dutAttr->nominalBatch, 100UZ)) << "it still processes one 100-sample window";
        expect(approx(dutAttr->period, 0.25f, 1e-6f)) << "but only once per 250 samples of stream";
    };

    "a strided resampling block ignores every batch ceiling"_test = [] {
        // decision 5, row 1: computeResampling() caps it at one chunk and clamps a smaller request
        // back up, so the ceiling binds in neither direction. Reporting a clamped number would
        // imply the knob did something.
        gr::Graph graph;
        auto&     src = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dut = graph.emplaceBlock<StridedWindow<float, 100U, 50U>>();
        expect(graph.connect<"out", "in">(src, dut).has_value());
        std::ignore = graph.blocks()[1]->settings().set({{"max_batch_size", gr::Size_t{4000}}});
        std::ignore = graph.blocks()[1]->settings().activateContext();
        std::ignore = graph.blocks()[1]->settings().applyStagedParameters();

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet, 8UZ);
        const auto* dutAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(dutAttr->executionCeiling, 100UZ)) << "neither the block's 4000 nor the scheduler's 8 can move it off one window";
        expect(eq(dutAttr->nominalBatch, 100UZ));
        expect(eq(dutAttr->batchFloor, 100UZ)) << "release needs a whole window";
    };

    "a strided 1:1 block takes its window from the port bound"_test = [] {
        // decision 5, row 2: the pre-2024 contract, still reachable. The window is whatever bounds
        // it, and the advance is the stride.
        gr::Graph graph;
        auto&     src = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dut = graph.emplaceBlock<StridedPassThrough<float, 64U>>();
        expect(graph.connect<"out", "in">(src, dut).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet, 256UZ);
        const auto* dutAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(dutAttr->executionCeiling, 256UZ)) << "a bounded window is derivable";
        expect(dutAttr->batchOrigin == AttributeOrigin::configured);
        expect(approx(dutAttr->period, 64.f / 1000.f, 1e-6f)) << "it advances 64 samples per invocation regardless of the 256 it processes";
    };

    "a strided 1:1 block with no bounded window yields no period"_test = [] {
        // decision 5, row 3: the window is min(available, max_work_items) -- a scheduler artefact,
        // not a property of the block. Refusing to derive is the honest answer here, and the only
        // configuration for which it is.
        gr::Graph graph;
        auto&     src = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dut = graph.emplaceBlock<StridedPassThrough<float, 64U>>();
        expect(graph.connect<"out", "in">(src, dut).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* dutAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(dutAttr->period, 0.f)) << "an unbounded strided window must not be modelled";
        expect(dutAttr->batchOrigin == AttributeOrigin::unknown);
        expect(std::ranges::any_of(analysis.diagnostics, [](const std::string& d) { return d.contains("scheduler-dependent"); })) << "and must say why";
    };

    "an unbounded block keeps an unbounded execution ceiling"_test = [] {
        // F2: the strategy's 4096 stand-in is a *modelling* value. Letting it reach the worker
        // would cap every block in every stock graph, so only `nominalBatch` may carry it.
        UnanchoredChain g;
        const auto      analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet);

        const auto* mid = analysis.find(*g.mid);
        expect(eq(mid->executionCeiling, kUnboundedBatch)) << "nothing bounds this block: the worker must still request everything available";
        expect(eq(mid->nominalBatch, 4096UZ)) << "the modelling stand-in must fill in where the ceiling is unbounded";
        expect(mid->batchOrigin == AttributeOrigin::assumedBatch);
    };

    "a block inherits the scheduler's ceiling when it sets none"_test = [] {
        UnanchoredChain g;
        const auto      analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet, 1024UZ);

        const auto* mid = analysis.find(*g.mid);
        expect(eq(mid->executionCeiling, 1024UZ)) << "an explicit scheduler ceiling must reach the block";
        expect(eq(mid->nominalBatch, 1024UZ)) << "a bounded ceiling is the operating point; no stand-in needed";
        expect(mid->batchOrigin == AttributeOrigin::configured) << "a configured value is not an assumption";
    };

    "a block's own ceiling outranks the scheduler's"_test = [] {
        UnanchoredChain g;
        std::ignore = g.mid->settings().set({{"max_batch_size", gr::Size_t{256}}});
        std::ignore = g.mid->settings().activateContext();
        std::ignore = g.mid->settings().applyStagedParameters();

        const auto  analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet, 1024UZ);
        const auto* mid      = analysis.find(*g.mid);

        expect(eq(mid->executionCeiling, 256UZ)) << "a per-block ceiling must win over the global one";
        expect(mid->batchOrigin == AttributeOrigin::userSet);
        expect(eq(analysis.find(*g.sink)->executionCeiling, 1024UZ)) << "and must not leak to its neighbours";
    };

    "the period's provenance follows the batch's"_test = [] {
        AnchoredChain g{atRate(1000.f)};
        const auto    analysis = deriveSchedulingAttributes(g.graph, defaultStrategy, noneUserSet, 500UZ);

        const auto* mid = analysis.find(*g.mid);
        expect(mid->batchOrigin == AttributeOrigin::configured);
        expect(mid->periodOrigin == AttributeOrigin::configured) << "a period computed at a configured batch is not an assumed one";
        expect(approx(mid->period, 0.5f, 1e-6f)) << "500 samples of a 1 kHz stream is half a second";
    };

    "a ceiling below the release threshold cannot take effect"_test = [] {
        // requestedWork is a *soft* cut: computeResampling() clamps it up to minSync, so a
        // ceiling under the threshold is not a smaller batch, it is no change at all.
        gr::Graph graph;
        auto&     src = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dec = graph.emplaceBlock<Decimate<float, 8U>>();
        expect(graph.connect<"out", "in">(src, dec).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet, 3UZ);
        const auto* decAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(decAttr->batchFloor, 8UZ)) << "a resampling block cannot be released below one chunk";
        expect(eq(decAttr->executionCeiling, 8UZ)) << "a ceiling of 3 must be reported as the 8 the block will actually run";
    };

    "a ceiling is floored to whole resampling chunks"_test = [] {
        gr::Graph graph;
        auto&     src = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dec = graph.emplaceBlock<Decimate<float, 8U>>();
        expect(graph.connect<"out", "in">(src, dec).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet, 100UZ);
        const auto* decAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(decAttr->executionCeiling, 96UZ)) << "100 samples is 12 whole 8-sample chunks, not 12.5";
    };

    "port bounds are read from the type-erased snapshot"_test = [] {
        // F4: DynamicPort holds *copies* of min_samples/max_samples taken once, lazily, when
        // initDynamicPorts() first runs -- normally at connect(). A bound applied to the typed
        // port after that is honoured by computeSampleLimits() but invisible here. This test
        // pins the behaviour rather than asserting it is desirable.
        gr::Graph graph;
        auto&     src      = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     mid      = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink     = graph.emplaceBlock<gr::testing::NullSink<float>>();
        mid.in.max_samples = 512UZ; // set *before* connecting
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* midAttr  = analysis.find(*graph.blocks()[1]);

        // N.B. the assertion is on `nominalBatch`, not `executionCeiling`: a port bound is modelling
        // input, never an execution instruction. The block already enforces its own port limits in
        // computeSampleLimits(), and restating them as a `requestedWork` ceiling changes what it
        // runs -- see §14.13, where doing exactly that broke qa_Block.
        expect(eq(midAttr->nominalBatch, 512UZ)) << "a port bound set before connection must reach the resolver";
        expect(eq(midAttr->executionCeiling, kUnboundedBatch)) << "but must not be executed against";
        expect(midAttr->batchOrigin == AttributeOrigin::derivedFromRate) << "a port-derived bound rests on the rate model, not on configuration";
    };

    "a port bound changed after the first derivation does not reach a later one"_test = [] {
        // F4: DynamicPort holds *copies* of min_samples/max_samples, taken once under
        // std::call_once when initDynamicPorts() first runs and never refreshed. Connection does
        // not trigger it -- the resolver itself is normally the first accessor, so a first
        // derivation does see current values. What it cannot see is a later change.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     mid  = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());

        mid.in.max_samples = 512UZ;
        const auto first   = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        expect(eq(first.find(*graph.blocks()[1])->nominalBatch, 512UZ)) << "the first derivation snapshots the live value";

        mid.in.max_samples = 128UZ;
        const auto second  = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        expect(eq(second.find(*graph.blocks()[1])->nominalBatch, 512UZ)) << "a re-derivation still sees the snapshot, not the new bound -- known limitation, not a desired property";
    };

    "a ceiling above a port bound does not raise the modelled batch"_test = [] {
        // Review finding: the block clamps `requestedWork` to its own port bounds, so a ceiling of
        // 4096 against a 512-sample port yields a 512-sample batch. Reporting 4096 would make the
        // derived period 8x too long -- the modelling error `nominalBatch` exists to avoid.
        gr::Graph graph;
        auto&     src      = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     mid      = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink     = graph.emplaceBlock<gr::testing::NullSink<float>>();
        mid.in.max_samples = 512UZ;
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet, 4096UZ);
        const auto* midAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(midAttr->executionCeiling, 4096UZ)) << "the request is still what the scheduler asks for";
        expect(eq(midAttr->nominalBatch, 512UZ)) << "but the batch it runs is bounded by the port";
        expect(midAttr->batchOrigin == AttributeOrigin::derivedFromRate) << "and the port bound is what binds, not the request";
    };

    "an output-port threshold is converted into input samples"_test = [] {
        // Review finding: `min_samples`/`max_samples` on an *output* port are in output samples.
        // For an 8:1 decimator a 4-sample output threshold is a 32-sample input threshold; taking
        // it as 4 understates the floor. Same unit error as the one qa_Block caught in the ceiling.
        gr::Graph graph;
        auto&     src       = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     dec       = graph.emplaceBlock<Decimate<float, 8U>>();
        dec.out.min_samples = 4UZ;
        expect(graph.connect<"out", "in">(src, dec).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* decAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(decAttr->batchFloor, 32UZ)) << "4 output samples of an 8:1 decimator need 32 input samples";
    };

    "each job list gets its own scheduling state"_test = [] {
        // The single-threaded tests only ever exercise `_schedStates[0]`; a multithreaded scheduler
        // partitions blocks across several job lists, and each needs its own parallel state.
        gr::Graph graph;
        auto&     src   = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{4096}}});
        auto&     probe = graph.emplaceBlock<BatchProbe<float>>({{"max_batch_size", gr::Size_t{16}}});
        auto&     sink  = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, probe).has_value());
        expect(graph.connect<"out", "in">(probe, sink).has_value());
        auto* probePtr = std::addressof(probe);

        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.runAndWait().has_value());

        expect(probePtr->maxSeen > 0UZ) << "the probe must have run";
        expect(probePtr->maxSeen <= 16UZ) << std::format("a per-block ceiling must bind on every job list, saw {}", probePtr->maxSeen);
    };

    "independent chains keep independent timebases"_test = [] {
        // Debt D3. Before this, one global anchor governed the whole graph: these two chains both
        // came out at 4.096 s, leaving the 48 kHz chain 48x too slow.
        gr::Graph graph;
        auto&     slowSrc  = graph.emplaceBlock<RateSource<float>>({{"sample_rate", 1000.f}});
        auto&     slowSink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        auto&     fastSrc  = graph.emplaceBlock<RateSource<float>>({{"sample_rate", 48000.f}});
        auto&     fastSink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(slowSrc, slowSink).has_value());
        expect(graph.connect<"out", "in">(fastSrc, fastSink).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto  blocks   = graph.blocks();
        const auto* slowAttr = analysis.find(*blocks[1]);
        const auto* fastAttr = analysis.find(*blocks[3]);

        expect(approx(slowAttr->period, 4096.f / 1000.f, 1e-4f)) << "the 1 kHz chain must be anchored at 1 kHz";
        expect(approx(fastAttr->period, 4096.f / 48000.f, 1e-6f)) << "and the 48 kHz chain at 48 kHz";
        expect(fastAttr->period < slowAttr->period) << "a faster source must yield shorter periods, not the same ones";
    };

    "the timebase does not depend on block naming"_test = [] {
        // findSourceBlocks() sorts by name, so under a single global anchor the *alphabetically
        // first* source decided every period in the graph -- renaming a block rescaled the lot.
        const auto periodsFor = [](std::string slowName, std::string fastName) {
            gr::Graph graph;
            auto&     slowSrc  = graph.emplaceBlock<RateSource<float>>({{"sample_rate", 1000.f}, {"name", std::move(slowName)}});
            auto&     slowSink = graph.emplaceBlock<gr::testing::NullSink<float>>();
            auto&     fastSrc  = graph.emplaceBlock<RateSource<float>>({{"sample_rate", 48000.f}, {"name", std::move(fastName)}});
            auto&     fastSink = graph.emplaceBlock<gr::testing::NullSink<float>>();
            expect(graph.connect<"out", "in">(slowSrc, slowSink).has_value());
            expect(graph.connect<"out", "in">(fastSrc, fastSink).has_value());

            const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
            const auto blocks   = graph.blocks();
            return std::pair{analysis.find(*blocks[1])->period, analysis.find(*blocks[3])->period};
        };

        const auto [slowA, fastA] = periodsFor("A_slow", "B_fast");
        const auto [slowB, fastB] = periodsFor("Z_slow", "B_fast"); // ordering flipped

        expect(approx(slowA, slowB, 1e-6f)) << "renaming a source must not change any period";
        expect(approx(fastA, fastB, 1e-6f)) << "renaming a source must not change any period";
    };

    "an unanchored chain does not borrow another chain's timebase"_test = [] {
        gr::Graph graph;
        auto&     anchored   = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     sinkA      = graph.emplaceBlock<gr::testing::NullSink<float>>();
        auto&     unanchored = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     sinkU      = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(anchored, sinkA).has_value());
        expect(graph.connect<"out", "in">(unanchored, sinkU).has_value());

        const auto analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto blocks   = graph.blocks();

        expect(analysis.find(*blocks[1])->period > 0.f) << "the anchored chain still gets a period";
        expect(eq(analysis.find(*blocks[3])->period, 0.f)) << "the unanchored chain must stay unset, not inherit 1 kHz";
        expect(std::ranges::any_of(analysis.diagnostics, [](const std::string& d) { return d.contains("no positive 'sample_rate'"); })) << "and must say which source lacks an anchor";
    };

    "flatten hoists a transparent subgraph's blocks into the analysis"_test = [] {
        // Debt D2. Every other graph in this suite is flat, so `flatten()`'s actual purpose --
        // hoisting the children of a TransparentBlockGroup -- went unexercised. The sub-*scheduler*
        // test above does not cover it: a ScheduledBlockGroup is deliberately *not* traversed by
        // `flatten<TransparentBlockGroup>`, so its children are never hoisted.
        gr::Graph inner;
        auto&     innerSrc  = inner.emplaceBlock<RateSource<float>>({{"sample_rate", 1000.f}, {"name", std::string("inner_src")}});
        auto&     innerDec  = inner.emplaceBlock<Decimate<float, 8U>>({{"name", std::string("inner_dec")}});
        auto&     innerSink = inner.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("inner_sink")}});
        expect(inner.connect<"out", "in">(innerSrc, innerDec).has_value());
        expect(inner.connect<"out", "in">(innerDec, innerSink).has_value());

        gr::Graph outer;
        std::ignore = outer.addBlock(std::static_pointer_cast<gr::BlockModel>(std::make_shared<gr::GraphWrapper<gr::Graph>>(std::move(inner))));
        expect(eq(outer.blocks().size(), 1UZ)) << "the parent holds one opaque block before flattening";

        gr::Graph flat = gr::graph::flatten(outer);
        expect(eq(flat.blocks().size(), 4UZ)) << "flattening must hoist the three children alongside their wrapper";
        expect(eq(flat.edges().size(), 2UZ)) << "and carry the nested edges with them";

        const auto analysis = deriveSchedulingAttributes(flat, defaultStrategy, noneUserSet);
        for (const auto& block : flat.blocks()) {
            expect(analysis.find(*block) != nullptr) << std::format("every hoisted block must be analysed, missing '{}'", block->name());
        }
    };

    "rates and periods derive correctly across a subgraph boundary"_test = [] {
        gr::Graph inner;
        auto&     innerSrc  = inner.emplaceBlock<RateSource<float>>({{"sample_rate", 1000.f}, {"name", std::string("nested_src")}});
        auto&     innerDec  = inner.emplaceBlock<Decimate<float, 8U>>({{"name", std::string("nested_dec")}});
        auto&     innerSink = inner.emplaceBlock<gr::testing::NullSink<float>>({{"name", std::string("nested_sink")}});
        expect(inner.connect<"out", "in">(innerSrc, innerDec).has_value());
        expect(inner.connect<"out", "in">(innerDec, innerSink).has_value());

        gr::Graph outer;
        std::ignore = outer.addBlock(std::static_pointer_cast<gr::BlockModel>(std::make_shared<gr::GraphWrapper<gr::Graph>>(std::move(inner))));

        gr::Graph  flat     = gr::graph::flatten(outer);
        const auto analysis = deriveSchedulingAttributes(flat, defaultStrategy, noneUserSet);

        // N.B. looked up by *name*: the analysis is keyed by `BlockModel*`, which is the type-erased
        // wrapper, not the address of the user block returned by `emplaceBlock`.
        const auto attributesOf = [&](std::string_view name) -> const DerivedAttributes* {
            for (const auto& block : flat.blocks()) {
                if (block->name() == name) {
                    return analysis.find(*block);
                }
            }
            return nullptr;
        };

        const auto* src  = attributesOf("nested_src");
        const auto* dec  = attributesOf("nested_dec");
        const auto* sink = attributesOf("nested_sink");
        expect(src != nullptr and dec != nullptr and sink != nullptr) << "all three nested blocks must be hoisted and analysed";

        expect(approx(src->relativeSampleRate, 1.0, 1e-9)) << "the nested source anchors its own chain";
        expect(approx(dec->relativeSampleRate, 1.0, 1e-9)) << "the decimator still sees the full-rate stream";
        expect(approx(sink->relativeSampleRate, 1.0 / 8.0, 1e-9)) << "and the 8:1 reduction applies downstream of it, inside the subgraph";
        expect(approx(sink->period, 8.f * src->period, 1e-4f)) << "periods anchor to the nested source's own sample_rate";
    };

    "a container block produces no spurious unreachable diagnostic"_test = [] {
        // A TransparentBlockGroup wrapper carries no stream of its own; its children were hoisted
        // beside it. Reporting it as unreachable is noise that would appear for every subgraph.
        gr::Graph inner;
        auto&     a = inner.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     b = inner.emplaceBlock<gr::testing::NullSink<float>>();
        expect(inner.connect<"out", "in">(a, b).has_value());

        gr::Graph outer;
        std::ignore = outer.addBlock(std::static_pointer_cast<gr::BlockModel>(std::make_shared<gr::GraphWrapper<gr::Graph>>(std::move(inner))));

        gr::Graph  flat     = gr::graph::flatten(outer);
        const auto analysis = deriveSchedulingAttributes(flat, defaultStrategy, noneUserSet);

        expect(std::ranges::none_of(analysis.diagnostics, [](const std::string& d) { return d.contains("not reachable"); })) << "a subgraph wrapper is not an unreachable stream block";
    };

    "a nested block's analysis entry survives the scheduler taking the graph"_test = [] {
        // §9.8's lifetime reasoning -- `flatten()` re-adds the *same* `shared_ptr<BlockModel>`, so
        // pointers captured beforehand stay valid as analysis keys -- asserted for a nested graph
        // for the first time. Untested, this is exactly the kind of thing that fails as a missing
        // map entry long after the change that caused it.
        gr::Graph inner;
        auto&     innerSrc = inner.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     innerMid = inner.emplaceBlock<gr::testing::Copy<float>>();
        auto&     innerEnd = inner.emplaceBlock<gr::testing::NullSink<float>>();
        expect(inner.connect<"out", "in">(innerSrc, innerMid).has_value());
        expect(inner.connect<"out", "in">(innerMid, innerEnd).has_value());
        gr::BlockModel* capturedBeforeExchange = nullptr;

        gr::Graph outer;
        auto      wrapper = std::static_pointer_cast<gr::BlockModel>(std::make_shared<gr::GraphWrapper<gr::Graph>>(std::move(inner)));
        std::ignore       = outer.addBlock(wrapper);
        gr::Graph probe   = gr::graph::flatten(outer); // bound, not a temporary: blocks() is a view into it
        for (const auto& block : probe.blocks()) {
            if (block->name().starts_with("gr::testing::Copy")) {
                capturedBeforeExchange = block.get();
            }
        }
        expect(capturedBeforeExchange != nullptr) << "the nested block must be findable before the scheduler takes the graph";

        gr::scheduler::Simple<> sched;
        expect(sched.exchange(std::move(outer)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

        expect(sched.schedulingAnalysis().find(*capturedBeforeExchange) != nullptr) << "a pointer captured before exchange() must still key the analysis afterwards";

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "an interpolator raises the rate downstream of itself"_test = [] {
        // Every other resampling test in this suite decimates. Interpolation exercises the same
        // formula in the opposite direction -- `emitted = S_in x output_chunk_size / advance` with
        // the ratio above one -- and nothing covered it.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     itp  = graph.emplaceBlock<Interpolate<float, 4U>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, itp).has_value());
        expect(graph.connect<"out", "in">(itp, sink).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto  blocks   = graph.blocks();
        const auto* itpAttr  = analysis.find(*blocks[1]);
        const auto* sinkAttr = analysis.find(*blocks[2]);

        expect(approx(itpAttr->relativeSampleRate, 1.0, 1e-9)) << "the interpolator still sees the source's rate on its input";
        expect(approx(sinkAttr->relativeSampleRate, 4.0, 1e-9)) << "and emits four samples for every one consumed";
        expect(approx(sinkAttr->period, itpAttr->period / 4.f, 1e-6f)) << "so the block behind it fires four times as often";
    };

    "an interpolator's output bound converts into input samples"_test = [] {
        // The dividing direction of the unit conversion: 1000 *output* samples of a 1:4
        // interpolator is 250 *input* samples. The decimator test covers the multiplying direction.
        gr::Graph graph;
        auto&     src       = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     itp       = graph.emplaceBlock<Interpolate<float, 4U>>();
        itp.out.max_samples = 1000UZ;
        expect(graph.connect<"out", "in">(src, itp).has_value());

        const auto  analysis = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto* itpAttr  = analysis.find(*graph.blocks()[1]);

        expect(eq(itpAttr->nominalBatch, 250UZ)) << "1000 output samples at 1:4 is 250 input samples, not 1000";
        expect(approx(itpAttr->period, 0.25f, 1e-6f)) << "and the period follows the input-sample batch";
    };

    "per-block batches set periods independently of one another"_test = [] {
        // The payoff of D1 (§14.12): a block's batch sets *its own* period and leaves the stream
        // rate alone, so neighbours are free to run at different batches. Before the fix a single
        // graph-wide batch was baked into the propagated quantity.
        gr::Graph graph;
        auto&     src   = graph.emplaceBlock<RateSource<float>>(atRate(1000.f));
        auto&     big   = graph.emplaceBlock<gr::testing::Copy<float>>({{"max_batch_size", gr::Size_t{1024}}});
        auto&     small = graph.emplaceBlock<gr::testing::Copy<float>>({{"max_batch_size", gr::Size_t{256}}});
        auto&     sink  = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, big).has_value());
        expect(graph.connect<"out", "in">(big, small).has_value());
        expect(graph.connect<"out", "in">(small, sink).has_value());

        const auto  analysis  = deriveSchedulingAttributes(graph, defaultStrategy, noneUserSet);
        const auto  blocks    = graph.blocks();
        const auto* bigAttr   = analysis.find(*blocks[1]);
        const auto* smallAttr = analysis.find(*blocks[2]);
        const auto* sinkAttr  = analysis.find(*blocks[3]);

        expect(approx(bigAttr->period, 1024.f / 1000.f, 1e-6f)) << "a 1024-sample batch of a 1 kHz stream is a 1.024 s period";
        expect(approx(smallAttr->period, 256.f / 1000.f, 1e-6f)) << "and a 256-sample batch a quarter of that";
        expect(approx(smallAttr->period, bigAttr->period / 4.f, 1e-6f)) << "periods scale with each block's own batch";

        // The property that makes per-block batches expressible at all.
        for (const auto* attributes : {bigAttr, smallAttr, sinkAttr}) {
            expect(approx(attributes->relativeSampleRate, 1.0, 1e-9)) << "a 1:1 chain carries one stream rate regardless of what batches its blocks run";
        }
    };

    "the batch strategy is replaceable"_test = [] {
        struct FixedBatch : BatchStrategy {
            [[nodiscard]] std::string_view name() const noexcept override { return "FixedBatch"; }
            [[nodiscard]] BatchResolution  resolve(gr::BlockModel&, std::size_t) const override { return {.floor = 1UZ, .executionCeiling = 64UZ, .nominalBatch = 64UZ, .origin = AttributeOrigin::userSet}; }
        };

        AnchoredChain   g{atRate(1000.f)};
        gr::BlockModel* mid = g.mid;

        gr::scheduler::Simple<> sched;
        sched.setBatchStrategy(std::make_shared<FixedBatch>());
        expect(sched.exchange(std::move(g.graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

        const auto* attributes = sched.schedulingAnalysis().find(*mid);
        expect(eq(attributes->nominalBatch, 64UZ)) << "the injected strategy must be the one used";
        expect(approx(attributes->period, 64.f / 1000.f, 1e-6f));

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "an implicit deadline follows a user-set period"_test = [] {
        AnchoredChain   g{atRate(1000.f)};
        gr::BlockModel* mid = g.mid;
        std::ignore         = mid->settings().set({{"period", 0.25f}});
        std::ignore         = mid->settings().activateContext();
        std::ignore         = mid->settings().applyStagedParameters();

        const auto userSet = [&](const gr::BlockModel& block) { //
            return std::addressof(block) == mid ? UserSetAttributes{.priority = false, .period = true, .deadline = false, .wcet = false} : UserSetAttributes{};
        };

        const auto* attributes = deriveSchedulingAttributes(g.graph, defaultStrategy, userSet).find(*mid);
        expect(approx(attributes->relativeDeadline, 0.25f, 1e-6f)) << "an unset deadline must follow the user's period, not the derived one";
        expect(attributes->deadlineOrigin == AttributeOrigin::userSet) << "the deadline inherits the period's provenance";
    };

    "a null batch strategy is ignored"_test = [] {
        AnchoredChain g{atRate(1000.f)};

        gr::scheduler::Simple<> sched;
        sched.setBatchStrategy(nullptr); // must not clear the default and crash on init()
        expect(sched.exchange(std::move(g.graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
        expect(eq(sched.schedulingAnalysis().perBlock.size(), 3UZ));

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "origins stringify"_test = [] {
        expect(eq(toString(AttributeOrigin::userSet), std::string_view{"user_set"}));
        expect(eq(toString(AttributeOrigin::derivedFromRate), std::string_view{"derived_from_rate"}));
        expect(eq(toString(AttributeOrigin::assumedBatch), std::string_view{"assumed_batch"}));
        expect(eq(toString(AttributeOrigin::unknown), std::string_view{"unknown"}));
    };

    "a stock graph is still asked for an unbounded batch"_test = [] {
        // The F2 gate. `NominalBatchStrategy` reports a 4096 stand-in for `nominalBatch` where
        // nothing bounds a block -- had that reached the worker it would cap every graph in the
        // project at 4096 samples while claiming to be behaviour-neutral.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{4096}}});
        auto&     mid  = graph.emplaceBlock<gr::testing::Copy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
        gr::BlockModel* midModel = graph.blocks()[1].get();

        gr::scheduler::Simple<> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());

        const auto* attributes = sched.schedulingAnalysis().find(*midModel);
        expect(attributes != nullptr);
        expect(eq(attributes->executionCeiling, kUnboundedBatch)) << "the worker must still be told 'as much as is available'";
        expect(eq(attributes->nominalBatch, 4096UZ)) << "while the derivation keeps its stand-in for the period";

        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a per-block ceiling reaches work() and bounds the batch"_test = [] {
        constexpr std::size_t kCeiling = 32UZ;

        gr::Graph graph;
        auto&     src   = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{8192}}});
        auto&     probe = graph.emplaceBlock<BatchProbe<float>>({{"max_batch_size", gr::Size_t{kCeiling}}});
        auto&     sink  = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, probe).has_value());
        expect(graph.connect<"out", "in">(probe, sink).has_value());
        auto* probePtr = std::addressof(probe);

        gr::scheduler::Simple<> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.runAndWait().has_value());

        expect(probePtr->maxSeen > 0UZ) << "the probe must actually have run";
        expect(probePtr->maxSeen <= kCeiling) << std::format("a per-block ceiling of {} must bound the batch, saw {}", kCeiling, probePtr->maxSeen);
    };

    "an unbounded probe is handed much larger batches"_test = [] {
        // The control for the test above: without a ceiling the same graph runs far bigger
        // batches, so the previous test measures the knob rather than a coincidence.
        gr::Graph graph;
        auto&     src   = graph.emplaceBlock<gr::testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t{8192}}});
        auto&     probe = graph.emplaceBlock<BatchProbe<float>>();
        auto&     sink  = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, probe).has_value());
        expect(graph.connect<"out", "in">(probe, sink).has_value());
        auto* probePtr = std::addressof(probe);

        gr::scheduler::Simple<> sched;
        expect(sched.exchange(std::move(graph)).has_value());
        expect(sched.runAndWait().has_value());

        expect(probePtr->maxSeen > 32UZ) << std::format("without a ceiling the scheduler should hand over far more than 32 samples, saw {}", probePtr->maxSeen);
    };
};

int main() { /* tests are statically executed */ }
