#include <boost/ut.hpp>

#include <algorithm>
#include <array>
#include <format>
#include <span>
#include <vector>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

using namespace boost::ut;
using namespace gr::scheduler;

namespace {

template<typename T, gr::Size_t kDecim>
struct Decimate : gr::Block<Decimate<T, kDecim>, gr::Resampling<kDecim, 1UZ>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(Decimate, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) const noexcept {
        const std::size_t nOut = std::min(output.size(), input.size() / kDecim);
        output.publish(nOut);
        std::ignore = input.consume(nOut * kDecim);
        return gr::work::Status::OK;
    }
};

template<typename T, gr::Size_t kInterp>
struct Interpolate : gr::Block<Interpolate<T, kInterp>, gr::Resampling<1UZ, kInterp>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(Interpolate, in, out);

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) const noexcept {
        const std::size_t nIn = std::min(input.size(), output.size() / kInterp);
        output.publish(nIn * kInterp);
        std::ignore = input.consume(nIn);
        return gr::work::Status::OK;
    }
};

/// Window of `kWindow` samples advancing by `kStride` -- the work and consumption quanta differ.
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

template<typename T>
struct Join2 : gr::Block<Join2<T>> {
    gr::PortIn<T>  in0;
    gr::PortIn<T>  in1;
    gr::PortOut<T> out;

    GR_MAKE_REFLECTABLE(Join2, in0, in1, out);

    [[nodiscard]] constexpr T processOne(T a, T b) const noexcept { return a + b; }
};

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

/// A block declaring an *explicit* message input port. `all_input_ports` includes it, the stream-only
/// `PortCache` does not, and it reports `isSynchronous() == true` -- so before the gating rule was
/// centralised it landed in the capacity minimum and shrank the bound (DEVLOG_M3 §15).
template<typename T>
struct MsgGatedCopy : gr::Block<MsgGatedCopy<T>> {
    gr::PortIn<T>  in;
    gr::PortOut<T> out;
    gr::MsgPortIn  control;

    GR_MAKE_REFLECTABLE(MsgGatedCopy, in, out, control);

    [[nodiscard]] constexpr T processOne(T value) const noexcept { return value; }
};

constexpr std::size_t kProbeCeiling = 1024UZ;

/// Outcome of running the predicate and `work()` side by side (DEVLOG_M3 §5B.4).
///
/// `falseNegatives` is the number that matters: the predicate said "not runnable" and the block
/// then moved data anyway. Under the release model that is not a missed tick but a permanent
/// stall -- a block that never releases never runs -- so it must be zero.
///
/// `optimistic` is the opposite direction and is *expected* to be non-zero: readiness is gated on
/// input alone, while `work()` also honours output space, tag boundaries and chunk alignment.
/// It costs a ring slot, never correctness, so it is reported rather than asserted.
struct CrossCheck {
    std::size_t calls          = 0UZ;
    std::size_t progressed     = 0UZ;
    std::size_t falseNegatives = 0UZ;
    std::size_t optimistic     = 0UZ;
};

std::vector<std::size_t> snapshot(std::span<const std::size_t> values) { return {values.begin(), values.end()}; }

/// True where any port's available count fell, i.e. samples were actually consumed or published.
bool anyDecreased(std::span<const std::size_t> before, const std::vector<std::size_t>& after) {
    for (std::size_t i = 0UZ; i < std::min(before.size(), after.size()); ++i) {
        if (before[i] != gr::undefined_size && after[i] < before[i]) {
            return true;
        }
    }
    return false;
}

/// N.B. progress is measured from the *buffers*, not from `work::Result::performed_work`.
/// For a block whose inputs are all asynchronous, `performed_work` reports the resampling
/// computation over `requestedWork` rather than anything the ports did, so it is non-zero even
/// when nothing is consumed and nothing is published -- see the dedicated test below.
bool movedData(gr::BlockModel& block, std::size_t ceiling, gr::work::Result& result) {
    const std::vector<std::size_t> inputBefore  = snapshot(block.availableInputSamples(true));
    const std::vector<std::size_t> outputBefore = snapshot(block.availableOutputSamples(true));

    result = block.work(ceiling);

    const std::vector<std::size_t> inputAfter  = snapshot(block.availableInputSamples(true));
    const std::vector<std::size_t> outputAfter = snapshot(block.availableOutputSamples(true));

    return anyDecreased(inputBefore, inputAfter) || anyDecreased(outputBefore, outputAfter);
}

CrossCheck compareAgainstWork(gr::BlockModel& block, std::size_t iterations) {
    CrossCheck        out;
    const std::size_t threshold = gr::scheduler::detail::releaseThreshold(block);

    for (std::size_t i = 0UZ; i < iterations; ++i) {
        const Readiness  readiness = inputReadiness(block, threshold);
        gr::work::Result result{};
        const bool       progressed = movedData(block, kProbeCeiling, result);
        ++out.calls;

        if (progressed) {
            ++out.progressed;
            if (!readiness.runnable) {
                ++out.falseNegatives;
            }
        } else if (readiness.runnable) {
            ++out.optimistic;
        }

        if (result.status == gr::work::Status::ERROR || result.status == gr::work::Status::DONE) {
            break;
        }
    }
    return out;
}

void activate(gr::BlockModel& block) {
    expect(block.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
    expect(block.changeStateTo(gr::lifecycle::State::RUNNING).has_value());
}

/// `ConstantSource -> under test -> NullSink`, edges connected and every buffer empty. Only the
/// block under test is ever activated, so its input holds exactly what the test primes.
template<typename TUnder>
struct OneInput {
    gr::Graph       graph;
    gr::BlockModel* under = nullptr;

    explicit OneInput(gr::property_map settings = {}) {
        auto& src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto& mid  = graph.emplaceBlock<TUnder>(std::move(settings));
        auto& sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.template connect<"out", "in">(src, mid).has_value());
        expect(graph.template connect<"out", "in">(mid, sink).has_value());
        expect(graph.connectPendingEdges());
        under = graph.blocks()[1].get();
    }

    void prime(std::size_t n) { expect(under->primeInputPort(0UZ, n).has_value()); }
};

template<typename TUnder, gr::meta::fixed_string kPort0, gr::meta::fixed_string kPort1>
struct TwoInput {
    gr::Graph       graph;
    gr::BlockModel* under = nullptr;

    TwoInput() {
        auto& src0 = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto& src1 = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto& mid  = graph.emplaceBlock<TUnder>();
        auto& sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.template connect<"out", kPort0>(src0, mid).has_value());
        expect(graph.template connect<"out", kPort1>(src1, mid).has_value());
        expect(graph.template connect<"out", "in">(mid, sink).has_value());
        expect(graph.connectPendingEdges());
        under = graph.blocks()[2].get();
    }

    void prime(std::size_t n0, std::size_t n1) {
        expect(under->primeInputPort(0UZ, n0).has_value());
        expect(under->primeInputPort(1UZ, n1).has_value());
    }
};

constexpr std::array<std::size_t, 8> kPrimeCounts{0UZ, 1UZ, 3UZ, 4UZ, 7UZ, 8UZ, 64UZ, 512UZ};

} // namespace

const boost::ut::suite<"readiness predicate vs work()"> crossCheckTests = [] {
    "1:1 block -- predicate never under-reports"_test = [] {
        for (std::size_t primed : kPrimeCounts) {
            OneInput<gr::testing::Copy<float>> harness;
            harness.prime(primed);
            activate(*harness.under);
            const CrossCheck cc = compareAgainstWork(*harness.under, 4UZ);
            expect(eq(cc.falseNegatives, 0UZ)) << std::format("Copy primed with {}", primed);
        }
    };

    "decimator -- chunk alignment does not make the predicate strict"_test = [] {
        for (std::size_t primed : kPrimeCounts) {
            OneInput<Decimate<float, 4>> harness;
            harness.prime(primed);
            activate(*harness.under);
            const CrossCheck cc = compareAgainstWork(*harness.under, 4UZ);
            expect(eq(cc.falseNegatives, 0UZ)) << std::format("Decimate<4> primed with {}", primed);
        }
    };

    "interpolator -- output-side expansion does not make the predicate strict"_test = [] {
        for (std::size_t primed : kPrimeCounts) {
            OneInput<Interpolate<float, 3>> harness;
            harness.prime(primed);
            activate(*harness.under);
            const CrossCheck cc = compareAgainstWork(*harness.under, 4UZ);
            expect(eq(cc.falseNegatives, 0UZ)) << std::format("Interpolate<3> primed with {}", primed);
        }
    };

    "strided window -- work and consumption quanta differ"_test = [] {
        for (std::size_t primed : kPrimeCounts) {
            OneInput<StridedWindow<float, 8, 4>> harness;
            harness.prime(primed);
            activate(*harness.under);
            const CrossCheck cc = compareAgainstWork(*harness.under, 4UZ);
            expect(eq(cc.falseNegatives, 0UZ)) << std::format("StridedWindow<8,4> primed with {}", primed);
        }
    };

    "two synchronous inputs -- the slowest port gates"_test = [] {
        for (std::size_t primed0 : kPrimeCounts) {
            for (std::size_t primed1 : {0UZ, 1UZ, 8UZ, 64UZ}) {
                TwoInput<Join2<float>, "in0", "in1"> harness;
                harness.prime(primed0, primed1);
                activate(*harness.under);
                const CrossCheck cc = compareAgainstWork(*harness.under, 4UZ);
                expect(eq(cc.falseNegatives, 0UZ)) << std::format("Join2 primed with {}/{}", primed0, primed1);
            }
        }
    };

    "two asynchronous inputs -- the fastest port gates"_test = [] {
        for (std::size_t primed0 : kPrimeCounts) {
            for (std::size_t primed1 : {0UZ, 1UZ, 8UZ, 64UZ}) {
                TwoInput<Join2Async<float>, "in0", "in1"> harness;
                harness.prime(primed0, primed1);
                activate(*harness.under);
                const CrossCheck cc = compareAgainstWork(*harness.under, 4UZ);
                expect(eq(cc.falseNegatives, 0UZ)) << std::format("Join2Async primed with {}/{}", primed0, primed1);
            }
        }
    };

    "mixed synchronous and asynchronous inputs"_test = [] {
        for (std::size_t primedSync : kPrimeCounts) {
            for (std::size_t primedAsync : {0UZ, 1UZ, 8UZ, 64UZ}) {
                TwoInput<MixedJoin<float>, "inSync", "inAsync"> harness;
                harness.prime(primedSync, primedAsync);
                activate(*harness.under);
                const CrossCheck cc = compareAgainstWork(*harness.under, 4UZ);
                expect(eq(cc.falseNegatives, 0UZ)) << std::format("MixedJoin primed with sync {} / async {}", primedSync, primedAsync);
            }
        }
    };

    "an asynchronous join is optimistic, and that divergence is real"_test = [] {
        // in0 has data, in1 has none. The `max`-across-async rule reports the block runnable, but
        // `processBulk` takes `min(a, b, out)` and moves nothing. Asserted rather than merely
        // noted, so that narrowing the predicate later shows up here as a deliberate change
        // (DEVLOG_M3 §5A.6, §5B.4).
        TwoInput<Join2Async<float>, "in0", "in1"> harness;
        harness.prime(64UZ, 0UZ);
        activate(*harness.under);

        const std::size_t threshold = gr::scheduler::detail::releaseThreshold(*harness.under);
        const Readiness   readiness = inputReadiness(*harness.under, threshold);
        expect(readiness.runnable) << "max-across-async reports runnable";
        expect(eq(readiness.available, 64UZ));

        gr::work::Result result{};
        expect(!movedData(*harness.under, kProbeCeiling, result)) << "but the block itself needs both ports";
    };

    // Found while building the cross-check, and the reason progress is measured from the buffers.
    // For a block whose inputs are *all* asynchronous there is no synchronous port to bound
    // `availableToProcess`, so `computeSampleLimits()` folds `requestedWork` through unchanged and
    // `performed_work` comes back equal to it -- with both input buffers empty, nothing consumed
    // and nothing published. Any loop treating `performed_work > 0` as "this block did something"
    // is therefore misled by such a block; M2e's selection loop restarts on exactly that signal.
    "performed_work is non-zero for an idle all-asynchronous block"_test = [] {
        TwoInput<Join2Async<float>, "in0", "in1"> harness;
        harness.prime(0UZ, 0UZ);
        activate(*harness.under);

        gr::work::Result result{};
        const bool       moved = movedData(*harness.under, kProbeCeiling, result);

        expect(!moved) << "no samples consumed and none published";
        expect(gt(result.performed_work, 0UZ)) << "yet performed_work is non-zero";
        expect(eq(result.performed_work, kProbeCeiling)) << "it is the requested work, fed straight through";
    };
};

const boost::ut::suite<"readiness edge cases"> edgeCaseTests = [] {
    "a source is gated by output space, never by SIZE_MAX"_test = [] {
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, sink).has_value());
        expect(graph.connectPendingEdges());

        gr::BlockModel& source    = *graph.blocks()[0];
        const Readiness readiness = inputReadiness(source, gr::scheduler::detail::releaseThreshold(source));

        expect(readiness.runnable) << "an empty downstream buffer is space to write into";
        expect(lt(readiness.available, gr::undefined_size));
        expect(gt(readiness.available, 0UZ));
    };

    "an unconnected block is not runnable"_test = [] {
        gr::Graph       graph;
        auto&           orphan = graph.emplaceBlock<gr::testing::Copy<float>>();
        gr::BlockModel& block  = *graph.blocks()[0];
        std::ignore            = orphan;

        const Readiness readiness = inputReadiness(block, gr::scheduler::detail::releaseThreshold(block));
        expect(!readiness.runnable);
        expect(eq(readiness.available, 0UZ));
    };

    "unassignedSamples saturates instead of wrapping"_test = [] {
        expect(eq(unassignedSamples(100UZ, 40UZ), 60UZ));
        expect(eq(unassignedSamples(40UZ, 40UZ), 0UZ));
        expect(eq(unassignedSamples(40UZ, 100UZ), 0UZ)) << "must fail closed, not wrap to SIZE_MAX";
        expect(eq(unassignedSamples(0UZ, 1UZ), 0UZ));
    };
};

const boost::ut::suite<"gating rule membership"> gatingMembershipTests = [] {
    "a connected message port does not enter the capacity bound"_test = [] {
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     mid  = graph.emplaceBlock<MsgGatedCopy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
        expect(graph.connectPendingEdges());

        gr::MsgPortOut controller;
        expect(controller.connect(mid.control).has_value()) << fatal << "the message port must actually be connected";

        gr::BlockModel& model = *graph.blocks()[1];

        std::size_t streamCapacity  = 0UZ;
        std::size_t messageCapacity = 0UZ;
        for (std::size_t i = 0UZ; i < model.dynamicInputPortsSize(); ++i) {
            auto port = model.dynamicInputPort(i);
            expect(port.has_value()) << fatal;
            expect(port.value()->isConnected()) << "both input ports are connected in this fixture";
            (gr::port::isStream(port.value()->portMaskInfo()) ? streamCapacity : messageCapacity) = port.value()->bufferSize();
        }

        expect(gt(streamCapacity, 0UZ)) << fatal;
        expect(gt(messageCapacity, 0UZ)) << fatal;
        expect(lt(messageCapacity, streamCapacity)) << fatal << "the fixture only bites while the message buffer is the smaller one";

        expect(eq(gatingCapacity(model), streamCapacity)) << "the bound must come from the stream port alone";

        // Sized past the clamp: below a floor of 64 the cap hides the difference entirely.
        constexpr std::size_t kFloor = 128UZ;
        constexpr std::size_t kCap   = 64UZ;
        expect(eq(maxOutstandingJobs(model, kFloor, kCap), std::min(streamCapacity / kFloor, kCap))) << "a message port must not shrink the ring";
    };

    "the capacity bound and the availability query see the same ports"_test = [] {
        // The invariant that would have caught the defect directly: `gatingCapacity()` walks all
        // dynamic input ports, `inputReadiness()` walks the stream-only cache, and the two agree only
        // if the non-stream ones are filtered out.
        gr::Graph graph;
        auto&     src  = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     mid  = graph.emplaceBlock<MsgGatedCopy<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in">(src, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
        expect(graph.connectPendingEdges());

        gr::BlockModel& model = *graph.blocks()[1];

        std::size_t streamInputPorts = 0UZ;
        for (std::size_t i = 0UZ; i < model.dynamicInputPortsSize(); ++i) {
            auto port = model.dynamicInputPort(i);
            if (port.has_value() && gr::port::isStream(port.value()->portMaskInfo())) {
                ++streamInputPorts;
            }
        }

        expect(eq(model.dynamicInputPortsSize(), 2UZ)) << "stream `in` plus message `control`";
        expect(eq(model.blockInputTypes().size(), 1UZ)) << "the stream-only cache sees one";
        expect(eq(streamInputPorts, model.blockInputTypes().size())) << "the two port sets must agree once non-stream ports are filtered";
    };

    "the capacity bound takes the fastest asynchronous port"_test = [] {
        gr::Graph graph;
        auto&     src0 = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     src1 = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     mid  = graph.emplaceBlock<Join2Async<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "in0">(src0, mid).has_value());
        expect(graph.connect<"out", "in1">(src1, mid, {.minBufferSize = 4096UZ}).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
        expect(graph.connectPendingEdges());

        gr::BlockModel& model = *graph.blocks()[2];

        std::size_t largest = 0UZ;
        for (std::size_t i = 0UZ; i < model.dynamicInputPortsSize(); ++i) {
            auto port = model.dynamicInputPort(i);
            expect(port.has_value()) << fatal;
            largest = std::max(largest, port.value()->bufferSize());
        }
        expect(eq(gatingCapacity(model), largest)) << "asynchronous ports combine with max, not min";
    };

    "a mixed block takes the larger of the two groups"_test = [] {
        gr::Graph graph;
        auto&     src0 = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     src1 = graph.emplaceBlock<gr::testing::ConstantSource<float>>();
        auto&     mid  = graph.emplaceBlock<MixedJoin<float>>();
        auto&     sink = graph.emplaceBlock<gr::testing::NullSink<float>>();
        expect(graph.connect<"out", "inSync">(src0, mid, {.minBufferSize = 4096UZ}).has_value());
        expect(graph.connect<"out", "inAsync">(src1, mid).has_value());
        expect(graph.connect<"out", "in">(mid, sink).has_value());
        expect(graph.connectPendingEdges());

        gr::BlockModel& model = *graph.blocks()[2];

        std::size_t syncCapacity  = 0UZ;
        std::size_t asyncCapacity = 0UZ;
        for (std::size_t i = 0UZ; i < model.dynamicInputPortsSize(); ++i) {
            auto port = model.dynamicInputPort(i);
            expect(port.has_value()) << fatal;
            (port.value()->isSynchronous() ? syncCapacity : asyncCapacity) = port.value()->bufferSize();
        }
        expect(neq(syncCapacity, asyncCapacity)) << fatal << "the two groups must differ for this to mean anything";
        expect(eq(gatingCapacity(model), std::max(syncCapacity, asyncCapacity))) << "slowest sync vs fastest async, whichever is larger";
    };
};

int main() { /* tests are statically registered */ }
