#include <gnuradio-4.0/BlockingSync.hpp>
#include <gnuradio-4.0/EarliestDeadlineFirst.hpp>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <format>
#include <limits>
#include <print>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

// Executes a sporadic task set of the kind schedcat generates: every task (C_i, T_i) becomes a wall-clock
// paced chain source -> load -> sink whose job is one fixed chunk costing ~C_i, released every T_i, with the
// implicit deadline D_i = T_i. Priorities for the fixed-priority run are rate-monotonic. Misses are counted
// per job at the sink, identically under every policy, so measured schedulability can be compared directly
// against analytical predictions (QPA, processor-demand, response-time analysis) for the same task set.

namespace {

using SteadyClock = std::chrono::steady_clock;

constexpr std::uint64_t kChunk = 256UZ; // samples per job, identical for every task

[[nodiscard]] std::int64_t nowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now().time_since_epoch()).count(); }

struct NominalTimeSource : gr::Block<NominalTimeSource>, gr::BlockingSync<NominalTimeSource, SteadyClock> {
    gr::PortOut<std::int64_t> out;

    gr::Annotated<float, "sample_rate">        sample_rate         = 25'600.f;
    gr::Annotated<gr::Size_t, "chunk_size">    chunk_size          = static_cast<gr::Size_t>(kChunk);
    gr::Annotated<bool, "use_internal_thread"> use_internal_thread = true;

    GR_MAKE_REFLECTABLE(NominalTimeSource, out, sample_rate, chunk_size, use_internal_thread);

    std::uint64_t _nChunksProduced = 0UZ;

    void start() { this->blockingSyncStart(); }
    void stop() { this->blockingSyncStop(); }

    gr::work::Status processBulk(gr::OutputSpanLike auto& outSpan) {
        const double        elapsedSeconds = std::chrono::duration<double>(SteadyClock::now() - this->blockingSyncStartTime()).count();
        const auto          chunk          = static_cast<std::uint64_t>(chunk_size.value);
        const auto          owedChunks     = static_cast<std::uint64_t>(elapsedSeconds * static_cast<double>(sample_rate)) / chunk;
        const std::uint64_t nChunks        = std::min(owedChunks - std::min(owedChunks, _nChunksProduced), static_cast<std::uint64_t>(outSpan.size()) / chunk);
        if (nChunks == 0UZ) {
            outSpan.publish(0UZ);
            return gr::work::Status::INSUFFICIENT_INPUT_ITEMS;
        }

        const std::int64_t anchorNs   = std::chrono::duration_cast<std::chrono::nanoseconds>(this->blockingSyncStartTime().time_since_epoch()).count();
        const double       nsPerChunk = 1e9 * static_cast<double>(chunk) / static_cast<double>(sample_rate);
        for (std::uint64_t chunkIndex = 0UZ; chunkIndex < nChunks; ++chunkIndex) {
            const std::int64_t boundaryNs = anchorNs + static_cast<std::int64_t>(static_cast<double>(_nChunksProduced + chunkIndex + 1UZ) * nsPerChunk);
            for (std::uint64_t sample = 0UZ; sample < chunk; ++sample) {
                outSpan[chunkIndex * chunk + sample] = boundaryNs;
            }
        }
        outSpan.publish(nChunks * chunk);
        _nChunksProduced += nChunks;
        return gr::work::Status::OK;
    }
};

struct BusyWork : gr::Block<BusyWork> {
    gr::PortIn<std::int64_t>  in;
    gr::PortOut<std::int64_t> out;

    gr::Annotated<gr::Size_t, "ops_per_sample"> ops_per_sample = 16U;

    GR_MAKE_REFLECTABLE(BusyWork, in, out, ops_per_sample);

    float _sink = 0.f; // keeps the dependent chain observable so the loop cannot be elided

    [[nodiscard]] std::int64_t processOne(std::int64_t sample) noexcept {
        float value = _sink + 1.f;
        for (gr::Size_t op = 0U; op < ops_per_sample; op++) {
            value = value * 1.0000001f + 1e-9f;
        }
        _sink = value;
        return sample;
    }
};

struct LatencySink : gr::Block<LatencySink> {
    gr::PortIn<std::int64_t> in;

    GR_MAKE_REFLECTABLE(LatencySink, in);

    std::vector<std::int64_t> latenciesNs;

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan) {
        const std::int64_t arrival = nowNs();
        for (const std::int64_t nominal : inSpan) {
            latenciesNs.push_back(arrival - nominal);
        }
        return gr::work::Status::OK;
    }
};

// the identical dependent-chain kernel as BusyWork::processOne, timed to convert C_i into ops_per_sample
[[nodiscard]] double calibrateNsPerOp() {
    constexpr std::uint64_t kOps = 40'000'000UZ;
    float                   value = 1.f;
    const auto              start = SteadyClock::now();
    for (std::uint64_t op = 0UZ; op < kOps; op++) {
        value = value * 1.0000001f + 1e-9f;
    }
    const auto stop = SteadyClock::now();
    // fold the result into observable state so the loop survives optimisation
    if (value < 0.f) {
        std::println("");
    }
    return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count()) / static_cast<double>(kOps);
}

struct TaskSpec {
    std::int64_t periodNs = 0;
    std::int64_t costNs   = 0;
};

struct TaskChain {
    LatencySink* sink   = nullptr;
    TaskSpec     spec{};
    gr::Size_t   ops    = 1U;
};

[[nodiscard]] std::int64_t readInt(std::string_view text, std::string_view name) {
    std::int64_t parsed = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (error != std::errc{} || end != text.data() + text.size() || parsed <= 0) {
        throw std::invalid_argument(std::format("{} must be a positive integer (got '{}')", name, text));
    }
    return parsed;
}

struct BuiltGraph {
    gr::Graph              flow;
    std::vector<TaskChain> chains;
};

[[nodiscard]] BuiltGraph makeGraph(const std::vector<TaskSpec>& specs, double nsPerOp) {
    using namespace gr::scheduler;
    BuiltGraph built;

    // rate-monotonic ranks: shorter period = smaller gr:priority = more urgent
    std::vector<std::size_t> rank(specs.size());
    std::iota(rank.begin(), rank.end(), 0UZ);
    std::ranges::sort(rank, [&specs](std::size_t a, std::size_t b) { return specs[a].periodNs < specs[b].periodNs; });
    std::vector<std::int64_t> priority(specs.size());
    for (std::size_t position = 0UZ; position < rank.size(); ++position) {
        priority[rank[position]] = static_cast<std::int64_t>(position);
    }

    for (std::size_t index = 0UZ; index < specs.size(); ++index) {
        const TaskSpec& spec = specs[index];
        const auto rate      = static_cast<float>(1e9 * static_cast<double>(kChunk) / static_cast<double>(spec.periodNs));
        const auto ops       = static_cast<gr::Size_t>(std::max(1.0, std::round(static_cast<double>(spec.costNs) / (static_cast<double>(kChunk) * nsPerOp))));

        auto& source = built.flow.emplaceBlock<NominalTimeSource>({{"name", std::format("src{}", index)}, {"sample_rate", rate}});
        auto& load   = built.flow.emplaceBlock<BusyWork>({{"name", std::format("load{}", index)}, {"ops_per_sample", ops}});
        auto& sink   = built.flow.emplaceBlock<LatencySink>({{"name", std::format("sink{}", index)}});
        sink.latenciesNs.reserve(1UZ << 20);

        for (auto* annotated : std::initializer_list<gr::property_map*>{
                 std::addressof(source.meta_information.value), std::addressof(load.meta_information.value), std::addressof(sink.meta_information.value)}) {
            (*annotated)[std::string(kBatchSizeKey)] = kChunk;
            (*annotated)[std::string(kPeriodKey)]    = static_cast<std::uint64_t>(spec.periodNs);
            (*annotated)[std::string(kDeadlineKey)]  = static_cast<std::uint64_t>(spec.periodNs); // implicit deadline
            (*annotated)[std::string(kPriorityKey)]  = priority[index];
        }

        constexpr gr::Size_t kBuffer = static_cast<gr::Size_t>(kChunk) * 32U;
        if (!built.flow.connect<"out", "in">(source, load, {.minBufferSize = kBuffer}).has_value() || //
            !built.flow.connect<"out", "in">(load, sink, {.minBufferSize = kBuffer}).has_value()) {
            throw std::runtime_error("failed to connect task chain");
        }
        built.chains.push_back(TaskChain{.sink = std::addressof(sink), .spec = spec, .ops = ops});
    }
    return built;
}

template<typename TScheduler>
void runOnce(std::string_view policy, const std::vector<TaskSpec>& specs, double seconds, double nsPerOp) {
    BuiltGraph built = makeGraph(specs, nsPerOp);
    TScheduler scheduler({{"max_work_items", static_cast<std::size_t>(kChunk)}});
    if (auto exchanged = scheduler.exchange(std::move(built.flow)); !exchanged.has_value()) {
        throw std::runtime_error(std::format("scheduler exchange failed: {}", exchanged.error()));
    }

    std::jthread stopper([&scheduler, seconds] {
        while (scheduler.state() != gr::lifecycle::State::RUNNING) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
        scheduler.requestStop();
    });

    if (auto result = scheduler.runAndWait(); !result.has_value()) {
        throw std::runtime_error(std::format("scheduler run failed: {}", result.error()));
    }
    if constexpr (requires(const TScheduler& instance) { instance.statistics(); }) {
        const auto statistics = scheduler.statistics();
        std::println("# sched,{},dispatches={},selections={},sweeps={},withdrawn={},probes={},misses={}", policy, statistics.nDispatches, statistics.nSelections, statistics.nSweeps, statistics.nWithdrawn, statistics.nProbes, statistics.nMisses);
    }

    // per-job response = the latest arrival among the job's chunk of samples; a job misses when it exceeds
    // the implicit deadline. The trailing partial job (cut off by requestStop) is dropped.
    for (std::size_t index = 0UZ; index < built.chains.size(); ++index) {
        const TaskChain& chain = built.chains[index];
        const std::vector<std::int64_t>& lat = chain.sink->latenciesNs;
        const std::size_t nJobs = lat.size() / kChunk;
        std::size_t nMissed = 0UZ;
        std::int64_t maxResponse = 0;
        for (std::size_t job = 0UZ; job < nJobs; ++job) {
            const auto begin = lat.begin() + static_cast<std::ptrdiff_t>(job * kChunk);
            const std::int64_t response = *std::max_element(begin, begin + static_cast<std::ptrdiff_t>(kChunk));
            maxResponse = std::max(maxResponse, response);
            if (response > chain.spec.periodNs) {
                nMissed++;
            }
        }
        std::println("task,{},{},{},{},{},{},{},{:.1f}", policy, index, chain.spec.periodNs, chain.spec.costNs, chain.ops, nJobs, nMissed, static_cast<double>(maxResponse) / 1e3);
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 4) {
        std::println("usage: {} <rr|edf|fp> <seconds> <period_ns:cost_ns> [<period_ns:cost_ns> ...]", argv[0]);
        return 1;
    }
    const std::string_view policy(argv[1]);
    const auto             seconds = static_cast<double>(readInt(argv[2], "seconds"));

    std::vector<TaskSpec> specs;
    for (int arg = 3; arg < argc; ++arg) {
        const std::string_view pair(argv[arg]);
        const std::size_t      colon = pair.find(':');
        if (colon == std::string_view::npos) {
            throw std::invalid_argument("task spec must be <period_ns:cost_ns>");
        }
        specs.push_back(TaskSpec{.periodNs = readInt(pair.substr(0, colon), "period_ns"), .costNs = readInt(pair.substr(colon + 1), "cost_ns")});
    }

    const double nsPerOp = calibrateNsPerOp();
    std::println("# ns_per_op={:.4f}, chunk={}, tasks={}, seconds={}", nsPerOp, kChunk, specs.size(), seconds);
    std::println("row,policy,task,period_ns,cost_ns,ops,jobs,missed,max_response_us");

    if (policy == "rr") {
        runOnce<gr::scheduler::Simple<>>("rr", specs, seconds, nsPerOp);
    } else if (policy == "edf") {
        runOnce<gr::scheduler::EarliestDeadlineFirst<>>("edf", specs, seconds, nsPerOp);
    } else if (policy == "fp") {
        runOnce<gr::scheduler::FixedPriority<>>("fp", specs, seconds, nsPerOp);
    } else {
        throw std::invalid_argument("policy must be rr, edf, or fp");
    }
}
