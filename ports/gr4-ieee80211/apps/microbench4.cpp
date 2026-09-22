/*
 * microbench4 -- the scheduler's per-invocation cost under RR, EDF and RM on a
 * synthetic graph that does almost no work: C chains of L gr::testing::Copy
 * blocks between a ConstantSource and a NullSink, every block capped at N
 * samples per call, unthrottled, on T workers.  What remains when the copies
 * are that cheap is the loop around them: the sweep or the release/select
 * bookkeeping, the probes, and the trace markers if a mask is given.
 *
 *   microbench4 --policy rr|edf|rm [--chains C] [--length L] [--batch N] [--samples S]
 *               [--threads T] [--deadlines D0,D1,...] [--trace MASK] [--json]
 *
 * Prints one JSON object: elapsed seconds, invocations (C x (L+2) x S/N) and
 * nanoseconds per invocation.  With --deadlines, chain k's blocks carry an
 * explicit relative deadline Dk (seconds) and a tiny period, as rx_latency4
 * sets them; without, deadlines are left unset (EDF orders by index).
 */
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>
#include <gnuradio-4.0/thread/thread_pool.hpp>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <print>
#include <string>
#include <vector>

namespace {
struct Opt {
    std::string        policy  = "rr";
    unsigned           chains  = 1, length = 9, threads = 1;
    gr::Size_t         batch   = 1024;
    gr::Size_t         samples = 20'000'000;
    std::vector<float> deadlines;
    std::uint32_t      trace = 0;
};

template<typename Sched>
double run(Opt& o, gr::Graph&& graph) {
    Sched sched;
    sched.selection_strategy = gr::scheduler::SelectionStrategy::readyHeap;
    if (auto r = sched.exchange(std::move(graph)); !r) {
        std::println(stderr, "microbench4: exchange failed: {}", r.error().message);
        std::exit(1);
    }
    if (o.trace) {
        std::ignore = sched.settings().set({{"trace_buffer_size", gr::Size_t(1U << 20)}, {"trace_categories", gr::Size_t(o.trace)}});
        std::ignore = sched.settings().activateContext();
        std::ignore = sched.settings().applyStagedParameters();
    }
    const auto t0 = std::chrono::steady_clock::now();
    if (auto r = sched.runAndWait(); !r) {
        std::println(stderr, "microbench4: run failed: {}", r.error().message);
        std::exit(1);
    }
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}
} // namespace

int main(int argc, char** argv) {
    Opt o;
    for (int i = 1; i < argc; i++) {
        const std::string a = argv[i];
        auto next = [&]() -> const char* { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--policy") { o.policy = next(); }
        else if (a == "--chains") { o.chains = static_cast<unsigned>(std::strtoul(next(), nullptr, 10)); }
        else if (a == "--length") { o.length = static_cast<unsigned>(std::strtoul(next(), nullptr, 10)); }
        else if (a == "--batch") { o.batch = static_cast<gr::Size_t>(std::strtoul(next(), nullptr, 10)); }
        else if (a == "--samples") { o.samples = static_cast<gr::Size_t>(std::strtoul(next(), nullptr, 10)); }
        else if (a == "--threads") { o.threads = static_cast<unsigned>(std::strtoul(next(), nullptr, 10)); }
        else if (a == "--trace") { o.trace = static_cast<std::uint32_t>(std::strtoul(next(), nullptr, 0)); }
        else if (a == "--deadlines") {
            std::string s = next(), tok;
            for (char ch : s + ",") { if (ch == ',') { if (!tok.empty()) { o.deadlines.push_back(std::strtof(tok.c_str(), nullptr)); tok.clear(); } } else { tok += ch; } }
        } else { std::println(stderr, "usage: microbench4 --policy rr|edf|rm [--chains C] [--length L] [--batch N] [--samples S] [--threads T] [--deadlines D0,..] [--trace MASK]"); return 2; }
    }
    using namespace gr;
    Graph graph;
    for (unsigned c = 0; c < o.chains; c++) {
        auto tm = [&](property_map m) {
            m.insert_or_assign("max_batch_size", o.batch);
            if (!o.deadlines.empty()) {
                const float d = c < o.deadlines.size() ? o.deadlines[c] : o.deadlines.back();
                m.insert_or_assign("period", 1e-6f);
                m.insert_or_assign("relative_deadline", d);
            }
            return m;
        };
        auto& src = graph.emplaceBlock<testing::ConstantSource<float>>(tm({{"name", std::format("c{}_src", c)}, {"n_samples_max", o.samples}}));
        gr::BlockModel* prev = nullptr;
        std::vector<testing::Copy<float>*> copies;
        for (unsigned l = 0; l < o.length; l++) {
            copies.push_back(&graph.emplaceBlock<testing::Copy<float>>(tm({{"name", std::format("c{}_copy{}", c, l)}})));
        }
        auto& sink = graph.emplaceBlock<testing::NullSink<float>>(tm({{"name", std::format("c{}_sink", c)}}));
        if (copies.empty()) {
            std::ignore = graph.connect<"out", "in">(src, sink);
        } else {
            std::ignore = graph.connect<"out", "in">(src, *copies[0]);
            for (std::size_t l = 1; l < copies.size(); l++) { std::ignore = graph.connect<"out", "in">(*copies[l - 1], *copies[l]); }
            std::ignore = graph.connect<"out", "in">(*copies.back(), sink);
        }
        (void)prev;
    }
    {
        using namespace gr::thread_pool;
        auto cpu = std::make_shared<ThreadPoolWrapper>(std::make_unique<BasicThreadPool>(std::string(kDefaultCpuPoolId), TaskType::CPU_BOUND, o.threads, o.threads), "CPU");
        Manager::instance().replacePool(std::string(kDefaultCpuPoolId), std::move(cpu));
    }
    using namespace gr::scheduler;
    double el;
    if (o.policy == "edf") { el = run<Simple<ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, EdfPolicy>>(o, std::move(graph)); }
    else if (o.policy == "rm") { el = run<Simple<ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, RateMonotonicPolicy>>(o, std::move(graph)); }
    else { el = run<Simple<ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, RoundRobinPolicy>>(o, std::move(graph)); }
    const double inv = static_cast<double>(o.chains) * (o.length + 2.0) * (static_cast<double>(o.samples) / static_cast<double>(o.batch));
    const auto   rs  = gr::trace::ringStats();
    std::println("{{\"policy\": \"{}\", \"chains\": {}, \"length\": {}, \"batch\": {}, \"samples\": {}, \"threads\": {}, \"deadlines\": {}, \"trace_mask\": {}, \"elapsed_s\": {:.4f}, \"invocations\": {:.0f}, \"ns_per_invocation\": {:.1f}, \"ns_per_sample_per_block\": {:.3f}, \"trace_records\": {}}}",
                 o.policy, o.chains, o.length, o.batch, o.samples, o.threads, o.deadlines.empty() ? "null" : std::to_string(o.deadlines.size()), o.trace, el, inv, el * 1e9 / inv, el * 1e9 / (static_cast<double>(o.samples) * o.chains * (o.length + 2.0)), rs.recorded + rs.lost);
    return 0;
}
