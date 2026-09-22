// Smoke test: does a GR4 graph build and run on this box?
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>
#include <print>

int main() {
    using namespace gr;
    Graph graph;
    auto& src  = graph.emplaceBlock<testing::ConstantSource<float>>({{"n_samples_max", gr::Size_t(100000)}});
    auto& sink = graph.emplaceBlock<testing::CountingSink<float>>();
    if (auto r = graph.connect<"out", "in">(src, sink); !r) { std::println(stderr, "connect failed"); return 1; }
    scheduler::Simple<scheduler::ExecutionPolicy::multiThreaded> sched;
    if (auto ret = sched.exchange(std::move(graph)); !ret) { std::println(stderr, "exchange failed"); return 1; }
    const bool ok = sched.runAndWait().has_value();
    std::println("hello: ran {} samples, ok={}", sink.count, ok);
    return ok ? 0 : 1;
}
