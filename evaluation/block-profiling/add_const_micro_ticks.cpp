#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/packet-modem/vector_source.hpp>
#include <gnuradio-4.0/packet-modem/null_sink.hpp>
#include <gnuradio-4.0/packet-modem/add.hpp>
#include <fmt/core.h>
#include <complex>
#include <vector>
int main() {
  using c64 = std::complex<float>;
  const std::size_t N = 2'000'000;
  std::vector<c64> v(N, c64{1.0f, 0.0f});

  gr::Graph fg;
  auto& src  = fg.emplaceBlock<gr::packet_modem::VectorSource<c64>>({{"repeat", false}});
  src.data   = v;
  auto& zero = fg.emplaceBlock<gr::packet_modem::VectorSource<c64>>({{"repeat", true}});
  zero.data  = std::vector<c64>(1024, c64{0.0f, 0.0f});
  auto& add  = fg.emplaceBlock<gr::packet_modem::Add<c64>>();
  auto& sink = fg.emplaceBlock<gr::packet_modem::NullSink<c64>>();
  fg.connect<"out">(src).to<"in0">(add);
  fg.connect<"out">(zero).to<"in1">(add);
  fg.connect<"out">(add).to<"in">(sink);

  add.reset_perf_counters();
  gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded> sched{std::move(fg)};
  auto ret = sched.runAndWait();
  if (!ret.has_value()) return 1;

  const double ticks_total = add.pc_work_time_total();
  const double items = static_cast<double>(N);
  fmt::print("Add: ticks_total={}  ticks/item={}\n", ticks_total, ticks_total/items);
  return 0;
}
