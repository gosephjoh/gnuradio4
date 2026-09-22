// probe -- diagnostic: which block of the chain does not run?
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/fileio/BasicFileIo.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>
#include "gr4ieee80211/chain.hpp"
#include "gr4ieee80211/FileSourceRaw.hpp"
#include "gr4ieee80211/basic_blocks.hpp"

#include <print>
#include <thread>
#include <atomic>

using namespace gr4wifi;
int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "fsrc";
    const std::string file = argc > 2 ? argv[2] : "/home/gr3/gr3-ieee802-11-project/data/phase10/smoke/rx_stimulus.cf32";
    gr::Graph g;
    gr::testing::CountingSink<cf>* cnt = nullptr;
    gr::testing::CountingSink<float>* fcnt = nullptr;
    ArrivalStamper* st = nullptr;
    ChainBlocks cb;
    if (mode == "fsrc") {
        auto& src = g.emplaceBlock<gr::blocks::fileio::BasicFileSource<cf>>({{"file_name", file}, {"repeat", false}, {"trigger_name", std::string("")}});
        auto& c = g.emplaceBlock<gr::testing::CountingSink<cf>>();
        cnt = &c;
        if (auto r = g.connect<"out", "in">(src, c); !r) { std::println("connect failed {}", r.error().message); return 1; }
    } else if (mode == "raw") {
        auto& src = g.emplaceBlock<FileSourceRaw<cf>>({{"file_name", file}});
        auto& c = g.emplaceBlock<gr::testing::CountingSink<cf>>();
        cnt = &c;
        if (auto r = g.connect<"out", "in">(src, c); !r) { std::println("connect failed {}", r.error().message); return 1; }
    } else if (mode == "m1" || mode == "m2" || mode == "m3" || mode == "m4" || mode == "m5") {
        auto& src = g.emplaceBlock<FileSourceRaw<cf>>({{"file_name", file}});
        if (mode == "m1") {  // src -> mag2 -> count
            auto& b = g.emplaceBlock<MagSquared>(); auto& c = g.emplaceBlock<gr::testing::CountingSink<float>>();
            std::ignore = g.connect<"out", "in">(src, b); std::ignore = g.connect<"out", "in">(b, c); fcnt = &c;
        } else if (mode == "m2") { // src -> dly16 -> count
            auto& b = g.emplaceBlock<SampleDelay<cf>>({{"delay", gr::Size_t(16)}}); auto& c = g.emplaceBlock<gr::testing::CountingSink<cf>>();
            std::ignore = g.connect<"out", "in">(src, b); std::ignore = g.connect<"out", "in">(b, c); cnt = &c;
        } else if (mode == "m3") { // src -> mag2 -> avgpow -> count
            auto& b = g.emplaceBlock<MagSquared>(); auto& a = g.emplaceBlock<MovingAverage<float>>({{"length", gr::Size_t(64)}}); auto& c = g.emplaceBlock<gr::testing::CountingSink<float>>();
            std::ignore = g.connect<"out", "in">(src, b); std::ignore = g.connect<"out", "in">(b, a); std::ignore = g.connect<"out", "in">(a, c); fcnt = &c;
        } else if (mode == "m4") { // src -> mult.in#0 ; src -> conj -> mult.in#1 ; mult -> count
            auto& cj = g.emplaceBlock<Conjugate>(); auto& m = g.emplaceBlock<Multiply2<cf>>(); auto& c = g.emplaceBlock<gr::testing::CountingSink<cf>>();
            std::ignore = g.connect<"out", "in0">(src, m); std::ignore = g.connect<"out", "in">(src, cj); std::ignore = g.connect<"out", "in1">(cj, m); std::ignore = g.connect<"out", "in">(m, c); cnt = &c;
        } else { // m5: src -> mag2 -> count ; src -> stamper  (fan-out of the source)
            auto& b = g.emplaceBlock<MagSquared>(); auto& c = g.emplaceBlock<gr::testing::CountingSink<float>>(); auto& s = g.emplaceBlock<ArrivalStamper>(); s.setFrames({100}, {1000}); st = &s;
            std::ignore = g.connect<"out", "in">(src, b); std::ignore = g.connect<"out", "in">(b, c); std::ignore = g.connect<"out", "in">(src, s); fcnt = &c;
        }
    } else if (mode == "stamp") {
        auto& src = g.emplaceBlock<gr::blocks::fileio::BasicFileSource<cf>>({{"file_name", file}, {"repeat", false}, {"trigger_name", std::string("")}});
        auto& s = g.emplaceBlock<ArrivalStamper>();
        s.setFrames({100, 2000}, {1000, 3000});
        st = &s;
        if (auto r = g.connect<"out", "in">(src, s); !r) { std::println("connect failed {}", r.error().message); return 1; }
    } else {
        ChainConfig cfg; cfg.input = file; cfg.rate = (mode == "full10") ? 10e6 : 0; cfg.prefix = "c0_";
        cb = buildChain(g, cfg);
        cb.stamper->setFrames({100, 2000}, {1000, 3000});
        cb.sink->setFrames(200);
        st = cb.stamper;
    }
    const bool single = argc > 3 && std::string(argv[3]) == "single";
    auto run = [&](auto& sched) {
        if (auto r = sched.exchange(std::move(g)); !r) { std::println("exchange failed {}", r.error().message); return; }
        std::atomic<bool> done{false};
        std::thread wd([&] { for (int i = 0; i < 100 && !done; i++) std::this_thread::sleep_for(std::chrono::milliseconds(100)); if (!done) { std::println("probe: timeout, requesting stop"); sched.requestStop(); } });
        auto ok = sched.runAndWait();
        done = true; wd.join();
        std::println("runAndWait ok={} single={}", ok.has_value(), single);
        for (const auto& b : sched.graph().blocks()) {
            std::println("  block {:<28} state {}", b->name(), gr::meta::enumName(b->state()).value_or("?"));
        }
    };
    if (single) { gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded> sched; run(sched); }
    else { gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> sched; run(sched); }
    if (cnt) std::println("counting sink: {}", cnt->count);
    if (fcnt) std::println("counting sink(float): {}", fcnt->count);
    if (st) std::println("stamper items: {} t_first[0]={} t_last[1]={}", st->_items, st->_t_first[0], st->_t_last[1]);
    if (cb.counters) { auto c = cb.counters(); std::println("ss {} sl {} sig {}/{} dec {}/{} crc {}", c.sync_short_detections, c.sync_long_frames, c.signal_ok, c.signal_bad, c.frames_started, c.frames_decoded, c.crc_failed);
      std::println("items: avgpow {} dly16 {} avgcor {} ss {} (calls {}, max_cor {}) dly320 {} sl {} (calls {}) fft {} eq {} dec {}", c.items_avgpow, c.items_dly16, c.items_avgcor, c.items_ss, c.calls_ss, c.max_cor, c.items_dly320, c.items_sl, c.calls_sl, c.items_fft, c.items_eq, c.items_dec); }
    return 0;
}
