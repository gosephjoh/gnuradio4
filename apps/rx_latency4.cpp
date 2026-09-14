/*
 * rx_latency4 -- the GR4 twin of the GR3 project's rx_latency (Phase 10).
 *
 *   for k in 0..chains-1:
 *     file_source(--input) -> throttle(--rate, --chunk) -> wifi rx chain -> latency_sink
 *                                        \-> arrival_stamper
 *
 * Reads the GR3 run directory's manifest.json (frame offsets, PHY parameters)
 * and payloads.bin (to check every decoded payload byte for byte), replays
 * the same rx_stimulus.cf32, and writes latency.csv and latency_summary.json
 * in the GR3 formats plus a `correct` column.  --record also writes
 * rx_pdus.bin / rx_log.csv / rx_symbols.cf32 / rx_symbols.csv for chain 0 so
 * the GR3 `compare --rx-only` tool can judge the port against the fixture.
 *
 * Never modifies the run directory: the GR3 tree is read-only reference.
 */
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>

#include "gr4ieee80211/chain.hpp"

#include <nlohmann/json.hpp>

#include <getopt.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <print>
#include <thread>
#include <vector>

using json = nlohmann::ordered_json;
using namespace gr4wifi;

namespace {

struct Options {
    std::string run_dir, input, out_dir;
    double      rate        = 10e6;
    unsigned    chunk       = 4096;
    int         chains      = 1;
    double      deadline_ms = 100.0;
    double      timeout_s   = 300.0;
    bool        record      = false;
    bool        check       = true;
};

void usage() {
    std::println(stderr,
        "usage: rx_latency4 --run-dir DIR [options]\n"
        "  --run-dir DIR       GR3 run directory (manifest.json + payloads.bin)\n"
        "  --input PATH        stimulus; default <run-dir>/rx_stimulus.cf32\n"
        "  --out-dir DIR       outputs; default = run-dir\n"
        "  --rate SPS          throttle, default 10e6; 0 = unthrottled\n"
        "  --chunk N           throttle items per wake-up, default 4096\n"
        "  --chains N          concurrent receivers on the same file, default 1\n"
        "  --deadline-ms F     default 100\n"
        "  --timeout-s S       default 300\n"
        "  --record            write rx_pdus.bin/rx_log.csv/rx_symbols.* (chain 0) for compare\n"
        "  --no-check          do not compare decoded payloads against payloads.bin");
}

double percentile(const std::vector<double>& sorted, double q) {
    if (sorted.empty()) {
        return std::nan("");
    }
    long idx = static_cast<long>(std::ceil(q * static_cast<double>(sorted.size()))) - 1;
    idx      = std::max(0L, std::min(idx, static_cast<long>(sorted.size()) - 1));
    return sorted[static_cast<std::size_t>(idx)];
}

} // namespace

int main(int argc, char** argv) {
    Options opt;
    static const struct option kOpts[] = {{"run-dir", required_argument, nullptr, 1}, {"input", required_argument, nullptr, 2}, {"out-dir", required_argument, nullptr, 3}, {"rate", required_argument, nullptr, 4}, {"chunk", required_argument, nullptr, 5}, {"chains", required_argument, nullptr, 6}, {"deadline-ms", required_argument, nullptr, 7}, {"timeout-s", required_argument, nullptr, 8}, {"record", no_argument, nullptr, 9}, {"no-check", no_argument, nullptr, 10}, {"help", no_argument, nullptr, 'h'}, {nullptr, 0, nullptr, 0}};
    int c;
    while ((c = getopt_long(argc, argv, "h", kOpts, nullptr)) != -1) {
        switch (c) {
        case 1: opt.run_dir = optarg; break;
        case 2: opt.input = optarg; break;
        case 3: opt.out_dir = optarg; break;
        case 4: opt.rate = std::atof(optarg); break;
        case 5: opt.chunk = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 6: opt.chains = std::atoi(optarg); break;
        case 7: opt.deadline_ms = std::atof(optarg); break;
        case 8: opt.timeout_s = std::atof(optarg); break;
        case 9: opt.record = true; break;
        case 10: opt.check = false; break;
        default: usage(); return 2;
        }
    }
    if (opt.run_dir.empty() || opt.chains < 1 || opt.rate < 0 || opt.chunk < 1) {
        usage();
        return 2;
    }
    if (opt.out_dir.empty()) {
        opt.out_dir = opt.run_dir;
    }
    if (opt.input.empty()) {
        opt.input = opt.run_dir + "/rx_stimulus.cf32";
    }
    if (!std::filesystem::exists(opt.input)) {
        std::println(stderr, "rx_latency4: {} does not exist", opt.input);
        return 2;
    }
    std::filesystem::create_directories(opt.out_dir);

    // ---- manifest + payloads -------------------------------------------
    json manifest;
    try {
        std::ifstream f(opt.run_dir + "/manifest.json");
        f >> manifest;
    } catch (const std::exception& e) {
        std::println(stderr, "rx_latency4: cannot read {}/manifest.json: {}", opt.run_dir, e.what());
        return 2;
    }
    const auto& phy = manifest.at("phy");
    const long pad_front = phy.value("pad_front", 0L), pad_tail = phy.value("pad_tail", 0L);
    const double frequency = phy.value("frequency_hz", 5.89e9), bandwidth = phy.value("bandwidth_hz", 10e6), sensitivity = phy.value("sensitivity", 0.56);
    std::vector<uint64_t> first, last;
    std::vector<uint32_t> lengths;
    std::vector<uint64_t> offsets;
    bool                  order_mode = false; // every seq null -> pair by order (GR3 payload_checker)
    for (const auto& fr : manifest.at("frames")) {
        const uint64_t so = fr.at("sample_offset").get<uint64_t>(), ns = fr.at("samples").get<uint64_t>();
        first.push_back(so + static_cast<uint64_t>(pad_front));
        last.push_back(so + ns - static_cast<uint64_t>(pad_tail) - 1);
        lengths.push_back(fr.at("length").get<uint32_t>());
        offsets.push_back(fr.at("offset").get<uint64_t>());
        const long seq = fr.at("seq").is_null() ? -1L : fr.at("seq").get<long>();
        if (seq < 0) {
            order_mode = true;
        } else if (seq != static_cast<long>(first.size() - 1)) {
            std::println(stderr, "rx_latency4: frame {} has seq {}; pairing needs seq == index", first.size() - 1, seq);
            return 2;
        }
    }
    const std::size_t frames = first.size();
    if (order_mode) {
        for (const auto& fr : manifest.at("frames")) {
            if (!fr.at("seq").is_null()) {
                std::println(stderr, "rx_latency4: mixed null and numbered seq -- cannot pair (GR3 refuses this too)");
                return 2;
            }
        }
        std::println(stderr, "rx_latency4: no frame carries a sequence number -- pairing by order");
    }

    // payloads.bin: GR3PAYL1, u32 frame_count, u32 seq_width, then bytes at frames[].offset
    std::vector<uint8_t> payloads;
    if (opt.check) {
        std::ifstream pf(opt.run_dir + "/payloads.bin", std::ios::binary);
        std::vector<uint8_t> all((std::istreambuf_iterator<char>(pf)), std::istreambuf_iterator<char>());
        if (all.size() < 16 || std::string(all.begin(), all.begin() + 8) != "GR3PAYL1") {
            std::println(stderr, "rx_latency4: {}/payloads.bin missing or bad magic; use --no-check", opt.run_dir);
            return 2;
        }
        payloads.assign(all.begin() + 16, all.end());
    }

    // ---- graph ------------------------------------------------------------
    gr::Graph                 graph;
    std::vector<ChainBlocks> chains;
    for (int k = 0; k < opt.chains; k++) {
        ChainConfig cfg;
        cfg.input       = opt.input;
        cfg.rate        = opt.rate;
        cfg.chunk       = opt.chunk;
        cfg.frequency   = frequency;
        cfg.bandwidth   = bandwidth;
        cfg.sensitivity = sensitivity;
        cfg.record      = opt.record && k == 0;
        cfg.out_dir     = opt.out_dir;
        cfg.prefix      = "c" + std::to_string(k) + "_";
        ChainBlocks cb  = buildChain(graph, cfg);
        cb.stamper->setFrames(first, last);
        cb.sink->setFrames(frames);
        cb.sink->_order_mode = order_mode;
        if (opt.check) {
            for (std::size_t i = 0; i < frames; i++) {
                cb.sink->_expected[i]     = payloads.data() + offsets[i];
                cb.sink->_expected_len[i] = lengths[i];
            }
        }
        chains.push_back(std::move(cb));
    }
    const unsigned hw_threads = std::thread::hardware_concurrency();

    std::println(stderr, "rx_latency4: replaying {} ({} frames) at {}, {} chain(s) x {} blocks, deadline {} ms, scheduler multiThreaded on {} hw threads", opt.input, frames, opt.rate > 0 ? std::format("{} Msample/s chunk {}", opt.rate / 1e6, opt.chunk) : std::string("unthrottled"), opt.chains, chains[0].block_count, opt.deadline_ms, hw_threads);

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> sched;
    if (auto r = sched.exchange(std::move(graph)); !r) {
        std::println(stderr, "rx_latency4: scheduler exchange failed: {}", r.error().message);
        return 1;
    }
    std::atomic<bool> timed_out{false}, finished{false};
    std::thread watchdog([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::duration<double>(opt.timeout_s);
        while (!finished.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (!finished.load()) {
            timed_out = true;
            sched.requestStop();
        }
    });
    const auto t0 = std::chrono::steady_clock::now();
    const bool ok = sched.runAndWait().has_value() && !timed_out.load();
    const double elapsed_s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    finished = true;
    watchdog.join();

    // ---- latency.csv + latency_summary.json ------------------------------
    const std::string csv_path = opt.out_dir + "/latency.csv";
    std::FILE* csv = std::fopen(csv_path.c_str(), "w");
    if (!csv) {
        std::println(stderr, "rx_latency4: cannot open {}", csv_path);
        return 1;
    }
    std::fprintf(csv, "chain,seq,length,t_first_ns,t_last_ns,t_decode_ns,lat_first_us,lat_last_us,decoded,correct\n");
    json per_chain = json::array();
    uint64_t decoded_total = 0, missing_total = 0, misses_total = 0, wrong_total = 0;
    const double deadline_us = opt.deadline_ms * 1000.0;
    for (std::size_t k = 0; k < chains.size(); k++) {
        const ChainBlocks& cb = chains[k];
        std::vector<double> lat;
        json                missing = json::array(), wrong = json::array();
        uint64_t            missing_n = 0, misses = 0, unstamped = 0;
        double              first_us = std::nan("");
        for (std::size_t i = 0; i < frames; i++) {
            const uint64_t tf = cb.stamper->_t_first[i], tl = cb.stamper->_t_last[i], td = cb.sink->_t_decode[i];
            if (tf == 0 || tl == 0) {
                ++unstamped;
            }
            if (td == 0) {
                ++missing_n;
                if (missing.size() < 50) {
                    missing.push_back(i);
                }
                std::fprintf(csv, "%zu,%zu,%u,%llu,%llu,,,,0,\n", k, i, lengths[i], static_cast<unsigned long long>(tf), static_cast<unsigned long long>(tl));
                continue;
            }
            const double lf = (static_cast<double>(td) - static_cast<double>(tf)) / 1e3;
            const double ll = (static_cast<double>(td) - static_cast<double>(tl)) / 1e3;
            if (std::isnan(first_us)) {
                first_us = ll;
            }
            lat.push_back(ll);
            if (ll > deadline_us) {
                ++misses;
            }
            const int correct = opt.check ? cb.sink->_correct[i] : -1;
            if (opt.check && !correct && wrong.size() < 50) {
                wrong.push_back(i);
            }
            std::fprintf(csv, "%zu,%zu,%u,%llu,%llu,%llu,%.3f,%.3f,1,%s\n", k, i, lengths[i], static_cast<unsigned long long>(tf), static_cast<unsigned long long>(tl), static_cast<unsigned long long>(td), lf, ll, correct < 0 ? "" : (correct ? "1" : "0"));
        }
        std::sort(lat.begin(), lat.end());
        const double p50 = percentile(lat, 0.50), p95 = percentile(lat, 0.95), p99 = percentile(lat, 0.99);
        const double mx = lat.empty() ? std::nan("") : lat.back(), mn = lat.empty() ? std::nan("") : lat.front();
        const ChainCounters cc = cb.counters();
        json s = {{"chain", k}, {"frames", frames}, {"decoded", cb.sink->_count}, {"missing", missing_n}, {"missing_seqs", missing}, {"duplicates", cb.sink->_duplicates}, {"unpairable", cb.sink->_unpairable}, {"wrong_payload", opt.check ? json(cb.sink->_wrong_payload) : json(nullptr)}, {"wrong_seqs", wrong}, {"unstamped", unstamped}, {"samples_seen", cb.stamper->_items}, {"first_us", first_us}, {"min_us", mn}, {"p50_us", p50}, {"p95_us", p95}, {"p99_us", p99}, {"max_us", mx}, {"deadline_misses", misses}, {"counts", {{"sync_short_detections", cc.sync_short_detections}, {"sync_long_frames", cc.sync_long_frames}, {"signal_ok", cc.signal_ok}, {"signal_bad", cc.signal_bad}, {"frames_started", cc.frames_started}, {"frames_decoded", cc.frames_decoded}, {"crc_failed", cc.crc_failed}, {"too_large", cc.too_large}}}};
        per_chain.push_back(s);
        decoded_total += cb.sink->_count;
        missing_total += missing_n;
        misses_total += misses;
        wrong_total += cb.sink->_wrong_payload;
        std::println(stderr, "rx_latency4: chain {} -- {}/{} decoded, {} missing, {} dup, {} unpairable, {} wrong payload; last->decode us: first {:.1f} p50 {:.1f} p95 {:.1f} p99 {:.1f} max {:.1f}; {} over {} ms; sync_short {} sync_long {} signal ok/bad {}/{} crc failed {}", k, cb.sink->_count, frames, missing_n, cb.sink->_duplicates, cb.sink->_unpairable, cb.sink->_wrong_payload, first_us, p50, p95, p99, mx, misses, opt.deadline_ms, cc.sync_short_detections, cc.sync_long_frames, cc.signal_ok, cc.signal_bad, cc.crc_failed);
    }
    std::fclose(csv);

    json summary = {{"app", "rx_latency4"}, {"runtime", "gnuradio4"}, {"scheduler", "Simple<multiThreaded>"}, {"hw_threads", hw_threads}, {"chains", opt.chains}, {"order_mode", order_mode}, {"blocks_per_chain", chains[0].block_count}, {"frames", frames}, {"decoded_total", decoded_total}, {"missing_total", missing_total}, {"wrong_payload_total", opt.check ? json(wrong_total) : json(nullptr)}, {"deadline_misses_total", misses_total}, {"elapsed_s", elapsed_s}, {"latency", {{"rate", opt.rate}, {"chunk", opt.chunk}, {"deadline_ms", opt.deadline_ms}, {"stamp_bias_max_us", opt.rate > 0 ? opt.chunk / opt.rate * 1e6 : 0.0}, {"input", opt.input}}}, {"result", ok ? "ok" : (timed_out.load() ? "timeout" : "error")}, {"per_chain", per_chain}};
    {
        std::ofstream f(opt.out_dir + "/latency_summary.json");
        f << summary.dump(2) << "\n";
    }
    std::println(stderr, "rx_latency4: {:.3f} s; wrote {} and {}/latency_summary.json", elapsed_s, csv_path, opt.out_dir);
    if (timed_out.load()) {
        std::println(stderr, "rx_latency4: TIMEOUT after {} s", opt.timeout_s);
        return 1;
    }
    return ok ? 0 : 1;
}
