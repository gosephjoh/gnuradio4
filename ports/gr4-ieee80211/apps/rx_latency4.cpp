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
 *
 * Decision 0036 additions (batch response time under RR / EDF / RM):
 *   --policy rr|edf|rm      the scheduling policy (a template argument of the
 *                           GR4 scheduler: RoundRobinPolicy, EdfPolicy,
 *                           RateMonotonicPolicy -- no scheduler code changed)
 *   --fixed-batch N         every pre-gate block takes exactly N samples per
 *                           call, the throttle publishes whole N-chunks, the
 *                           edge buffers default to 16 N; implies --batch N
 *                           --chunk N
 *   --trace-categories M    GR4 trace-marker mask ("all" = every category);
 *   --trace-buffer R        records per emitting thread; --trace-out PATH
 *                           dumps the capture after the run; trace_meta.json
 *                           beside it maps GR4's block names to our roles
 *   --sched-ratio N, --selection scan|heap, --max-outstanding-jobs N
 *                           the scheduler settings 0036 names
 */
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>
#include <gnuradio-4.0/thread/thread_pool.hpp>

#include "gr4ieee80211/chain.hpp"

#include <nlohmann/json.hpp>

#include <getopt.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
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
    std::size_t buffer      = 0;
    bool        catch_up    = false;
    bool        single      = false;
    unsigned    threads     = 0;   // CPU pool size; 0 = GR4 default (hardware_concurrency)
    unsigned    batch       = 0;
    // decision 0036
    std::string policy      = "rr";
    unsigned    fixed_batch = 0;
    float       tiny_period = 1e-6f; // on the source and throttle in fixed-batch mode
    uint32_t    trace_mask  = 0;
    std::size_t trace_buffer = 0;    // records per thread; 0 = GR4 default (65536)
    std::size_t trace_limit_mb = 0;  // ring ceiling per thread; 0 = GR4 default (4 GiB)
    std::string trace_out;
    unsigned    sched_ratio = 0;     // process_stream_to_message_ratio; 0 = default (16)
    std::string selection   = "heap";
    unsigned    max_outstanding_jobs = 0; // 0 = default (64)
    uint64_t    max_samples = 0;     // replay only the first M samples (0 = whole file)
};

uint32_t parseMask(const char* a) {
    const std::string s(a);
    if (s == "all") { return gr::trace::kAllCategories; }
    if (s == "none" || s == "0") { return 0; }
    return static_cast<uint32_t>(std::strtoul(a, nullptr, 0)); // 0x.. or decimal
}

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
        "  --no-check          do not compare decoded payloads against payloads.bin\n"
        "  --buffer N          edge buffer size in items (default GR4's 65536)\n"
        "  --catch-up          throttle releases the whole backlog when behind\n"
        "  --single            single-threaded scheduler\n"
        "  --threads N         CPU thread-pool size (default: hardware threads)\n"
        "  --batch N           max_batch_size on every block (default 0 = unbounded)\n"
        "decision 0036 -- batch response time under a scheduling policy:\n"
        "  --policy P          rr (default, GR4's RoundRobinPolicy) | edf (EdfPolicy) | rm (RateMonotonicPolicy)\n"
        "  --fixed-batch N     pre-gate blocks take exactly N samples per call; throttle publishes whole\n"
        "                      N-chunks; implies --batch N --chunk N and --buffer 16N unless given\n"
        "  --tiny-period S     explicit `period` on source and throttle in fixed-batch mode (default 1e-6)\n"
        "  --trace-categories M  trace-marker mask: a number (0x1ff) or `all`; default 0 = off\n"
        "  --trace-buffer R    trace records retained per emitting thread (default GR4's 65536)\n"
        "  --trace-limit-mb M  per-thread ring ceiling in MB (default GR4's 4096)\n"
        "  --trace-out PATH    write the capture (.gr4trace) here after the run\n"
        "  --sched-ratio N     process_stream_to_message_ratio (default GR4's 16)\n"
        "  --selection S       EDF selector: heap (default) | scan\n"
        "  --max-outstanding-jobs N  EDF: cap on a block's admitted jobs (default GR4's 64)\n"
        "  --max-samples M     replay only the first M samples of the file (frames beyond are dropped from\n"
        "                      the tables), so a run lasts M/rate seconds whatever the rate; 0 = whole file");
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
    static const struct option kOpts[] = {{"run-dir", required_argument, nullptr, 1}, {"input", required_argument, nullptr, 2}, {"out-dir", required_argument, nullptr, 3}, {"rate", required_argument, nullptr, 4}, {"chunk", required_argument, nullptr, 5}, {"chains", required_argument, nullptr, 6}, {"deadline-ms", required_argument, nullptr, 7}, {"timeout-s", required_argument, nullptr, 8}, {"record", no_argument, nullptr, 9}, {"no-check", no_argument, nullptr, 10}, {"buffer", required_argument, nullptr, 11}, {"catch-up", no_argument, nullptr, 12}, {"single", no_argument, nullptr, 13}, {"threads", required_argument, nullptr, 14}, {"batch", required_argument, nullptr, 15}, {"policy", required_argument, nullptr, 16}, {"fixed-batch", required_argument, nullptr, 17}, {"tiny-period", required_argument, nullptr, 18}, {"trace-categories", required_argument, nullptr, 19}, {"trace-buffer", required_argument, nullptr, 20}, {"trace-limit-mb", required_argument, nullptr, 21}, {"trace-out", required_argument, nullptr, 22}, {"sched-ratio", required_argument, nullptr, 23}, {"selection", required_argument, nullptr, 24}, {"max-outstanding-jobs", required_argument, nullptr, 25}, {"max-samples", required_argument, nullptr, 26}, {"help", no_argument, nullptr, 'h'}, {nullptr, 0, nullptr, 0}};
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
        case 11: opt.buffer = std::strtoul(optarg, nullptr, 10); break;
        case 12: opt.catch_up = true; break;
        case 13: opt.single = true; break;
        case 14: opt.threads = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 15: opt.batch = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 16: opt.policy = optarg; break;
        case 17: opt.fixed_batch = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 18: opt.tiny_period = std::strtof(optarg, nullptr); break;
        case 19: opt.trace_mask = parseMask(optarg); break;
        case 20: opt.trace_buffer = std::strtoul(optarg, nullptr, 10); break;
        case 21: opt.trace_limit_mb = std::strtoul(optarg, nullptr, 10); break;
        case 22: opt.trace_out = optarg; break;
        case 23: opt.sched_ratio = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 24: opt.selection = optarg; break;
        case 25: opt.max_outstanding_jobs = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 26: opt.max_samples = std::strtoull(optarg, nullptr, 10); break;
        default: usage(); return 2;
        }
    }
    if (opt.run_dir.empty() || opt.chains < 1 || opt.rate < 0 || opt.chunk < 1) {
        usage();
        return 2;
    }
    if (opt.policy != "rr" && opt.policy != "edf" && opt.policy != "rm") {
        std::println(stderr, "rx_latency4: --policy must be rr, edf or rm");
        return 2;
    }
    if (opt.selection != "heap" && opt.selection != "scan") {
        std::println(stderr, "rx_latency4: --selection must be heap or scan");
        return 2;
    }
    if (opt.single && opt.policy != "rr") {
        std::println(stderr, "rx_latency4: --single is round robin only; use --threads 1 for one worker under edf/rm");
        return 2;
    }
    if (opt.fixed_batch > 0) {
        if (opt.rate <= 0) {
            std::println(stderr, "rx_latency4: --fixed-batch needs a throttle (--rate > 0): the batch is the throttle chunk");
            return 2;
        }
        opt.batch = opt.fixed_batch;
        opt.chunk = opt.fixed_batch;
        if (opt.buffer == 0) {
            opt.buffer = 16UZ * opt.fixed_batch; // 0036 item 9
        }
    }
    if (opt.trace_mask != 0 && !gr::trace::kEnabled) {
        std::println(stderr, "rx_latency4: tracing requested but not compiled in (configure with -DGR4WIFI_TRACING=ON)");
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
    uint64_t frames_dropped = 0;
    for (const auto& fr : manifest.at("frames")) {
        const uint64_t so = fr.at("sample_offset").get<uint64_t>(), ns = fr.at("samples").get<uint64_t>();
        if (opt.max_samples > 0 && so + ns > opt.max_samples) {
            ++frames_dropped; // beyond the replayed part of the file (the manifest is in offset order)
            continue;
        }
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
    if (frames_dropped > 0) {
        std::println(stderr, "rx_latency4: --max-samples {}: replaying {} of {} frames", opt.max_samples, frames, frames + frames_dropped);
    }
    if (frames == 0) {
        std::println(stderr, "rx_latency4: no frame ends before --max-samples {}", opt.max_samples);
        return 2;
    }
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
        cfg.buffer      = opt.buffer;
        cfg.catch_up    = opt.catch_up;
        cfg.batch       = opt.batch;
        cfg.fixed_batch = opt.fixed_batch;
        cfg.tiny_period = opt.fixed_batch > 0 ? opt.tiny_period : 0.f;
        cfg.max_samples = opt.max_samples;
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
    if (opt.threads > 0) {
        using namespace gr::thread_pool;
        auto cpu = std::make_shared<ThreadPoolWrapper>(std::make_unique<BasicThreadPool>(std::string(kDefaultCpuPoolId), TaskType::CPU_BOUND, opt.threads, opt.threads), "CPU");
        gr::thread_pool::Manager::instance().replacePool(std::string(kDefaultCpuPoolId), std::move(cpu));
    }
    const unsigned pool_threads = opt.threads > 0 ? opt.threads : hw_threads;

    std::println(stderr, "rx_latency4: replaying {} ({} frames) at {}, {} chain(s) x {} blocks, deadline {} ms, policy {}, {} worker(s) of {} hw threads{}{}", opt.input, frames, opt.rate > 0 ? std::format("{} Msample/s chunk {}", opt.rate / 1e6, opt.chunk) : std::string("unthrottled"), opt.chains, chains[0].block_count, opt.deadline_ms, opt.policy, opt.single ? 1U : pool_threads, hw_threads, opt.fixed_batch ? std::format(", fixed batch {}", opt.fixed_batch) : std::string(), opt.trace_mask ? std::format(", trace mask 0x{:x}", opt.trace_mask) : std::string());
    if (opt.trace_limit_mb > 0) {
        gr::trace::setRingCapacityLimitBytes(opt.trace_limit_mb * 1024UZ * 1024UZ);
    }

    std::atomic<bool> timed_out{false}, finished{false};
    double elapsed_s = 0;
    bool   ok        = false;
    json   analysis  = json::array(), diagnostics = json::array();
    std::string policy_name, sched_name;
    auto run = [&](auto& sched) {
        using Sched = std::remove_cvref_t<decltype(sched)>;
        policy_name = std::string(Sched::schedulingPolicyName());
        sched_name  = opt.single ? "Simple<singleThreaded>" : "Simple<multiThreaded>";
        // the EDF selector is a member, not a string setting (SchedulingPolicy.hpp)
        sched.selection_strategy = opt.selection == "heap" ? gr::scheduler::SelectionStrategy::readyHeap : gr::scheduler::SelectionStrategy::linearScan;
        if (auto r = sched.exchange(std::move(graph)); !r) {
            std::println(stderr, "rx_latency4: scheduler exchange failed: {}", r.error().message);
            return false;
        }
        // scheduler settings (0036): staged, then applied before the first
        // record could be emitted -- set() alone changes nothing
        // (LIGHTWEIGHT_TRACING_REFERENCE 5.2); buffer size before the mask.
        gr::property_map sset;
        if (opt.sched_ratio > 0) { sset["process_stream_to_message_ratio"] = gr::Size_t(opt.sched_ratio); }
        if (opt.max_outstanding_jobs > 0) { sset["max_outstanding_jobs"] = gr::Size_t(opt.max_outstanding_jobs); }
        if (opt.trace_buffer > 0) { sset["trace_buffer_size"] = gr::Size_t(opt.trace_buffer); }
        if (opt.trace_mask != 0) { sset["trace_categories"] = gr::Size_t(opt.trace_mask); }
        if (!sset.empty()) {
            if (const auto failed = sched.settings().set(sset); !failed.empty()) {
                for (const auto& [k, v] : failed) { std::println(stderr, "rx_latency4: scheduler setting {} rejected", k); }
                return false;
            }
            std::ignore = sched.settings().activateContext();
            std::ignore = sched.settings().applyStagedParameters();
        }
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
        ok            = sched.runAndWait().has_value() && !timed_out.load();
        elapsed_s     = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        finished      = true;
        watchdog.join();
        // what GR4 derived per block (RT reference 6.6): period, deadline,
        // priority, batch floor/ceiling and where each came from.  Derived in
        // init(), which runAndWait() enters, so it is read after the run.
        const gr::scheduler::SchedulingAnalysis& an = sched.schedulingAnalysis();
        for (const auto& block : sched.graph().blocks()) {
            if (const gr::scheduler::DerivedAttributes* a = an.find(*block)) {
                analysis.push_back({{"unique_name", std::string(block->uniqueName())}, {"name", std::string(block->name())}, {"period_s", a->period}, {"relative_deadline_s", a->relativeDeadline}, {"priority", a->priority}, {"batch_floor", a->batchFloor}, {"execution_ceiling", a->executionCeiling == gr::scheduler::kUnboundedBatch ? json(nullptr) : json(a->executionCeiling)}, {"nominal_batch", a->nominalBatch}, {"relative_sample_rate", a->relativeSampleRate}, {"period_origin", std::string(gr::scheduler::toString(a->periodOrigin))}, {"deadline_origin", std::string(gr::scheduler::toString(a->deadlineOrigin))}, {"priority_origin", std::string(gr::scheduler::toString(a->priorityOrigin))}, {"batch_origin", std::string(gr::scheduler::toString(a->batchOrigin))}});
            }
        }
        for (const std::string& note : an.diagnostics) { diagnostics.push_back(note); }
        return true;
    };
    // The scheduler owns the graph and therefore the blocks whose tables are
    // read below: it must outlive the reporting (an earlier version let it go
    // out of scope here and read freed memory).
    using namespace gr::scheduler;
    using SchedSingle = Simple<ExecutionPolicy::singleThreaded>;
    using SchedRR     = Simple<ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, RoundRobinPolicy>;
    using SchedEDF    = Simple<ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, EdfPolicy>;
    using SchedRM     = Simple<ExecutionPolicy::multiThreaded, gr::profiling::null::Profiler, RateMonotonicPolicy>;
    std::unique_ptr<SchedSingle> sched_single;
    std::unique_ptr<SchedRR>     sched_rr;
    std::unique_ptr<SchedEDF>    sched_edf;
    std::unique_ptr<SchedRM>     sched_rm;
    if (opt.single) {
        sched_single = std::make_unique<SchedSingle>();
        if (!run(*sched_single)) { return 1; }
    } else if (opt.policy == "edf") {
        sched_edf = std::make_unique<SchedEDF>();
        if (!run(*sched_edf)) { return 1; }
    } else if (opt.policy == "rm") {
        sched_rm = std::make_unique<SchedRM>();
        if (!run(*sched_rm)) { return 1; }
    } else {
        sched_rr = std::make_unique<SchedRR>();
        if (!run(*sched_rr)) { return 1; }
    }

    // ---- trace capture ----------------------------------------------------
    // runAndWait() returned, so the workers are parked and dump() is defined
    // (it reads rings other threads write, LIGHTWEIGHT_TRACING_REFERENCE 5.6).
    const gr::trace::RingStats ring = gr::trace::ringStats();
    json trace_info = {{"compiled_in", gr::trace::kEnabled}, {"mask", opt.trace_mask}, {"buffer_records", opt.trace_buffer}, {"limit_mb", opt.trace_limit_mb}, {"recorded", ring.recorded}, {"lost", ring.lost}, {"rings", ring.rings}, {"path", opt.trace_out.empty() ? json(nullptr) : json(opt.trace_out)}, {"written", nullptr}};
    if (!opt.trace_out.empty() && opt.trace_mask != 0) {
        if (const auto w = gr::trace::dump(opt.trace_out); w) {
            trace_info["written"] = *w;
            std::println(stderr, "rx_latency4: trace: {} records written to {} ({} lost, {} rings)", *w, opt.trace_out, ring.lost, ring.rings);
        } else {
            std::println(stderr, "rx_latency4: trace dump failed: {}", w.error().message);
            trace_info["written"] = json(nullptr);
            trace_info["error"]   = w.error().message;
        }
    }

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
        json s = {{"chain", k}, {"frames", frames}, {"decoded", cb.sink->_count}, {"missing", missing_n}, {"missing_seqs", missing}, {"duplicates", cb.sink->_duplicates}, {"unpairable", cb.sink->_unpairable}, {"wrong_payload", opt.check ? json(cb.sink->_wrong_payload) : json(nullptr)}, {"wrong_seqs", wrong}, {"unstamped", unstamped}, {"samples_seen", cb.stamper->_items}, {"first_us", first_us}, {"min_us", mn}, {"p50_us", p50}, {"p95_us", p95}, {"p99_us", p99}, {"max_us", mx}, {"deadline_misses", misses}, {"counts", {{"sync_short_detections", cc.sync_short_detections}, {"sync_long_frames", cc.sync_long_frames}, {"signal_ok", cc.signal_ok}, {"signal_bad", cc.signal_bad}, {"frames_started", cc.frames_started}, {"frames_decoded", cc.frames_decoded}, {"crc_failed", cc.crc_failed}, {"too_large", cc.too_large}, {"sl_neg_tags", cc.sl_neg_tags}, {"sl_far_tags", cc.sl_far_tags}, {"sl_tags_seen", cc.sl_tags_seen}, {"sl_max_copy_run", cc.sl_max_copy_run}, {"sl_short_calls", cc.sl_short_calls}, {"fft_tags_in", cc.fft_tags_in}, {"fft_tags_out", cc.fft_tags_out}, {"eq_tags_in", cc.eq_tags_in}, {"src_calls", cc.src_calls}, {"src_max_read_us", cc.src_max_read_ns / 1e3}, {"src_mean_read_us", cc.src_calls ? cc.src_sum_read_ns / 1e3 / cc.src_calls : 0.0}, {"src_max_read_items", cc.src_max_read_items}, {"thr_calls", cc.thr_calls}, {"thr_max_gap_us", cc.thr_max_gap_ns / 1e3}, {"thr_mean_gap_us", cc.thr_calls ? cc.thr_sum_gap_ns / 1e3 / cc.thr_calls : 0.0}, {"thr_max_backlog_samples", cc.thr_max_backlog}, {"stamper_reentry", cc.stamper_reentry}}}};
        per_chain.push_back(s);
        decoded_total += cb.sink->_count;
        missing_total += missing_n;
        misses_total += misses;
        wrong_total += cb.sink->_wrong_payload;
        std::println(stderr, "rx_latency4: chain {} -- {}/{} decoded, {} missing, {} dup, {} unpairable, {} wrong payload; last->decode us: first {:.1f} p50 {:.1f} p95 {:.1f} p99 {:.1f} max {:.1f}; {} over {} ms; sync_short {} sync_long {} signal ok/bad {}/{} crc failed {}", k, cb.sink->_count, frames, missing_n, cb.sink->_duplicates, cb.sink->_unpairable, cb.sink->_wrong_payload, first_us, p50, p95, p99, mx, misses, opt.deadline_ms, cc.sync_short_detections, cc.sync_long_frames, cc.signal_ok, cc.signal_bad, cc.crc_failed);
    }
    std::fclose(csv);

    json summary = {{"app", "rx_latency4"}, {"runtime", "gnuradio4"}, {"scheduler", sched_name}, {"policy", opt.policy}, {"policy_name", policy_name}, {"fixed_batch", opt.fixed_batch}, {"tiny_period_s", opt.fixed_batch > 0 ? opt.tiny_period : 0.f}, {"selection_strategy", opt.selection}, {"sched_ratio", opt.sched_ratio}, {"max_outstanding_jobs", opt.max_outstanding_jobs}, {"trace", trace_info}, {"max_samples", opt.max_samples}, {"frames_dropped", frames_dropped}, {"buffer", opt.buffer}, {"catch_up", opt.catch_up}, {"threads", opt.single ? 1U : pool_threads}, {"batch", opt.batch}, {"hw_threads", hw_threads}, {"chains", opt.chains}, {"order_mode", order_mode}, {"blocks_per_chain", chains[0].block_count}, {"frames", frames}, {"decoded_total", decoded_total}, {"missing_total", missing_total}, {"wrong_payload_total", opt.check ? json(wrong_total) : json(nullptr)}, {"deadline_misses_total", misses_total}, {"elapsed_s", elapsed_s}, {"latency", {{"rate", opt.rate}, {"chunk", opt.chunk}, {"deadline_ms", opt.deadline_ms}, {"stamp_bias_max_us", opt.rate > 0 ? opt.chunk / opt.rate * 1e6 : 0.0}, {"input", opt.input}}}, {"result", ok ? "ok" : (timed_out.load() ? "timeout" : "error")}, {"per_chain", per_chain}};
    {
        std::ofstream f(opt.out_dir + "/latency_summary.json");
        f << summary.dump(2) << "\n";
    }
    // trace_meta.json: what an offline reader needs beside the capture --
    // GR4's block names against our roles, the nominal-arrival anchor of
    // every throttle, the derived scheduling attributes, and the knobs.
    {
        json jchains = json::array();
        for (std::size_t k = 0; k < chains.size(); k++) {
            const ChainCounters cc = chains[k].counters();
            json blocks = json::array();
            for (const auto& [uname, role] : chains[k].roles) { blocks.push_back({{"unique_name", uname}, {"role", role}}); }
            jchains.push_back({{"chain", k}, {"throttle_start_ns", cc.thr_start_ns}, {"throttle_chunks", cc.thr_chunks}, {"throttle_max_chunks_per_call", cc.thr_max_chunks_per_call}, {"throttle_max_backlog_samples", cc.thr_max_backlog}, {"blocks", blocks}});
        }
        json meta = {{"app", "rx_latency4"}, {"run_dir", opt.run_dir}, {"input", opt.input}, {"out_dir", opt.out_dir}, {"policy", opt.policy}, {"policy_name", policy_name}, {"scheduler", sched_name}, {"threads", opt.single ? 1U : pool_threads}, {"hw_threads", hw_threads}, {"rate", opt.rate}, {"chunk", opt.chunk}, {"fixed_batch", opt.fixed_batch}, {"batch", opt.batch}, {"buffer", opt.buffer}, {"tiny_period_s", opt.fixed_batch > 0 ? opt.tiny_period : 0.f}, {"selection_strategy", opt.selection}, {"sched_ratio", opt.sched_ratio}, {"max_outstanding_jobs", opt.max_outstanding_jobs}, {"frames", frames}, {"max_samples", opt.max_samples}, {"elapsed_s", elapsed_s}, {"result", ok ? "ok" : (timed_out.load() ? "timeout" : "error")}, {"trace", trace_info}, {"chains", jchains}, {"analysis", analysis}, {"diagnostics", diagnostics}};
        std::ofstream f(opt.out_dir + "/trace_meta.json");
        f << meta.dump(2) << "\n";
    }
    std::println(stderr, "rx_latency4: {:.3f} s; wrote {} and {}/latency_summary.json", elapsed_s, csv_path, opt.out_dir);
    if (timed_out.load()) {
        std::println(stderr, "rx_latency4: TIMEOUT after {} s", opt.timeout_s);
        return 1;
    }
    return ok ? 0 : 1;
}
