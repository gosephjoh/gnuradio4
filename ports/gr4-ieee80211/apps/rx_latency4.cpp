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
 *
 * --feed pacer replaces the file source, throttle and arrival stamper with a
 * thread outside the scheduler (Pacer.hpp) that writes each chunk into the
 * receiver's entry block at its nominal instant and logs when it did:
 *
 *   pacer thread --> entry(Copy) -> wifi rx chain -> latency_sink
 *
 * latency.csv's arrival stamps are then the pacer's write instants, pacer.csv
 * holds every chunk's nominal and actual write time, and the sink stamps a
 * decoded packet at the end of the call that delivered it.
 */
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>
#include <gnuradio-4.0/SchedulingPolicy.hpp>
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceFile.hpp>
#include <gnuradio-4.0/thread/thread_pool.hpp>

#include "gr4ieee80211/Pacer.hpp"
#include "gr4ieee80211/chain.hpp"

#include <nlohmann/json.hpp>

#include <getopt.h>
#include <sched.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <print>
#include <sstream>
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
    unsigned    max_selections = 0;      // max_selections_per_pass; 0 = GR4's auto (4 x block count)
    unsigned    max_pass_duration = 0;   // max_pass_duration_us; 0 = GR4's auto (a quarter of the shortest declared period)
    uint64_t    max_samples = 0;     // replay only the first M samples (0 = whole file)
    std::vector<float> deadline_classes; // per chain relative-deadline factor (modification A); missing = 1
    float       frame_deadline = 1.f;    // post-gate relative-deadline factor (modification B)
    std::vector<float> rates;            // per chain throttle rate (multi-rate receivers); missing = --rate
    double      run_s = 0;               // replay run_s seconds per chain (max_samples = run_s x chain rate)
    unsigned    rotate = 0;              // rotate chain k's construction order by rotate x k slots
    bool        rm_tiny_periods = false; // RM: keep the tiny periods (default: true per-receiver periods N/rate)
    std::vector<unsigned> cpus;          // pin worker k to cpus[k]; empty = no pinning
    int         rt_prio = 0;             // SCHED_FIFO priority of the workers; 0 = SCHED_OTHER
    std::string feed = "throttle";       // throttle | pacer
    int         pacer_cpu = -1;          // pin the pacer thread; -1 = anywhere
    int         pacer_prio = 0;          // SCHED_FIFO priority of the pacer; 0 = SCHED_OTHER
    unsigned    pacer_spin_us = 60;      // sleep until this long before a chunk is due, then spin
    unsigned    pacer_delay_ms = 500;    // from the scheduler running to the first chunk's clock
};

// "4-7" or "4,5,6" -> {4,5,6,7}; empty on a malformed list
std::vector<unsigned> parseCpuList(const char* a) {
    std::vector<unsigned> v;
    std::string s(a), tok;
    std::stringstream ss(s);
    while (std::getline(ss, tok, ',')) {
        const std::size_t dash = tok.find('-');
        char* end = nullptr;
        const unsigned lo = static_cast<unsigned>(std::strtoul(tok.c_str(), &end, 10));
        if (end == tok.c_str()) { return {}; }
        unsigned hi = lo;
        if (dash != std::string::npos) {
            const char* p = tok.c_str() + dash + 1;
            hi = static_cast<unsigned>(std::strtoul(p, &end, 10));
            if (end == p || hi < lo) { return {}; }
        }
        for (unsigned c = lo; c <= hi; c++) { v.push_back(c); }
    }
    return v;
}

std::vector<float> parseFloats(const char* a) {
    std::vector<float> v;
    std::string s(a), tok;
    for (char ch : s) {
        if (ch == ',') { if (!tok.empty()) { v.push_back(std::strtof(tok.c_str(), nullptr)); tok.clear(); } }
        else { tok += ch; }
    }
    if (!tok.empty()) { v.push_back(std::strtof(tok.c_str(), nullptr)); }
    return v;
}

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
        "  --max-selections N  EDF/RM: max_selections_per_pass, work() calls before the loop returns to the\n"
        "                      backstop that releases the sources (default GR4's auto = 4 x block count)\n"
        "  --max-pass-duration US  EDF: max_pass_duration_us, the same bound in wall-clock time. GR4's auto is a\n"
        "                      quarter of the shortest `period` declared on the worker, which in fixed-batch mode\n"
        "                      is --tiny-period, so state it here to bound the pass by the batch period instead\n"
        "  --max-samples M     replay only the first M samples of the file (frames beyond are dropped from\n"
        "                      the tables), so a run lasts M/rate seconds whatever the rate; 0 = whole file\n"
        "deadline structure (fixed-batch mode; a block's relative deadline = factor x batch period):\n"
        "  --deadline-classes F0,F1,..  per-receiver class factor (chain k gets Fk, missing = 1): 0.25 marks the\n"
        "                      control-channel receiver, 1 a service-channel receiver\n"
        "  --frame-deadline F  factor for the frame path after the gate (dly320, sync_long, fft, eq, decode, sink)\n"
        "                      of every receiver; a post-gate block gets min(class, frame); default 1\n"
        "multi-rate receivers (4-worker experiment):\n"
        "  --rates R0,R1,..    per-receiver throttle rate in samples/s (missing = --rate); batch period, deadline\n"
        "                      and RM period follow each receiver's own rate\n"
        "  --run-s S           replay S seconds per receiver (max_samples = S x its rate); frames beyond are dropped\n"
        "  --rotate K          rotate receiver k's block construction order by K x k slots (spreads the heavy blocks\n"
        "                      over GR4's striped workers); 0 = graph order as built\n"
        "  --rm-tiny-periods   RM: keep the tiny periods (default under --policy rm: each receiver's blocks carry\n"
        "                      their true period N/rate, so RM ranks receivers by rate)\n"
        "worker placement (needs --threads):\n"
        "  --cpus LIST         pin worker k to the k-th CPU of LIST (\"4-7\" or \"4,5,6\"); LIST must hold at least\n"
        "                      --threads CPUs; one worker per CPU, never two\n"
        "  --rt-prio N         run the workers under SCHED_FIFO at priority N (needs an rtprio limit >= N or root);\n"
        "                      keep N below the kernel's irq/* and rcuc/* threads (50) unless you mean to outrank them\n"
        "input:\n"
        "  --feed F            throttle (default): file source -> throttle in the graph, arrival stamper beside it;\n"
        "                      pacer: a thread outside the scheduler writes each chunk into an entry block at its\n"
        "                      nominal instant (Pacer.hpp); pacer.csv logs every write; under EDF every period is 0\n"
        "  --pacer-cpu C       pin the pacer thread to CPU C (a housekeeping CPU, not one of --cpus)\n"
        "  --pacer-prio N      run the pacer under SCHED_FIFO at priority N (default SCHED_OTHER)\n"
        "  --pacer-spin-us U   sleep until U us before a chunk is due, then spin (default 60)\n"
        "  --pacer-delay-ms M  wait M ms after the scheduler starts before the first chunk's clock (default 500)");
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
    static const struct option kOpts[] = {{"run-dir", required_argument, nullptr, 1}, {"input", required_argument, nullptr, 2}, {"out-dir", required_argument, nullptr, 3}, {"rate", required_argument, nullptr, 4}, {"chunk", required_argument, nullptr, 5}, {"chains", required_argument, nullptr, 6}, {"deadline-ms", required_argument, nullptr, 7}, {"timeout-s", required_argument, nullptr, 8}, {"record", no_argument, nullptr, 9}, {"no-check", no_argument, nullptr, 10}, {"buffer", required_argument, nullptr, 11}, {"catch-up", no_argument, nullptr, 12}, {"single", no_argument, nullptr, 13}, {"threads", required_argument, nullptr, 14}, {"batch", required_argument, nullptr, 15}, {"policy", required_argument, nullptr, 16}, {"fixed-batch", required_argument, nullptr, 17}, {"tiny-period", required_argument, nullptr, 18}, {"trace-categories", required_argument, nullptr, 19}, {"trace-buffer", required_argument, nullptr, 20}, {"trace-limit-mb", required_argument, nullptr, 21}, {"trace-out", required_argument, nullptr, 22}, {"sched-ratio", required_argument, nullptr, 23}, {"selection", required_argument, nullptr, 24}, {"max-outstanding-jobs", required_argument, nullptr, 25}, {"max-samples", required_argument, nullptr, 26}, {"deadline-classes", required_argument, nullptr, 27}, {"frame-deadline", required_argument, nullptr, 28}, {"max-selections", required_argument, nullptr, 29}, {"rates", required_argument, nullptr, 30}, {"run-s", required_argument, nullptr, 31}, {"rotate", required_argument, nullptr, 32}, {"rm-tiny-periods", no_argument, nullptr, 33}, {"max-pass-duration", required_argument, nullptr, 34}, {"cpus", required_argument, nullptr, 35}, {"rt-prio", required_argument, nullptr, 36}, {"feed", required_argument, nullptr, 37}, {"pacer-cpu", required_argument, nullptr, 38}, {"pacer-prio", required_argument, nullptr, 39}, {"pacer-spin-us", required_argument, nullptr, 40}, {"pacer-delay-ms", required_argument, nullptr, 41}, {"help", no_argument, nullptr, 'h'}, {nullptr, 0, nullptr, 0}};
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
        case 27: opt.deadline_classes = parseFloats(optarg); break;
        case 28: opt.frame_deadline = std::strtof(optarg, nullptr); break;
        case 29: opt.max_selections = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 30: opt.rates = parseFloats(optarg); break;
        case 31: opt.run_s = std::atof(optarg); break;
        case 32: opt.rotate = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 33: opt.rm_tiny_periods = true; break;
        case 34: opt.max_pass_duration = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 35: opt.cpus = parseCpuList(optarg); if (opt.cpus.empty()) { std::println(stderr, "rx_latency4: --cpus: bad list {}", optarg); return 2; } break;
        case 36: opt.rt_prio = std::atoi(optarg); break;
        case 37: opt.feed = optarg; break;
        case 38: opt.pacer_cpu = std::atoi(optarg); break;
        case 39: opt.pacer_prio = std::atoi(optarg); break;
        case 40: opt.pacer_spin_us = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
        case 41: opt.pacer_delay_ms = static_cast<unsigned>(std::strtoul(optarg, nullptr, 10)); break;
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
    if (opt.feed != "throttle" && opt.feed != "pacer") {
        std::println(stderr, "rx_latency4: --feed must be throttle or pacer");
        return 2;
    }
    const bool pacing = opt.feed == "pacer";
    if (pacing && opt.rate <= 0) {
        std::println(stderr, "rx_latency4: --feed pacer needs a rate (--rate > 0): the pacer is the clock");
        return 2;
    }
    if (pacing && opt.pacer_cpu >= 0 && std::ranges::find(opt.cpus, static_cast<unsigned>(opt.pacer_cpu)) != opt.cpus.end()) {
        std::println(stderr, "rx_latency4: --pacer-cpu {} is one of the workers' --cpus; give the pacer a CPU of its own", opt.pacer_cpu);
        return 2;
    }
    if (pacing && opt.pacer_cpu >= static_cast<int>(std::thread::hardware_concurrency())) {
        std::println(stderr, "rx_latency4: --pacer-cpu {} is not one of the {} CPUs", opt.pacer_cpu, std::thread::hardware_concurrency());
        return 2;
    }
    if (pacing && opt.pacer_prio != 0) {
        if (opt.pacer_prio < sched_get_priority_min(SCHED_FIFO) || opt.pacer_prio > sched_get_priority_max(SCHED_FIFO)) {
            std::println(stderr, "rx_latency4: --pacer-prio must be in [{}, {}]", sched_get_priority_min(SCHED_FIFO), sched_get_priority_max(SCHED_FIFO));
            return 2;
        }
        struct rlimit rl {};
        if (geteuid() != 0 && (getrlimit(RLIMIT_RTPRIO, &rl) != 0 || (rl.rlim_cur != RLIM_INFINITY && static_cast<int>(rl.rlim_cur) < opt.pacer_prio))) {
            std::println(stderr, "rx_latency4: --pacer-prio {}: the rtprio limit is too low (ulimit -r); raise it or run as root", opt.pacer_prio);
            return 2;
        }
    }
    if (opt.selection != "heap" && opt.selection != "scan") {
        std::println(stderr, "rx_latency4: --selection must be heap or scan");
        return 2;
    }
    if (opt.single && opt.policy != "rr") {
        std::println(stderr, "rx_latency4: --single is round robin only; use --threads 1 for one worker under edf/rm");
        return 2;
    }
    if (!opt.cpus.empty() || opt.rt_prio != 0) {
        if (opt.threads == 0 || opt.single) {
            std::println(stderr, "rx_latency4: --cpus and --rt-prio apply to the worker pool: give --threads N, not --single");
            return 2;
        }
        const unsigned ncpu = std::thread::hardware_concurrency();
        if (!opt.cpus.empty() && opt.cpus.size() < opt.threads) {
            std::println(stderr, "rx_latency4: --cpus lists {} CPU(s) for {} workers; one CPU per worker is required", opt.cpus.size(), opt.threads);
            return 2;
        }
        for (unsigned c : opt.cpus) {
            if (c >= ncpu) { std::println(stderr, "rx_latency4: --cpus: CPU {} is not one of the {} CPUs", c, ncpu); return 2; }
        }
        if (opt.rt_prio != 0) {
            const int lo = sched_get_priority_min(SCHED_FIFO), hi = sched_get_priority_max(SCHED_FIFO);
            if (opt.rt_prio < lo || opt.rt_prio > hi) { std::println(stderr, "rx_latency4: --rt-prio must be in [{}, {}]", lo, hi); return 2; }
            struct rlimit rl {};
            if (geteuid() != 0 && (getrlimit(RLIMIT_RTPRIO, &rl) != 0 || (rl.rlim_cur != RLIM_INFINITY && static_cast<int>(rl.rlim_cur) < opt.rt_prio))) {
                std::println(stderr, "rx_latency4: --rt-prio {}: the rtprio limit is {} (ulimit -r); raise it in /etc/security/limits.d or run as root", opt.rt_prio, rl.rlim_cur == RLIM_INFINITY ? std::string("unlimited") : std::to_string(rl.rlim_cur));
                return 2;
            }
        }
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
    for (float f : opt.deadline_classes) {
        if (!(f > 0.f)) { std::println(stderr, "rx_latency4: --deadline-classes factors must be > 0"); return 2; }
    }
    if (!(opt.frame_deadline > 0.f)) { std::println(stderr, "rx_latency4: --frame-deadline must be > 0"); return 2; }
    if ((!opt.deadline_classes.empty() || opt.frame_deadline != 1.f) && opt.fixed_batch == 0) {
        std::println(stderr, "rx_latency4: --deadline-classes / --frame-deadline need --fixed-batch (deadlines are batch periods)");
        return 2;
    }
    for (float r : opt.rates) {
        if (!(r > 0.f)) { std::println(stderr, "rx_latency4: --rates entries must be > 0"); return 2; }
    }
    if ((!opt.rates.empty() || opt.run_s > 0) && opt.fixed_batch == 0) {
        std::println(stderr, "rx_latency4: --rates / --run-s need --fixed-batch");
        return 2;
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
    // per chain: its rate, how many samples it replays, and how many frames end inside that
    std::vector<double>      chain_rate(static_cast<std::size_t>(opt.chains), opt.rate);
    std::vector<uint64_t>    chain_max(static_cast<std::size_t>(opt.chains), opt.max_samples);
    std::vector<std::size_t> chain_frames(static_cast<std::size_t>(opt.chains), frames);
    for (std::size_t k = 0; k < static_cast<std::size_t>(opt.chains); k++) {
        if (k < opt.rates.size()) { chain_rate[k] = opt.rates[k]; }
        if (opt.run_s > 0) { chain_max[k] = static_cast<uint64_t>(opt.run_s * chain_rate[k]); }
        if (chain_max[k] > 0) {
            std::size_t n = 0;
            for (const auto& fr : manifest.at("frames")) {
                const uint64_t so = fr.at("sample_offset").get<uint64_t>(), ns = fr.at("samples").get<uint64_t>();
                if (so + ns > chain_max[k]) { break; }
                n++;
            }
            chain_frames[k] = std::min(n, frames);
        }
    }
    // Declared before the scheduler, so it is destroyed after it: its ports own the entry buffers'
    // writer side, and the pacer thread must be joined before anything it writes into goes away.
    std::unique_ptr<Pacer> pacer;
    if (pacing) {
        pacer = std::make_unique<Pacer>(PacerSettings{.chunk         = opt.chunk,
                                                      .padToMultiple = opt.fixed_batch,
                                                      .padMinTail    = opt.fixed_batch > 0 ? fixedBatchFlushTail(opt.fixed_batch) : 0ULL,
                                                      .buffer        = opt.buffer > 0 ? opt.buffer : 65536UZ,
                                                      .cpu           = opt.pacer_cpu,
                                                      .fifoPriority  = opt.pacer_prio,
                                                      .spinNs        = static_cast<uint64_t>(opt.pacer_spin_us) * 1000ULL,
                                                      .startDelayNs  = static_cast<uint64_t>(opt.pacer_delay_ms) * 1'000'000ULL});
        if (auto opened = pacer->open(opt.input); !opened) {
            std::println(stderr, "rx_latency4: pacer: {}", opened.error());
            return 2;
        }
    }
    gr::Graph                 graph;
    std::vector<ChainBlocks> chains;
    for (int k = 0; k < opt.chains; k++) {
        ChainConfig cfg;
        cfg.input       = opt.input;
        cfg.rate        = chain_rate[static_cast<std::size_t>(k)];
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
        cfg.max_samples = chain_max[static_cast<std::size_t>(k)];
        cfg.deadline_factor       = static_cast<std::size_t>(k) < opt.deadline_classes.size() ? opt.deadline_classes[static_cast<std::size_t>(k)] : 1.f;
        cfg.frame_deadline_factor = opt.frame_deadline;
        cfg.rotate          = opt.rotate;
        cfg.chain_index     = static_cast<unsigned>(k);
        cfg.rm_true_periods = opt.policy == "rm" && !opt.rm_tiny_periods;
        cfg.feed            = pacing ? ChainConfig::Feed::pacer : ChainConfig::Feed::throttle;
        cfg.zero_period     = pacing && opt.policy == "edf"; // every pacer-mode block carries an explicit deadline
        ChainBlocks cb  = buildChain(graph, cfg);
        const std::size_t fk = chain_frames[static_cast<std::size_t>(k)];
        if (pacing) {
            PacerStream& stream = pacer->addStream(chain_rate[static_cast<std::size_t>(k)], chain_max[static_cast<std::size_t>(k)]);
            if (auto connected = stream.out.connect(*cb.feed_in); !connected) {
                std::println(stderr, "rx_latency4: pacer: cannot connect receiver {}'s entry block: {}", k, connected.error().message);
                return 2;
            }
        } else {
            cb.stamper->setFrames(std::vector<uint64_t>(first.begin(), first.begin() + static_cast<std::ptrdiff_t>(fk)), std::vector<uint64_t>(last.begin(), last.begin() + static_cast<std::ptrdiff_t>(fk)));
        }
        cb.sink->setFrames(fk);
        cb.sink->_stamp_at_call_end = pacing;
        cb.sink->_order_mode = order_mode;
        if (opt.check) {
            for (std::size_t i = 0; i < fk; i++) {
                cb.sink->_expected[i]     = payloads.data() + offsets[i];
                cb.sink->_expected_len[i] = lengths[i];
            }
        }
        chains.push_back(std::move(cb));
    }
    const unsigned hw_threads = std::thread::hardware_concurrency();
    if (opt.threads > 0) {
        using namespace gr::thread_pool;
        auto pool = std::make_unique<BasicThreadPool>(std::string(kDefaultCpuPoolId), TaskType::CPU_BOUND, opt.threads, opt.threads);
        // the pool spreads its threads over the set CPUs of the mask, thread k on the k-th: a mask of
        // exactly --threads CPUs pins one worker per CPU (thread_pool.hpp distributeThreadAffinityAcrossCores)
        if (!opt.cpus.empty()) {
            std::vector<bool> mask(std::thread::hardware_concurrency(), false);
            for (unsigned k = 0; k < opt.threads; k++) { mask[opt.cpus[k]] = true; }
            pool->setAffinityMask(mask);
        }
        if (opt.rt_prio != 0) { pool->setThreadSchedulingPolicy(thread::Policy::FIFO, opt.rt_prio); }
        auto cpu = std::make_shared<ThreadPoolWrapper>(std::move(pool), "CPU");
        gr::thread_pool::Manager::instance().replacePool(std::string(kDefaultCpuPoolId), std::move(cpu));
    }
    const unsigned pool_threads = opt.threads > 0 ? opt.threads : hw_threads;

    std::println(stderr, "rx_latency4: replaying {} ({} frames) at {}, {} chain(s) x {} blocks, deadline {} ms, policy {}, {} worker(s) of {} hw threads{}{}", opt.input, frames, opt.rate > 0 ? std::format("{} Msample/s chunk {}", opt.rate / 1e6, opt.chunk) : std::string("unthrottled"), opt.chains, chains[0].block_count, opt.deadline_ms, opt.policy, opt.single ? 1U : pool_threads, hw_threads, opt.fixed_batch ? std::format(", fixed batch {}", opt.fixed_batch) : std::string(), opt.trace_mask ? std::format(", trace mask 0x{:x}", opt.trace_mask) : std::string());
    if (!opt.cpus.empty() || opt.rt_prio != 0) {
        std::println(stderr, "rx_latency4: workers on CPUs [{}]{}", opt.cpus.empty() ? std::string("any") : [&] { std::string t; for (unsigned k = 0; k < opt.threads; k++) { t += (k ? "," : "") + std::to_string(opt.cpus[k]); } return t; }(), opt.rt_prio ? std::format(", SCHED_FIFO {}", opt.rt_prio) : std::string(", SCHED_OTHER"));
    }
    if (opt.trace_limit_mb > 0) {
        gr::trace::setRingCapacityLimitBytes(opt.trace_limit_mb * 1024UZ * 1024UZ);
    }
    if (pacer) {
        pacer->prefault();
        std::println(stderr, "rx_latency4: pacer on CPU {}{}, spin {} us, first chunk {} ms after the scheduler starts", opt.pacer_cpu >= 0 ? std::to_string(opt.pacer_cpu) : std::string("any"), opt.pacer_prio ? std::format(", SCHED_FIFO {}", opt.pacer_prio) : std::string(", SCHED_OTHER"), opt.pacer_spin_us, opt.pacer_delay_ms);
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
        if (opt.max_selections > 0) { sset["max_selections_per_pass"] = gr::Size_t(opt.max_selections); }
        if (opt.max_pass_duration > 0) { sset["max_pass_duration_us"] = gr::Size_t(opt.max_pass_duration); }
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
        if (pacer) {
            pacer->start([&sched] { return sched.state() == gr::lifecycle::State::RUNNING; });
        }
        const auto t0 = std::chrono::steady_clock::now();
        ok            = sched.runAndWait().has_value() && !timed_out.load();
        elapsed_s     = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        finished      = true;
        watchdog.join();
        if (pacer) {
            pacer->stop(); // already done unless the run ended early
            if (!pacer->error().empty()) { std::println(stderr, "rx_latency4: pacer: {}", pacer->error()); }
            ok = ok && pacer->finished();
        }
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
    // pacer mode: a frame's arrival is the write of the chunk holding its sample, and the nominal
    // columns measure from the instant that chunk was due -- their difference is the pacer's own error
    std::fprintf(csv, "chain,seq,length,t_first_ns,t_last_ns,t_decode_ns,lat_first_us,lat_last_us,decoded,correct,t_last_nominal_ns,lat_last_nominal_us\n");
    json per_chain = json::array();
    uint64_t decoded_total = 0, missing_total = 0, misses_total = 0, wrong_total = 0;
    const double deadline_us = opt.deadline_ms * 1000.0;
    for (std::size_t k = 0; k < chains.size(); k++) {
        const ChainBlocks& cb = chains[k];
        const std::size_t  frames_k = chain_frames[k];
        std::vector<double> lat;
        json                missing = json::array(), wrong = json::array();
        uint64_t            missing_n = 0, misses = 0, unstamped = 0;
        double              first_us = std::nan("");
        const PacerStream*  stream   = pacer ? &pacer->streams()[k] : nullptr;
        const bool          clocked  = stream && stream->t0Ns != 0; // false when the run ended before the pacer started
        for (std::size_t i = 0; i < frames_k; i++) {
            const uint64_t tf = stream ? stream->log[stream->chunkOf(first[i])].writtenNs : cb.stamper->_t_first[i];
            const uint64_t tl = stream ? stream->log[stream->chunkOf(last[i])].writtenNs : cb.stamper->_t_last[i];
            const uint64_t tn = clocked ? stream->nominalNs(stream->chunkOf(last[i])) : 0;
            const uint64_t td = cb.sink->_t_decode[i];
            if (tf == 0 || tl == 0) {
                ++unstamped;
            }
            if (td == 0) {
                ++missing_n;
                if (missing.size() < 50) {
                    missing.push_back(i);
                }
                std::fprintf(csv, "%zu,%zu,%u,%llu,%llu,,,,0,,%s,\n", k, i, lengths[i], static_cast<unsigned long long>(tf), static_cast<unsigned long long>(tl), tn ? std::to_string(tn).c_str() : "");
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
            const std::string nominal = tn ? std::format("{},{:.3f}", tn, (static_cast<double>(td) - static_cast<double>(tn)) / 1e3) : std::string(",");
            std::fprintf(csv, "%zu,%zu,%u,%llu,%llu,%llu,%.3f,%.3f,1,%s,%s\n", k, i, lengths[i], static_cast<unsigned long long>(tf), static_cast<unsigned long long>(tl), static_cast<unsigned long long>(td), lf, ll, correct < 0 ? "" : (correct ? "1" : "0"), nominal.c_str());
        }
        std::sort(lat.begin(), lat.end());
        const double p50 = percentile(lat, 0.50), p95 = percentile(lat, 0.95), p99 = percentile(lat, 0.99);
        const double mx = lat.empty() ? std::nan("") : lat.back(), mn = lat.empty() ? std::nan("") : lat.front();
        const ChainCounters cc = cb.counters();
        json pacer_k = nullptr;
        uint64_t samples_seen = cb.stamper ? cb.stamper->_items : 0;
        if (stream) {
            // write lag: how late each chunk was written after it was due; above ten batch periods at p95
            // the pipeline did not keep up (the saturation criterion the throttle's lag used to carry)
            std::vector<double> lag;
            uint64_t            retries = 0, written = 0;
            for (const PacerStream::Chunk& chunk : stream->log) {
                retries += chunk.retries;
                if (chunk.writtenNs == 0) { continue; }
                ++written;
                lag.push_back((static_cast<double>(chunk.writtenNs) - static_cast<double>(stream->t0Ns + chunk.dueNs)) / 1e3);
            }
            std::sort(lag.begin(), lag.end());
            samples_seen                 = std::min<uint64_t>(stream->total, written * stream->chunk);
            const double period_us       = static_cast<double>(stream->chunk) / stream->rate * 1e6;
            pacer_k = {{"chunks", stream->log.size()}, {"written", written}, {"samples_from_file", stream->fromFile}, {"samples_total", stream->total}, {"retries", retries}, {"period_us", period_us},
                {"write_lag_us", {{"p50", percentile(lag, 0.50)}, {"p95", percentile(lag, 0.95)}, {"p99", percentile(lag, 0.99)}, {"p999", percentile(lag, 0.999)}, {"max", lag.empty() ? std::nan("") : lag.back()}}},
                {"saturated", !lag.empty() && percentile(lag, 0.95) > 10.0 * period_us}};
        }
        json s = {{"chain", k}, {"frames", frames_k}, {"rate", chain_rate[k]}, {"max_samples", chain_max[k]}, {"decoded", cb.sink->_count}, {"missing", missing_n}, {"missing_seqs", missing}, {"duplicates", cb.sink->_duplicates}, {"unpairable", cb.sink->_unpairable}, {"wrong_payload", opt.check ? json(cb.sink->_wrong_payload) : json(nullptr)}, {"wrong_seqs", wrong}, {"unstamped", unstamped}, {"samples_seen", samples_seen}, {"pacer", pacer_k}, {"first_us", first_us}, {"min_us", mn}, {"p50_us", p50}, {"p95_us", p95}, {"p99_us", p99}, {"max_us", mx}, {"deadline_misses", misses}, {"counts", {{"sync_short_detections", cc.sync_short_detections}, {"sync_long_frames", cc.sync_long_frames}, {"signal_ok", cc.signal_ok}, {"signal_bad", cc.signal_bad}, {"frames_started", cc.frames_started}, {"frames_decoded", cc.frames_decoded}, {"crc_failed", cc.crc_failed}, {"too_large", cc.too_large}, {"sl_neg_tags", cc.sl_neg_tags}, {"sl_far_tags", cc.sl_far_tags}, {"sl_tags_seen", cc.sl_tags_seen}, {"sl_max_copy_run", cc.sl_max_copy_run}, {"sl_short_calls", cc.sl_short_calls}, {"fft_tags_in", cc.fft_tags_in}, {"fft_tags_out", cc.fft_tags_out}, {"eq_tags_in", cc.eq_tags_in}, {"src_calls", cc.src_calls}, {"src_max_read_us", cc.src_max_read_ns / 1e3}, {"src_mean_read_us", cc.src_calls ? cc.src_sum_read_ns / 1e3 / cc.src_calls : 0.0}, {"src_max_read_items", cc.src_max_read_items}, {"thr_calls", cc.thr_calls}, {"thr_max_gap_us", cc.thr_max_gap_ns / 1e3}, {"thr_mean_gap_us", cc.thr_calls ? cc.thr_sum_gap_ns / 1e3 / cc.thr_calls : 0.0}, {"thr_max_backlog_samples", cc.thr_max_backlog}, {"stamper_reentry", cc.stamper_reentry}}}};
        per_chain.push_back(s);
        decoded_total += cb.sink->_count;
        missing_total += missing_n;
        misses_total += misses;
        wrong_total += cb.sink->_wrong_payload;
        std::println(stderr, "rx_latency4: chain {} -- {}/{} decoded, {} missing, {} dup, {} unpairable, {} wrong payload; last->decode us: first {:.1f} p50 {:.1f} p95 {:.1f} p99 {:.1f} max {:.1f}; {} over {} ms; sync_short {} sync_long {} signal ok/bad {}/{} crc failed {}", k, cb.sink->_count, frames, missing_n, cb.sink->_duplicates, cb.sink->_unpairable, cb.sink->_wrong_payload, first_us, p50, p95, p99, mx, misses, opt.deadline_ms, cc.sync_short_detections, cc.sync_long_frames, cc.signal_ok, cc.signal_bad, cc.crc_failed);
    }
    std::fclose(csv);
    if (pacer) {
        const std::string pacer_path = opt.out_dir + "/pacer.csv";
        if (std::FILE* pf = std::fopen(pacer_path.c_str(), "w")) {
            std::fprintf(pf, "chain,chunk,first_sample,samples,nominal_ns,written_ns,retries\n");
            for (std::size_t k = 0; k < pacer->streams().size(); ++k) {
                const PacerStream& st = pacer->streams()[k];
                for (std::size_t j = 0; j < st.log.size(); ++j) {
                    const uint64_t first_sample = static_cast<uint64_t>(j) * st.chunk;
                    std::fprintf(pf, "%zu,%zu,%llu,%llu,%llu,%llu,%u\n", k, j, static_cast<unsigned long long>(first_sample), static_cast<unsigned long long>(std::min<uint64_t>(st.chunk, st.total - first_sample)), static_cast<unsigned long long>(st.t0Ns != 0 ? st.nominalNs(j) : 0ULL), static_cast<unsigned long long>(st.log[j].writtenNs), st.log[j].retries);
                }
            }
            std::fclose(pf);
        } else {
            std::println(stderr, "rx_latency4: cannot open {}", pacer_path);
        }
    }

    const std::vector<unsigned> worker_cpus(opt.cpus.begin(), opt.cpus.begin() + static_cast<std::ptrdiff_t>(std::min<std::size_t>(opt.cpus.size(), opt.threads)));
    json summary = {{"app", "rx_latency4"}, {"runtime", "gnuradio4"}, {"scheduler", sched_name}, {"policy", opt.policy}, {"policy_name", policy_name}, {"fixed_batch", opt.fixed_batch}, {"tiny_period_s", opt.fixed_batch > 0 ? opt.tiny_period : 0.f}, {"selection_strategy", opt.selection}, {"sched_ratio", opt.sched_ratio}, {"max_outstanding_jobs", opt.max_outstanding_jobs}, {"max_selections_per_pass", opt.max_selections}, {"max_pass_duration_us", opt.max_pass_duration}, {"trace", trace_info}, {"max_samples", opt.max_samples}, {"run_s", opt.run_s}, {"rates", opt.rates}, {"rotate", opt.rotate}, {"frames_dropped", frames_dropped}, {"deadline_classes", opt.deadline_classes}, {"frame_deadline", opt.frame_deadline}, {"buffer", opt.buffer}, {"catch_up", opt.catch_up}, {"threads", opt.single ? 1U : pool_threads}, {"worker_cpus", worker_cpus}, {"rt_prio", opt.rt_prio}, {"batch", opt.batch}, {"hw_threads", hw_threads}, {"chains", opt.chains}, {"order_mode", order_mode}, {"blocks_per_chain", chains[0].block_count}, {"frames", frames}, {"decoded_total", decoded_total}, {"missing_total", missing_total}, {"wrong_payload_total", opt.check ? json(wrong_total) : json(nullptr)}, {"deadline_misses_total", misses_total}, {"elapsed_s", elapsed_s}, {"feed", opt.feed}, {"pacer", pacer ? json{{"cpu", opt.pacer_cpu}, {"fifo_priority", opt.pacer_prio}, {"spin_us", opt.pacer_spin_us}, {"delay_ms", opt.pacer_delay_ms}, {"t0_ns", pacer->t0Ns()}, {"finished", pacer->finished()}, {"error", pacer->error()}} : json(nullptr)}, {"sink_stamp", pacer ? "call_end" : "per_packet"}, {"latency", {{"rate", opt.rate}, {"chunk", opt.chunk}, {"deadline_ms", opt.deadline_ms}, {"stamp_bias_max_us", pacer ? 0.0 : (opt.rate > 0 ? opt.chunk / opt.rate * 1e6 : 0.0)}, {"input", opt.input}}}, {"result", ok ? "ok" : (timed_out.load() ? "timeout" : "error")}, {"per_chain", per_chain}};
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
            // pacer mode: every receiver's clock is the pacer's t0; it stands in as the throttle's anchor too,
            // so readers of the nominal-arrival grid need not know which feed produced it
            const uint64_t anchor = pacer ? pacer->t0Ns() : cc.thr_start_ns;
            jchains.push_back({{"chain", k}, {"rate", chain_rate[k]}, {"max_samples", chain_max[k]}, {"frames", chain_frames[k]}, {"pacer_t0_ns", pacer ? json(pacer->t0Ns()) : json(nullptr)}, {"throttle_start_ns", anchor}, {"throttle_chunks", cc.thr_chunks}, {"throttle_max_chunks_per_call", cc.thr_max_chunks_per_call}, {"throttle_max_backlog_samples", cc.thr_max_backlog}, {"blocks", blocks}});
        }
        json meta = {{"app", "rx_latency4"}, {"run_dir", opt.run_dir}, {"input", opt.input}, {"out_dir", opt.out_dir}, {"feed", opt.feed}, {"policy", opt.policy}, {"policy_name", policy_name}, {"scheduler", sched_name}, {"threads", opt.single ? 1U : pool_threads}, {"worker_cpus", worker_cpus}, {"rt_prio", opt.rt_prio}, {"hw_threads", hw_threads}, {"rate", opt.rate}, {"chunk", opt.chunk}, {"fixed_batch", opt.fixed_batch}, {"batch", opt.batch}, {"buffer", opt.buffer}, {"tiny_period_s", opt.fixed_batch > 0 ? opt.tiny_period : 0.f}, {"selection_strategy", opt.selection}, {"sched_ratio", opt.sched_ratio}, {"max_outstanding_jobs", opt.max_outstanding_jobs}, {"max_pass_duration_us", opt.max_pass_duration}, {"frames", frames}, {"max_samples", opt.max_samples}, {"run_s", opt.run_s}, {"rates", opt.rates}, {"rotate", opt.rotate}, {"rm_true_periods", opt.policy == "rm" && !opt.rm_tiny_periods}, {"deadline_classes", opt.deadline_classes}, {"frame_deadline", opt.frame_deadline}, {"elapsed_s", elapsed_s}, {"result", ok ? "ok" : (timed_out.load() ? "timeout" : "error")}, {"trace", trace_info}, {"chains", jchains}, {"analysis", analysis}, {"diagnostics", diagnostics}};
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
