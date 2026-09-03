#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>
#include <gnuradio-4.0/math/Math.hpp>
#include <gnuradio-4.0/filter/time_domain_filter.hpp>
#include <gnuradio-4.0/algorithm/filter/FilterTool.hpp>

#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <string>
#include <iomanip>
#include <numeric>
#include <algorithm>
#include <random>
#include <cmath>
#include <memory>

using namespace gr;
using namespace gr::scheduler;

struct RunResult {
    size_t run_id;
    size_t batch_size;
    size_t block_count;
    double duration_ms;
    double msps;
};

struct SummaryStats {
    size_t batch_size;
    double mean_ms;
    double median_ms;
    double min_ms;
    double max_ms;
    double stddev_ms;
    double mean_msps;
    double median_msps;
    double avg_block_count;
};

SummaryStats calculate_stats(size_t batch_size, const std::vector<RunResult>& results) {
    size_t n = results.size();
    if (n == 0) return {.batch_size = batch_size};

    std::vector<double> times_ms(n);
    std::vector<double> msps_vals(n);
    double sum_ms = 0.0;
    double sum_msps = 0.0;
    double sum_blocks = 0.0;

    for (size_t i = 0; i < n; ++i) {
        times_ms[i] = results[i].duration_ms;
        msps_vals[i] = results[i].msps;
        sum_ms += results[i].duration_ms;
        sum_msps += results[i].msps;
        sum_blocks += results[i].block_count;
    }

    double mean_ms = sum_ms / n;
    double mean_msps = sum_msps / n;

    std::sort(times_ms.begin(), times_ms.end());
    std::sort(msps_vals.begin(), msps_vals.end());

    double median_ms = (n % 2 == 0) ? (times_ms[n/2 - 1] + times_ms[n/2]) / 2.0 : times_ms[n/2];
    double median_msps = (n % 2 == 0) ? (msps_vals[n/2 - 1] + msps_vals[n/2]) / 2.0 : msps_vals[n/2];

    double variance_sq = 0.0;
    for (double t : times_ms) {
        variance_sq += (t - mean_ms) * (t - mean_ms);
    }
    double stddev_ms = std::sqrt(variance_sq / n);

    return SummaryStats{
        .batch_size = batch_size,
        .mean_ms = mean_ms,
        .median_ms = median_ms,
        .min_ms = times_ms.front(),
        .max_ms = times_ms.back(),
        .stddev_ms = stddev_ms,
        .mean_msps = mean_msps,
        .median_msps = median_msps,
        .avg_block_count = sum_blocks / n
    };
}

// Builds a 100% valid Random Directed Acyclic Graph (DAG) with configured edge buffer batch_size
gr::Graph build_random_dag(uint32_t seed, size_t sample_count, const std::vector<float>& taps_b, size_t batch_size, size_t& out_block_count) {
    std::mt19937 rng(seed);

    std::uniform_int_distribution<size_t> dist_sources(3, 4);
    size_t num_sources = dist_sources(rng);

    std::uniform_int_distribution<size_t> dist_interm(7, 10);
    size_t num_intermediate = dist_interm(rng);

    size_t num_sinks = num_intermediate;
    size_t total_blocks = num_sources + num_intermediate + num_sinks;
    out_block_count = total_blocks;

    gr::Graph fg;

    std::vector<std::shared_ptr<gr::BlockModel>> sources;
    std::vector<std::shared_ptr<gr::BlockModel>> intermediate;
    std::vector<std::shared_ptr<gr::BlockModel>> sinks;

    sources.reserve(num_sources);
    intermediate.reserve(num_intermediate);
    sinks.reserve(num_sinks);

    // 1. Emplace Sources
    for (size_t i = 0; i < num_sources; ++i) {
        auto& src = fg.emplaceBlock<gr::testing::CountingSource<float>>({{"n_samples_max", sample_count}});
        sources.push_back(gr::graph::findBlock(fg, src).value());
    }

    // 2. Emplace Intermediate Processing Blocks
    std::uniform_int_distribution<int> dist_type(0, 2);
    for (size_t i = 0; i < num_intermediate; ++i) {
        int btype = dist_type(rng);
        if (btype == 0) {
            auto& blk = fg.emplaceBlock<gr::filter::fir_filter<float>>({{"b", taps_b}});
            intermediate.push_back(gr::graph::findBlock(fg, blk).value());
        } else if (btype == 1) {
            auto& blk = fg.emplaceBlock<gr::blocks::math::MultiplyConst<float>>({{"value", 1.5f + static_cast<float>(i)}});
            intermediate.push_back(gr::graph::findBlock(fg, blk).value());
        } else {
            auto& blk = fg.emplaceBlock<gr::blocks::math::AddConst<float>>({{"value", 0.5f + static_cast<float>(i)}});
            intermediate.push_back(gr::graph::findBlock(fg, blk).value());
        }
    }

    // 3. Emplace Sinks
    for (size_t i = 0; i < num_sinks; ++i) {
        auto& snk = fg.emplaceBlock<gr::testing::NullSink<float>>();
        sinks.push_back(gr::graph::findBlock(fg, snk).value());
    }

    const gr::PortDefinition pOut("out");
    const gr::PortDefinition pIn("in");
    const gr::EdgeParameters edgeParams{.minBufferSize = batch_size};

    std::vector<std::shared_ptr<gr::BlockModel>> available_upstream = sources;

    // Connect input of each intermediate block with edge batch_size
    for (size_t i = 0; i < num_intermediate; ++i) {
        std::uniform_int_distribution<size_t> dist_up(0, available_upstream.size() - 1);
        size_t up_idx = dist_up(rng);

        std::ignore = fg.connect(available_upstream[up_idx], pOut, intermediate[i], pIn, edgeParams);
        available_upstream.push_back(intermediate[i]);
    }

    // Connect output of each intermediate block to its dedicated NullSink with edge batch_size
    for (size_t i = 0; i < num_intermediate; ++i) {
        std::ignore = fg.connect(intermediate[i], pOut, sinks[i], pIn, edgeParams);
    }

    return fg;
}

template<typename TScheduler>
std::vector<RunResult> run_batching_benchmark_series(
    const std::string& name,
    size_t repetitions,
    size_t sample_count,
    const std::vector<float>& taps_b,
    size_t batch_size,
    uint32_t base_seed,
    std::ofstream& csv_file
) {
    std::vector<RunResult> results;
    results.reserve(repetitions);

    for (size_t r = 0; r < repetitions; ++r) {
        uint32_t current_seed = base_seed + static_cast<uint32_t>(r * 17);
        size_t block_count = 0;

        gr::Graph fg = build_random_dag(current_seed, sample_count, taps_b, batch_size, block_count);

        TScheduler sched;
        sched.exchange(std::move(fg));

        auto start = std::chrono::high_resolution_clock::now();
        sched.runAndWait();
        auto end = std::chrono::high_resolution_clock::now();

        double duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0;
        double msps = (sample_count / (duration_ms / 1000.0)) / 1'000'000.0;

        results.push_back({r + 1, batch_size, block_count, duration_ms, msps});

        csv_file << r + 1 << "," << batch_size << "," << block_count << ",\"" << name << "\","
                 << std::fixed << std::setprecision(4) << duration_ms << "," << msps << "\n";
    }

    return results;
}

int main(int argc, char* argv[]) {
    size_t repetitions_per_batch = 50;
    if (argc > 1) {
        repetitions_per_batch = static_cast<size_t>(std::atoi(argv[1]));
    }

    const size_t sample_count = 1'500'000;
    const float samp_rate = 192000.0f;
    const uint32_t base_seed = 42;

    std::vector<size_t> batch_sizes = {256, 1024, 8192, 32768, 131072};

    std::cout << "===================================================================================\n";
    std::cout << "  GNU Radio 4.0 Batching & Buffer Size Sweep Benchmark across 5 Schedulers  \n";
    std::cout << "  Sweep Batch Sizes: 256, 1024, 8192, 32768, 131072 samples/batch  \n";
    std::cout << "===================================================================================\n\n";

    gr::filter::FilterParameters lpf_params{
        .fLow = 6000.0,
        .attenuationDb = 60.0,
        .fs = samp_rate
    };
    auto taps = gr::filter::fir::designFilter<float>(gr::filter::Type::LOWPASS, lpf_params);

    std::ofstream csv_file("gr4_batching_sweep_benchmark.csv");
    csv_file << "Run_ID,Batch_Size,Block_Count,Scheduler_Name,Execution_Time_ms,Throughput_MSps\n";

    using Policy = ExecutionPolicy;

    struct PolicySweep {
        std::string name;
        std::vector<SummaryStats> batch_stats;
    };

    std::vector<PolicySweep> sweep_results;

    // Helper macro to run sweep for a scheduler
    auto sweep_policy = [&](const std::string& name, auto sched_tag) {
        using SchedType = decltype(sched_tag);
        std::cout << "\n------------------------------------------------------------\n";
        std::cout << "Running Batch Size Sweep for: " << name << "\n";
        std::cout << "------------------------------------------------------------\n";

        PolicySweep ps{.name = name};

        for (size_t bs : batch_sizes) {
            std::cout << "  -> Testing Batch Size: " << std::setw(6) << bs << " samples... " << std::flush;
            auto res = run_batching_benchmark_series<SchedType>(
                name, repetitions_per_batch, sample_count, taps.b, bs, base_seed, csv_file);
            auto stats = calculate_stats(bs, res);
            ps.batch_stats.push_back(stats);
            std::cout << "Mean: " << std::fixed << std::setprecision(2) << stats.mean_ms << " ms | "
                      << std::setprecision(2) << stats.mean_msps << " MSps\n";
        }
        sweep_results.push_back(ps);
    };

    sweep_policy("Simple (Single-Threaded)", Simple<Policy::singleThreaded>{});
    sweep_policy("Simple (Multi-Threaded)", Simple<Policy::multiThreaded>{});
    sweep_policy("Breadth-First (Single-Threaded)", BreadthFirst<Policy::singleThreaded>{});
    sweep_policy("Depth-First (Single-Threaded)", DepthFirst<Policy::singleThreaded>{});
    sweep_policy("Simple (Single-Threaded Blocking)", Simple<Policy::singleThreadedBlocking>{});

    csv_file.close();
    std::cout << "\nRaw sweep data written to gr4_batching_sweep_benchmark.csv\n\n";

    std::cout << "===================================================================================================================\n";
    std::cout << "  BATCH SIZE SWEEP THROUGHPUT COMPARISON (MSps)  \n";
    std::cout << "===================================================================================================================\n";
    std::cout << std::left << std::setw(36) << "Scheduler Policy"
              << std::setw(14) << "256 samples"
              << std::setw(14) << "1024 samples"
              << std::setw(14) << "8192 samples"
              << std::setw(14) << "32768 samples"
              << std::setw(14) << "131072 samples" << "\n";
    std::cout << "-------------------------------------------------------------------------------------------------------------------\n";

    for (const auto& ps : sweep_results) {
        std::cout << std::left << std::setw(36) << ps.name;
        for (const auto& st : ps.batch_stats) {
            std::cout << std::setw(14) << (std::to_string(st.mean_msps).substr(0, 6) + " MSps");
        }
        std::cout << "\n";
    }
    std::cout << "===================================================================================================================\n";

    return 0;
}
