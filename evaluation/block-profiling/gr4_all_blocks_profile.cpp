#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>
#include <gnuradio-4.0/testing/Delay.hpp>
#include <gnuradio-4.0/math/Math.hpp>
#include <gnuradio-4.0/math/Rotator.hpp>
#include <gnuradio-4.0/filter/time_domain_filter.hpp>
#include <gnuradio-4.0/filter/SavitzkyGolayFilter.hpp>
#include <gnuradio-4.0/algorithm/filter/FilterTool.hpp>

#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <string>
#include <iomanip>
#include <numeric>
#include <algorithm>
#include <complex>

using namespace gr;

// Custom Null Sink for float
struct NullSinkFloat : public gr::Block<NullSinkFloat> {
    gr::PortIn<float> in;
    GR_MAKE_REFLECTABLE(NullSinkFloat, in);
    constexpr void processOne(float) noexcept {}
};

// Custom Null Sink for std::complex<float>
struct NullSinkComplex : public gr::Block<NullSinkComplex> {
    gr::PortIn<std::complex<float>> in;
    GR_MAKE_REFLECTABLE(NullSinkComplex, in);
    constexpr void processOne(std::complex<float>) noexcept {}
};

// Helper template to profile a float block
template<typename TBlock>
void profile_float_block(
    const std::string& block_name,
    const property_map& block_params,
    const std::vector<size_t>& sample_sizes,
    int repetitions,
    std::ofstream& csv
) {
    std::cout << "Profiling float block: " << block_name << "...\n" << std::flush;

    for (size_t sample_count : sample_sizes) {
        std::vector<double> times_us;
        times_us.reserve(repetitions);

        for (int r = 0; r < repetitions; ++r) {
            gr::Graph fg;

            auto& source = fg.emplaceBlock<gr::testing::CountingSource<float>>({
                {"n_samples_max", sample_count}
            });

            auto& target_block = fg.emplaceBlock<TBlock>(block_params);
            auto& sink = fg.emplaceBlock<NullSinkFloat>();

            fg.connect<"out", "in">(source, target_block);
            fg.connect<"out", "in">(target_block, sink);

            gr::scheduler::Simple<> scheduler;
            scheduler.exchange(std::move(fg));

            auto start = std::chrono::high_resolution_clock::now();
            scheduler.runAndWait();
            auto end = std::chrono::high_resolution_clock::now();

            double duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            times_us.push_back(duration_us);
        }

        double avg_time_us = std::accumulate(times_us.begin(), times_us.end(), 0.0) / times_us.size();
        double msps = (sample_count / (avg_time_us / 1'000'000.0)) / 1'000'000.0;

        csv << "\"" << block_name << "\"," << sample_count << ","
            << std::fixed << std::setprecision(2) << avg_time_us << ","
            << std::setprecision(4) << msps << "\n";
    }
}

// Helper template to profile a complex<float> block
template<typename TBlock>
void profile_complex_block(
    const std::string& block_name,
    const property_map& block_params,
    const std::vector<size_t>& sample_sizes,
    int repetitions,
    std::ofstream& csv
) {
    std::cout << "Profiling complex block: " << block_name << "...\n" << std::flush;

    for (size_t sample_count : sample_sizes) {
        std::vector<double> times_us;
        times_us.reserve(repetitions);

        for (int r = 0; r < repetitions; ++r) {
            gr::Graph fg;

            auto& source = fg.emplaceBlock<gr::testing::CountingSource<std::complex<float>>>({
                {"n_samples_max", sample_count}
            });

            auto& target_block = fg.emplaceBlock<TBlock>(block_params);
            auto& sink = fg.emplaceBlock<NullSinkComplex>();

            fg.connect<"out", "in">(source, target_block);
            fg.connect<"out", "in">(target_block, sink);

            gr::scheduler::Simple<> scheduler;
            scheduler.exchange(std::move(fg));

            auto start = std::chrono::high_resolution_clock::now();
            scheduler.runAndWait();
            auto end = std::chrono::high_resolution_clock::now();

            double duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            times_us.push_back(duration_us);
        }

        double avg_time_us = std::accumulate(times_us.begin(), times_us.end(), 0.0) / times_us.size();
        double msps = (sample_count / (avg_time_us / 1'000'000.0)) / 1'000'000.0;

        csv << "\"" << block_name << "\"," << sample_count << ","
            << std::fixed << std::setprecision(2) << avg_time_us << ","
            << std::setprecision(4) << msps << "\n";
    }
}

int main() {
    std::vector<size_t> sample_sizes;
    for (size_t i = 1; i <= 10; ++i) {
        sample_sizes.push_back(i * 2'000'000); // 2M, 4M, 6M, ..., 20M samples
    }

    const int repetitions = 20;

    std::ofstream csv("gr4_all_blocks_profiling.csv");
    csv << "Block_Name,Sample_Size,Avg_Time_us,Throughput_MSps\n";

    std::cout << "=====================================================================\n";
    std::cout << "  GNU Radio 4.0 Full Codebase Block Execution Profiler               \n";
    std::cout << "=====================================================================\n\n";

    // --- 1. MATH DOMAIN BLOCKS (Float) ---
    profile_float_block<gr::blocks::math::MultiplyConst<float>>(
        "MultiplyConst<float>", {{"value", 2.0f}}, sample_sizes, repetitions, csv);

    profile_float_block<gr::blocks::math::AddConst<float>>(
        "AddConst<float>", {{"value", 1.5f}}, sample_sizes, repetitions, csv);

    profile_float_block<gr::blocks::math::SubtractConst<float>>(
        "SubtractConst<float>", {{"value", 0.5f}}, sample_sizes, repetitions, csv);

    profile_float_block<gr::blocks::math::DivideConst<float>>(
        "DivideConst<float>", {{"value", 3.0f}}, sample_sizes, repetitions, csv);

    // --- 2. MATH DOMAIN BLOCKS (Complex Float) ---
    profile_complex_block<gr::blocks::math::MultiplyConst<std::complex<float>>>(
        "MultiplyConst<complex<float>>", {{"value", std::complex<float>{1.5f, 0.5f}}}, sample_sizes, repetitions, csv);

    profile_complex_block<gr::blocks::math::AddConst<std::complex<float>>>(
        "AddConst<complex<float>>", {{"value", std::complex<float>{1.0f, 2.0f}}}, sample_sizes, repetitions, csv);

    profile_complex_block<gr::blocks::math::Rotator<std::complex<float>>>(
        "Rotator<complex<float>>", {{"phase_increment", 0.05f}}, sample_sizes, repetitions, csv);

    // --- 3. FILTER DOMAIN BLOCKS ---
    const float samp_rate = 192000.0f;
    gr::filter::FilterParameters lpf_params{
        .fLow = 6000.0,
        .attenuationDb = 60.0,
        .fs = samp_rate
    };
    auto taps_float = gr::filter::fir::designFilter<float>(gr::filter::Type::LOWPASS, lpf_params);

    profile_float_block<gr::filter::fir_filter<float>>(
        "fir_filter<float>", {{"b", taps_float.b}}, sample_sizes, repetitions, csv);

    profile_float_block<gr::filter::iir_filter<float, gr::filter::IIRForm::DF_I>>(
        "iir_filter<float>", {{"b", std::vector<float>{0.1f, 0.2f, 0.1f}}, {"a", std::vector<float>{1.0f, -0.5f, 0.1f}}}, sample_sizes, repetitions, csv);

    profile_float_block<gr::filter::Decimator<float>>(
        "Decimator<float>", {{"decim", 2UZ}}, sample_sizes, repetitions, csv);

    profile_float_block<gr::filter::SavitzkyGolayFilter<float>>(
        "SavitzkyGolayFilter<float>", {{"window_size", 11UZ}, {"poly_order", 4UZ}}, sample_sizes, repetitions, csv);

    // --- 4. TESTING / UTILITY BLOCKS ---
    profile_float_block<gr::testing::Delay<float>>(
        "Delay<float>", {{"delay_ms", 0U}}, sample_sizes, repetitions, csv);

    csv.close();
    std::cout << "\nFull codebase profiling results successfully saved to gr4_all_blocks_profiling.csv\n";

    return 0;
}
