#include <gnuradio-4.0/Message.hpp>
#include <gnuradio-4.0/meta/formatter.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <chrono>
#include <iostream>
#include <vector>
#include <fstream>
#include <numeric>

using namespace gr;

#include <gnuradio-4.0/math/Math.hpp>

// 1. Count Source Block: Generates numbers from 1 to N
template<typename T>
struct CountSource : public gr::Block<CountSource<T>> {
    gr::PortOut<T> out;
    gr::Size_t     n_samples_max = 0;
    gr::Size_t     count = 0;

    GR_MAKE_REFLECTABLE(CountSource, out, n_samples_max);

    [[nodiscard]] constexpr T processOne() {
        count++;
        if (count >= n_samples_max) {
            this->requestStop();
        }
        return static_cast<T>(count);
    }
};

// 2. Null Sink Block: Discards data
struct NullSink : public gr::Block<NullSink> {
    gr::PortIn<float> in;
    GR_MAKE_REFLECTABLE(NullSink, in);

    constexpr void processOne(float) noexcept {}
};

int main() {
    std::vector<std::size_t> sample_sizes;
    for (int i = 1; i <= 20; i++) {
        sample_sizes.push_back(i * 2000000); // 2M, 4M, ..., 40M
    }

    std::ofstream csv("gr4_threshold_performance.csv");
    csv << "Samples Consumed,Avg Time (us),Items Produced\n";

    std::cout << "Starting GR4 C++ Profiling...\n";

    const int repetitions = 100;

    for (auto consume_amount : sample_sizes) {
        std::vector<double> times;
        
        for (int r = 0; r < repetitions; r++) {
            gr::Graph fg;
            
            auto& source = fg.emplaceBlock<CountSource<float>>({
                {"n_samples_max", consume_amount}
            });
            
            auto& math_block = fg.emplaceBlock<gr::blocks::math::MultiplyConst<float>>({
                {"value", 2.0f}
            });
            
            auto& sink = fg.emplaceBlock<NullSink>();

            fg.connect<"out", "in">(source, math_block);
            fg.connect<"out", "in">(math_block, sink);

            
            
            gr::scheduler::Simple<> scheduler;
            scheduler.exchange(std::move(fg));

            auto start = std::chrono::high_resolution_clock::now();
            scheduler.runAndWait();
            
            auto end = std::chrono::high_resolution_clock::now();
            
            double duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            times.push_back(duration_us);
        }

        double avg_time = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
        
        std::cout << "Sample size: " << consume_amount << "\n";
        std::cout << "  Avg items produced: " << consume_amount << "\n";
        std::cout << "  Avg execution time: " << avg_time << " us\n\n";

        csv << consume_amount << "," << avg_time << "," << consume_amount << "\n";
    }

    return 0;
}
