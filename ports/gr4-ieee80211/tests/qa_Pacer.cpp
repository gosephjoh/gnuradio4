#include <boost/ut.hpp>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

#include "gr4ieee80211/FileSourceRaw.hpp"
#include "gr4ieee80211/Pacer.hpp"
#include "gr4ieee80211/chain.hpp"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <thread>
#include <vector>

using namespace boost::ut;
using namespace gr4wifi;
using namespace std::chrono_literals;

namespace {

struct CollectSink : gr::Block<CollectSink> {
    using Description = gr::Doc<"keeps every sample it receives">;
    gr::PortIn<cf> in;
    GR_MAKE_REFLECTABLE(CollectSink, in);
    std::vector<cf>  _samples;
    gr::work::Status processBulk(gr::InputSpanLike auto& input) {
        _samples.insert(_samples.end(), input.begin(), input.end());
        return gr::work::Status::OK;
    }
};

// a raw complex64 file whose sample i is (i, -i)
std::string writeStimulus(std::size_t n) {
    const auto path = (std::filesystem::temp_directory_path() / std::format("qa_Pacer_{}.cf32", ::getpid())).string();
    std::FILE* f    = std::fopen(path.c_str(), "wb");
    for (std::size_t i = 0; i < n; ++i) {
        const cf v{static_cast<float>(i), -static_cast<float>(i)};
        std::fwrite(&v, sizeof(v), 1, f);
    }
    std::fclose(f);
    return path;
}

// runs `graph` to completion, stopping it after `limit` should it not end by itself
template<typename TSched>
bool runWithLimit(TSched& sched, std::chrono::milliseconds limit) {
    std::atomic<bool> done{false}, timedOut{false};
    std::thread       watchdog([&] {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (!done && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(5ms);
        }
        if (!done) {
            timedOut = true;
            sched.requestStop();
        }
    });
    const bool        ok = sched.runAndWait().has_value();
    done                 = true;
    watchdog.join();
    return ok && !timedOut;
}

constexpr unsigned kBatch = 1024U;

} // namespace

const boost::ut::suite<"Pacer"> pacerTests = [] {
    "the replay is FileSourceRaw's, padding included, and end of stream ends the graph"_test = [] {
        const std::string path       = writeStimulus(50'000UZ);
        const uint64_t    maxSamples = 40'000ULL; // fewer than the file holds: the cut, then the padding

        std::vector<cf> fromFileSource;
        {
            gr::Graph graph;
            auto&     src  = graph.emplaceBlock<FileSourceRaw<cf>>({{"file_name", path}, {"max_items", maxSamples}, {"pad_to_multiple", gr::Size_t{kBatch}}, {"pad_min_tail", gr::Size_t(fixedBatchFlushTail(kBatch))}});
            auto&     sink = graph.emplaceBlock<CollectSink>();
            expect(graph.connect<"out", "in">(src, sink).has_value());
            gr::scheduler::Simple<> sched;
            expect(sched.exchange(std::move(graph)).has_value());
            expect(runWithLimit(sched, 10s)) << "the file source must end the graph";
            fromFileSource = sink._samples;
        }

        std::vector<cf> fromPacer;
        {
            Pacer pacer(PacerSettings{.chunk = kBatch, .padToMultiple = kBatch, .padMinTail = fixedBatchFlushTail(kBatch), .buffer = 16UZ * kBatch, .spinNs = 0, .startDelayNs = 1'000'000});
            expect(pacer.open(path).has_value());
            gr::Graph graph;
            auto&     entry = graph.emplaceBlock<gr::testing::Copy<cf>>();
            auto&     sink  = graph.emplaceBlock<CollectSink>();
            expect(graph.connect<"out", "in">(entry, sink).has_value());
            PacerStream& stream = pacer.addStream(50e6, maxSamples);
            expect(stream.out.connect(entry.in).has_value());
            gr::scheduler::Simple<> sched;
            expect(sched.exchange(std::move(graph)).has_value());
            pacer.start([&sched] { return sched.state() == gr::lifecycle::State::RUNNING; });
            expect(runWithLimit(sched, 10s)) << "the pacer's end-of-stream tag must end the graph";
            pacer.stop();
            expect(pacer.finished());
            fromPacer = sink._samples;
            expect(eq(stream.total % kBatch, 0ULL)) << "padded to whole batches";
        }
        std::filesystem::remove(path);

        expect(eq(fromPacer.size(), fromFileSource.size())) << "the same number of samples, the padding included";
        expect(fromPacer == fromFileSource) << "the same samples in the same order";
        expect(eq(fromFileSource.size() % kBatch, 0UZ));
        expect(fromFileSource.size() >= maxSamples + fixedBatchFlushTail(kBatch));
    };

    "chunk j is due when its last sample is due, the short last chunk included"_test = [] {
        const std::string path = writeStimulus(2'500UZ);
        Pacer             pacer(PacerSettings{.chunk = 1000UZ});
        expect(pacer.open(path).has_value());
        const PacerStream& stream = pacer.addStream(1e6, 0ULL); // 1 Msps: a sample is 1 us
        std::filesystem::remove(path);

        expect(eq(stream.total, 2'500ULL)) << "no padding outside fixed-batch mode";
        expect(eq(stream.log.size(), 3UZ));
        expect(eq(stream.log[0].dueNs, 1'000'000ULL));
        expect(eq(stream.log[1].dueNs, 2'000'000ULL));
        expect(eq(stream.log[2].dueNs, 2'500'000ULL)) << "the last chunk holds 500 samples and is due when they are";
        expect(eq(stream.chunkOf(999ULL), 0UZ));
        expect(eq(stream.chunkOf(1'000ULL), 1UZ));
    };

    // A receiver whose entry buffer stays full must not hold up another. Deterministic: the first
    // receiver's reader never consumes, so the first stays blocked for good, and the second is either
    // written in full or -- as when one full buffer stalled the pacer's single thread -- not at all.
    "a full entry buffer delays only its own receiver"_test = [] {
        const std::string path = writeStimulus(400'000UZ);
        Pacer             pacer(PacerSettings{.chunk = kBatch, .buffer = 16UZ * kBatch, .spinNs = 0, .startDelayNs = 0});
        expect(pacer.open(path).has_value());
        PacerStream&   stuck = pacer.addStream(100e6, 400'000ULL);   // outgrows its buffer within milliseconds
        PacerStream&   fine  = pacer.addStream(10e6, 8ULL * kBatch); // fits its buffer whole
        gr::PortIn<cf> neverRead, alsoUnread;
        expect(stuck.out.connect(neverRead).has_value());
        expect(fine.out.connect(alsoUnread).has_value());

        pacer.start([] { return true; });
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::atomic_ref(fine.log.back().writtenNs).load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        pacer.stop();
        std::filesystem::remove(path);

        expect(std::ranges::all_of(fine.log, [](const PacerStream::Chunk& c) { return c.writtenNs != 0; })) << "every chunk of the receiver that keeps up is written";
        expect(fine.log.front().retries == 0U) << "and never had to wait";
        expect(stuck.log.back().writtenNs == 0ULL) << "the stuck receiver stays stuck";
        expect(std::ranges::any_of(stuck.log, [](const PacerStream::Chunk& c) { return c.retries > 0U; })) << "and its waits are counted";
        expect(!pacer.finished()) << "a stopped pacer has not finished its replay";
    };
};

int main() { /* not needed for UT */ }
