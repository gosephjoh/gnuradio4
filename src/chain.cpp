#include "gr4ieee80211/chain.hpp"

#include <gnuradio-4.0/basic/ConverterBlocks.hpp>

#include "gr4ieee80211/DecodeMac.hpp"
#include "gr4ieee80211/Fft64.hpp"
#include "gr4ieee80211/FileSourceRaw.hpp"
#include "gr4ieee80211/FrameEqualizer.hpp"
#include "gr4ieee80211/SyncLong.hpp"
#include "gr4ieee80211/SyncShort.hpp"
#include "gr4ieee80211/basic_blocks.hpp"

#include <stdexcept>
#include <time.h>

namespace gr4wifi {

uint64_t monotonic_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

namespace {
void must(std::expected<void, gr::Error> r, const char* what) {
    if (!r) {
        throw std::runtime_error(std::string("connect failed: ") + what + ": " + r.error().message);
    }
}
} // namespace

ChainBlocks buildChain(gr::Graph& g, const ChainConfig& cfg) {
    using namespace gr;
    const auto nm = [&](const char* s) { return std::string(cfg.prefix) + s; };
    ChainBlocks cb;

    // GR4's BasicFileSource delivered no samples on this tree under either
    // scheduler (apps/probe.cpp modes "fsrc"); FileSourceRaw is GR3's
    // file_source semantics in a dozen lines.
    auto& src = g.emplaceBlock<FileSourceRaw<cf>>({{"name", nm("fsrc")}, {"file_name", cfg.input}});
    cb.block_count++;

    // the stock GR3 front end, block for block
    auto& mag2   = g.emplaceBlock<MagSquared>({{"name", nm("mag2")}});
    auto& avgpow = g.emplaceBlock<MovingAverage<float>>({{"name", nm("avgpow")}, {"length", Size_t(64)}, {"max_iter", Size_t(4000)}});
    auto& dly16  = g.emplaceBlock<SampleDelay<cf>>({{"name", nm("dly16")}, {"delay", Size_t(16)}});
    auto& conj   = g.emplaceBlock<Conjugate>({{"name", nm("conj")}});
    auto& mult   = g.emplaceBlock<Multiply2<cf>>({{"name", nm("mul")}});
    auto& avgcor = g.emplaceBlock<MovingAverage<cf>>({{"name", nm("avgcor")}, {"length", Size_t(48)}, {"max_iter", Size_t(4000)}});
    auto& mag    = g.emplaceBlock<blocks::type::converter::Abs<cf>>({{"name", nm("mag")}});
    auto& div    = g.emplaceBlock<Divide2<float>>({{"name", nm("div")}});
    auto& ss     = g.emplaceBlock<SyncShort>({{"name", nm("syncshort")}, {"threshold", cfg.sensitivity}, {"min_plateau", Size_t(2)}});
    auto& dly320 = g.emplaceBlock<SampleDelay<cf>>({{"name", nm("dly320")}, {"delay", Size_t(320)}});
    auto& sl     = g.emplaceBlock<SyncLong>({{"name", nm("synclong")}, {"sync_length", Size_t(320)}});
    auto& fft    = g.emplaceBlock<Fft64>({{"name", nm("fft")}});
    auto& eq     = g.emplaceBlock<FrameEqualizer>({{"name", nm("eq")}, {"freq", cfg.frequency}, {"bw", cfg.bandwidth}});
    auto& dec    = g.emplaceBlock<DecodeMac>({{"name", nm("decode")}});
    cb.block_count += 14;

    auto& stamper = g.emplaceBlock<ArrivalStamper>({{"name", nm("stamp")}});
    auto& sink    = g.emplaceBlock<LatencySink>({{"name", nm("latsink")}});
    cb.block_count += 2;

    // feed = throttle output, or the file source when unthrottled
    if (cfg.rate > 0) {
        auto& thr = g.emplaceBlock<Throttle<cf>>({{"name", nm("throttle")}, {"sample_rate", static_cast<float>(cfg.rate)}, {"chunk_size", Size_t(cfg.chunk)}});
        cb.block_count++;
        must(g.connect<"out", "in">(src, thr), "src->throttle");
        must(g.connect<"out", "in">(thr, mag2), "throttle->mag2");
        must(g.connect<"out", "in">(thr, dly16), "throttle->dly16");
        must(g.connect<"out", "in0">(thr, mult), "throttle->mul.in0");
        must(g.connect<"out", "in">(thr, stamper), "throttle->stamper");
    } else {
        must(g.connect<"out", "in">(src, mag2), "src->mag2");
        must(g.connect<"out", "in">(src, dly16), "src->dly16");
        must(g.connect<"out", "in0">(src, mult), "src->mul.in0");
        must(g.connect<"out", "in">(src, stamper), "src->stamper");
    }
    must(g.connect<"out", "in">(mag2, avgpow), "mag2->avgpow");
    must(g.connect<"out", "in1">(avgpow, div), "avgpow->div.in1");
    must(g.connect<"out", "in">(dly16, conj), "dly16->conj");
    must(g.connect<"out", "in1">(conj, mult), "conj->mul.in1");
    must(g.connect<"out", "in">(mult, avgcor), "mul->avgcor");
    must(g.connect<"out", "in">(avgcor, mag), "avgcor->mag");
    must(g.connect<"abs", "in0">(mag, div), "mag->div.in0");
    must(g.connect<"out", "in_abs">(avgcor, ss), "avgcor->ss.in_abs");
    must(g.connect<"out", "in">(dly16, ss), "dly16->ss.in");
    must(g.connect<"out", "in_cor">(div, ss), "div->ss.in_cor");
    must(g.connect<"out", "in">(ss, dly320), "ss->dly320");
    must(g.connect<"out", "in_delayed">(dly320, sl), "dly320->sl.in_delayed");
    must(g.connect<"out", "in">(ss, sl), "ss->sl.in");
    must(g.connect<"out", "in">(sl, fft), "sl->fft");
    must(g.connect<"out", "in">(fft, eq), "fft->eq");
    must(g.connect<"out", "in">(eq, dec), "eq->decode");
    must(g.connect<"out", "in">(dec, sink), "decode->latsink");

    if (cfg.record) {
        auto& pdu = g.emplaceBlock<PduRecorder>({{"name", nm("pdurec")}});
        auto& sym = g.emplaceBlock<SymbolsRecorder>({{"name", nm("symrec")}});
        cb.block_count += 2;
        pdu._bin_path = cfg.out_dir + "/rx_pdus.bin";
        pdu._csv_path = cfg.out_dir + "/rx_log.csv";
        sym._bin_path = cfg.out_dir + "/rx_symbols.cf32";
        sym._csv_path = cfg.out_dir + "/rx_symbols.csv";
        must(g.connect<"out", "in">(dec, pdu), "decode->pdurec");
        must(g.connect<"symbols", "in">(eq, sym), "eq.symbols->symrec");
        cb.pdu = &pdu;
        cb.sym = &sym;
    }

    cb.stamper  = &stamper;
    cb.sink     = &sink;
    cb.counters = [&ss, &sl, &eq, &dec, &avgpow, &dly16, &avgcor, &dly320, &fft]() {
        ChainCounters c;
        c.sync_short_detections = ss._frames_detected;
        c.sync_long_frames      = sl._frames_aligned;
        c.signal_ok             = eq._signal_ok;
        c.signal_bad            = eq._signal_bad;
        c.frames_started        = dec._frames_started;
        c.frames_decoded        = dec._frames_decoded;
        c.crc_failed            = dec._crc_failed;
        c.too_large             = dec._too_large;
        c.items_avgpow = avgpow._items; c.items_dly16 = dly16._items; c.items_avgcor = avgcor._items; c.items_ss = ss._items; c.items_dly320 = dly320._items;
        c.items_sl = sl._items; c.items_fft = fft._items; c.items_eq = eq._items; c.items_dec = dec._items; c.calls_ss = ss._calls; c.calls_sl = sl._calls; c.max_cor = ss._max_cor;
        return c;
    };
    return cb;
}

} // namespace gr4wifi
