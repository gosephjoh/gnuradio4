#include "gr4ieee80211/chain.hpp"

#include <gnuradio-4.0/basic/ConverterBlocks.hpp>

#include "gr4ieee80211/DecodeMac.hpp"
#include "gr4ieee80211/Fft64.hpp"
#include "gr4ieee80211/FileSourceRaw.hpp"
#include "gr4ieee80211/FrameEqualizer.hpp"
#include "gr4ieee80211/SyncLong.hpp"
#include "gr4ieee80211/SyncShort.hpp"
#include "gr4ieee80211/basic_blocks.hpp"

#include <algorithm>
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
    const unsigned N = cfg.fixed_batch;
    // every block gets the same per-invocation batch ceiling (0 = GR4's
    // default, unbounded); with fixed batching the file source and the
    // throttle are left unbounded so they can run ahead of / catch up with
    // the N-sample pipeline behind them
    auto S = [&](gr::property_map m, bool ceiling = true) {
        if (cfg.batch > 0 && ceiling) {
            m.insert_or_assign("max_batch_size", Size_t(cfg.batch));
        }
        return m;
    };
    // record a block's GR4 unique name against our role name
    auto R = [&](auto& blk, const char* role) { cb.roles.emplace_back(std::string(std::string_view(blk.unique_name)), role); };
    // fixed batching: an input port requires exactly N samples per call
    auto F = [&](auto& port) {
        if (N > 0) {
            port.min_samples = N;
            port.max_samples = N;
        }
    };
    // GR3's moving_average processes at most max_iter items per call; a
    // fixed batch must fit in one call or the batch alignment is lost
    const Size_t max_iter = std::max<Size_t>(4000U, N);
    // Timing parameters in fixed-batch mode.  GR4 derives period = N/rate for
    // every block and its release gate treats the period as a *minimum
    // separation between releases* (sporadic model, RT reference 4.4): a block
    // then admits at most one batch per batch period and can never catch up
    // after any hiccup -- under EDF two of the throttle's readers sat a
    // constant fifteen batches behind it and the throttle's buffer stayed
    // full.  So every block gets a tiny explicit `period` (the temporal gate
    // never binds; releases are data-driven) and an explicit
    // `relative_deadline` of one batch period, which is what the implicit
    // deadline would have been.  The source, throttle and stamper keep a
    // deadline as short as their period so EDF runs them first, and their
    // periods are staggered so RateMonotonic ranks them source > throttle >
    // stamper > the rest (equal periods tie).  docs/batch-rt-experiments.md 6.
    const float tp = cfg.tiny_period;
    const float batch_period_s = (N > 0 && cfg.rate > 0) ? static_cast<float>(N) / static_cast<float>(cfg.rate) : 0.f;
    auto T = [&](gr::property_map m, float period, float deadline) {
        if (N > 0 && tp > 0.f) {
            m.insert_or_assign("period", period);
            if (deadline > 0.f) { m.insert_or_assign("relative_deadline", deadline); }
        }
        return m;
    };
    auto TP = [&](gr::property_map m) { return T(std::move(m), tp, batch_period_s); }; // a pipeline block

    // GR4's BasicFileSource delivered no samples on this tree under either
    // scheduler (apps/probe.cpp modes "fsrc"); FileSourceRaw is GR3's
    // file_source semantics in a dozen lines.
    gr::property_map srcm{{"name", nm("fsrc")}, {"file_name", cfg.input}};
    if (cfg.declare_rate && cfg.rate > 0) { srcm.insert_or_assign("sample_rate", static_cast<float>(cfg.rate)); }
    if (N > 0) {
        srcm.insert_or_assign("pad_to_multiple", Size_t(N));
        srcm.insert_or_assign("pad_min_tail", Size_t(std::max(3U * N, 2560U))); // the fixed-batch flush tail (FileSourceRaw.hpp)
    }
    auto& src = g.emplaceBlock<FileSourceRaw<cf>>(S(T(srcm, 0.25f * tp, 0.f), N == 0));
    cb.block_count++;
    R(src, "fsrc");

    // Graph order is the round-robin sweep order and the striping order, so
    // it follows the data: source, throttle, arrival stamper, then the chain.
    // (The throttle used to be emplaced last: it then ran at the end of every
    // pass and every batch paid one extra pass; the stamper ran after the
    // decoder and stamped a frame's arrival after its decode.)
    Throttle<cf>* thrp = nullptr;
    gr::EdgeParameters ep{};
    if (cfg.buffer > 0) { ep.minBufferSize = cfg.buffer; }
    if (cfg.rate > 0) {
        gr::property_map thrm{{"name", nm("throttle")}, {"sample_rate", static_cast<float>(cfg.rate)}, {"chunk_size", Size_t(cfg.chunk)}, {"catch_up", cfg.catch_up}, {"whole_chunks", N > 0}};
        auto& thr = g.emplaceBlock<Throttle<cf>>(S(T(thrm, 0.5f * tp, 0.f), N == 0));
        thrp = &thr;
        cb.block_count++;
        R(thr, "throttle");
    }
    auto& stamper = g.emplaceBlock<ArrivalStamper>(S(T({{"name", nm("stamp")}}, 0.75f * tp, batch_period_s)));
    cb.block_count++;
    R(stamper, "stamp");

    // the stock GR3 front end, block for block
    auto& mag2   = g.emplaceBlock<MagSquared>(S(TP({{"name", nm("mag2")}})));
    auto& avgpow = g.emplaceBlock<MovingAverage<float>>(S(TP({{"name", nm("avgpow")}, {"length", Size_t(64)}, {"max_iter", max_iter}})));
    auto& dly16  = g.emplaceBlock<SampleDelay<cf>>(S(TP({{"name", nm("dly16")}, {"delay", Size_t(16)}})));
    auto& conj   = g.emplaceBlock<Conjugate>(S(TP({{"name", nm("conj")}})));
    auto& mult   = g.emplaceBlock<Multiply2<cf>>(S(TP({{"name", nm("mul")}})));
    auto& avgcor = g.emplaceBlock<MovingAverage<cf>>(S(TP({{"name", nm("avgcor")}, {"length", Size_t(48)}, {"max_iter", max_iter}})));
    auto& mag    = g.emplaceBlock<blocks::type::converter::Abs<cf>>(S(TP({{"name", nm("mag")}})));
    auto& div    = g.emplaceBlock<Divide2<float>>(S(TP({{"name", nm("div")}})));
    gr::property_map ssm{{"name", nm("syncshort")}, {"threshold", cfg.sensitivity}, {"min_plateau", Size_t(2)}};
    if (N > 0) { ssm.insert_or_assign("max_batch_size", Size_t(2U * N)); } // the gate's ceiling is 2N (see below)
    auto& ss     = g.emplaceBlock<SyncShort>(S(TP(ssm), N == 0));
    auto& dly320 = g.emplaceBlock<SampleDelay<cf>>(S(TP({{"name", nm("dly320")}, {"delay", Size_t(320)}})));
    auto& sl     = g.emplaceBlock<SyncLong>(S(TP({{"name", nm("synclong")}, {"sync_length", Size_t(320)}})));
    auto& fft    = g.emplaceBlock<Fft64>(S(TP({{"name", nm("fft")}})));
    auto& eq     = g.emplaceBlock<FrameEqualizer>(S(TP({{"name", nm("eq")}, {"freq", cfg.frequency}, {"bw", cfg.bandwidth}})));
    auto& dec    = g.emplaceBlock<DecodeMac>(S(TP({{"name", nm("decode")}})));
    cb.block_count += 14;
    R(mag2, "mag2"); R(avgpow, "avgpow"); R(dly16, "dly16"); R(conj, "conj"); R(mult, "mul"); R(avgcor, "avgcor"); R(mag, "mag"); R(div, "div");
    R(ss, "syncshort"); R(dly320, "dly320"); R(sl, "synclong"); R(fft, "fft"); R(eq, "eq"); R(dec, "decode");
    // the fixed-batch section: everything the throttle feeds, up to the gate's
    // inputs (0036 items 2 and 4).  The gate itself gets no floor and a 2N
    // ceiling: sync_short's state machine consumes variable amounts (it stops
    // at a detection), so its input positions drift off the batch grid, and
    // an exact-N contract on it would make the call that covers batch j's
    // last sample wait for batch j+1 -- one whole period of lag that is an
    // artefact of the contract, not of the scheduler (docs/batch-rt-experiments.md 6).
    F(mag2.in); F(avgpow.in); F(dly16.in); F(conj.in); F(mult.in0); F(mult.in1); F(avgcor.in); F(mag.in); F(div.in0); F(div.in1);
    if (N > 0) {
        ss.in.max_samples     = 2UZ * N;
        ss.in_abs.max_samples = 2UZ * N;
        ss.in_cor.max_samples = 2UZ * N;
    }

    auto& sink = g.emplaceBlock<LatencySink>(S(TP({{"name", nm("latsink")}})));
    cb.block_count += 1;
    R(sink, "latsink");

    // feed = throttle output, or the file source when unthrottled
    if (thrp) {
        must(g.connect<"out", "in">(src, *thrp, ep), "src->throttle");
        must(g.connect<"out", "in">(*thrp, mag2, ep), "throttle->mag2");
        must(g.connect<"out", "in">(*thrp, dly16, ep), "throttle->dly16");
        must(g.connect<"out", "in0">(*thrp, mult, ep), "throttle->mul.in0");
        must(g.connect<"out", "in">(*thrp, stamper, ep), "throttle->stamper");
    } else {
        must(g.connect<"out", "in">(src, mag2, ep), "src->mag2");
        must(g.connect<"out", "in">(src, dly16, ep), "src->dly16");
        must(g.connect<"out", "in0">(src, mult, ep), "src->mul.in0");
        must(g.connect<"out", "in">(src, stamper, ep), "src->stamper");
    }
    must(g.connect<"out", "in">(mag2, avgpow, ep), "mag2->avgpow");
    must(g.connect<"out", "in1">(avgpow, div, ep), "avgpow->div.in1");
    must(g.connect<"out", "in">(dly16, conj, ep), "dly16->conj");
    must(g.connect<"out", "in1">(conj, mult, ep), "conj->mul.in1");
    must(g.connect<"out", "in">(mult, avgcor, ep), "mul->avgcor");
    must(g.connect<"out", "in">(avgcor, mag, ep), "avgcor->mag");
    must(g.connect<"abs", "in0">(mag, div, ep), "mag->div.in0");
    must(g.connect<"out", "in_abs">(avgcor, ss, ep), "avgcor->ss.in_abs");
    must(g.connect<"out", "in">(dly16, ss, ep), "dly16->ss.in");
    must(g.connect<"out", "in_cor">(div, ss, ep), "div->ss.in_cor");
    must(g.connect<"out", "in">(ss, dly320, ep), "ss->dly320");
    must(g.connect<"out", "in_delayed">(dly320, sl, ep), "dly320->sl.in_delayed");
    must(g.connect<"out", "in">(ss, sl, ep), "ss->sl.in");
    must(g.connect<"out", "in">(sl, fft, ep), "sl->fft");
    must(g.connect<"out", "in">(fft, eq, ep), "fft->eq");
    must(g.connect<"out", "in">(eq, dec, ep), "eq->decode");
    must(g.connect<"out", "in">(dec, sink, ep), "decode->latsink");

    if (cfg.record) {
        auto& pdu = g.emplaceBlock<PduRecorder>(S({{"name", nm("pdurec")}}));
        auto& sym = g.emplaceBlock<SymbolsRecorder>(S({{"name", nm("symrec")}}));
        cb.block_count += 2;
        R(pdu, "pdurec"); R(sym, "symrec");
        pdu._bin_path = cfg.out_dir + "/rx_pdus.bin";
        pdu._csv_path = cfg.out_dir + "/rx_log.csv";
        sym._bin_path = cfg.out_dir + "/rx_symbols.cf32";
        sym._csv_path = cfg.out_dir + "/rx_symbols.csv";
        must(g.connect<"out", "in">(dec, pdu, ep), "decode->pdurec");
        must(g.connect<"symbols", "in">(eq, sym, ep), "eq.symbols->symrec");
        cb.pdu = &pdu;
        cb.sym = &sym;
    }

    cb.stamper  = &stamper;
    cb.sink     = &sink;
    cb.counters = [&ss, &sl, &eq, &dec, &avgpow, &dly16, &avgcor, &dly320, &fft, &src, thrp, &stamper]() {
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
        c.stamper_reentry = stamper._reentry;
        c.src_calls = src._calls; c.src_max_read_ns = src._max_read_ns; c.src_sum_read_ns = src._sum_read_ns; c.src_max_read_items = src._max_read_items;
        if (thrp) { c.thr_calls = thrp->_calls; c.thr_max_gap_ns = thrp->_max_gap_ns; c.thr_sum_gap_ns = thrp->_sum_gap_ns; c.thr_max_backlog = thrp->_max_backlog; c.thr_start_ns = thrp->_start_ns; c.thr_chunks = thrp->_chunks_published; c.thr_max_chunks_per_call = thrp->_max_chunks_per_call; }
        c.fft_tags_in = fft._tags_in; c.fft_tags_out = fft._tags_out; c.eq_tags_in = eq._tags_in;
        c.sl_neg_tags = sl._neg_tags; c.sl_far_tags = sl._far_tags; c.sl_tags_seen = sl._tags_seen; c.sl_max_copy_run = sl._max_copy_run; c.sl_short_calls = sl._short_calls;
        return c;
    };
    return cb;
}

} // namespace gr4wifi
