#include "gr4ieee80211/chain.hpp"

#include <gnuradio-4.0/basic/ConverterBlocks.hpp>
#include <gnuradio-4.0/testing/NullSources.hpp>

#include "gr4ieee80211/DecodeMac.hpp"
#include "gr4ieee80211/Fft64.hpp"
#include "gr4ieee80211/FileSourceRaw.hpp"
#include "gr4ieee80211/FrameEqualizer.hpp"
#include "gr4ieee80211/SyncLong.hpp"
#include "gr4ieee80211/SyncShort.hpp"
#include "gr4ieee80211/basic_blocks.hpp"

#include <algorithm>
#include <functional>
#include <vector>
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
    // With `zero_period` every period is declared as exactly 0, so the release gate is off by
    // declaration rather than by a period too short to bind -- and the implicit deadline, which is
    // the period, would then be none at all: every block must bring its own.
    auto T = [&](gr::property_map m, float period, float deadline) {
        if (N > 0 && tp > 0.f) {
            if (cfg.zero_period && !(deadline > 0.f)) {
                throw std::runtime_error("buildChain: a zero period needs an explicit deadline on every block");
            }
            m.insert_or_assign("period", cfg.zero_period ? 0.f : period);
            if (deadline > 0.f) { m.insert_or_assign("relative_deadline", deadline); }
        }
        return m;
    };
    // a pre-gate block (and the gate, and the stamper): the receiver's class deadline
    const float pre_deadline_s  = batch_period_s * cfg.deadline_factor;
    // a post-gate block: the frame path, the tighter of the class and the frame factor
    const float post_deadline_s = batch_period_s * std::min(cfg.deadline_factor, cfg.frame_deadline_factor);
    auto TP  = [&](gr::property_map m) { return T(std::move(m), tp, pre_deadline_s); };
    auto TPO = [&](gr::property_map m) { return T(std::move(m), tp, post_deadline_s); };

    // Block construction happens through a list of creators, run in a rotated
    // order (see ChainConfig::rotate): the construction order is GR4's graph
    // order, and the graph order decides both round robin's sweep and which
    // worker each block is dealt to.  The dataflow wiring below is the same
    // whatever the order.
    FileSourceRaw<cf>* src = nullptr; Throttle<cf>* thrp = nullptr; ArrivalStamper* stamper = nullptr; gr::testing::Copy<cf>* entry = nullptr;
    MagSquared* mag2 = nullptr; MovingAverage<float>* avgpow = nullptr; SampleDelay<cf>* dly16 = nullptr; Conjugate* conj = nullptr;
    Multiply2<cf>* mult = nullptr; MovingAverage<cf>* avgcor = nullptr; blocks::type::converter::Abs<cf>* mag = nullptr; Divide2<float>* div = nullptr;
    SyncShort* ss = nullptr; SampleDelay<cf>* dly320 = nullptr; SyncLong* sl = nullptr; Fft64* fft = nullptr; FrameEqualizer* eq = nullptr;
    DecodeMac* dec = nullptr; LatencySink* sink = nullptr;
    // RateMonotonic: the pipeline blocks' `period` is the real one when asked
    const float pipe_period = cfg.rm_true_periods && batch_period_s > 0.f ? batch_period_s : tp;
    auto TPr  = [&](gr::property_map m) { return T(std::move(m), pipe_period, pre_deadline_s); };
    auto TPOr = [&](gr::property_map m) { return T(std::move(m), pipe_period, post_deadline_s); };
    const bool pacer = cfg.feed == ChainConfig::Feed::pacer;
    std::vector<std::function<void()>> make;
    if (pacer) {
        // The pacer's entry block: a copy, timed like the pre-gate blocks it feeds (the throttle's tiny
        // deadline existed because the throttle was the clock; the pacer is, outside the graph), and
        // left without a batch ceiling in fixed-batch mode so it catches up after a hiccup.
        make.push_back([&] { entry = &g.emplaceBlock<gr::testing::Copy<cf>>(S(TPr({{"name", nm("feed")}}), N == 0)); R(*entry, "feed"); });
    } else {
        make.push_back([&] {
            // GR4's BasicFileSource delivered no samples on this tree under either
            // scheduler (apps/probe.cpp modes "fsrc"); FileSourceRaw is GR3's
            // file_source semantics in a dozen lines.
            gr::property_map srcm{{"name", nm("fsrc")}, {"file_name", cfg.input}};
            if (cfg.declare_rate && cfg.rate > 0) { srcm.insert_or_assign("sample_rate", static_cast<float>(cfg.rate)); }
            if (cfg.max_samples > 0) { srcm.insert_or_assign("max_items", cfg.max_samples); }
            if (N > 0) {
                srcm.insert_or_assign("pad_to_multiple", Size_t(N));
                srcm.insert_or_assign("pad_min_tail", Size_t(fixedBatchFlushTail(N)));
            }
            src = &g.emplaceBlock<FileSourceRaw<cf>>(S(T(srcm, 0.25f * tp, 0.f), N == 0));
            R(*src, "fsrc");
        });
        if (cfg.rate > 0) {
            make.push_back([&] {
                gr::property_map thrm{{"name", nm("throttle")}, {"sample_rate", static_cast<float>(cfg.rate)}, {"chunk_size", Size_t(cfg.chunk)}, {"catch_up", cfg.catch_up}, {"whole_chunks", N > 0}};
                thrp = &g.emplaceBlock<Throttle<cf>>(S(T(thrm, 0.5f * tp, 0.f), N == 0));
                R(*thrp, "throttle");
            });
        }
        make.push_back([&] { stamper = &g.emplaceBlock<ArrivalStamper>(S(T({{"name", nm("stamp")}}, 0.75f * tp, pre_deadline_s))); R(*stamper, "stamp"); });
    }
    // the stock GR3 front end, block for block
    make.push_back([&] { mag2   = &g.emplaceBlock<MagSquared>(S(TPr({{"name", nm("mag2")}}))); R(*mag2, "mag2"); F(mag2->in); });
    make.push_back([&] { avgpow = &g.emplaceBlock<MovingAverage<float>>(S(TPr({{"name", nm("avgpow")}, {"length", Size_t(64)}, {"max_iter", max_iter}}))); R(*avgpow, "avgpow"); F(avgpow->in); });
    make.push_back([&] { dly16  = &g.emplaceBlock<SampleDelay<cf>>(S(TPr({{"name", nm("dly16")}, {"delay", Size_t(16)}}))); R(*dly16, "dly16"); F(dly16->in); });
    make.push_back([&] { conj   = &g.emplaceBlock<Conjugate>(S(TPr({{"name", nm("conj")}}))); R(*conj, "conj"); F(conj->in); });
    make.push_back([&] { mult   = &g.emplaceBlock<Multiply2<cf>>(S(TPr({{"name", nm("mul")}}))); R(*mult, "mul"); F(mult->in0); F(mult->in1); });
    make.push_back([&] { avgcor = &g.emplaceBlock<MovingAverage<cf>>(S(TPr({{"name", nm("avgcor")}, {"length", Size_t(48)}, {"max_iter", max_iter}}))); R(*avgcor, "avgcor"); F(avgcor->in); });
    make.push_back([&] { mag    = &g.emplaceBlock<blocks::type::converter::Abs<cf>>(S(TPr({{"name", nm("mag")}}))); R(*mag, "mag"); F(mag->in); });
    make.push_back([&] { div    = &g.emplaceBlock<Divide2<float>>(S(TPr({{"name", nm("div")}}))); R(*div, "div"); F(div->in0); F(div->in1); });
    make.push_back([&] {
        // the gate: no floor, a 2N ceiling -- sync_short's state machine consumes
        // variable amounts (it stops at a detection), so its input positions drift
        // off the batch grid, and an exact-N contract would make the call that
        // covers batch j's last sample wait for batch j+1 (docs/batch-rt-experiments.md 6)
        gr::property_map ssm{{"name", nm("syncshort")}, {"threshold", cfg.sensitivity}, {"min_plateau", Size_t(2)}};
        if (N > 0) { ssm.insert_or_assign("max_batch_size", Size_t(2U * N)); }
        ss = &g.emplaceBlock<SyncShort>(S(TPr(ssm), N == 0));
        R(*ss, "syncshort");
        if (N > 0) { ss->in.max_samples = 2UZ * N; ss->in_abs.max_samples = 2UZ * N; ss->in_cor.max_samples = 2UZ * N; }
    });
    make.push_back([&] { dly320 = &g.emplaceBlock<SampleDelay<cf>>(S(TPOr({{"name", nm("dly320")}, {"delay", Size_t(320)}}))); R(*dly320, "dly320"); });
    make.push_back([&] { sl     = &g.emplaceBlock<SyncLong>(S(TPOr({{"name", nm("synclong")}, {"sync_length", Size_t(320)}}))); R(*sl, "synclong"); });
    make.push_back([&] { fft    = &g.emplaceBlock<Fft64>(S(TPOr({{"name", nm("fft")}}))); R(*fft, "fft"); });
    make.push_back([&] { eq     = &g.emplaceBlock<FrameEqualizer>(S(TPOr({{"name", nm("eq")}, {"freq", cfg.frequency}, {"bw", cfg.bandwidth}}))); R(*eq, "eq"); });
    make.push_back([&] { dec    = &g.emplaceBlock<DecodeMac>(S(TPOr({{"name", nm("decode")}}))); R(*dec, "decode"); });
    make.push_back([&] { sink   = &g.emplaceBlock<LatencySink>(S(TPOr({{"name", nm("latsink")}}))); R(*sink, "latsink"); });
    {
        const std::size_t n     = make.size();
        const std::size_t shift = n ? (static_cast<std::size_t>(cfg.rotate) * cfg.chain_index) % n : 0;
        for (std::size_t i = 0; i < n; i++) { make[(i + shift) % n](); }
        cb.block_count += static_cast<int>(n);
    }
    gr::EdgeParameters ep{};
    if (cfg.buffer > 0) { ep.minBufferSize = cfg.buffer; }

    // feed = the pacer's entry block, the throttle output, or the file source when unthrottled
    if (entry) {
        must(g.connect<"out", "in">(*entry, *mag2, ep), "feed->mag2");
        must(g.connect<"out", "in">(*entry, *dly16, ep), "feed->dly16");
        must(g.connect<"out", "in0">(*entry, *mult, ep), "feed->mul.in0");
    } else if (thrp) {
        must(g.connect<"out", "in">(*src, *thrp, ep), "src->throttle");
        must(g.connect<"out", "in">(*thrp, *mag2, ep), "throttle->mag2");
        must(g.connect<"out", "in">(*thrp, *dly16, ep), "throttle->dly16");
        must(g.connect<"out", "in0">(*thrp, *mult, ep), "throttle->mul.in0");
        must(g.connect<"out", "in">(*thrp, *stamper, ep), "throttle->stamper");
    } else {
        must(g.connect<"out", "in">(*src, *mag2, ep), "src->mag2");
        must(g.connect<"out", "in">(*src, *dly16, ep), "src->dly16");
        must(g.connect<"out", "in0">(*src, *mult, ep), "src->mul.in0");
        must(g.connect<"out", "in">(*src, *stamper, ep), "src->stamper");
    }
    must(g.connect<"out", "in">(*mag2, *avgpow, ep), "mag2->avgpow");
    must(g.connect<"out", "in1">(*avgpow, *div, ep), "avgpow->div.in1");
    must(g.connect<"out", "in">(*dly16, *conj, ep), "dly16->conj");
    must(g.connect<"out", "in1">(*conj, *mult, ep), "conj->mul.in1");
    must(g.connect<"out", "in">(*mult, *avgcor, ep), "mul->avgcor");
    must(g.connect<"out", "in">(*avgcor, *mag, ep), "avgcor->mag");
    must(g.connect<"abs", "in0">(*mag, *div, ep), "mag->div.in0");
    must(g.connect<"out", "in_abs">(*avgcor, *ss, ep), "avgcor->ss.in_abs");
    must(g.connect<"out", "in">(*dly16, *ss, ep), "dly16->ss.in");
    must(g.connect<"out", "in_cor">(*div, *ss, ep), "div->ss.in_cor");
    must(g.connect<"out", "in">(*ss, *dly320, ep), "ss->dly320");
    must(g.connect<"out", "in_delayed">(*dly320, *sl, ep), "dly320->sl.in_delayed");
    must(g.connect<"out", "in">(*ss, *sl, ep), "ss->sl.in");
    must(g.connect<"out", "in">(*sl, *fft, ep), "sl->fft");
    must(g.connect<"out", "in">(*fft, *eq, ep), "fft->eq");
    must(g.connect<"out", "in">(*eq, *dec, ep), "eq->decode");
    must(g.connect<"out", "in">(*dec, *sink, ep), "decode->latsink");

    if (cfg.record) {
        auto& pdu = g.emplaceBlock<PduRecorder>(S({{"name", nm("pdurec")}}));
        auto& sym = g.emplaceBlock<SymbolsRecorder>(S({{"name", nm("symrec")}}));
        cb.block_count += 2;
        R(pdu, "pdurec"); R(sym, "symrec");
        pdu._bin_path = cfg.out_dir + "/rx_pdus.bin";
        pdu._csv_path = cfg.out_dir + "/rx_log.csv";
        sym._bin_path = cfg.out_dir + "/rx_symbols.cf32";
        sym._csv_path = cfg.out_dir + "/rx_symbols.csv";
        must(g.connect<"out", "in">(*dec, pdu, ep), "decode->pdurec");
        must(g.connect<"symbols", "in">(*eq, sym, ep), "eq.symbols->symrec");
        cb.pdu = &pdu;
        cb.sym = &sym;
    }

    if (entry) {
        // Pacer.hpp: an entry block must take no in-graph input, or the release scan may skip it
        // while the pacer's data waits
        for (const gr::Edge& edge : g.edges()) {
            if (edge.destinationBlock()->uniqueName() == entry->unique_name) {
                throw std::runtime_error("buildChain: the pacer's entry block must take no input from inside the graph");
            }
        }
        cb.feed_in = &entry->in;
    }
    cb.stamper  = stamper;
    cb.sink     = sink;
    cb.counters = [ss, sl, eq, dec, avgpow, dly16, avgcor, dly320, fft, src, thrp, stamper]() {
        ChainCounters c;
        c.sync_short_detections = ss->_frames_detected;
        c.sync_long_frames      = sl->_frames_aligned;
        c.signal_ok             = eq->_signal_ok;
        c.signal_bad            = eq->_signal_bad;
        c.frames_started        = dec->_frames_started;
        c.frames_decoded        = dec->_frames_decoded;
        c.crc_failed            = dec->_crc_failed;
        c.too_large             = dec->_too_large;
        c.items_avgpow = avgpow->_items; c.items_dly16 = dly16->_items; c.items_avgcor = avgcor->_items; c.items_ss = ss->_items; c.items_dly320 = dly320->_items;
        c.items_sl = sl->_items; c.items_fft = fft->_items; c.items_eq = eq->_items; c.items_dec = dec->_items; c.calls_ss = ss->_calls; c.calls_sl = sl->_calls; c.max_cor = ss->_max_cor;
        if (stamper) { c.stamper_reentry = stamper->_reentry; }
        if (src) { c.src_calls = src->_calls; c.src_max_read_ns = src->_max_read_ns; c.src_sum_read_ns = src->_sum_read_ns; c.src_max_read_items = src->_max_read_items; }
        if (thrp) { c.thr_calls = thrp->_calls; c.thr_max_gap_ns = thrp->_max_gap_ns; c.thr_sum_gap_ns = thrp->_sum_gap_ns; c.thr_max_backlog = thrp->_max_backlog; c.thr_start_ns = thrp->_start_ns; c.thr_chunks = thrp->_chunks_published; c.thr_max_chunks_per_call = thrp->_max_chunks_per_call; }
        c.fft_tags_in = fft->_tags_in; c.fft_tags_out = fft->_tags_out; c.eq_tags_in = eq->_tags_in;
        c.sl_neg_tags = sl->_neg_tags; c.sl_far_tags = sl->_far_tags; c.sl_tags_seen = sl->_tags_seen; c.sl_max_copy_run = sl->_max_copy_run; c.sl_short_calls = sl->_short_calls;
        return c;
    };
    return cb;
}

} // namespace gr4wifi
