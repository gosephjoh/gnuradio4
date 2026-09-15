/*
 * chain.hpp -- one receiver chain of the Phase 10 graph, built into a
 * gr::Graph.  The heavy block instantiations live in src/chain.cpp so the
 * app's translation unit stays small; the app only sees the harness blocks
 * it has to read after the run.
 *
 *   file_source(input) -> [throttle(rate, chunk)] -> wifi rx chain -> latency_sink
 *                                   \-> arrival_stamper
 *   with --record: decode_mac -> pdu_recorder, frame_equalizer.symbols -> symbols_recorder
 *
 * The rx chain is GR3's wifi_phy_rx block for block (src/phy/wifi_phy_rx.cc
 * of the GR3 project): mag2 -> avgpow(64) -> divide.in#1; delay16 -> conj ->
 * mult.in#1; in -> mult.in#0; mult -> avgcor(48) -> mag -> divide.in#0;
 * avgcor -> sync_short.in_abs; delay16 -> sync_short.in; divide ->
 * sync_short.in_cor; sync_short -> delay320 -> sync_long.in_delayed;
 * sync_short -> sync_long.in; sync_long -> fft64 -> frame_equalizer ->
 * decode_mac.
 */
#pragma once

#include <gnuradio-4.0/Graph.hpp>

#include "harness_blocks.hpp"

#include <cstdint>
#include <functional>
#include <string>

namespace gr4wifi {

struct ChainConfig {
    std::string input;             // rx_stimulus.cf32
    double      rate        = 10e6; // throttle samples/s; 0 = no throttle
    unsigned    chunk       = 4096;
    double      frequency   = 5.89e9;
    double      bandwidth   = 10e6;
    double      sensitivity = 0.56;
    bool        record      = false;
    std::string out_dir;           // for the recorders
    std::string prefix;            // block name prefix, e.g. "c0_"
    std::size_t buffer      = 0;   // edge minBufferSize in items; 0 = GR4 default (65536)
    bool        catch_up    = false; // Throttle::catch_up
    unsigned    batch       = 0;     // max_batch_size on every block; 0 = GR4 default (unbounded)
};

struct ChainCounters {
    uint64_t sync_short_detections = 0;
    uint64_t sync_long_frames      = 0;
    uint64_t signal_ok = 0, signal_bad = 0;
    uint64_t frames_started = 0, frames_decoded = 0, crc_failed = 0, too_large = 0;
    // items consumed per stage (diagnostics)
    uint64_t items_avgpow = 0, items_dly16 = 0, items_avgcor = 0, items_ss = 0, items_dly320 = 0, items_sl = 0, items_fft = 0, items_eq = 0, items_dec = 0;
    uint64_t calls_ss = 0, calls_sl = 0;
    uint64_t stamper_reentry = 0;
    uint64_t src_calls = 0, src_max_read_ns = 0, src_sum_read_ns = 0, src_max_read_items = 0;
    uint64_t thr_calls = 0, thr_max_gap_ns = 0, thr_sum_gap_ns = 0, thr_max_backlog = 0;
    uint64_t fft_tags_in = 0, fft_tags_out = 0, eq_tags_in = 0;
    uint64_t sl_neg_tags = 0, sl_far_tags = 0, sl_tags_seen = 0, sl_max_copy_run = 0, sl_short_calls = 0;
    float    max_cor = 0;
};

struct ChainBlocks {
    ArrivalStamper*  stamper = nullptr;
    LatencySink*     sink    = nullptr;
    PduRecorder*     pdu     = nullptr; // record only
    SymbolsRecorder* sym     = nullptr; // record only
    std::function<ChainCounters()> counters;
    int block_count = 0;
};

ChainBlocks buildChain(gr::Graph& graph, const ChainConfig& cfg);

} // namespace gr4wifi
