/*
 * SyncShort -- port of gr-ieee802-11 lib/sync_short.cc (ad0598e).
 *
 * Inputs: in (the delayed samples), in_abs (the 16-sample autocorrelation
 * average), in_cor (its magnitude over the power average); output: the
 * frequency-corrected samples of a detected frame, tagged `wifi_start` (value
 * = the frequency offset in radians/sample) at the frame's first sample.
 *
 * Differences from upstream, all outside the arithmetic: no tag propagation
 * (upstream sets TPP_DONT); the SEARCH->COPY tag, which upstream attaches to
 * the *next* output item before producing it, is held and published on the
 * first item of the COPY that follows -- same absolute index.
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>

#include "wifi_codec.hpp"

#include <optional>

namespace gr4wifi {

struct SyncShort : gr::Block<SyncShort, gr::NoTagPropagation> {
    using Description = gr::Doc<"802.11 short-training-field detector (gr-ieee802-11 sync_short)">;

    gr::PortIn<cf>    in;
    gr::PortIn<cf>    in_abs;
    gr::PortIn<float> in_cor;
    gr::PortOut<cf>   out;

    double     threshold   = 0.56;
    gr::Size_t min_plateau = 2U;

    GR_MAKE_REFLECTABLE(SyncShort, in, in_abs, in_cor, out, threshold, min_plateau);

    static constexpr int MIN_GAP     = 480;
    static constexpr int MAX_SAMPLES = 540 * 80;

    enum State { SEARCH, COPY };
    State                 _state       = SEARCH;
    int                   _copied      = 0;
    int                   _plateau     = 0;
    float                 _freq_offset = 0;
    std::optional<double> _pending_tag; // SEARCH->COPY tag, published with the first COPY output

    // counters for the harness
    uint64_t _frames_detected = 0;
    uint64_t _items = 0, _calls = 0;
    float    _max_cor = 0;

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn, gr::InputSpanLike auto& sAbs, gr::InputSpanLike auto& sCor, gr::OutputSpanLike auto& sOut) {
        const cf*    inp    = sIn.data();
        const cf*    in_a   = sAbs.data();
        const float* in_c   = sCor.data();
        cf*          outp   = sOut.data();
        const int    ninput = static_cast<int>(std::min({sIn.size(), sAbs.size(), sCor.size()}));
        const int    nout   = static_cast<int>(sOut.size());

        _calls++;
        for (int q = 0; q < ninput; q++) { _max_cor = std::max(_max_cor, in_c[q]); }
        auto consume = [&](int k) {
            _items += static_cast<uint64_t>(k);
            std::ignore = sIn.consume(static_cast<std::size_t>(k));
            std::ignore = sAbs.consume(static_cast<std::size_t>(k));
            std::ignore = sCor.consume(static_cast<std::size_t>(k));
        };

        switch (_state) {
        case SEARCH: {
            int i;
            for (i = 0; i < ninput; i++) {
                if (in_c[i] > threshold) {
                    if (_plateau < static_cast<int>(min_plateau)) {
                        _plateau++;
                    } else {
                        _state       = COPY;
                        _copied      = 0;
                        _freq_offset = std::arg(in_a[i]) / 16;
                        _plateau     = 0;
                        _pending_tag = static_cast<double>(_freq_offset);
                        _frames_detected++;
                        break;
                    }
                } else {
                    _plateau = 0;
                }
            }
            consume(i);
            sOut.publish(0);
            return gr::work::Status::OK;
        }
        case COPY: {
            int o = 0;
            while (o < ninput && o < nout && _copied < MAX_SAMPLES) {
                if (in_c[o] > threshold) {
                    if (_plateau < static_cast<int>(min_plateau)) {
                        _plateau++;
                    } else if (_copied > MIN_GAP) {
                        _copied      = 0;
                        _plateau     = 0;
                        _freq_offset = std::arg(in_a[o]) / 16;
                        sOut.publishTag(gr::property_map{{"wifi_start", static_cast<double>(_freq_offset)}}, static_cast<std::size_t>(o));
                        _frames_detected++;
                        break;
                    }
                } else {
                    _plateau = 0;
                }
                if (_pending_tag && o == 0) {
                    sOut.publishTag(gr::property_map{{"wifi_start", *_pending_tag}}, 0UZ);
                    _pending_tag.reset();
                }
                outp[o] = inp[o] * std::exp(cf(0, -_freq_offset * static_cast<float>(_copied)));
                o++;
                _copied++;
            }
            if (_copied == MAX_SAMPLES) {
                _state = SEARCH;
            }
            consume(o);
            sOut.publish(static_cast<std::size_t>(o));
            return gr::work::Status::OK;
        }
        }
        return gr::work::Status::ERROR;
    }
};

} // namespace gr4wifi
