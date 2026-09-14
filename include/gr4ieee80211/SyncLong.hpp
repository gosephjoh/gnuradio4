/*
 * SyncLong -- port of gr-ieee802-11 lib/sync_long.cc (ad0598e).
 *
 * Inputs: in (sync_short's output, carrying the `wifi_start` tags) and
 * in_delayed (the same, delayed by sync_length); output: the frame's
 * samples with the cyclic prefixes removed (the 128 LTF samples, then 64 per
 * OFDM symbol), corrected by the fine frequency offset, tagged `wifi_start`
 * at the frame's first output sample with value (coarse - fine) offset.
 *
 * The LTF correlation is upstream's fir_filter_ccc(LONG).filterN: GR3's
 * kernel reverses the taps, so cor[k] = sum_j LONG[63-j] * in[k+j].
 * Candidates are sorted by magnitude, stable, as std::list::sort is.
 *
 * Two departures, both in branches upstream never reaches on the fixture
 * (no frame is ever tagged while the block is mid-SYNC, MIN_GAP forbids it):
 * upstream throws "wtf" there; this port logs and resets.  And a RESET that
 * completes falls straight into SYNC within the same call, where upstream
 * relied on being rescheduled -- same samples, one fewer no-progress call.
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>

#include "wifi_codec.hpp"

#include <algorithm>
#include <print>
#include <utility>
#include <vector>

namespace gr4wifi {

struct SyncLong : gr::Block<SyncLong, gr::NoTagPropagation> {
    using Description = gr::Doc<"802.11 long-training-field synchroniser (gr-ieee802-11 sync_long)">;

    gr::PortIn<cf, gr::RequiredSamples<64, 8192>> in;
    gr::PortIn<cf, gr::RequiredSamples<64, 8192>> in_delayed;
    gr::PortOut<cf>                               out;

    gr::Size_t sync_length = 320U;

    GR_MAKE_REFLECTABLE(SyncLong, in, in_delayed, out, sync_length);

    enum State { SYNC, COPY, RESET };
    State  _state             = SYNC;
    int    _count             = 0;
    int    _offset            = 0;
    int    _frame_start       = 0;
    float  _freq_offset       = 0;
    double _freq_offset_short = 0;

    std::vector<cf>                   _correlation;
    std::vector<std::pair<cf, int>>   _cor;
    uint64_t                          _frames_aligned = 0;
    uint64_t                          _items = 0, _calls = 0;

    void start() {
        _correlation.assign(8192, cf{});
        _cor.clear();
        _cor.reserve(sync_length + 1);
        _state = SYNC; _count = 0; _offset = 0;
    }

    // fir_filter_ccc(LONG).filterN(out, in, n): reversed taps.
    static void correlate(cf* outp, const cf* inp, int n) {
        for (int k = 0; k < n; k++) {
            cf acc{};
            for (int j = 0; j < 64; j++) {
                acc += LONG_SYNC[static_cast<std::size_t>(63 - j)] * inp[k + j];
            }
            outp[k] = acc;
        }
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn, gr::InputSpanLike auto& sDel, gr::OutputSpanLike auto& sOut) {
        const cf* inp     = sIn.data();
        const cf* in_del  = sDel.data();
        cf*       outp    = sOut.data();
        const int noutput = static_cast<int>(sOut.size());
        int       ninput  = static_cast<int>(std::min({sIn.size(), sDel.size(), 8192UZ}));

        // tags on input 0 within [0, ninput): the earliest one decides
        bool   have_tag  = false;
        int    tag_rel   = 0;
        double tag_value = 0;
        for (const auto& [rel, map] : sIn.tags()) {
            if (rel < 0 || rel >= ninput) {
                continue;
            }
            const auto v = map.get().template value_or<double>("wifi_start", std::numeric_limits<double>::quiet_NaN());
            if (std::isnan(v)) {
                continue;
            }
            if (!have_tag || rel < tag_rel) {
                have_tag  = true;
                tag_rel   = static_cast<int>(rel);
                tag_value = v;
            }
        }
        if (have_tag) {
            if (tag_rel > 0) {
                ninput = tag_rel;
            } else {
                if (_offset && _state == SYNC) {
                    std::println(stderr, "SyncLong: frame tag arrived mid-SYNC (offset {}) -- upstream throws here; resetting", _offset);
                    _offset = 0;
                }
                if (_state == COPY) {
                    _state = RESET;
                }
                _freq_offset_short = tag_value;
            }
        }

        int i = 0, o = 0;

        if (_state == RESET) {
            while (o < noutput) {
                if (((_count + o) % 64) == 0) {
                    _offset = 0;
                    _state  = SYNC;
                    break;
                }
                outp[o] = cf{};
                o++;
            }
            // departure: fall through into SYNC in the same call if it completed
        }

        if (_state == SYNC) {
            const int ncor = std::min(static_cast<int>(sync_length), std::max(ninput - 63, 0));
            correlate(_correlation.data(), inp, ncor);
            while (i + 63 < ninput) {
                _cor.emplace_back(_correlation[static_cast<std::size_t>(i)], _offset);
                i++;
                _offset++;
                if (_offset == static_cast<int>(sync_length)) {
                    search_frame_start();
                    _offset = 0;
                    _count  = 0;
                    _state  = COPY;
                    _frames_aligned++;
                    break;
                }
            }
            if (i == 0 && o == 0 && ninput < 64 && have_tag && tag_rel > 0) {
                // departure: fewer than 64 samples before a new frame tag while
                // searching -- upstream would make no progress; skip to the tag.
                i = ninput;
            }
        } else if (_state == COPY) {
            while (i < ninput && o < noutput) {
                const int rel = _offset - _frame_start;
                if (!rel) {
                    sOut.publishTag(gr::property_map{{"wifi_start", _freq_offset_short - static_cast<double>(_freq_offset)}}, static_cast<std::size_t>(o));
                }
                if (rel >= 0 && (rel < 128 || ((rel - 128) % 80) > 15)) {
                    outp[o] = in_del[i] * std::exp(cf(0, static_cast<float>(_offset) * _freq_offset));
                    o++;
                }
                i++;
                _offset++;
            }
        }

        _count += o;
        _items += static_cast<uint64_t>(i);
        _calls++;
        std::ignore = sIn.consume(static_cast<std::size_t>(i));
        std::ignore = sDel.consume(static_cast<std::size_t>(i));
        sIn.consumeTags(static_cast<std::size_t>(i));
        sOut.publish(static_cast<std::size_t>(o));
        return gr::work::Status::OK;
    }

    void search_frame_start() {
        // list::sort(compare_abs) is stable; highest |correlation| first
        std::stable_sort(_cor.begin(), _cor.end(), [](const auto& a, const auto& b) { return std::abs(a.first) > std::abs(b.first); });
        std::vector<std::pair<cf, int>> vec(_cor.begin(), _cor.end());
        _cor.clear();

        _frame_start = static_cast<int>(sync_length);
        for (int a = 0; a < 3; a++) {
            for (int k = a + 1; k < 4; k++) {
                cf first, second;
                if (vec[static_cast<std::size_t>(a)].second > vec[static_cast<std::size_t>(k)].second) {
                    first  = vec[static_cast<std::size_t>(k)].first;
                    second = vec[static_cast<std::size_t>(a)].first;
                } else {
                    first  = vec[static_cast<std::size_t>(a)].first;
                    second = vec[static_cast<std::size_t>(k)].first;
                }
                const int diff = std::abs(vec[static_cast<std::size_t>(a)].second - vec[static_cast<std::size_t>(k)].second);
                if (diff == 64) {
                    _frame_start = std::min(vec[static_cast<std::size_t>(a)].second, vec[static_cast<std::size_t>(k)].second);
                    _freq_offset = std::arg(first * std::conj(second)) / 64;
                    return;
                } else if (diff == 63) {
                    _frame_start = std::min(vec[static_cast<std::size_t>(a)].second, vec[static_cast<std::size_t>(k)].second);
                    _freq_offset = std::arg(first * std::conj(second)) / 63;
                } else if (diff == 65) {
                    _frame_start = std::min(vec[static_cast<std::size_t>(a)].second, vec[static_cast<std::size_t>(k)].second);
                    _freq_offset = std::arg(first * std::conj(second)) / 65;
                }
            }
        }
    }
};

} // namespace gr4wifi
