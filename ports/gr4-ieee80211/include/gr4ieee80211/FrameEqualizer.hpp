/*
 * FrameEqualizer -- port of gr-ieee802-11 lib/frame_equalizer_impl.cc
 * (ad0598e) with the LS equaliser (lib/equalizer/ls.cc), the only algorithm
 * the GR3 harness ever ran (chan_est = LS in every golden cell).
 *
 * Input: 64 samples per OFDM symbol (Fft64's output), `wifi_start` tag on the
 * first sample of a frame.  Outputs: `out` -- 48 hard-decision bytes per DATA
 * symbol (bit k = coded bit k); `symbols` -- the 48 equalised points of the
 * same symbol (GR3's `carrier` message plane, here a stream; leave it
 * unconnected when it is not being recorded).
 *
 * Tags: at the first byte of a frame's first data symbol, the SIGNAL
 * field's `frame bytes`, `encoding`, `snr`, `nominal frequency`,
 * `frequency offset`, `beta` (upstream also attaches `csi`, unused
 * downstream).  Upstream attaches them to the next output item from inside
 * symbol 2; here they are held and published on that item -- same index.
 *
 * Arithmetic follows upstream's mixed float/double evaluation exactly: the
 * sampling-offset phase is a double expression narrowed to float inside
 * exp(gr_complex(0, x)), beta/er come from arg() of complex<float>.
 * POLARITY[(sym-2) % 127] is read out of bounds for sym < 2 upstream
 * (gotchas #4); its value only reaches `er`, which is discarded there, so
 * this port guards the index (port-requirements 1.3, item 3).
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>

#include "wifi_codec.hpp"

#include <cmath>
#include <numbers>
#include <optional>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace gr4wifi {

struct FrameEqualizer : gr::Block<FrameEqualizer, gr::NoTagPropagation> {
    using Description = gr::Doc<"802.11 OFDM frame equaliser, LS channel estimate, SIGNAL decode (gr-ieee802-11 frame_equalizer)">;

    gr::PortIn<cf, gr::RequiredSamples<64>>   in;
    gr::PortOut<uint8_t>                      out;
    gr::PortOut<cf, gr::Optional>             symbols;

    double freq = 5.89e9;
    double bw   = 10e6;

    GR_MAKE_REFLECTABLE(FrameEqualizer, in, out, symbols, freq, bw);

    int      _current_symbol = 0;
    int      _frame_symbols  = 0;
    int      _frame_bytes    = 0;
    int      _frame_encoding = 0;
    Encoding _frame_mod      = BPSK_1_2;
    double   _freq_offset_from_synclong = 0.0;
    double   _er       = 0.0;
    double   _epsilon0 = 0.0;
    cf       _prev_pilots[4]{};
    cf       _H[64]{};
    double   _snr = 0.0;
    ViterbiDecoder                 _decoder;
    std::optional<gr::property_map> _pending_tag;
    uint64_t                        _signal_ok = 0, _signal_bad = 0;
    uint64_t                        _items = 0, _calls = 0, _tags_in = 0;
    std::FILE*                      _trace = nullptr;

    void start() {
        if (const char* t = std::getenv("GR4_SYNCLONG_TRACE")) {
            _trace = std::fopen((std::string(t) + "." + std::string(this->name.value)).c_str(), "w");
        }
    }
    void stop() {
        if (_trace) { std::fclose(_trace); _trace = nullptr; }
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn, gr::OutputSpanLike auto& sOut, gr::OutputSpanLike auto& sSym) {
        const cf* inp      = sIn.data();
        uint8_t*  outp     = sOut.data();
        const bool withSym = sSym.isConnected;
        const std::size_t nsym_in  = sIn.size() / 64;
        const std::size_t out_cap  = sOut.size() / 48;
        const std::size_t sym_cap  = withSym ? sSym.size() / 48 : std::numeric_limits<std::size_t>::max();

        // wifi_start tags of this span, by input symbol index
        std::vector<std::pair<std::size_t, double>> starts;
        _calls++;
        for (const auto& [rel, map] : sIn.tags()) {
            _tags_in++;
            if (_trace) { std::fprintf(_trace, "%llu,in=%zu,out=%zu,sym=%zu,nsym=%zu,tag_rel=%td\n", static_cast<unsigned long long>(_calls), sIn.size(), sOut.size(), sSym.size(), nsym_in, static_cast<std::ptrdiff_t>(rel)); }
            if (rel >= 0 && static_cast<std::size_t>(rel) < nsym_in * 64) {
                const auto v = map.get().template value_or<double>("wifi_start", std::numeric_limits<double>::quiet_NaN());
                if (!std::isnan(v)) {
                    starts.emplace_back(static_cast<std::size_t>(rel) / 64, v);
                }
            }
        }

        std::size_t i = 0, o = 0;
        cf          symbols_buf[48];
        cf          current_symbol[64];
        uint8_t     bits[48];

        while (i < nsym_in && o < out_cap && o < sym_cap) {
            for (const auto& [sidx, v] : starts) {
                if (sidx == i) {
                    _current_symbol = 0;
                    _frame_symbols  = 0;
                    _frame_mod      = BPSK_1_2;
                    _freq_offset_from_synclong = v * bw / (2 * std::numbers::pi);
                    _epsilon0                  = v * bw / (2 * std::numbers::pi * freq);
                    _er                        = 0;
                }
            }

            if (_current_symbol > (_frame_symbols + 2)) { // not interesting -> skip
                i++;
                continue;
            }

            std::memcpy(current_symbol, inp + i * 64, 64 * sizeof(cf));

            // compensate sampling offset (upstream's expression, double -> float)
            for (int k = 0; k < 64; k++) {
                current_symbol[k] *= std::exp(cf(0, static_cast<float>(2 * M_PI * _current_symbol * 80 * (_epsilon0 + _er) * (k - 32) / 64)));
            }

            const cf p = _current_symbol >= 2 ? cf(POLARITY[static_cast<std::size_t>((_current_symbol - 2) % 127)], 0.f) : cf(1.f, 0.f);

            double beta;
            if (_current_symbol < 2) {
                beta = std::arg(current_symbol[11] - current_symbol[25] + current_symbol[39] + current_symbol[53]);
            } else {
                beta = std::arg((current_symbol[11] * p) + (current_symbol[39] * p) + (current_symbol[25] * p) + (current_symbol[53] * -p));
            }

            double er = std::arg((std::conj(_prev_pilots[0]) * current_symbol[11] * p) + (std::conj(_prev_pilots[1]) * current_symbol[25] * p) + (std::conj(_prev_pilots[2]) * current_symbol[39] * p) + (std::conj(_prev_pilots[3]) * current_symbol[53] * -p));
            er *= bw / (2 * M_PI * freq * 80);

            if (_current_symbol < 2) {
                _prev_pilots[0] = current_symbol[11];
                _prev_pilots[1] = -current_symbol[25];
                _prev_pilots[2] = current_symbol[39];
                _prev_pilots[3] = current_symbol[53];
            } else {
                _prev_pilots[0] = current_symbol[11] * p;
                _prev_pilots[1] = current_symbol[25] * p;
                _prev_pilots[2] = current_symbol[39] * p;
                _prev_pilots[3] = current_symbol[53] * -p;
            }

            // compensate residual frequency offset
            for (int k = 0; k < 64; k++) {
                current_symbol[k] *= std::exp(cf(0, static_cast<float>(-beta)));
            }

            if (_current_symbol >= 2) {
                const double alpha = 0.1;
                _er = (1 - alpha) * _er + alpha * er;
            }

            // LS equaliser (equalizer/ls.cc)
            equalize_ls(current_symbol, _current_symbol, symbols_buf, bits, _frame_mod);

            if (_current_symbol == 2) {
                if (decode_signal_field(bits)) {
                    gr::property_map tag;
                    tag.insert_or_assign("frame bytes", static_cast<uint64_t>(_frame_bytes));
                    tag.insert_or_assign("encoding", static_cast<uint64_t>(_frame_encoding));
                    tag.insert_or_assign("snr", _snr);
                    tag.insert_or_assign("nominal frequency", freq);
                    tag.insert_or_assign("frequency offset", _freq_offset_from_synclong);
                    tag.insert_or_assign("beta", beta);
                    _pending_tag = std::move(tag);
                    _signal_ok++;
                } else {
                    _signal_bad++;
                }
            }

            if (_current_symbol > 2) {
                if (_pending_tag) {
                    sOut.publishTag(*_pending_tag, o * 48);
                    _pending_tag.reset();
                }
                std::memcpy(outp + o * 48, bits, 48);
                if (withSym) {
                    std::memcpy(sSym.data() + o * 48, symbols_buf, 48 * sizeof(cf));
                }
                o++;
            }

            i++;
            _current_symbol++;
        }

        _items += i * 64;
        _calls++;
        std::ignore = sIn.consume(i * 64);
        sIn.consumeTags(i * 64);
        sOut.publish(o * 48);
        if (withSym) {
            sSym.publish(o * 48);
        } else {
            sSym.publish(0);
        }
        return gr::work::Status::OK;
    }

    void equalize_ls(const cf* inp, int n, cf* syms, uint8_t* bits, Encoding mod) {
        if (n == 0) {
            std::memcpy(_H, inp, 64 * sizeof(cf));
        } else if (n == 1) {
            double signal = 0, noise = 0;
            for (int k = 0; k < 64; k++) {
                if ((k == 32) || (k < 6) || (k > 58)) {
                    continue;
                }
                noise += std::pow(std::abs(_H[k] - inp[k]), 2);
                signal += std::pow(std::abs(_H[k] + inp[k]), 2);
                _H[k] += inp[k];
                _H[k] /= cf(LONG_LTF[static_cast<std::size_t>(k)], 0.f) * cf(2, 0);
            }
            _snr = 10 * std::log10(signal / noise / 2);
        } else {
            int c = 0;
            for (int k = 0; k < 64; k++) {
                if ((k == 11) || (k == 25) || (k == 32) || (k == 39) || (k == 53) || (k < 6) || (k > 58)) {
                    continue;
                }
                syms[c] = inp[k] / _H[k];
                bits[c] = static_cast<uint8_t>(decision(mod, syms[c]));
                c++;
            }
        }
    }

    bool decode_signal_field(const uint8_t* rx_bits) {
        static const ofdm_param  ofdm(BPSK_1_2);
        static const frame_param frame(ofdm, 0);
        uint8_t deinterleaved[48];
        for (int k = 0; k < 48; k++) {
            deinterleaved[k] = rx_bits[SIGNAL_INTERLEAVER[static_cast<std::size_t>(k)]];
        }
        const uint8_t* decoded = _decoder.decode(ofdm, frame, deinterleaved);
        return parse_signal(decoded);
    }

    bool parse_signal(const uint8_t* decoded_bits) {
        int  r      = 0;
        bool parity = false;
        _frame_bytes = 0;
        for (int k = 0; k < 17; k++) {
            parity ^= decoded_bits[k];
            if ((k < 4) && decoded_bits[k]) {
                r = r | (1 << k);
            }
            if (decoded_bits[k] && (k > 4) && (k < 17)) {
                _frame_bytes = _frame_bytes | (1 << (k - 5));
            }
        }
        if (parity != static_cast<bool>(decoded_bits[17])) {
            return false;
        }
        const auto set = [&](int enc, int n_dbps, Encoding mod) {
            _frame_encoding = enc;
            _frame_symbols  = static_cast<int>(std::ceil((16 + 8 * _frame_bytes + 6) / static_cast<double>(n_dbps)));
            _frame_mod      = mod;
        };
        switch (r) {
        case 11: set(0, 24, BPSK_1_2); break;
        case 15: set(1, 36, BPSK_1_2); break;
        case 10: set(2, 48, QPSK_1_2); break;
        case 14: set(3, 72, QPSK_1_2); break;
        case 9:  set(4, 96, QAM16_1_2); break;
        case 13: set(5, 144, QAM16_1_2); break;
        case 8:  set(6, 192, QAM64_2_3); break;
        case 12: set(7, 216, QAM64_2_3); break;
        default: return false;
        }
        return true;
    }
};

} // namespace gr4wifi
