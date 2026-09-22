/*
 * DecodeMac -- port of gr-ieee802-11 lib/decode_mac.cc (ad0598e).
 *
 * Input: 48 bytes per OFDM data symbol, the frame's tag (`frame bytes`,
 * `encoding`) on the frame's first byte.  Output: one gr::Packet<uint8_t>
 * per frame whose CRC passes -- the PSDU with the FCS stripped, from byte 0
 * (24-byte MAC header + MSDU), with the frame's tag map and `dlt = 105` as
 * its meta-information.  GR3 published that as a PDU message; a stream of
 * packets on an asynchronous port is the GR4 equivalent hop.
 *
 * Upstream `break`s out of its loop as soon as a frame is complete; so does
 * this, consuming up to and including the completing symbol.  If the packet
 * port has no room the completing symbol is left unconsumed until it has.
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>
#include <gnuradio-4.0/DataSet.hpp>

#include "wifi_codec.hpp"

#include <vector>

namespace gr4wifi {

struct DecodeMac : gr::Block<DecodeMac, gr::NoTagPropagation> {
    using Description = gr::Doc<"802.11 DATA-field decoder: deinterleave, Viterbi, descramble, CRC-32 (gr-ieee802-11 decode_mac)">;

    gr::PortIn<uint8_t, gr::RequiredSamples<48>>   in;
    gr::PortOut<gr::Packet<uint8_t>, gr::Async>    out;

    GR_MAKE_REFLECTABLE(DecodeMac, in, out);

    frame_param    _frame{};
    ofdm_param     _ofdm{};
    int            _copied         = 0;
    bool           _frame_complete = true;
    gr::property_map _meta;
    ViterbiDecoder _decoder;
    std::vector<uint8_t> _rx_symbols       = std::vector<uint8_t>(48 * MAX_SYM);
    std::vector<uint8_t> _rx_bits          = std::vector<uint8_t>(MAX_ENCODED_BITS);
    std::vector<uint8_t> _deinterleaved    = std::vector<uint8_t>(MAX_ENCODED_BITS);
    std::vector<uint8_t> _out_bytes        = std::vector<uint8_t>(MAX_PSDU_SIZE + 2);
    uint64_t _frames_started = 0, _frames_decoded = 0, _crc_failed = 0, _too_large = 0;
    uint64_t _items = 0, _calls = 0;

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn, gr::OutputSpanLike auto& sOut) {
        const uint8_t*    inp   = sIn.data();
        const std::size_t nsyms = sIn.size() / 48;
        std::size_t       i     = 0;
        std::size_t       npub  = 0;

        // tags in this span, by symbol index
        std::vector<std::pair<std::size_t, gr::property_map>> tags;
        for (const auto& [rel, map] : sIn.tags()) {
            if (rel >= 0 && static_cast<std::size_t>(rel) < nsyms * 48 && (static_cast<std::size_t>(rel) % 48) == 0) {
                gr::property_map m;
                for (const auto& [k, v] : map.get()) {
                    m.insert_or_assign(k, v);
                }
                tags.emplace_back(static_cast<std::size_t>(rel) / 48, std::move(m));
            }
        }

        while (i < nsyms) {
            for (auto& [sidx, m] : tags) {
                if (sidx == i) {
                    _frame_complete = false;
                    _meta           = m;
                    const int len_data = static_cast<int>(_meta.template value_or<uint64_t>("frame bytes", static_cast<uint64_t>(MAX_PSDU_SIZE + 1)));
                    const int encoding = static_cast<int>(_meta.template value_or<uint64_t>("encoding", 0ULL));
                    const ofdm_param  ofdm(static_cast<Encoding>(encoding));
                    const frame_param frame(ofdm, len_data);
                    if (frame.n_sym <= MAX_SYM && frame.psdu_size <= MAX_PSDU_SIZE) {
                        _ofdm   = ofdm;
                        _frame  = frame;
                        _copied = 0;
                        _frames_started++;
                    } else {
                        _too_large++;
                    }
                }
            }

            if (_copied < _frame.n_sym) {
                if (_copied + 1 == _frame.n_sym && sOut.size() == 0) {
                    break; // no room for the packet this symbol would complete
                }
                std::memcpy(_rx_symbols.data() + _copied * 48, inp + i * 48, 48);
                _copied++;
                if (_copied == _frame.n_sym) {
                    if (decode(sOut, npub)) {
                        npub++;
                    }
                    i++;
                    _frame_complete = true;
                    break;
                }
            }
            i++;
        }

        _items += i * 48;
        _calls++;
        std::ignore = sIn.consume(i * 48);
        sIn.consumeTags(i * 48);
        sOut.publish(npub);
        return gr::work::Status::OK;
    }

    bool decode(auto& sOut, std::size_t slot) {
        for (int k = 0; k < _frame.n_sym * 48; k++) {
            for (int b = 0; b < _ofdm.n_bpsc; b++) {
                _rx_bits[static_cast<std::size_t>(k * _ofdm.n_bpsc + b)] = !!(_rx_symbols[static_cast<std::size_t>(k)] & (1 << b));
            }
        }
        deinterleave(_ofdm, _frame, _rx_bits.data(), _deinterleaved.data());
        const uint8_t* decoded = _decoder.decode(_ofdm, _frame, _deinterleaved.data());
        descramble(_frame, decoded, _out_bytes.data());

        if (crc32(_out_bytes.data() + 2, static_cast<std::size_t>(_frame.psdu_size)) != CRC_RESIDUE) {
            _crc_failed++;
            return false;
        }
        _frames_decoded++;

        gr::Packet<uint8_t>& pkt = sOut[slot];
        pkt.timestamp = static_cast<std::int64_t>(monotonic_ns());
        pkt.signal_values.assign(_out_bytes.begin() + 2, _out_bytes.begin() + 2 + (_frame.psdu_size - 4));
        _meta.insert_or_assign("dlt", static_cast<int64_t>(105));
        pkt.meta_information.assign(1, _meta);
        return true;
    }
};

} // namespace gr4wifi
