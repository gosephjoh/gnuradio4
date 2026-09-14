/*
 * harness_blocks.hpp -- the measurement blocks of the GR3 harness, in GR4:
 *
 *   ArrivalStamper   second reader on the throttle's output; stamps the
 *                    moment a frame's first and last sample became visible
 *   LatencySink      stamps every decoded packet, pairs on the sequence number
 *   PduRecorder      rx_pdus.bin + rx_log.csv in the GR3 formats (for compare)
 *   SymbolsRecorder  rx_symbols.cf32 + rx_symbols.csv in the GR3 formats
 *
 * Semantics as in the GR3 project's src/blocks/{arrival_stamper,latency_sink,
 * pdu_recorder}.  Configuration that is not a scalar setting (the frame
 * offset tables, file paths) is set on the block object after emplaceBlock
 * and before the scheduler starts.
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>
#include <gnuradio-4.0/DataSet.hpp>

#include "wifi_codec.hpp"

#include <atomic>
#include <cmath>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace gr4wifi {

struct ArrivalStamper : gr::Block<ArrivalStamper> {
    using Description = gr::Doc<"stamps CLOCK_MONOTONIC when the item count crosses each frame's first/last sample offset">;

    gr::PortIn<cf> in;

    GR_MAKE_REFLECTABLE(ArrivalStamper, in);

    struct Event {
        uint64_t offset;
        uint32_t frame;
        bool     last;
    };
    std::vector<Event>    _events; // sorted, set by the app
    std::vector<uint64_t> _t_first, _t_last;
    std::size_t           _next  = 0;
    uint64_t              _items = 0;

    void setFrames(const std::vector<uint64_t>& first, const std::vector<uint64_t>& last) {
        _events.clear();
        for (std::size_t k = 0; k < first.size(); k++) {
            _events.push_back({first[k], static_cast<uint32_t>(k), false});
            _events.push_back({last[k], static_cast<uint32_t>(k), true});
        }
        _t_first.assign(first.size(), 0);
        _t_last.assign(first.size(), 0);
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn) {
        const uint64_t end = _items + sIn.size();
        if (_next < _events.size() && _events[_next].offset < end) {
            const uint64_t now = monotonic_ns();
            while (_next < _events.size() && _events[_next].offset < end) {
                const Event& e = _events[_next];
                (e.last ? _t_last : _t_first)[e.frame] = now;
                ++_next;
            }
        }
        _items = end;
        return gr::work::Status::OK;
    }
};

struct LatencySink : gr::Block<LatencySink> {
    using Description = gr::Doc<"stamps each decoded packet and pairs it on the u32 sequence number at bytes 24..27">;

    gr::PortIn<gr::Packet<uint8_t>> in;

    GR_MAKE_REFLECTABLE(LatencySink, in);

    std::vector<uint64_t> _t_decode;   // 0 = not decoded; first arrival wins
    std::vector<uint32_t> _len;
    std::vector<uint8_t>  _correct;    // 1 if the payload matched payloads.bin (when given)
    std::vector<const uint8_t*> _expected; // per frame: expected PDU bytes from 24 on, or nullptr
    std::vector<uint32_t>       _expected_len;
    uint64_t _count = 0, _duplicates = 0, _unpairable = 0, _wrong_payload = 0;
    // GR3 payload_checker's order mode: when no frame carries a sequence
    // number (payloads under 4 bytes, the fixture's `edge` cell) the i-th
    // decoded packet is paired with the i-th frame.
    bool     _order_mode = false;

    void setFrames(std::size_t n) {
        _t_decode.assign(n, 0);
        _len.assign(n, 0);
        _correct.assign(n, 0);
        _expected.assign(n, nullptr);
        _expected_len.assign(n, 0);
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn) {
        for (const gr::Packet<uint8_t>& p : sIn) {
            const uint64_t t = monotonic_ns();
            const auto&    b = p.signal_values;
            uint32_t       seq;
            if (_order_mode) {
                seq = static_cast<uint32_t>(_count);
            } else if (b.size() < 28) {
                _unpairable++;
                continue;
            } else {
                seq = b[24] | (b[25] << 8) | (b[26] << 16) | (static_cast<uint32_t>(b[27]) << 24);
            }
            if (seq >= _t_decode.size()) {
                _unpairable++;
            } else if (_t_decode[seq] != 0) {
                _duplicates++;
            } else {
                _t_decode[seq] = t;
                _len[seq]      = static_cast<uint32_t>(b.size() - 24);
                _count++;
                if (_expected[seq]) {
                    const bool ok = (b.size() - 24 == _expected_len[seq]) && std::equal(b.begin() + 24, b.end(), _expected[seq]);
                    _correct[seq] = ok ? 1 : 0;
                    if (!ok) {
                        _wrong_payload++;
                    }
                }
            }
        }
        return gr::work::Status::OK;
    }
};

struct PduRecorder : gr::Block<PduRecorder> {
    using Description = gr::Doc<"writes rx_pdus.bin (GR3RXPD1) and rx_log.csv as the GR3 pdu_recorder does">;

    gr::PortIn<gr::Packet<uint8_t>> in;

    GR_MAKE_REFLECTABLE(PduRecorder, in);

    std::string _bin_path, _csv_path;
    std::FILE*  _bin = nullptr;
    std::FILE*  _csv = nullptr;
    uint64_t    _count = 0;

    void start() {
        if (!_bin_path.empty()) {
            _bin = std::fopen(_bin_path.c_str(), "wb");
            std::fwrite("GR3RXPD1", 1, 8, _bin);
        }
        if (!_csv_path.empty()) {
            _csv = std::fopen(_csv_path.c_str(), "w");
            std::fprintf(_csv, "index,t_recv_ns,length,seq,encoding\n");
        }
    }
    void stop() { close(); }
    void close() {
        if (_bin) { std::fclose(_bin); _bin = nullptr; }
        if (_csv) { std::fclose(_csv); _csv = nullptr; }
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn) {
        for (const gr::Packet<uint8_t>& p : sIn) {
            const uint64_t t   = monotonic_ns();
            const auto&    b   = p.signal_values;
            const uint32_t len = static_cast<uint32_t>(b.size());
            if (_bin) {
                std::fwrite(&t, sizeof(t), 1, _bin);
                std::fwrite(&len, sizeof(len), 1, _bin);
                std::fwrite(b.data(), 1, len, _bin);
            }
            if (_csv) {
                long seq = -1;
                if (len >= 28) {
                    seq = static_cast<long>(b[24] | (b[25] << 8) | (b[26] << 16) | (static_cast<uint32_t>(b[27]) << 24));
                }
                long enc = -1;
                if (!p.meta_information.empty()) {
                    enc = static_cast<long>(p.meta_information[0].template value_or<uint64_t>("encoding", static_cast<uint64_t>(-1)));
                    if (enc == static_cast<long>(static_cast<uint64_t>(-1))) {
                        enc = -1;
                    }
                }
                std::fprintf(_csv, "%llu,%llu,%u,%ld,%ld\n", static_cast<unsigned long long>(_count), static_cast<unsigned long long>(t), len, seq, enc);
            }
            _count++;
        }
        return gr::work::Status::OK;
    }
};

struct SymbolsRecorder : gr::Block<SymbolsRecorder> {
    using Description = gr::Doc<"writes rx_symbols.cf32 (raw) and rx_symbols.csv (one row per 48 points) as the GR3 pdu_recorder SYMBOLS mode does">;

    gr::PortIn<cf> in;

    GR_MAKE_REFLECTABLE(SymbolsRecorder, in);

    std::string _bin_path, _csv_path;
    std::FILE*  _bin = nullptr;
    std::FILE*  _csv = nullptr;
    uint64_t    _messages = 0, _items = 0;
    std::size_t _partial  = 0; // points of the current 48-block already seen

    void start() {
        if (!_bin_path.empty()) {
            _bin = std::fopen(_bin_path.c_str(), "wb");
        }
        if (!_csv_path.empty()) {
            _csv = std::fopen(_csv_path.c_str(), "w");
            std::fprintf(_csv, "index,t_recv_ns,n_symbols,encoding,snr\n");
        }
    }
    void stop() { close(); }
    void close() {
        if (_bin) { std::fclose(_bin); _bin = nullptr; }
        if (_csv) { std::fclose(_csv); _csv = nullptr; }
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn) {
        const uint64_t t = monotonic_ns();
        if (_bin) {
            std::fwrite(sIn.data(), sizeof(cf), sIn.size(), _bin);
        }
        _items += sIn.size();
        _partial += sIn.size();
        while (_partial >= 48) {
            if (_csv) {
                std::fprintf(_csv, "%llu,%llu,48,-1,nan\n", static_cast<unsigned long long>(_messages), static_cast<unsigned long long>(t));
            }
            _messages++;
            _partial -= 48;
        }
        return gr::work::Status::OK;
    }
};

} // namespace gr4wifi
