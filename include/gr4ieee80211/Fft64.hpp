/*
 * Fft64 -- GR3's stream_to_vector(64) + fft_v<gr_complex, true>(64,
 * rectangular window, shift = true) as one GR4 block: for every 64 input
 * samples, the 64-point forward FFT with its halves swapped (GR3's shift for
 * a forward transform swaps the OUTPUT halves; gr-fft/lib/fft_v_fftw.cc).
 * The window is 1.0 everywhere, an exact no-op, so it is omitted.
 *
 * GR3 computes with single-precision FFTW; this uses GR4's own
 * gr::algorithm::FFT in single precision.  The two agree to float rounding,
 * which is why the symbols plane is toleranced (port-requirements 1.2).
 *
 * Tags (`wifi_start` from sync_long, always on a 64-boundary) are forwarded
 * at the same offset.
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>
#include <gnuradio-4.0/algorithm/fourier/fft.hpp>

#include "wifi_codec.hpp"

#include <vector>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace gr4wifi {

struct Fft64 : gr::Block<Fft64, gr::Resampling<64UZ, 64UZ, true>, gr::NoTagPropagation> {
    using Description = gr::Doc<"64-point forward FFT per 64 samples, output halves swapped (GR3 fft_vcc shift=true)">;

    gr::PortIn<cf>  in;
    gr::PortOut<cf> out;

    GR_MAKE_REFLECTABLE(Fft64, in, out);

    gr::algorithm::FFT<cf, cf> _fft;
    std::vector<cf>            _in  = std::vector<cf>(64);
    std::vector<cf>            _out = std::vector<cf>(64);
    uint64_t                   _items = 0, _tags_in = 0, _tags_out = 0, _calls = 0;
    std::FILE*                 _trace = nullptr;

    void start() {
        if (const char* t = std::getenv("GR4_SYNCLONG_TRACE")) {
            _trace = std::fopen((std::string(t) + "." + std::string(this->name.value)).c_str(), "w");
        }
    }
    void stop() {
        if (_trace) { std::fclose(_trace); _trace = nullptr; }
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& sIn, gr::OutputSpanLike auto& sOut) {
        const std::size_t n = std::min(sIn.size(), sOut.size()) / 64 * 64;
        for (std::size_t k = 0; k < n; k += 64) {
            std::copy_n(sIn.begin() + static_cast<std::ptrdiff_t>(k), 64, _in.begin());
            _fft.compute(_in, _out);
            // fft_v_fftw.cc, forward + shift: len = (64+1)/2 = 32
            std::copy_n(_out.begin() + 32, 32, sOut.begin() + static_cast<std::ptrdiff_t>(k));
            std::copy_n(_out.begin(), 32, sOut.begin() + static_cast<std::ptrdiff_t>(k + 32));
        }
        _calls++;
        for (const auto& [rel, map] : sIn.tags()) {
            _tags_in++;
            if (_trace) { std::fprintf(_trace, "%llu,in=%zu,out=%zu,n=%zu,tag_rel=%td\n", static_cast<unsigned long long>(_calls), sIn.size(), sOut.size(), n, static_cast<std::ptrdiff_t>(rel)); }
            if (rel >= 0 && static_cast<std::size_t>(rel) < n) {
                sOut.publishTag(map.get(), static_cast<std::size_t>(rel));
                _tags_out++;
            }
        }
        _items += n;
        std::ignore = sIn.consume(n);
        sIn.consumeTags(n);
        sOut.publish(n);
        return gr::work::Status::OK;
    }
};

} // namespace gr4wifi
