/*
 * FileSourceRaw -- a raw complex64 file source with GR3 file_source
 * semantics: read the file front to back into the output buffer, no header,
 * host byte order, DONE at end of file.
 *
 * GR4's BasicFileSource reads through an asynchronous reader on the IO thread
 * pool; on this tree (gosephjoh/gnuradio4 modular-scheduling, 29320de) it
 * delivered no sample under either the single- or the multi-threaded
 * scheduler (apps/probe.cpp mode "fsrc": counting sink 0 after 10 s, every
 * block STOPPED).  A synchronous reader is what GR3's file_source is and is
 * all this graph needs.
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>

#include <algorithm>
#include <cstdio>
#include <string>
#include <time.h>

namespace gr4wifi {

template<typename T>
struct FileSourceRaw : gr::Block<FileSourceRaw<T>> {
    using Description = gr::Doc<"raw binary file source (GR3 blocks::file_source semantics)">;

    gr::PortOut<T> out;

    std::string file_name;
    // sample_rate: the rate at which the samples represent air time.  Not
    // used to pace anything (the Throttle does that); it exists so GR4's
    // scheduling analysis can derive a period per block from the graph
    // (RT reference 4.2 -- derivation only anchors on a *source* that
    // declares `sample_rate`).  0 = undeclared.
    float      sample_rate = 0.f;
    // pad_to_multiple: at end of file, append zero samples until the total
    // item count is a multiple of this.  With fixed batches (input ports
    // requiring exactly N samples) a tail shorter than N would otherwise sit
    // behind the floor; the padding lets every block drain.  0 = no padding.
    gr::Size_t pad_to_multiple = 0U;
    // pad_min_tail: at least this many zero samples after the file, before
    // the rounding above.  The cell's own flush tail (448 samples, decision
    // 0035) is what GR3's variable-size calls need to push the last frame
    // through delay(320) -> sync_long -> fft -> equalizer -> decoder; with
    // fixed N-sample calls the flush needs whole batches: the last frame was
    // lost at N <= 1024 with the 448 alone and decoded at N = 2048 (2008
    // trailing samples).  chain.cpp asks for max(3 N, 2560).
    gr::Size_t pad_min_tail = 0U;

    GR_MAKE_REFLECTABLE(FileSourceRaw, out, file_name, sample_rate, pad_to_multiple, pad_min_tail);

    std::FILE* _f        = nullptr;
    uint64_t   _items    = 0;
    uint64_t   _pad_left = 0; // zero samples still to publish after EOF
    uint64_t   _calls = 0, _max_read_ns = 0, _sum_read_ns = 0, _max_read_items = 0;

    void start() {
        _f = std::fopen(file_name.c_str(), "rb");
        if (!_f) {
            throw gr::exception(std::format("FileSourceRaw: cannot open '{}'", file_name));
        }
        _items = 0;
    }
    void stop() {
        if (_f) {
            std::fclose(_f);
            _f = nullptr;
        }
    }

    gr::work::Status processBulk(gr::OutputSpanLike auto& sOut) {
        if (!_f) {
            if (_pad_left > 0) {
                const std::size_t n = static_cast<std::size_t>(std::min<uint64_t>(_pad_left, sOut.size()));
                std::fill_n(sOut.data(), n, T{});
                _pad_left -= n;
                _items += n;
                sOut.publish(n);
                return _pad_left > 0 ? gr::work::Status::OK : gr::work::Status::DONE;
            }
            sOut.publish(0);
            return gr::work::Status::DONE;
        }
        struct timespec a, b;
        clock_gettime(CLOCK_MONOTONIC, &a);
        const std::size_t n = std::fread(sOut.data(), sizeof(T), sOut.size(), _f);
        clock_gettime(CLOCK_MONOTONIC, &b);
        const uint64_t dt = static_cast<uint64_t>(b.tv_sec - a.tv_sec) * 1000000000ull + static_cast<uint64_t>(b.tv_nsec - a.tv_nsec);
        _calls++;
        _sum_read_ns += dt;
        if (dt > _max_read_ns) { _max_read_ns = dt; }
        if (n > _max_read_items) { _max_read_items = n; }
        _items += n;
        if (n < sOut.size()) {
            std::fclose(_f);
            _f = nullptr;
            _pad_left = pad_min_tail;
            if (pad_to_multiple > 0U) {
                const uint64_t rem = (_items + _pad_left) % pad_to_multiple;
                _pad_left += rem == 0 ? 0 : pad_to_multiple - rem;
            }
            // pad what fits into this call, the rest on the next
            const std::size_t room = sOut.size() - n;
            const std::size_t k    = static_cast<std::size_t>(std::min<uint64_t>(_pad_left, room));
            std::fill_n(sOut.data() + n, k, T{});
            _pad_left -= k;
            _items += k;
            sOut.publish(n + k);
            return _pad_left > 0 ? gr::work::Status::OK : gr::work::Status::DONE;
        }
        sOut.publish(n);
        return gr::work::Status::OK;
    }
};

} // namespace gr4wifi
