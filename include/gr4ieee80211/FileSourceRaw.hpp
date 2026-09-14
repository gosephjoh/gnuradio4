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

#include <cstdio>
#include <string>

namespace gr4wifi {

template<typename T>
struct FileSourceRaw : gr::Block<FileSourceRaw<T>> {
    using Description = gr::Doc<"raw binary file source (GR3 blocks::file_source semantics)">;

    gr::PortOut<T> out;

    std::string file_name;

    GR_MAKE_REFLECTABLE(FileSourceRaw, out, file_name);

    std::FILE* _f     = nullptr;
    uint64_t   _items = 0;

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
            sOut.publish(0);
            return gr::work::Status::DONE;
        }
        const std::size_t n = std::fread(sOut.data(), sizeof(T), sOut.size(), _f);
        _items += n;
        sOut.publish(n);
        if (n < sOut.size()) {
            std::fclose(_f);
            _f = nullptr;
            return gr::work::Status::DONE;
        }
        return gr::work::Status::OK;
    }
};

} // namespace gr4wifi
