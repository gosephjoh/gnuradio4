/*
 * Pacer.hpp -- the clock-driven input from outside the scheduler.
 *
 * One thread writes every receiver's stimulus into that receiver's entry
 * block at the nominal instant of each chunk, and logs when it did.  The
 * workers only ever see data arriving, as they would from a front end: no
 * block of the graph holds the clock, so no block is polled for it and no
 * arrival waits for the scheduler under test.
 *
 * The replay is FileSourceRaw's, sample for sample: the first `samples` of
 * the file (the whole file when shorter), then `pad_min_tail` zeros rounded
 * up to a multiple of `pad_to_multiple`.  Chunk j of a receiver covers
 * samples [j C, min((j+1) C, total)) and is due at t0 + end_j / rate, the
 * instant the throttle's whole_chunks mode was entitled to publish it.
 *
 * The pacer never drops data: a chunk that does not fit is retried every
 * 5 us and written late, and the lag is the saturation evidence.  A full
 * buffer delays only its own receiver -- the others' chunks are written when
 * due meanwhile -- so one receiver falling behind cannot move another's
 * arrivals.  After a receiver's last chunk it publishes end of stream, which
 * ends the graph.
 *
 * An entry block must take no in-graph input (chain.cpp refuses to build one): the
 * scheduler's release scan learns who feeds a block from graph edges, and an
 * in-graph producer next to this port would let it skip the block while the
 * pacer's data waits.
 */
#pragma once

#include <gnuradio-4.0/Port.hpp>
#include <gnuradio-4.0/Tag.hpp>

#include "wifi_codec.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <expected>
#include <format>
#include <functional>
#include <limits>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace gr4wifi {

struct PacerStream {
    gr::PortOut<cf> out;
    double          rate     = 0.0;
    std::size_t     chunk    = 1024UZ;
    uint64_t        fromFile = 0; // samples taken from the file
    uint64_t        total    = 0; // with the zero padding

    uint64_t t0Ns = 0; // the pacer's clock origin, set when it starts

    struct Chunk {
        uint64_t dueNs     = 0; // after t0: the instant its last sample is due at `rate`
        uint64_t writtenNs = 0; // 0 = never written
        uint32_t retries   = 0;
    };
    std::vector<Chunk> log; // one row per chunk, filled before the run so none of it costs time at t0

    [[nodiscard]] std::size_t chunkCount() const noexcept { return (total + chunk - 1) / chunk; }
    [[nodiscard]] std::size_t chunkOf(uint64_t sample) const noexcept { return sample / chunk; }
    [[nodiscard]] uint64_t    nominalNs(std::size_t j) const noexcept { return t0Ns + log[j].dueNs; }
};

struct PacerSettings {
    std::size_t chunk         = 1024UZ;
    uint64_t    padToMultiple = 0;
    uint64_t    padMinTail    = 0;
    std::size_t buffer        = 16384UZ; // items in each receiver's entry buffer
    int         cpu           = -1;      // pin the pacer thread; -1 = anywhere
    int         fifoPriority  = 0;       // SCHED_FIFO priority; 0 = SCHED_OTHER
    uint64_t    spinNs        = 60'000;  // sleep until this long before a chunk is due, then spin
    uint64_t    startDelayNs  = 500'000'000;
};

class Pacer {
    // owns an mmap and a thread: a class for the RAII, not for an interface
    PacerSettings           _settings;
    int                     _fd          = -1;
    const cf*               _file        = nullptr;
    uint64_t                _fileSamples = 0;
    std::size_t             _mappedBytes = 0;
    std::deque<PacerStream> _streams; // deque: a connected port must never move
    std::thread             _thread;
    std::atomic<bool>       _stop{false};
    std::atomic<bool>       _finished{false};
    uint64_t                _t0Ns = 0;
    std::string             _error;

    static uint64_t nowNs() noexcept { return monotonic_ns(); }

    void sleepUntil(uint64_t dueNs) const noexcept {
        const uint64_t wakeNs = dueNs > _settings.spinNs ? dueNs - _settings.spinNs : dueNs;
        if (wakeNs > nowNs()) {
            timespec ts{.tv_sec = static_cast<time_t>(wakeNs / 1'000'000'000ULL), .tv_nsec = static_cast<long>(wakeNs % 1'000'000'000ULL)};
            while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr) != 0 && !_stop.load(std::memory_order_relaxed)) {
            }
        }
        while (nowNs() < dueNs && !_stop.load(std::memory_order_relaxed)) {
        }
    }

    // copy [first, first + n) of the replay, zeros past the file's part
    void fill(std::span<cf> dst, const PacerStream& s, uint64_t first) const noexcept {
        const uint64_t fromFile = first < s.fromFile ? std::min<uint64_t>(dst.size(), s.fromFile - first) : 0;
        if (fromFile > 0) {
            std::copy_n(_file + first, fromFile, dst.begin());
        }
        std::fill(dst.begin() + static_cast<std::ptrdiff_t>(fromFile), dst.end(), cf{});
    }

    // one attempt: false when the entry buffer has no room for the whole chunk
    bool tryWriteChunk(PacerStream& s, std::size_t j) {
        const uint64_t    first = j * s.chunk;
        const std::size_t n     = static_cast<std::size_t>(std::min<uint64_t>(s.chunk, s.total - first));
        auto              span  = s.out.tryReserve<gr::SpanReleasePolicy::ProcessAll>(n);
        if (span.size() < n) {
            span.publish(0UZ);
            return false;
        }
        fill(std::span<cf>(span.data(), n), s, first);
        span.publish(n);
        std::atomic_ref(s.log[j].writtenNs).store(nowNs(), std::memory_order_release); // may be watched while the pacer runs
        return true;
    }

    void configureThread() {
        prctl(PR_SET_TIMERSLACK, 1UL, 0UL, 0UL, 0UL);
        if (_settings.cpu >= 0) {
            cpu_set_t set;
            CPU_ZERO(&set);
            CPU_SET(static_cast<std::size_t>(_settings.cpu), &set);
            if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0) {
                _error = std::format("cannot pin the pacer to CPU {}", _settings.cpu);
            }
        }
        if (_settings.fifoPriority > 0) {
            const sched_param param{.sched_priority = _settings.fifoPriority};
            if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &param) != 0) {
                _error = std::format("cannot give the pacer SCHED_FIFO {}", _settings.fifoPriority);
            }
        }
    }

    void run(std::function<bool()> graphRunning) {
        configureThread();
        while (!graphRunning()) {
            if (_stop.load()) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        // the workers' first passes build their trace rings and touch their buffers; a pacer that
        // starts at once fills the entry buffers during that transient and measures it as lag
        std::this_thread::sleep_for(std::chrono::nanoseconds(_settings.startDelayNs));
        _t0Ns = nowNs();
        for (PacerStream& s : _streams) {
            s.t0Ns = _t0Ns;
        }
        std::vector<std::size_t> next(_streams.size(), 0UZ);
        const auto               pending = [&](std::size_t k) { return next[k] < _streams[k].log.size(); };
        while (!_stop.load(std::memory_order_relaxed)) {
            uint64_t earliest = std::numeric_limits<uint64_t>::max();
            for (std::size_t k = 0; k < _streams.size(); ++k) {
                if (pending(k)) {
                    earliest = std::min(earliest, _streams[k].nominalNs(next[k]));
                }
            }
            if (earliest == std::numeric_limits<uint64_t>::max()) {
                _finished = true; // every receiver has had its last chunk
                return;
            }
            sleepUntil(earliest);
            // every chunk now due, of every receiver: a full buffer only postpones its own receiver
            bool           wrote = false;
            const uint64_t now   = nowNs();
            for (std::size_t k = 0; k < _streams.size(); ++k) {
                PacerStream& s = _streams[k];
                if (!pending(k) || s.nominalNs(next[k]) > now) {
                    continue;
                }
                if (!tryWriteChunk(s, next[k])) {
                    ++s.log[next[k]].retries;
                    continue;
                }
                wrote = true;
                if (++next[k] == s.log.size()) {
                    s.out.publishTag(gr::property_map{{gr::tag::END_OF_STREAM, true}}, 0UZ);
                }
            }
            if (!wrote) {
                std::this_thread::sleep_for(std::chrono::microseconds(5));
            }
        }
    }

public:
    explicit Pacer(PacerSettings settings) : _settings(settings) {}
    Pacer(const Pacer&)            = delete;
    Pacer& operator=(const Pacer&) = delete;

    ~Pacer() {
        stop();
        if (_file != nullptr) {
            munmap(const_cast<cf*>(_file), _mappedBytes);
        }
        if (_fd >= 0) {
            close(_fd);
        }
    }

    [[nodiscard]] std::expected<void, std::string> open(const std::string& path) {
        _fd = ::open(path.c_str(), O_RDONLY);
        struct stat st{};
        if (_fd < 0 || fstat(_fd, &st) != 0) {
            return std::unexpected(std::format("cannot open {}", path));
        }
        _mappedBytes = static_cast<std::size_t>(st.st_size);
        _fileSamples = _mappedBytes / sizeof(cf);
        void* p      = mmap(nullptr, _mappedBytes, PROT_READ, MAP_SHARED, _fd, 0);
        if (p == MAP_FAILED) {
            return std::unexpected(std::format("cannot map {}", path));
        }
        _file = static_cast<const cf*>(p);
        return {};
    }

    // a receiver replaying the first `maxSamples` of the file (0 = all of it) at `rate`
    PacerStream& addStream(double rate, uint64_t maxSamples) {
        PacerStream& s = _streams.emplace_back();
        s.rate         = rate;
        s.chunk        = _settings.chunk;
        s.fromFile     = maxSamples > 0 ? std::min(maxSamples, _fileSamples) : _fileSamples;
        s.total        = s.fromFile + _settings.padMinTail;
        if (_settings.padToMultiple > 0 && s.total % _settings.padToMultiple != 0) {
            s.total += _settings.padToMultiple - s.total % _settings.padToMultiple;
        }
        s.log.resize(s.chunkCount());
        for (std::size_t j = 0; j < s.log.size(); ++j) {
            const uint64_t end = std::min<uint64_t>((j + 1) * s.chunk, s.total);
            s.log[j].dueNs     = static_cast<uint64_t>(static_cast<double>(end) / rate * 1e9);
        }
        std::ignore = s.out.resizeBuffer(_settings.buffer);
        return s;
    }

    // before the run: fault in every page a receiver will read, so no page fault lands on a write
    void prefault() const noexcept {
        uint64_t longest = 0;
        for (const PacerStream& s : _streams) {
            longest = std::max(longest, s.fromFile);
        }
        const std::size_t bytes = static_cast<std::size_t>(longest) * sizeof(cf);
        madvise(const_cast<cf*>(_file), bytes, MADV_WILLNEED);
        volatile unsigned char sink = 0;
        const auto*            base = reinterpret_cast<const unsigned char*>(_file);
        for (std::size_t off = 0; off < bytes; off += 4096UZ) {
            sink = static_cast<unsigned char>(sink + base[off]);
        }
    }

    void start(std::function<bool()> graphRunning) {
        _thread = std::thread([this, running = std::move(graphRunning)] { run(running); });
    }

    void stop() {
        _stop = true;
        if (_thread.joinable()) {
            _thread.join();
        }
    }

    [[nodiscard]] const std::deque<PacerStream>& streams() const noexcept { return _streams; }
    [[nodiscard]] uint64_t                       t0Ns() const noexcept { return _t0Ns; }
    [[nodiscard]] bool                           finished() const noexcept { return _finished.load(); }
    [[nodiscard]] const std::string&             error() const noexcept { return _error; }
};

} // namespace gr4wifi
