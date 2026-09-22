/*
 * basic_blocks.hpp -- the GR3 stock blocks the receiver uses that GR4 does
 * not ship: throttle, sample delay, |x|^2, conjugate, moving average.
 * Each reproduces the GR3 block's arithmetic (findings/cpp-build-notes.md
 * of the GR3 project names the block and its parameters; the GR3 sources are
 * gr-blocks/lib/{throttle,delay,moving_average}_impl.cc, maint-3.10).
 */
#pragma once

#include <gnuradio-4.0/Block.hpp>
#include <gnuradio-4.0/BlockingSync.hpp>

#include <algorithm>
#include <complex>
#include <time.h>
#include <numeric>
#include <vector>

namespace gr4wifi {

/*
 * Throttle -- GR3's blocks::throttle(itemsize, rate, ignore_tags=true, chunk).
 *
 * GR3 copies at most `chunk` items, sleeps until their end-of-air time, then
 * returns, so a chunk becomes visible downstream at the real-time arrival of
 * its last sample.  A GR4 block must not sleep inside the scheduler, so this
 * one uses the BlockingSync mixin: an internal timer thread wakes the
 * scheduler every chunk period and processBulk releases the samples that are
 * due (elapsed time x rate, capped at `chunk_size`).  Same visible behaviour
 * -- a chunk is released at or after its end-of-air time -- and the same
 * bound on the arrival-stamp bias, chunk_size / sample_rate.
 */
template<typename T>
struct Throttle : gr::Block<Throttle<T>>, gr::BlockingSync<Throttle<T>> {
    using Description = gr::Doc<"GR3-style throttle: releases samples at sample_rate, at most chunk_size per wake-up">;

    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    float      sample_rate         = 10e6f;
    gr::Size_t chunk_size          = 4096U;
    bool       use_internal_thread = true;
    // catch_up: when behind schedule, release everything that is due in one
    // call (GR3's thread loops without sleeping until it has caught up; a GR4
    // block gets one call per scheduler pass, so a per-call cap of chunk_size
    // drains a backlog only chunk_size per pass).  false = BlockingSync's cap.
    bool       catch_up            = false;
    // whole_chunks: publish only whole multiples of chunk_size, k*chunk_size
    // per call with k = min(chunks due by the wall clock, chunks that fit).
    // This is the fixed-batch arrival point of decision 0036: chunk j
    // (samples [j*N, (j+1)*N)) is nominally complete at start + (j+1)*N/rate
    // and becomes visible downstream at the first call after that.  A late
    // call publishes several chunks, so lateness is recoverable (catch_up is
    // implied).  The wall clock is CLOCK_MONOTONIC from `_start_ns`, the
    // same clock the trace layer and the arrival stamper use.
    bool       whole_chunks        = false;

    GR_MAKE_REFLECTABLE(Throttle, in, out, sample_rate, chunk_size, use_internal_thread, catch_up, whole_chunks);

    uint64_t _calls = 0, _released = 0, _last_call_ns = 0, _max_gap_ns = 0, _max_backlog = 0, _sum_gap_ns = 0;
    uint64_t _start_ns = 0;        // CLOCK_MONOTONIC at the first processBulk call: the nominal-arrival anchor
    uint64_t _lifecycle_start_ns = 0; // CLOCK_MONOTONIC at start(), for the record
    uint64_t _chunks_published = 0; // whole_chunks mode: chunks so far
    uint64_t _max_chunks_per_call = 0;
    static uint64_t nowNs() { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return static_cast<uint64_t>(t.tv_sec) * 1000000000ull + static_cast<uint64_t>(t.tv_nsec); }

    void start() {
        this->blockingSyncStart();
        _lifecycle_start_ns = nowNs();
        _start_ns           = 0; // set on the first call: the graph is only running then (start() precedes
                                 // the workers by tens of milliseconds, which is a start-up transient,
                                 // not a response time)
    }
    void stop() { this->blockingSyncStop(); }

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) {
        const std::size_t avail = std::min(input.size(), output.size());
        const uint64_t    now   = nowNs();
        if (_start_ns == 0) { _start_ns = now; }
        if (_last_call_ns) { const uint64_t g = now - _last_call_ns; _sum_gap_ns += g; if (g > _max_gap_ns) { _max_gap_ns = g; } }
        _last_call_ns = now;
        _calls++;
        // how far behind: samples due since the start minus samples released
        const double due_total = std::chrono::duration<double>(std::chrono::system_clock::now() - this->blockingSyncStartTime()).count() * static_cast<double>(sample_rate);
        const uint64_t backlog = due_total > static_cast<double>(_released) ? static_cast<uint64_t>(due_total - static_cast<double>(_released)) : 0;
        if (backlog > _max_backlog) { _max_backlog = backlog; }
        std::size_t n;
        if (whole_chunks) {
            // chunks due by the monotonic clock, minus chunks already out
            const double   due_total_mono = static_cast<double>(now - _start_ns) * 1e-9 * static_cast<double>(sample_rate);
            const uint64_t due            = due_total_mono > static_cast<double>(_released) ? static_cast<uint64_t>(due_total_mono - static_cast<double>(_released)) : 0;
            if (due > _max_backlog) { _max_backlog = due; }
            const std::size_t k = std::min(static_cast<std::size_t>(due / chunk_size), avail / chunk_size);
            n                   = k * chunk_size;
            _chunks_published += k;
            if (k > _max_chunks_per_call) { _max_chunks_per_call = k; }
        } else if (catch_up) {
            const std::size_t due = std::min(avail, static_cast<std::size_t>(backlog));
            n = due;
            // keep BlockingSync's clock in step: consume the same count from it
            std::size_t left = n;
            while (left > 0) { const std::size_t k = this->syncSamples(left); if (k == 0) { break; } left -= k; }
            n -= left;
        } else {
            n = this->syncSamples(avail);
        }
        _released += n;
        if (n == 0) {
            std::ignore = input.consume(0);
            output.publish(0);
            return gr::work::Status::INSUFFICIENT_INPUT_ITEMS;
        }
        std::copy_n(input.begin(), n, output.begin());
        std::ignore = input.consume(n);
        output.publish(n);
        return gr::work::Status::OK;
    }
};

/*
 * SampleDelay -- GR3's blocks::delay(itemsize, d): the first d outputs are
 * zero, then out[i] = in[i - d].
 */
template<typename T>
struct SampleDelay : gr::Block<SampleDelay<T>> {
    using Description = gr::Doc<"delay by `delay` samples; the first `delay` outputs are zero">;

    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    gr::Size_t delay = 0U;

    GR_MAKE_REFLECTABLE(SampleDelay, in, out, delay);

    std::vector<T> _hist;
    std::size_t    _pos = 0;
    uint64_t       _items = 0;

    void start() {
        _hist.assign(delay, T{});
        _pos = 0;
    }

    [[nodiscard]] T processOne(T x) noexcept {
        _items++;
        if (delay == 0U) {
            return x;
        }
        const T y   = _hist[_pos];
        _hist[_pos] = x;
        _pos        = (_pos + 1 == _hist.size()) ? 0 : _pos + 1;
        return y;
    }
};

// GR3 blocks::complex_to_mag_squared: re*re + im*im (volk_32fc_magnitude_squared_32f).
struct MagSquared : gr::Block<MagSquared> {
    using Description = gr::Doc<"|x|^2 of a complex stream, as re*re + im*im">;

    gr::PortIn<std::complex<float>> in;
    gr::PortOut<float>              out;

    GR_MAKE_REFLECTABLE(MagSquared, in, out);

    [[nodiscard]] constexpr float processOne(std::complex<float> x) const noexcept { return x.real() * x.real() + x.imag() * x.imag(); }
};

/*
 * Multiply2 / Divide2 -- GR3's blocks::multiply_cc / divide_ff with two
 * inputs.  GR4 has Multiply<T>/Divide<T> with a dynamic port vector
 * (in#0, in#1); on this tree a graph using them never ran (apps/probe.cpp
 * mode "m4": counting sink 0), so the two-input case is written out with
 * static ports.  Same arithmetic: std::complex operator* and float division.
 */
template<typename T>
struct Multiply2 : gr::Block<Multiply2<T>> {
    using Description = gr::Doc<"out = in0 * in1">;
    gr::PortIn<T>  in0;
    gr::PortIn<T>  in1;
    gr::PortOut<T> out;
    GR_MAKE_REFLECTABLE(Multiply2, in0, in1, out);
    [[nodiscard]] constexpr T processOne(T a, T b) const noexcept { return a * b; }
};

template<typename T>
struct Divide2 : gr::Block<Divide2<T>> {
    using Description = gr::Doc<"out = in0 / in1">;
    gr::PortIn<T>  in0;
    gr::PortIn<T>  in1;
    gr::PortOut<T> out;
    GR_MAKE_REFLECTABLE(Divide2, in0, in1, out);
    [[nodiscard]] constexpr T processOne(T a, T b) const noexcept { return a / b; }
};

// GR3 blocks::conjugate_cc.
struct Conjugate : gr::Block<Conjugate> {
    using Description = gr::Doc<"complex conjugate">;

    gr::PortIn<std::complex<float>>  in;
    gr::PortOut<std::complex<float>> out;

    GR_MAKE_REFLECTABLE(Conjugate, in, out);

    [[nodiscard]] constexpr std::complex<float> processOne(std::complex<float> x) const noexcept { return std::conj(x); }
};

/*
 * MovingAverage -- GR3's blocks::moving_average<T>(length, scale=1, max_iter):
 *
 *   history = length; per work() call: sum = accumulate(in[0 .. length-1))
 *   (the length-1 items of history), then for i < min(noutput, max_iter):
 *   sum += in[i + length - 1]; out[i] = sum * scale; sum -= in[i].
 *
 * So out[i] is the sum of the length items ending at i, re-accumulated from
 * the window at the start of every call, at most max_iter items per call.
 * That re-summation is the ULP mechanism port-requirements 1.2 describes;
 * it is kept, and the chunking (which the scheduler decides) is where GR3
 * and GR4 may differ at the last bit.  scale is 1: GR3 multiplies by
 * T(1) which is exact, so it is omitted.
 */
template<typename T>
struct MovingAverage : gr::Block<MovingAverage<T>> {
    using Description = gr::Doc<"GR3 moving_average: running sum over `length` items, re-summed every call, max_iter per call">;

    gr::PortIn<T>  in;
    gr::PortOut<T> out;

    gr::Size_t length   = 1U;
    gr::Size_t max_iter = 4000U;

    GR_MAKE_REFLECTABLE(MovingAverage, in, out, length, max_iter);

    std::vector<T> _window; // history (length-1 items) followed by the call's input
    uint64_t       _items = 0, _calls = 0;

    void start() { _window.assign(length > 0 ? length - 1 : 0, T{}); }

    gr::work::Status processBulk(gr::InputSpanLike auto& input, gr::OutputSpanLike auto& output) {
        const std::size_t L = length;
        const std::size_t n = std::min({input.size(), output.size(), static_cast<std::size_t>(max_iter)});
        if (n == 0) {
            std::ignore = input.consume(0);
            output.publish(0);
            return gr::work::Status::INSUFFICIENT_INPUT_ITEMS;
        }
        _window.resize(L - 1 + n);
        std::copy_n(input.begin(), n, _window.begin() + static_cast<std::ptrdiff_t>(L - 1));
        const T* w = _window.data();
        T sum = std::accumulate(&w[0], &w[L - 1], T{});
        for (std::size_t i = 0; i < n; i++) {
            sum += w[i + L - 1];
            output[i] = sum;
            sum -= w[i];
        }
        // keep the last length-1 items as the next call's history
        _items += n;
        _calls++;
        std::copy(_window.end() - static_cast<std::ptrdiff_t>(L - 1), _window.end(), _window.begin());
        _window.resize(L - 1);
        std::ignore = input.consume(n);
        output.publish(n);
        return gr::work::Status::OK;
    }
};

} // namespace gr4wifi
