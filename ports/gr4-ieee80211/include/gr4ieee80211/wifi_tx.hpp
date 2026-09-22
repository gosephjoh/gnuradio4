/*
 * wifi_tx.hpp -- the GR3 harness's sample generator, standalone.
 *
 * Reproduces, with no GNU Radio of either generation linked, the pipeline
 * that produced every cell of the GR3 project's data/ and data/golden/:
 *
 *   gen-input      payloads.bin (mt19937_64 bodies, u32 sequence numbers)
 *   mac            24-byte header + MSDU + CRC-32 (mac.cc)
 *   mapper         service/pad -> scramble(seed = (i mod 127)+1) -> tail reset
 *                  -> K=7 r=1/2 encode -> puncture -> interleave -> n_bpsc
 *                  bits per carrier (lib/utils.cc)
 *   signal_field   24-bit SIGNAL -> encode -> interleave, BPSK (signal_field_impl.cc)
 *   chunks_to_symbols  constellation points (constellations_impl.cc)
 *   ofdm_carrier_allocator_cvc(64, occupied, pilots, pilot_symbols, sync_words, shifted)
 *   fft_v<gr_complex, false>(64, window 1/sqrt(52), shift)   -- inverse FFT
 *   ofdm_cyclic_prefixer(64, 80, rolloff 2)                 -- (n_sym+5)*80+1 samples
 *   packet_pad2(pad_front, pad_tail) + noise_source_c(GR_GAUSSIAN, 0.01, 1234)
 *
 * Every rule is docs/port-requirements.md section 2 and 3 of the GR3 project;
 * the tables are its generated src/phy/wifi_tables.h (copied here as
 * wifi_tables.h).  Integer planes are bit-exact by construction; the sample
 * plane differs from FFTW's single-precision result at the last bit, which
 * the GR3 project's compare tool tolerates (max|d| <= 1e-5).  The noise is
 * GR3's own generator -- xoroshiro128+ seeded through splitmix64 and a jump,
 * std::uniform_real_distribution<float>, Marsaglia polar gasdev -- so the
 * stimulus reproduces GR3's to that same last bit.
 *
 * GPL-3.0-or-later (upstream gr-ieee802-11, GNU Radio).
 */
#pragma once

#include "wifi_codec.hpp"
#include "wifi_tables.h"

#include <cmath>
#include <cstdint>
#include <numbers>
#include <random>
#include <vector>

namespace gr4wifi::tx {

// --------------------------------------------------------- GR3's gr::random
struct Xoroshiro128p {
    using result_type = uint64_t;
    uint64_t s[2];
    static constexpr uint64_t min() { return 0; }
    static constexpr uint64_t max() { return UINT64_MAX; }
    static uint64_t rotl(uint64_t x, int k) { return (x << k) | (x >> (64 - k)); }
    uint64_t operator()() {
        const uint64_t s0 = s[0];
        uint64_t       s1 = s[1];
        const uint64_t r  = s0 + s1;
        s1 ^= s0;
        s[0] = rotl(s0, 55) ^ s1 ^ (s1 << 14);
        s[1] = rotl(s1, 36);
        return r;
    }
    void jump() {
        static const uint64_t JUMP[] = {0xbeac0467eba5facb, 0xd86b048b86aa9922};
        uint64_t s0 = 0, s1 = 0;
        for (unsigned i = 0; i < 2; ++i) {
            for (unsigned b = 0; b < 64; ++b) {
                if (JUMP[i] & (UINT64_C(1) << b)) {
                    s0 ^= s[0];
                    s1 ^= s[1];
                }
                (*this)();
            }
        }
        s[0] = s0;
        s[1] = s1;
    }
    explicit Xoroshiro128p(uint64_t seed) {
        // xoroshiro128p_seed: state[0] = seed; state[1] = splitmix64_next(state)
        // -- and splitmix64_next(state) increments *state, i.e. state[0], first.
        s[0]       = seed;
        uint64_t z = (s[0] += 0x9e3779b97f4a7c15);
        z          = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
        z          = (z ^ (z >> 27)) * 0x94d049bb133111eb;
        s[1]       = z ^ (z >> 31);
        jump();
    }
};

// gnuradio-runtime/lib/math/random.cc: ran1() and gasdev()
struct GrRandom {
    Xoroshiro128p                         rng;
    std::uniform_real_distribution<float> uni{0.f, 1.f};
    bool  stored = false;
    float value  = 0.f;
    explicit GrRandom(uint64_t seed) : rng(seed) {}
    float ran1() { return uni(rng); }
    float gasdev() {
        if (stored) {
            stored = false;
            return value;
        }
        float x, y, s;
        do {
            x = 2.0 * ran1() - 1.0;
            y = 2.0 * ran1() - 1.0;
            s = x * x + y * y;
        } while (s >= 1.0f || s == 0.0f);
        stored = true;
        value  = x * sqrtf(-2.0 * logf(s) / s);
        return y * sqrtf(-2.0 * logf(s) / s);
    }
};

// --------------------------------------------------------------- constants
constexpr int kFft = 64, kCp = 16, kSym = 80;
constexpr std::array<uint8_t, 6> kSrc = {0x23, 0x23, 0x23, 0x23, 0x23, 0x23};
constexpr std::array<uint8_t, 6> kDst = {0x42, 0x42, 0x42, 0x42, 0x42, 0x42};
constexpr std::array<uint8_t, 6> kBss = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// constellations_impl.cc, indexed by the n_bpsc-bit chunk
inline cf map_point(Encoding e, unsigned v) {
    switch (e) {
    case BPSK_1_2: case BPSK_3_4: {
        static const cf t[2] = {cf(-1, 0), cf(1, 0)};
        return t[v & 1];
    }
    case QPSK_1_2: case QPSK_3_4: {
        const float l = std::sqrt(float(0.5));
        static const cf t[4] = {cf(-l, -l), cf(l, -l), cf(-l, l), cf(l, l)};
        return t[v & 3];
    }
    case QAM16_1_2: case QAM16_3_4: {
        const float l = std::sqrt(float(0.1));
        static const cf t[16] = {cf(-3 * l, -3 * l), cf(3 * l, -3 * l), cf(-1 * l, -3 * l), cf(1 * l, -3 * l), cf(-3 * l, 3 * l), cf(3 * l, 3 * l), cf(-1 * l, 3 * l), cf(1 * l, 3 * l), cf(-3 * l, -1 * l), cf(3 * l, -1 * l), cf(-1 * l, -1 * l), cf(1 * l, -1 * l), cf(-3 * l, 1 * l), cf(3 * l, 1 * l), cf(-1 * l, 1 * l), cf(1 * l, 1 * l)};
        return t[v & 15];
    }
    default: {
        const float l = std::sqrt(float(1 / 42.0));
        static const cf t[64] = {cf(-7 * l, -7 * l), cf(7 * l, -7 * l), cf(-1 * l, -7 * l), cf(1 * l, -7 * l), cf(-5 * l, -7 * l), cf(5 * l, -7 * l), cf(-3 * l, -7 * l), cf(3 * l, -7 * l), cf(-7 * l, 7 * l), cf(7 * l, 7 * l), cf(-1 * l, 7 * l), cf(1 * l, 7 * l), cf(-5 * l, 7 * l), cf(5 * l, 7 * l), cf(-3 * l, 7 * l), cf(3 * l, 7 * l), cf(-7 * l, -1 * l), cf(7 * l, -1 * l), cf(-1 * l, -1 * l), cf(1 * l, -1 * l), cf(-5 * l, -1 * l), cf(5 * l, -1 * l), cf(-3 * l, -1 * l), cf(3 * l, -1 * l), cf(-7 * l, 1 * l), cf(7 * l, 1 * l), cf(-1 * l, 1 * l), cf(1 * l, 1 * l), cf(-5 * l, 1 * l), cf(5 * l, 1 * l), cf(-3 * l, 1 * l), cf(3 * l, 1 * l), cf(-7 * l, -5 * l), cf(7 * l, -5 * l), cf(-1 * l, -5 * l), cf(1 * l, -5 * l), cf(-5 * l, -5 * l), cf(5 * l, -5 * l), cf(-3 * l, -5 * l), cf(3 * l, -5 * l), cf(-7 * l, 5 * l), cf(7 * l, 5 * l), cf(-1 * l, 5 * l), cf(1 * l, 5 * l), cf(-5 * l, 5 * l), cf(5 * l, 5 * l), cf(-3 * l, 5 * l), cf(3 * l, 5 * l), cf(-7 * l, -3 * l), cf(7 * l, -3 * l), cf(-1 * l, -3 * l), cf(1 * l, -3 * l), cf(-5 * l, -3 * l), cf(5 * l, -3 * l), cf(-3 * l, -3 * l), cf(3 * l, -3 * l), cf(-7 * l, 3 * l), cf(7 * l, 3 * l), cf(-1 * l, 3 * l), cf(1 * l, 3 * l), cf(-5 * l, 3 * l), cf(5 * l, 3 * l), cf(-3 * l, 3 * l), cf(3 * l, 3 * l)};
        return t[v & 63];
    }
    }
}

// ------------------------------------------------------- lib/utils.cc, TX
inline int ones(int n) {
    int s = 0;
    for (int i = 0; i < 8; i++) {
        if (n & (1 << i)) {
            s++;
        }
    }
    return s;
}

inline void generate_bits(const uint8_t* psdu, uint8_t* bits, const frame_param& f) {
    std::memset(bits, 0, 16);
    bits += 16;
    for (int i = 0; i < f.psdu_size; i++) {
        for (int b = 0; b < 8; b++) {
            bits[i * 8 + b] = !!(psdu[i] & (1 << b));
        }
    }
}
inline void scramble(const uint8_t* in, uint8_t* out, const frame_param& f, int initial_state) {
    int state = initial_state;
    for (int i = 0; i < f.n_data_bits; i++) {
        const int feedback = (!!(state & 64)) ^ (!!(state & 8));
        out[i]             = static_cast<uint8_t>(feedback ^ in[i]);
        state              = ((state << 1) & 0x7e) | feedback;
    }
}
inline void reset_tail_bits(uint8_t* scrambled, const frame_param& f) { std::memset(scrambled + f.n_data_bits - f.n_pad - 6, 0, 6); }
inline void convolutional_encoding(const uint8_t* in, uint8_t* out, int n_bits) {
    int state = 0;
    for (int i = 0; i < n_bits; i++) {
        state          = ((state << 1) & 0x7e) | in[i];
        out[i * 2]     = static_cast<uint8_t>(ones(state & 0155) % 2);
        out[i * 2 + 1] = static_cast<uint8_t>(ones(state & 0117) % 2);
    }
}
inline void puncturing(const uint8_t* in, uint8_t* out, const frame_param& f, const ofdm_param& o) {
    for (int i = 0; i < f.n_data_bits * 2; i++) {
        switch (o.encoding) {
        case BPSK_1_2: case QPSK_1_2: case QAM16_1_2: *out++ = in[i]; break;
        case QAM64_2_3:
            if (i % 4 != 3) { *out++ = in[i]; }
            break;
        default: {
            const int mod = i % 6;
            if (!(mod == 3 || mod == 4)) { *out++ = in[i]; }
        }
        }
    }
}
inline void interleave(const uint8_t* in, uint8_t* out, int n_sym, const ofdm_param& o) {
    const int n_cbps = o.n_cbps;
    int       first[MAX_BITS_PER_SYM], second[MAX_BITS_PER_SYM];
    const int s = std::max(o.n_bpsc / 2, 1);
    for (int j = 0; j < n_cbps; j++) {
        first[j] = s * (j / s) + ((j + static_cast<int>(std::floor(16.0 * j / n_cbps))) % s);
    }
    for (int i = 0; i < n_cbps; i++) {
        second[i] = 16 * i - (n_cbps - 1) * static_cast<int>(std::floor(16.0 * i / n_cbps));
    }
    for (int i = 0; i < n_sym; i++) {
        for (int k = 0; k < n_cbps; k++) {
            out[i * n_cbps + k] = in[i * n_cbps + second[first[k]]];
        }
    }
}
inline void split_symbols(const uint8_t* in, uint8_t* out, const frame_param& f, const ofdm_param& o) {
    const int symbols = f.n_sym * 48;
    for (int i = 0; i < symbols; i++) {
        out[i] = 0;
        for (int k = 0; k < o.n_bpsc; k++) {
            out[i] |= static_cast<uint8_t>((*in) << k);
            in++;
        }
    }
}

// mac.cc:134-170 -- header + msdu + FCS (little-endian CRC-32)
inline std::vector<uint8_t> mac_frame(const uint8_t* msdu, int msdu_size, uint16_t seq12) {
    std::vector<uint8_t> psdu(static_cast<std::size_t>(28 + msdu_size));
    psdu[0] = 0x08; psdu[1] = 0x00; // frame_control 0x0008 LE
    psdu[2] = 0; psdu[3] = 0;       // duration
    std::memcpy(&psdu[4], kDst.data(), 6);
    std::memcpy(&psdu[10], kSrc.data(), 6);
    std::memcpy(&psdu[16], kBss.data(), 6);
    uint16_t seq_nr = 0;
    for (int i = 0; i < 12; i++) {
        if (seq12 & (1 << i)) {
            seq_nr |= static_cast<uint16_t>(1 << (i + 4));
        }
    }
    psdu[22] = static_cast<uint8_t>(seq_nr & 0xff);
    psdu[23] = static_cast<uint8_t>(seq_nr >> 8);
    std::memcpy(&psdu[24], msdu, static_cast<std::size_t>(msdu_size));
    const uint32_t fcs = crc32(psdu.data(), static_cast<std::size_t>(24 + msdu_size));
    psdu[24 + msdu_size + 0] = static_cast<uint8_t>(fcs & 0xff);
    psdu[24 + msdu_size + 1] = static_cast<uint8_t>((fcs >> 8) & 0xff);
    psdu[24 + msdu_size + 2] = static_cast<uint8_t>((fcs >> 16) & 0xff);
    psdu[24 + msdu_size + 3] = static_cast<uint8_t>((fcs >> 24) & 0xff);
    return psdu;
}

// signal_field_impl.cc:41-102 -- 48 coded SIGNAL bits
inline void signal_field(const frame_param& f, const ofdm_param& o, uint8_t* out48) {
    uint8_t   hdr[24], enc[48];
    const int length = f.psdu_size;
    for (int i = 0; i < 4; i++) {
        hdr[i] = (o.rate_field & (1 << (3 - i))) ? 1 : 0;
    }
    hdr[4] = 0;
    for (int i = 0; i < 12; i++) {
        hdr[5 + i] = (length & (1 << i)) ? 1 : 0;
    }
    int sum = 0;
    for (int i = 0; i < 17; i++) {
        sum += hdr[i];
    }
    hdr[17] = static_cast<uint8_t>(sum % 2);
    for (int i = 0; i < 6; i++) {
        hdr[18 + i] = 0;
    }
    const ofdm_param so(BPSK_1_2);
    convolutional_encoding(hdr, enc, 24);
    interleave(enc, out48, 1, so);
}

// ----------------------------------------------------- 64-point inverse FFT
// Unnormalised backward transform (FFTW_BACKWARD): X[n] = sum_k x[k] e^{+2pi i kn/N}.
inline void ifft64(const cf* in, cf* out) {
    static const auto tw = [] {
        std::array<cf, 32> t{};
        for (int k = 0; k < 32; k++) {
            const double a = 2.0 * std::numbers::pi * k / 64.0;
            t[static_cast<std::size_t>(k)] = cf(static_cast<float>(std::cos(a)), static_cast<float>(std::sin(a)));
        }
        return t;
    }();
    // bit-reversed copy
    for (int i = 0; i < 64; i++) {
        int r = 0;
        for (int b = 0; b < 6; b++) {
            r = (r << 1) | ((i >> b) & 1);
        }
        out[r] = in[i];
    }
    for (int len = 2; len <= 64; len <<= 1) {
        const int step = 64 / len;
        for (int i = 0; i < 64; i += len) {
            for (int j = 0; j < len / 2; j++) {
                const cf w = tw[static_cast<std::size_t>(j * step)];
                const cf u = out[i + j];
                const cf v = out[i + j + len / 2] * w;
                out[i + j]           = u + v;
                out[i + j + len / 2] = u - v;
            }
        }
    }
}

// ----------------------------------------------------------- one frame
struct FrameSamples {
    std::vector<uint8_t> psdu;     // 28 + msdu
    std::vector<cf>      samples;  // (n_sym + 5) * 80 + 1, the golden plane
};

// scrambler_seed: GR3's mapper counter, (i mod 127) + 1 for frame i of a fresh graph
// mac_seq: GR3's mac counter, i mod 4096
inline FrameSamples encode_frame(const uint8_t* msdu, int msdu_size, Encoding enc, int scrambler_seed, uint16_t mac_seq) {
    FrameSamples fs;
    fs.psdu = mac_frame(msdu, msdu_size, mac_seq);
    const ofdm_param  ofdm(enc);
    const frame_param frame(ofdm, static_cast<int>(fs.psdu.size()));

    // mapper_impl.cc:107-124
    std::vector<uint8_t> data_bits(static_cast<std::size_t>(frame.n_data_bits)), scrambled(static_cast<std::size_t>(frame.n_data_bits)), encoded(static_cast<std::size_t>(frame.n_data_bits) * 2), punctured(static_cast<std::size_t>(frame.n_encoded_bits)), interleaved(static_cast<std::size_t>(frame.n_encoded_bits)), symbols(static_cast<std::size_t>(frame.n_sym) * 48);
    generate_bits(fs.psdu.data(), data_bits.data(), frame);
    scramble(data_bits.data(), scrambled.data(), frame, scrambler_seed);
    reset_tail_bits(scrambled.data(), frame);
    convolutional_encoding(scrambled.data(), encoded.data(), frame.n_data_bits);
    puncturing(encoded.data(), punctured.data(), frame, ofdm);
    interleave(punctured.data(), interleaved.data(), frame.n_sym, ofdm);
    split_symbols(interleaved.data(), symbols.data(), frame, ofdm);

    // SIGNAL (48 BPSK points) then DATA (n_sym * 48 points): tagged_stream_mux order
    uint8_t sig[48];
    signal_field(frame, ofdm, sig);
    std::vector<cf> points;
    points.reserve(48 + symbols.size());
    for (int i = 0; i < 48; i++) {
        points.push_back(sig[i] ? cf(1, 0) : cf(-1, 0)); // chunks_to_symbols_bc({-1, 1})
    }
    for (uint8_t v : symbols) {
        points.push_back(map_point(enc, v));
    }

    // ofdm_carrier_allocator_cvc: 4 sync words, then one OFDM symbol per 48 points,
    // data on the (shifted) occupied carriers, pilots from the polarity table
    const int n_ofdm = static_cast<int>(points.size()) / 48; // 1 + n_sym
    std::vector<cf> freq(static_cast<std::size_t>(4 + n_ofdm) * 64, cf{});
    for (int w = 0; w < 4; w++) {
        std::copy(kSyncWords[static_cast<std::size_t>(w)].begin(), kSyncWords[static_cast<std::size_t>(w)].end(), freq.begin() + w * 64);
    }
    const auto& occ = kOccupiedCarriers[0];
    const auto& pil = kPilotCarriers[0];
    for (int s = 0; s < n_ofdm; s++) {
        cf* sym = freq.data() + (4 + s) * 64;
        for (int c = 0; c < 48; c++) {
            sym[(occ[static_cast<std::size_t>(c)] + 64 + 32) % 64] = points[static_cast<std::size_t>(s * 48 + c)];
        }
        const auto& ps = kPilotSymbols[static_cast<std::size_t>(s) % kPilotSymbols.size()];
        for (int k = 0; k < 4; k++) {
            sym[(pil[static_cast<std::size_t>(k)] + 64 + 32) % 64] = ps[static_cast<std::size_t>(k)];
        }
    }

    // fft_v<gr_complex, false>(64, window, shift): window applied while the
    // input halves are swapped, then the backward transform
    const int n_vec = 4 + n_ofdm;
    std::vector<cf> time(static_cast<std::size_t>(n_vec) * 64);
    cf in64[64], out64[64];
    for (int v = 0; v < n_vec; v++) {
        const cf* x = freq.data() + v * 64;
        for (int i = 0; i < 32; i++) {
            in64[32 + i] = x[i] * kIfftWindow[static_cast<std::size_t>(i)];
            in64[i]      = x[32 + i] * kIfftWindow[static_cast<std::size_t>(32 + i)];
        }
        ifft64(in64, out64);
        std::copy(out64, out64 + 64, time.begin() + v * 64);
    }

    // ofdm_cyclic_prefixer(64, 80, rolloff 2): flanks are 0.5, delay line 1 sample
    fs.samples.assign(static_cast<std::size_t>(n_vec) * 80 + 1, cf{});
    cf  delay{};
    cf* o = fs.samples.data();
    for (int v = 0; v < n_vec; v++) {
        const cf* in = time.data() + v * 64;
        std::copy(in, in + 64, o + 16);
        std::copy(in + 48, in + 64, o);
        o[0]  = o[0] * 0.5f + delay;
        delay = in[0] * 0.5f;
        o += 80;
    }
    *o = delay;
    return fs;
}

} // namespace gr4wifi::tx
