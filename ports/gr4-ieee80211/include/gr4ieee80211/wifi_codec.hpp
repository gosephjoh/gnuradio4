/*
 * wifi_codec.hpp -- the non-block arithmetic of the gr-ieee802-11 receiver,
 * ported from ~/gr-ieee802-11 (ad0598e, maint-3.10) for the GR4 graph.
 *
 * Everything here is a transcription of upstream's lib/utils.cc,
 * lib/viterbi_decoder/{base.cc,viterbi_decoder_x86.cc},
 * lib/equalizer/base.cc, lib/constellations_impl.cc and the descrambler /
 * deinterleaver of lib/decode_mac.cc.  docs/port-requirements.md of the GR3
 * project (sections 2.3, 4.3, 4.4) is the specification; the upstream line
 * numbers cited there are the ones this file follows.
 *
 * The ONE deliberate departure (decision 0035 of the GR3 project, question
 * "decoder"): the Viterbi decoder's input buffer is zero-filled past the
 * frame before every decode.  Upstream reads 16 x ntraceback symbols past the
 * depunctured frame from whatever its member array holds (gotchas #28, #36,
 * #38); zero is what a fresh process holds there, which is what the golden
 * fixture was captured with.  The arithmetic of the decoder itself -- 8-bit
 * wrapping metrics, signed survivor compare, minimum subtraction every 8
 * bits, fixed-delay traceback, never terminated -- is kept exactly.
 *
 * Original copyrights: Bastian Bloessl (gr-ieee802-11), Phil Karn / Bogdan
 * Diaconescu (the SSE2 Viterbi, via gr-dvbt).  GPL-3.0-or-later.
 */
#pragma once

// The SSE2 Viterbi (upstream viterbi_decoder_x86.cc) where the target has
// SSE2; upstream's portable viterbi_decoder_generic.cc otherwise (aarch64,
// e.g. Raspberry Pi 5) or when GR4WIFI_GENERIC_VITERBI is defined, which
// lets an x86 build prove the generic path against the fixture.
#if defined(__SSE2__) && !defined(GR4WIFI_GENERIC_VITERBI)
#define GR4WIFI_VITERBI_SSE2 1
#include <emmintrin.h>
#else
#define GR4WIFI_VITERBI_SSE2 0
#endif

#include <algorithm>
#include <array>
#include <cmath>
#include <complex>
#include <cstdint>
#include <cstring>
#include <vector>

namespace gr4wifi {

using cf = std::complex<float>;

enum Encoding : int { BPSK_1_2 = 0, BPSK_3_4, QPSK_1_2, QPSK_3_4, QAM16_1_2, QAM16_3_4, QAM64_2_3, QAM64_3_4 };

// utils.h
constexpr int MAX_PAYLOAD_SIZE  = 1500;
constexpr int MAX_PSDU_SIZE     = MAX_PAYLOAD_SIZE + 28;
constexpr int MAX_SYM           = ((16 + 8 * MAX_PSDU_SIZE + 6) / 24) + 1;
constexpr int MAX_BITS_PER_SYM  = 288;
constexpr int MAX_ENCODED_BITS  = (16 + 8 * MAX_PSDU_SIZE + 6) * 2 + MAX_BITS_PER_SYM;
constexpr int TRACEBACK_MAX     = 24;
// The decoder reads up to index 16*(n_data_bits/8 + ntraceback) - 5
// (port-requirements 4.4).  n_data_bits <= MAX_ENCODED_BITS/2 + n_dbps.
constexpr int DECODER_IN_SIZE   = MAX_ENCODED_BITS + 16 * TRACEBACK_MAX + 512;

// utils.cc:32-96
struct ofdm_param {
    Encoding encoding = BPSK_1_2;
    char     rate_field = 0x0D;
    int      n_bpsc = 1, n_cbps = 48, n_dbps = 24;

    constexpr ofdm_param() = default;
    constexpr explicit ofdm_param(Encoding e) : encoding(e) {
        switch (e) {
        case BPSK_1_2:  n_bpsc = 1; n_cbps = 48;  n_dbps = 24;  rate_field = 0x0D; break;
        case BPSK_3_4:  n_bpsc = 1; n_cbps = 48;  n_dbps = 36;  rate_field = 0x0F; break;
        case QPSK_1_2:  n_bpsc = 2; n_cbps = 96;  n_dbps = 48;  rate_field = 0x05; break;
        case QPSK_3_4:  n_bpsc = 2; n_cbps = 96;  n_dbps = 72;  rate_field = 0x07; break;
        case QAM16_1_2: n_bpsc = 4; n_cbps = 192; n_dbps = 96;  rate_field = 0x09; break;
        case QAM16_3_4: n_bpsc = 4; n_cbps = 192; n_dbps = 144; rate_field = 0x0B; break;
        case QAM64_2_3: n_bpsc = 6; n_cbps = 288; n_dbps = 192; rate_field = 0x01; break;
        case QAM64_3_4: n_bpsc = 6; n_cbps = 288; n_dbps = 216; rate_field = 0x03; break;
        }
    }
};

// utils.cc:110-124
struct frame_param {
    int psdu_size = 0, n_sym = 0, n_pad = 0, n_encoded_bits = 0, n_data_bits = 0;

    constexpr frame_param() = default;
    frame_param(const ofdm_param& ofdm, int psdu_length) {
        psdu_size      = psdu_length;
        n_sym          = static_cast<int>(std::ceil((16 + 8 * psdu_size + 6) / static_cast<double>(ofdm.n_dbps)));
        n_data_bits    = n_sym * ofdm.n_dbps;
        n_pad          = n_data_bits - (16 + 8 * psdu_size + 6);
        n_encoded_bits = n_sym * ofdm.n_cbps;
    }
};

// ---------------------------------------------------------------- tables

// equalizer/base.cc
inline constexpr std::array<float, 64> LONG_LTF = {0, 0, 0, 0, 0, 0, 1, 1, -1, -1, 1, 1, -1, 1, -1, 1, 1, 1, 1, 1, 1, -1, -1, 1, 1, -1, 1, -1, 1, 1, 1, 1, 0, 1, -1, -1, 1, 1, -1, 1, -1, 1, -1, -1, -1, -1, -1, 1, 1, -1, -1, 1, -1, 1, -1, 1, 1, 1, 1, 0, 0, 0, 0, 0};

inline constexpr std::array<float, 127> POLARITY = {1, 1, 1, 1, -1, -1, -1, 1, -1, -1, -1, -1, 1, 1, -1, 1, -1, -1, 1, 1, -1, 1, 1, -1, 1, 1, 1, 1, 1, 1, -1, 1, 1, 1, -1, 1, 1, -1, -1, 1, 1, 1, -1, 1, -1, -1, -1, 1, -1, 1, -1, -1, 1, -1, -1, 1, 1, 1, 1, 1, -1, -1, 1, 1, -1, -1, 1, -1, 1, -1, 1, 1, -1, -1, -1, 1, 1, -1, -1, -1, -1, 1, -1, -1, 1, -1, 1, 1, 1, 1, -1, 1, -1, 1, -1, 1, -1, -1, -1, -1, -1, 1, -1, 1, 1, -1, 1, -1, 1, 1, 1, -1, -1, 1, -1, -1, -1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1};

// sync_long.cc: the time-domain LTF the correlator matches
inline const std::array<cf, 64> LONG_SYNC = {cf(-0.0455f, -1.0679f), cf(0.3528f, -0.9865f), cf(0.8594f, 0.7348f), cf(0.1874f, 0.2475f), cf(0.5309f, -0.7784f), cf(-1.0218f, -0.4897f), cf(-0.3401f, -0.9423f), cf(0.8657f, -0.2298f), cf(0.4734f, 0.0362f), cf(0.0088f, -1.0207f), cf(-1.2142f, -0.4205f), cf(0.2172f, -0.5195f), cf(0.5207f, -0.1326f), cf(-0.1995f, 1.4259f), cf(1.0583f, -0.0363f), cf(0.5547f, -0.5547f), cf(0.3277f, 0.8728f), cf(-0.5077f, 0.3488f), cf(-1.1650f, 0.5789f), cf(0.7297f, 0.8197f), cf(0.6173f, 0.1253f), cf(-0.5353f, 0.7214f), cf(-0.5011f, -0.1935f), cf(-0.3110f, -1.3392f), cf(-1.0818f, -0.1470f), cf(-1.1300f, -0.1820f), cf(0.6663f, -0.6571f), cf(-0.0249f, 0.4773f), cf(-0.8155f, 1.0218f), cf(0.8140f, 0.9396f), cf(0.1090f, 0.8662f), cf(-1.3868f, -0.0000f), cf(0.1090f, -0.8662f), cf(0.8140f, -0.9396f), cf(-0.8155f, -1.0218f), cf(-0.0249f, -0.4773f), cf(0.6663f, 0.6571f), cf(-1.1300f, 0.1820f), cf(-1.0818f, 0.1470f), cf(-0.3110f, 1.3392f), cf(-0.5011f, 0.1935f), cf(-0.5353f, -0.7214f), cf(0.6173f, -0.1253f), cf(0.7297f, -0.8197f), cf(-1.1650f, -0.5789f), cf(-0.5077f, -0.3488f), cf(0.3277f, -0.8728f), cf(0.5547f, 0.5547f), cf(1.0583f, 0.0363f), cf(-0.1995f, -1.4259f), cf(0.5207f, 0.1326f), cf(0.2172f, 0.5195f), cf(-1.2142f, 0.4205f), cf(0.0088f, 1.0207f), cf(0.4734f, -0.0362f), cf(0.8657f, 0.2298f), cf(-0.3401f, 0.9423f), cf(-1.0218f, 0.4897f), cf(0.5309f, 0.7784f), cf(0.1874f, -0.2475f), cf(0.8594f, -0.7348f), cf(0.3528f, 0.9865f), cf(-0.0455f, 1.0679f), cf(1.3868f, -0.0000f)};

// frame_equalizer_impl.cc: SIGNAL-field deinterleaver
inline constexpr std::array<int, 48> SIGNAL_INTERLEAVER = {0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47};

// ------------------------------------------------------ hard decisions
// constellations_impl.cc:40,72,116,218 -- bit k of the return value is bit k.
inline unsigned decision_bpsk(const cf& s) { return std::real(s) > 0; }
inline unsigned decision_qpsk(const cf& s) { return 2 * (std::imag(s) > 0) + (std::real(s) > 0); }
inline unsigned decision_16qam(const cf& s) {
    unsigned    ret   = 0;
    const float level = std::sqrt(float(0.1));
    const float re = s.real(), im = s.imag();
    ret |= re > 0;
    ret |= (std::abs(re) < (2 * level)) << 1;
    ret |= (im > 0) << 2;
    ret |= (std::abs(im) < (2 * level)) << 3;
    return ret;
}
inline unsigned decision_64qam(const cf& s) {
    unsigned    ret   = 0;
    const float level = std::sqrt(float(1 / 42.0));
    const float re = s.real(), im = s.imag();
    ret |= re > 0;
    ret |= (std::abs(re) < (4 * level)) << 1;
    ret |= (std::abs(re) < (6 * level) && std::abs(re) > (2 * level)) << 2;
    ret |= (im > 0) << 3;
    ret |= (std::abs(im) < (4 * level)) << 4;
    ret |= (std::abs(im) < (6 * level) && std::abs(im) > (2 * level)) << 5;
    return ret;
}
inline unsigned decision(Encoding e, const cf& s) {
    switch (e) {
    case BPSK_1_2: case BPSK_3_4: return decision_bpsk(s);
    case QPSK_1_2: case QPSK_3_4: return decision_qpsk(s);
    case QAM16_1_2: case QAM16_3_4: return decision_16qam(s);
    default: return decision_64qam(s);
    }
}

// ------------------------------------------------------------ viterbi
// viterbi_decoder/base.{h,cc}: the parity table both decoders share.
inline constexpr unsigned char VITERBI_PARTAB[256] = {
    0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0,
};

#if GR4WIFI_VITERBI_SSE2
// viterbi_decoder/base.{h,cc} + viterbi_decoder_x86.cc, verbatim arithmetic.
class ViterbiDecoderSse2 {
public:
    ViterbiDecoderSse2() { std::memset(d_depunctured.data(), 0, d_depunctured.size()); }

    // Returns n_data_bits (rounded up to a byte) decoded bits, one per byte.
    // `in` holds frame.n_sym * ofdm.n_cbps bits.
    const uint8_t* decode(const ofdm_param& ofdm, const frame_param& frame, const uint8_t* in) {
        d_ofdm  = ofdm;
        d_frame = frame;
        reset();
        depuncture(in);

        int in_count = 0, out_count = 0, n_decoded = 0;
        while (n_decoded < d_frame.n_data_bits) {
            if ((in_count % 4) == 0) {
                viterbi_butterfly2_sse2(&d_depunctured[static_cast<std::size_t>(in_count & ~3)], d_metric0, d_metric1, d_path0, d_path1);
                if ((in_count > 0) && (in_count % 16) == 8) {
                    unsigned char c;
                    viterbi_get_output_sse2(d_metric0, d_path0, d_ntraceback, &c);
                    if (out_count >= d_ntraceback) {
                        for (int i = 0; i < 8; i++) {
                            d_decoded[static_cast<std::size_t>((out_count - d_ntraceback) * 8 + i)] = (c >> (7 - i)) & 0x1;
                            n_decoded++;
                        }
                    }
                    out_count++;
                }
            }
            in_count++;
        }
        return d_decoded.data();
    }

private:
    static constexpr unsigned char PUNCTURE_1_2[2] = {1, 1};
    static constexpr unsigned char PUNCTURE_2_3[4] = {1, 1, 1, 0};
    static constexpr unsigned char PUNCTURE_3_4[6] = {1, 1, 1, 0, 0, 1};

    union branchtab27 {
        unsigned char c[32];
        __m128i       v[2];
    } d_branchtab27_sse2[2];

    alignas(16) __m128i d_metric0[4];
    alignas(16) __m128i d_metric1[4];
    alignas(16) __m128i d_path0[4];
    alignas(16) __m128i d_path1[4];

    int d_store_pos = 0;
    alignas(16) unsigned char d_mmresult[64];
    alignas(16) unsigned char d_ppresult[TRACEBACK_MAX][64];

    int                  d_ntraceback = 5;
    int                  d_k          = 1;
    ofdm_param           d_ofdm;
    frame_param          d_frame;
    const unsigned char* d_depuncture_pattern = PUNCTURE_1_2;

    std::array<uint8_t, DECODER_IN_SIZE>        d_depunctured{};
    std::array<uint8_t, MAX_ENCODED_BITS>       d_decoded{};

    // base.cc:28-62, then the departure: zero everything the traceback can
    // read past the frame.  Upstream leaves it as it was (gotchas #28).
    void depuncture(const uint8_t* in) {
        const int n_cbps = d_ofdm.n_cbps;
        int       count  = 0;
        if (d_ntraceback == 5) {
            count = d_frame.n_sym * n_cbps;
            std::memcpy(d_depunctured.data(), in, static_cast<std::size_t>(count));
        } else {
            for (int i = 0; i < d_frame.n_sym; i++) {
                for (int k = 0; k < n_cbps; k++) {
                    while (d_depuncture_pattern[count % (2 * d_k)] == 0) {
                        d_depunctured[static_cast<std::size_t>(count)] = 2;
                        count++;
                    }
                    d_depunctured[static_cast<std::size_t>(count)] = in[i * n_cbps + k];
                    count++;
                    while (d_depuncture_pattern[count % (2 * d_k)] == 0) {
                        d_depunctured[static_cast<std::size_t>(count)] = 2;
                        count++;
                    }
                }
            }
        }
        const int last_read = 16 * (d_frame.n_data_bits / 8 + d_ntraceback) + 16; // >= the decoder's last index
        if (last_read > count) {
            std::memset(d_depunctured.data() + count, 0, static_cast<std::size_t>(std::min(last_read, DECODER_IN_SIZE) - count));
        }
    }

    void reset() {
        viterbi_chunks_init_sse2();
        switch (d_ofdm.encoding) {
        case BPSK_1_2: case QPSK_1_2: case QAM16_1_2:
            d_ntraceback = 5; d_depuncture_pattern = PUNCTURE_1_2; d_k = 1; break;
        case QAM64_2_3:
            d_ntraceback = 9; d_depuncture_pattern = PUNCTURE_2_3; d_k = 2; break;
        default:
            d_ntraceback = 10; d_depuncture_pattern = PUNCTURE_3_4; d_k = 3; break;
        }
    }

    void viterbi_chunks_init_sse2() {
        for (int i = 0; i < 4; i++) {
            d_metric0[i] = _mm_setzero_si128();
            d_path0[i]   = _mm_setzero_si128();
        }
        const int polys[2] = {0x6d, 0x4f};
        for (int i = 0; i < 32; i++) {
            d_branchtab27_sse2[0].c[i] = (polys[0] < 0) ^ VITERBI_PARTAB[(2 * i) & std::abs(polys[0])] ? 1 : 0;
            d_branchtab27_sse2[1].c[i] = (polys[1] < 0) ^ VITERBI_PARTAB[(2 * i) & std::abs(polys[1])] ? 1 : 0;
        }
        for (int i = 0; i < 64; i++) {
            d_mmresult[i] = 0;
            for (int j = 0; j < TRACEBACK_MAX; j++) {
                d_ppresult[j][i] = 0;
            }
        }
        d_store_pos = 0;
    }

    void viterbi_butterfly2_sse2(unsigned char* symbols, __m128i* mm0, __m128i* mm1, __m128i* pp0, __m128i* pp1) {
        __m128i *metric0 = mm0, *metric1 = mm1, *path0 = pp0, *path1 = pp1;
        __m128i  m0, m1, m2, m3, decision0, decision1, survivor0, survivor1;
        __m128i  metsv, metsvm, shift0, shift1, tmp0, tmp1, sym0v, sym1v;

        sym0v = _mm_set1_epi8(static_cast<char>(symbols[0]));
        sym1v = _mm_set1_epi8(static_cast<char>(symbols[1]));
        for (int i = 0; i < 2; i++) {
            if (symbols[0] == 2) {
                metsvm = _mm_xor_si128(d_branchtab27_sse2[1].v[i], sym1v);
                metsv  = _mm_sub_epi8(_mm_set1_epi8(1), metsvm);
            } else if (symbols[1] == 2) {
                metsvm = _mm_xor_si128(d_branchtab27_sse2[0].v[i], sym0v);
                metsv  = _mm_sub_epi8(_mm_set1_epi8(1), metsvm);
            } else {
                metsvm = _mm_add_epi8(_mm_xor_si128(d_branchtab27_sse2[0].v[i], sym0v), _mm_xor_si128(d_branchtab27_sse2[1].v[i], sym1v));
                metsv  = _mm_sub_epi8(_mm_set1_epi8(2), metsvm);
            }
            m0        = _mm_add_epi8(metric0[i], metsv);
            m1        = _mm_add_epi8(metric0[i + 2], metsvm);
            m2        = _mm_add_epi8(metric0[i], metsvm);
            m3        = _mm_add_epi8(metric0[i + 2], metsv);
            decision0 = _mm_cmpgt_epi8(_mm_sub_epi8(m0, m1), _mm_setzero_si128());
            decision1 = _mm_cmpgt_epi8(_mm_sub_epi8(m2, m3), _mm_setzero_si128());
            survivor0 = _mm_or_si128(_mm_and_si128(decision0, m0), _mm_andnot_si128(decision0, m1));
            survivor1 = _mm_or_si128(_mm_and_si128(decision1, m2), _mm_andnot_si128(decision1, m3));
            shift0    = _mm_slli_epi16(path0[i], 1);
            shift1    = _mm_slli_epi16(path0[2 + i], 1);
            shift1    = _mm_add_epi8(shift1, _mm_set1_epi8(1));
            metric1[2 * i]     = _mm_unpacklo_epi8(survivor0, survivor1);
            tmp0               = _mm_or_si128(_mm_and_si128(decision0, shift0), _mm_andnot_si128(decision0, shift1));
            metric1[2 * i + 1] = _mm_unpackhi_epi8(survivor0, survivor1);
            tmp1               = _mm_or_si128(_mm_and_si128(decision1, shift0), _mm_andnot_si128(decision1, shift1));
            path1[2 * i]       = _mm_unpacklo_epi8(tmp0, tmp1);
            path1[2 * i + 1]   = _mm_unpackhi_epi8(tmp0, tmp1);
        }

        metric0 = mm1; path0 = pp1; metric1 = mm0; path1 = pp0;
        sym0v = _mm_set1_epi8(static_cast<char>(symbols[2]));
        sym1v = _mm_set1_epi8(static_cast<char>(symbols[3]));
        for (int i = 0; i < 2; i++) {
            if (symbols[2] == 2) {
                metsvm = _mm_xor_si128(d_branchtab27_sse2[1].v[i], sym1v);
                metsv  = _mm_sub_epi8(_mm_set1_epi8(1), metsvm);
            } else if (symbols[3] == 2) {
                metsvm = _mm_xor_si128(d_branchtab27_sse2[0].v[i], sym0v);
                metsv  = _mm_sub_epi8(_mm_set1_epi8(1), metsvm);
            } else {
                metsvm = _mm_add_epi8(_mm_xor_si128(d_branchtab27_sse2[0].v[i], sym0v), _mm_xor_si128(d_branchtab27_sse2[1].v[i], sym1v));
                metsv  = _mm_sub_epi8(_mm_set1_epi8(2), metsvm);
            }
            m0        = _mm_add_epi8(metric0[i], metsv);
            m1        = _mm_add_epi8(metric0[i + 2], metsvm);
            m2        = _mm_add_epi8(metric0[i], metsvm);
            m3        = _mm_add_epi8(metric0[i + 2], metsv);
            decision0 = _mm_cmpgt_epi8(_mm_sub_epi8(m0, m1), _mm_setzero_si128());
            decision1 = _mm_cmpgt_epi8(_mm_sub_epi8(m2, m3), _mm_setzero_si128());
            survivor0 = _mm_or_si128(_mm_and_si128(decision0, m0), _mm_andnot_si128(decision0, m1));
            survivor1 = _mm_or_si128(_mm_and_si128(decision1, m2), _mm_andnot_si128(decision1, m3));
            shift0    = _mm_slli_epi16(path0[i], 1);
            shift1    = _mm_slli_epi16(path0[2 + i], 1);
            shift1    = _mm_add_epi8(shift1, _mm_set1_epi8(1));
            metric1[2 * i]     = _mm_unpacklo_epi8(survivor0, survivor1);
            tmp0               = _mm_or_si128(_mm_and_si128(decision0, shift0), _mm_andnot_si128(decision0, shift1));
            metric1[2 * i + 1] = _mm_unpackhi_epi8(survivor0, survivor1);
            tmp1               = _mm_or_si128(_mm_and_si128(decision1, shift0), _mm_andnot_si128(decision1, shift1));
            path1[2 * i]       = _mm_unpacklo_epi8(tmp0, tmp1);
            path1[2 * i + 1]   = _mm_unpackhi_epi8(tmp0, tmp1);
        }
    }

    unsigned char viterbi_get_output_sse2(__m128i* mm0, __m128i* pp0, int ntraceback, unsigned char* outbuf) {
        int bestmetric, minmetric, beststate = 0, pos = 0;
        d_store_pos = (d_store_pos + 1) % ntraceback;
        for (int i = 0; i < 4; i++) {
            _mm_store_si128(reinterpret_cast<__m128i*>(&d_mmresult[i * 16]), mm0[i]);
            _mm_store_si128(reinterpret_cast<__m128i*>(&d_ppresult[d_store_pos][i * 16]), pp0[i]);
        }
        bestmetric = d_mmresult[beststate];
        minmetric  = d_mmresult[beststate];
        for (int i = 1; i < 64; i++) {
            if (d_mmresult[i] > bestmetric) {
                bestmetric = d_mmresult[i];
                beststate  = i;
            }
            if (d_mmresult[i] < minmetric) {
                minmetric = d_mmresult[i];
            }
        }
        int i;
        for (i = 0, pos = d_store_pos; i < (ntraceback - 1); i++) {
            beststate = d_ppresult[pos][beststate] >> 2;
            pos       = (pos - 1 + ntraceback) % ntraceback;
        }
        *outbuf = d_ppresult[pos][beststate];
        for (i = 0; i < 4; i++) {
            pp0[i] = _mm_setzero_si128();
            mm0[i] = _mm_sub_epi8(mm0[i], _mm_set1_epi8(static_cast<char>(minmetric)));
        }
        return static_cast<unsigned char>(bestmetric);
    }
};
using ViterbiDecoder = ViterbiDecoderSse2;
#else

// viterbi_decoder/viterbi_decoder_generic.cc, verbatim arithmetic: the same
// Karn/Diaconescu decoder with the 16-lane SSE2 vectors written out as byte
// loops.  Same depuncture (including the port's zero-fill departure), same
// traceback, same output; on x86 it is checked against the fixture with
// -DGR4WIFI_GENERIC_VITERBI=ON (docs/batch-rt-experiments.md, "Raspberry Pi").
class ViterbiDecoderGeneric {
public:
    ViterbiDecoderGeneric() { std::memset(d_depunctured.data(), 0, d_depunctured.size()); }

    const uint8_t* decode(const ofdm_param& ofdm, const frame_param& frame, const uint8_t* in) {
        d_ofdm  = ofdm;
        d_frame = frame;
        reset();
        depuncture(in);

        int in_count = 0, out_count = 0, n_decoded = 0;
        while (n_decoded < d_frame.n_data_bits) {
            if ((in_count % 4) == 0) {
                viterbi_butterfly2_generic(&d_depunctured[static_cast<std::size_t>(in_count & ~3)], d_metric0, d_metric1, d_path0, d_path1);
                if ((in_count > 0) && (in_count % 16) == 8) {
                    unsigned char c;
                    viterbi_get_output_generic(d_metric0, d_path0, d_ntraceback, &c);
                    if (out_count >= d_ntraceback) {
                        for (int i = 0; i < 8; i++) {
                            d_decoded[static_cast<std::size_t>((out_count - d_ntraceback) * 8 + i)] = (c >> (7 - i)) & 0x1;
                            n_decoded++;
                        }
                    }
                    out_count++;
                }
            }
            in_count++;
        }
        return d_decoded.data();
    }

private:
    static constexpr unsigned char PUNCTURE_1_2[2] = {1, 1};
    static constexpr unsigned char PUNCTURE_2_3[4] = {1, 1, 1, 0};
    static constexpr unsigned char PUNCTURE_3_4[6] = {1, 1, 1, 0, 0, 1};

    struct branchtab27 {
        unsigned char c[32];
    } d_branchtab27[2];

    alignas(16) unsigned char d_metric0[64];
    alignas(16) unsigned char d_metric1[64];
    alignas(16) unsigned char d_path0[64];
    alignas(16) unsigned char d_path1[64];

    int d_store_pos = 0;
    alignas(16) unsigned char d_mmresult[64];
    alignas(16) unsigned char d_ppresult[TRACEBACK_MAX][64];

    int                  d_ntraceback = 5;
    int                  d_k          = 1;
    ofdm_param           d_ofdm;
    frame_param          d_frame;
    const unsigned char* d_depuncture_pattern = PUNCTURE_1_2;

    std::array<uint8_t, DECODER_IN_SIZE>  d_depunctured{};
    std::array<uint8_t, MAX_ENCODED_BITS> d_decoded{};

    void depuncture(const uint8_t* in) {
        const int n_cbps = d_ofdm.n_cbps;
        int       count  = 0;
        if (d_ntraceback == 5) {
            count = d_frame.n_sym * n_cbps;
            std::memcpy(d_depunctured.data(), in, static_cast<std::size_t>(count));
        } else {
            for (int i = 0; i < d_frame.n_sym; i++) {
                for (int k = 0; k < n_cbps; k++) {
                    while (d_depuncture_pattern[count % (2 * d_k)] == 0) {
                        d_depunctured[static_cast<std::size_t>(count)] = 2;
                        count++;
                    }
                    d_depunctured[static_cast<std::size_t>(count)] = in[i * n_cbps + k];
                    count++;
                    while (d_depuncture_pattern[count % (2 * d_k)] == 0) {
                        d_depunctured[static_cast<std::size_t>(count)] = 2;
                        count++;
                    }
                }
            }
        }
        const int last_read = 16 * (d_frame.n_data_bits / 8 + d_ntraceback) + 16;
        if (last_read > count) {
            std::memset(d_depunctured.data() + count, 0, static_cast<std::size_t>(std::min(last_read, DECODER_IN_SIZE) - count));
        }
    }

    void reset() {
        viterbi_chunks_init_generic();
        switch (d_ofdm.encoding) {
        case BPSK_1_2: case QPSK_1_2: case QAM16_1_2:
            d_ntraceback = 5; d_depuncture_pattern = PUNCTURE_1_2; d_k = 1; break;
        case QAM64_2_3:
            d_ntraceback = 9; d_depuncture_pattern = PUNCTURE_2_3; d_k = 2; break;
        default:
            d_ntraceback = 10; d_depuncture_pattern = PUNCTURE_3_4; d_k = 3; break;
        }
    }

    void viterbi_chunks_init_generic() {
        for (int i = 0; i < 64; i++) {
            d_metric0[i] = 0;
            d_path0[i]   = 0;
        }
        const int polys[2] = {0x6d, 0x4f};
        for (int i = 0; i < 32; i++) {
            d_branchtab27[0].c[i] = (polys[0] < 0) ^ VITERBI_PARTAB[(2 * i) & std::abs(polys[0])] ? 1 : 0;
            d_branchtab27[1].c[i] = (polys[1] < 0) ^ VITERBI_PARTAB[(2 * i) & std::abs(polys[1])] ? 1 : 0;
        }
        for (int i = 0; i < 64; i++) {
            d_mmresult[i] = 0;
            for (int j = 0; j < TRACEBACK_MAX; j++) {
                d_ppresult[j][i] = 0;
            }
        }
        d_store_pos = 0;
    }

    // one half of viterbi_butterfly2_generic: two symbols, 4x16 lanes
    void butterfly_half(const unsigned char* symbols, unsigned char* metric0, unsigned char* metric1, unsigned char* path0, unsigned char* path1) {
        unsigned char m0[16], m1[16], m2[16], m3[16], decision0[16], decision1[16], survivor0[16], survivor1[16];
        unsigned char metsv[16], metsvm[16], shift0[16], shift1[16], tmp0[16], tmp1[16], sym0v[16], sym1v[16];
        unsigned short simd_epi16;
        for (int j = 0; j < 16; j++) {
            sym0v[j] = symbols[0];
            sym1v[j] = symbols[1];
        }
        for (int i = 0; i < 2; i++) {
            if (symbols[0] == 2) {
                for (int j = 0; j < 16; j++) {
                    metsvm[j] = static_cast<unsigned char>(d_branchtab27[1].c[(i * 16) + j] ^ sym1v[j]);
                    metsv[j]  = static_cast<unsigned char>(1 - metsvm[j]);
                }
            } else if (symbols[1] == 2) {
                for (int j = 0; j < 16; j++) {
                    metsvm[j] = static_cast<unsigned char>(d_branchtab27[0].c[(i * 16) + j] ^ sym0v[j]);
                    metsv[j]  = static_cast<unsigned char>(1 - metsvm[j]);
                }
            } else {
                for (int j = 0; j < 16; j++) {
                    metsvm[j] = static_cast<unsigned char>((d_branchtab27[0].c[(i * 16) + j] ^ sym0v[j]) + (d_branchtab27[1].c[(i * 16) + j] ^ sym1v[j]));
                    metsv[j]  = static_cast<unsigned char>(2 - metsvm[j]);
                }
            }
            for (int j = 0; j < 16; j++) {
                m0[j] = static_cast<unsigned char>(metric0[(i * 16) + j] + metsv[j]);
                m1[j] = static_cast<unsigned char>(metric0[((i + 2) * 16) + j] + metsvm[j]);
                m2[j] = static_cast<unsigned char>(metric0[(i * 16) + j] + metsvm[j]);
                m3[j] = static_cast<unsigned char>(metric0[((i + 2) * 16) + j] + metsv[j]);
            }
            for (int j = 0; j < 16; j++) {
                // upstream generic: the promoted int difference (the SSE2 build
                // compares the wrapped signed byte; the two agree while the
                // renormalised metrics stay within 127 of each other, which
                // the per-byte renormalisation in get_output keeps them)
                decision0[j] = ((m0[j] - m1[j]) > 0) ? 0xff : 0x0;
                decision1[j] = ((m2[j] - m3[j]) > 0) ? 0xff : 0x0;
                survivor0[j] = static_cast<unsigned char>((decision0[j] & m0[j]) | ((~decision0[j]) & m1[j]));
                survivor1[j] = static_cast<unsigned char>((decision1[j] & m2[j]) | ((~decision1[j]) & m3[j]));
            }
            for (int j = 0; j < 16; j += 2) {
                simd_epi16 = path0[(i * 16) + j];
                simd_epi16 = static_cast<unsigned short>(simd_epi16 | (path0[(i * 16) + (j + 1)] << 8));
                simd_epi16 = static_cast<unsigned short>(simd_epi16 << 1);
                shift0[j]     = static_cast<unsigned char>(simd_epi16);
                shift0[j + 1] = static_cast<unsigned char>(simd_epi16 >> 8);
                simd_epi16 = path0[((i + 2) * 16) + j];
                simd_epi16 = static_cast<unsigned short>(simd_epi16 | (path0[((i + 2) * 16) + (j + 1)] << 8));
                simd_epi16 = static_cast<unsigned short>(simd_epi16 << 1);
                shift1[j]     = static_cast<unsigned char>(simd_epi16);
                shift1[j + 1] = static_cast<unsigned char>(simd_epi16 >> 8);
            }
            for (int j = 0; j < 16; j++) {
                shift1[j] = static_cast<unsigned char>(shift1[j] + 1);
            }
            for (int j = 0, k = 0; j < 16; j += 2, k++) {
                metric1[(2 * i * 16) + j]       = survivor0[k];
                metric1[(2 * i * 16) + (j + 1)] = survivor1[k];
            }
            for (int j = 0; j < 16; j++) {
                tmp0[j] = static_cast<unsigned char>((decision0[j] & shift0[j]) | ((~decision0[j]) & shift1[j]));
            }
            for (int j = 0, k = 8; j < 16; j += 2, k++) {
                metric1[((2 * i + 1) * 16) + j]       = survivor0[k];
                metric1[((2 * i + 1) * 16) + (j + 1)] = survivor1[k];
            }
            for (int j = 0; j < 16; j++) {
                tmp1[j] = static_cast<unsigned char>((decision1[j] & shift0[j]) | ((~decision1[j]) & shift1[j]));
            }
            for (int j = 0, k = 0; j < 16; j += 2, k++) {
                path1[(2 * i * 16) + j]       = tmp0[k];
                path1[(2 * i * 16) + (j + 1)] = tmp1[k];
            }
            for (int j = 0, k = 8; j < 16; j += 2, k++) {
                path1[((2 * i + 1) * 16) + j]       = tmp0[k];
                path1[((2 * i + 1) * 16) + (j + 1)] = tmp1[k];
            }
        }
    }

    void viterbi_butterfly2_generic(const unsigned char* symbols, unsigned char* mm0, unsigned char* mm1, unsigned char* pp0, unsigned char* pp1) {
        butterfly_half(symbols, mm0, mm1, pp0, pp1);
        butterfly_half(symbols + 2, mm1, mm0, pp1, pp0);
    }

    unsigned char viterbi_get_output_generic(unsigned char* mm0, unsigned char* pp0, int ntraceback, unsigned char* outbuf) {
        int bestmetric, minmetric, beststate = 0, pos = 0;
        d_store_pos = (d_store_pos + 1) % ntraceback;
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 16; j++) {
                d_mmresult[(i * 16) + j]              = mm0[(i * 16) + j];
                d_ppresult[d_store_pos][(i * 16) + j] = pp0[(i * 16) + j];
            }
        }
        bestmetric = d_mmresult[beststate];
        minmetric  = d_mmresult[beststate];
        for (int i = 1; i < 64; i++) {
            if (d_mmresult[i] > bestmetric) {
                bestmetric = d_mmresult[i];
                beststate  = i;
            }
            if (d_mmresult[i] < minmetric) {
                minmetric = d_mmresult[i];
            }
        }
        int i;
        for (i = 0, pos = d_store_pos; i < (ntraceback - 1); i++) {
            beststate = d_ppresult[pos][beststate] >> 2;
            pos       = (pos - 1 + ntraceback) % ntraceback;
        }
        *outbuf = d_ppresult[pos][beststate];
        for (i = 0; i < 4; i++) {
            for (int j = 0; j < 16; j++) {
                pp0[(i * 16) + j] = 0;
                mm0[(i * 16) + j] = static_cast<unsigned char>(mm0[(i * 16) + j] - minmetric);
            }
        }
        return static_cast<unsigned char>(bestmetric);
    }
};
using ViterbiDecoder = ViterbiDecoderGeneric;
#endif

// --------------------------------------------------- decode_mac helpers

// decode_mac.cc:162-185 -- the inverse of utils.cc's interleave.
inline void deinterleave(const ofdm_param& ofdm, const frame_param& frame, const uint8_t* rx_bits, uint8_t* out) {
    const int n_cbps = ofdm.n_cbps;
    int       first[MAX_BITS_PER_SYM], second[MAX_BITS_PER_SYM];
    const int s = std::max(ofdm.n_bpsc / 2, 1);
    for (int j = 0; j < n_cbps; j++) {
        first[j] = s * (j / s) + ((j + static_cast<int>(std::floor(16.0 * j / n_cbps))) % s);
    }
    for (int i = 0; i < n_cbps; i++) {
        second[i] = 16 * i - (n_cbps - 1) * static_cast<int>(std::floor(16.0 * i / n_cbps));
    }
    for (int i = 0; i < frame.n_sym; i++) {
        for (int k = 0; k < n_cbps; k++) {
            out[i * n_cbps + second[first[k]]] = rx_bits[i * n_cbps + k];
        }
    }
}

// decode_mac.cc:188-210 -- out has psdu_size + 2 bytes (SERVICE first).
inline void descramble(const frame_param& frame, const uint8_t* decoded_bits, uint8_t* out_bytes) {
    int state = 0;
    std::memset(out_bytes, 0, static_cast<std::size_t>(frame.psdu_size + 2));
    for (int i = 0; i < 7; i++) {
        if (decoded_bits[i]) {
            state |= 1 << (6 - i);
        }
    }
    out_bytes[0] = static_cast<uint8_t>(state);
    for (int i = 7; i < frame.psdu_size * 8 + 16; i++) {
        const int feedback = ((!!(state & 64))) ^ (!!(state & 8));
        const int bit      = feedback ^ (decoded_bits[i] & 0x1);
        out_bytes[i / 8] |= static_cast<uint8_t>(bit << (i % 8));
        state = ((state << 1) & 0x7e) | feedback;
    }
}

// boost::crc_32_type == zlib crc32: reflected 0xEDB88320, init/final 0xFFFFFFFF.
inline uint32_t crc32(const uint8_t* p, std::size_t n) {
    static const auto table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) {
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            t[i] = c;
        }
        return t;
    }();
    uint32_t c = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < n; i++) {
        c = table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}
constexpr uint32_t CRC_RESIDUE = 558161692u; // 0x2144DF1C, decode_mac.cc:145

// CLOCK_MONOTONIC nanoseconds -- the same timebase the GR3 harness uses.
uint64_t monotonic_ns();

} // namespace gr4wifi
