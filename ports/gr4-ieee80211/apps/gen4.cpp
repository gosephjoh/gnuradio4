/*
 * gen4 -- generate a run directory (payloads.bin, manifest.json,
 * rx_stimulus.cf32) with no GNU Radio of either generation: the GR3
 * project's gen-input + tx_capture, standalone (include/gr4ieee80211/wifi_tx.hpp).
 *
 *   gen4 --out DIR --frames N (--payload-range MIN,MAX | --payload-bytes L[,L...])
 *        [--mcs NAME] [--seed S] [--pad-front 500] [--pad-tail 448]
 *        [--noise-voltage 0.01] [--noise-seed 1234] [--bandwidth 10e6]
 *        [--tx-samples] [--force]
 *
 * Same arguments, same bytes as the GR3 tools for payloads.bin and the
 * manifest's frames[]; the sample planes agree with GR3's to float rounding
 * (scripts/check-gen4.py measures it against a GR3 cell).
 */
#include "gr4ieee80211/wifi_tx.hpp"

#include <nlohmann/json.hpp>

#include <getopt.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <print>
#include <random>
#include <sstream>
#include <string>
#include <vector>

using json = nlohmann::ordered_json;
using namespace gr4wifi;

namespace {
const char* encoding_name(Encoding e) {
    static const char* n[] = {"BPSK_1_2", "BPSK_3_4", "QPSK_1_2", "QPSK_3_4", "QAM16_1_2", "QAM16_3_4", "QAM64_2_3", "QAM64_3_4"};
    return n[static_cast<int>(e)];
}
bool parse_mcs(const std::string& s, Encoding* out) {
    for (int i = 0; i < 8; i++) {
        if (s == encoding_name(static_cast<Encoding>(i)) || (s.size() == 1 && s[0] == '0' + i)) {
            *out = static_cast<Encoding>(i);
            return true;
        }
    }
    return false;
}
void usage() {
    std::println(stderr, "usage: gen4 --out DIR --frames N (--payload-range MIN,MAX | --payload-bytes L[,L...]) [--mcs NAME] [--seed S]\n            [--pad-front 500] [--pad-tail 448] [--noise-voltage 0.01] [--noise-seed 1234] [--bandwidth 10e6] [--tx-samples] [--force]");
}
} // namespace

int main(int argc, char** argv) {
    std::string out;
    long        frames = 0, pad_front = 500, pad_tail = 448;
    long        range_min = -1, range_max = -1;
    std::vector<uint32_t> lengths;
    Encoding    mcs = QPSK_1_2;
    uint64_t    seed = 1;
    double      noise_voltage = 0.01, noise_seed = 1234, bandwidth = 10e6;
    bool        tx_samples = false, force = false;

    static const struct option kOpts[] = {{"out", required_argument, nullptr, 1}, {"frames", required_argument, nullptr, 2}, {"payload-range", required_argument, nullptr, 3}, {"payload-bytes", required_argument, nullptr, 4}, {"mcs", required_argument, nullptr, 5}, {"seed", required_argument, nullptr, 6}, {"pad-front", required_argument, nullptr, 7}, {"pad-tail", required_argument, nullptr, 8}, {"noise-voltage", required_argument, nullptr, 9}, {"noise-seed", required_argument, nullptr, 10}, {"bandwidth", required_argument, nullptr, 11}, {"tx-samples", no_argument, nullptr, 12}, {"force", no_argument, nullptr, 13}, {"help", no_argument, nullptr, 'h'}, {nullptr, 0, nullptr, 0}};
    int c;
    while ((c = getopt_long(argc, argv, "h", kOpts, nullptr)) != -1) {
        switch (c) {
        case 1: out = optarg; break;
        case 2: frames = std::strtol(optarg, nullptr, 10); break;
        case 3: {
            const std::string s(optarg);
            const auto comma = s.find(',');
            if (comma == std::string::npos) { usage(); return 2; }
            range_min = std::strtol(s.substr(0, comma).c_str(), nullptr, 10);
            range_max = std::strtol(s.substr(comma + 1).c_str(), nullptr, 10);
            if (range_min < 8 || range_max > MAX_PAYLOAD_SIZE || range_min > range_max) {
                std::println(stderr, "gen4: need 8 <= MIN <= MAX <= 1500");
                return 2;
            }
            break;
        }
        case 4: {
            std::stringstream ss(optarg);
            std::string tok;
            while (std::getline(ss, tok, ',')) {
                const long v = std::strtol(tok.c_str(), nullptr, 10);
                if (v < 0 || v > MAX_PAYLOAD_SIZE) { std::println(stderr, "gen4: payload 0..1500"); return 2; }
                lengths.push_back(static_cast<uint32_t>(v));
            }
            break;
        }
        case 5:
            if (!parse_mcs(optarg, &mcs)) { std::println(stderr, "gen4: bad --mcs"); return 2; }
            break;
        case 6: seed = std::strtoull(optarg, nullptr, 10); break;
        case 7: pad_front = std::strtol(optarg, nullptr, 10); break;
        case 8: pad_tail = std::strtol(optarg, nullptr, 10); break;
        case 9: noise_voltage = std::atof(optarg); break;
        case 10: noise_seed = std::atof(optarg); break;
        case 11: bandwidth = std::atof(optarg); break;
        case 12: tx_samples = true; break;
        case 13: force = true; break;
        default: usage(); return 2;
        }
    }
    if (out.empty() || frames < 1 || (range_min < 0 && lengths.empty()) || (range_min >= 0 && !lengths.empty())) {
        usage();
        return 2;
    }
    if (std::filesystem::exists(out + "/manifest.json") && !force) {
        std::println(stderr, "gen4: {}/manifest.json exists; --force to overwrite", out);
        return 1;
    }
    std::filesystem::create_directories(out);

    // ---- lengths: GR3 gen-input's rules ------------------------------------
    std::vector<uint32_t> per_frame(static_cast<std::size_t>(frames));
    if (range_min >= 0) {
        std::mt19937_64 len_rng(seed);
        std::uniform_int_distribution<uint32_t> dist(static_cast<uint32_t>(range_min), static_cast<uint32_t>(range_max));
        for (long i = 0; i < frames; i++) {
            per_frame[static_cast<std::size_t>(i)] = dist(len_rng);
        }
    } else {
        for (long i = 0; i < frames; i++) {
            per_frame[static_cast<std::size_t>(i)] = lengths[static_cast<std::size_t>(i) % lengths.size()];
        }
    }

    // ---- payloads.bin: seq (u32 LE) + mt19937_64 body ----------------------
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> dist(0, 255);
    std::vector<uint8_t> blob;
    std::vector<uint64_t> offsets(static_cast<std::size_t>(frames));
    for (long i = 0; i < frames; i++) {
        const uint32_t L = per_frame[static_cast<std::size_t>(i)];
        offsets[static_cast<std::size_t>(i)] = blob.size();
        uint32_t body = L;
        if (L >= 4) {
            const uint32_t s = static_cast<uint32_t>(i);
            blob.push_back(s & 0xff); blob.push_back((s >> 8) & 0xff); blob.push_back((s >> 16) & 0xff); blob.push_back((s >> 24) & 0xff);
            body = L - 4;
        }
        for (uint32_t k = 0; k < body; k++) {
            blob.push_back(static_cast<uint8_t>(dist(rng)));
        }
    }
    {
        std::ofstream pf(out + "/payloads.bin", std::ios::binary);
        const uint32_t n = static_cast<uint32_t>(frames), w = 4;
        pf.write("GR3PAYL1", 8);
        pf.write(reinterpret_cast<const char*>(&n), 4);
        pf.write(reinterpret_cast<const char*>(&w), 4);
        pf.write(reinterpret_cast<const char*>(blob.data()), static_cast<std::streamsize>(blob.size()));
    }

    // ---- frames -> samples --------------------------------------------------
    std::FILE* fst = std::fopen((out + "/rx_stimulus.cf32").c_str(), "wb");
    std::FILE* ftx = tx_samples ? std::fopen((out + "/tx_samples.cf32").c_str(), "wb") : nullptr;
    if (!fst) {
        std::println(stderr, "gen4: cannot write {}/rx_stimulus.cf32", out);
        return 1;
    }
    tx::GrRandom noise(static_cast<uint64_t>(noise_seed));
    // noise_source_impl<gr_complex>: d_ampl(ampl / sqrtf(2.0f)), out = d_ampl * rayleigh_complex()
    const float  ampl = static_cast<float>(noise_voltage) / sqrtf(2.0f);
    auto add_noise_and_write = [&](std::vector<cf>& v) {
        if (ampl > 0) {
            for (cf& s : v) {
                // random::rayleigh_complex(): gr_complex(gasdev(), gasdev()) -- GCC
                // evaluates the constructor's arguments right to left, so the
                // imaginary part draws first; then d_ampl * (complex), then add_cc.
                const float im = noise.gasdev();
                const float re = noise.gasdev();
                s += ampl * cf(re, im);
            }
        }
        std::fwrite(v.data(), sizeof(cf), v.size(), fst);
    };

    json     jframes = json::array();
    uint64_t sample_offset = 0, total_pure = 0, payload_bytes = 0;
    int      scrambler = 1; // mapper_impl.cc:36
    for (long i = 0; i < frames; i++) {
        const uint32_t L = per_frame[static_cast<std::size_t>(i)];
        const tx::FrameSamples fs = tx::encode_frame(blob.data() + offsets[static_cast<std::size_t>(i)], static_cast<int>(L), mcs, scrambler, static_cast<uint16_t>(i % 4096));
        scrambler++;
        if (scrambler > 127) {
            scrambler = 1;
        }
        if (ftx) {
            std::fwrite(fs.samples.data(), sizeof(cf), fs.samples.size(), ftx);
        }
        std::vector<cf> padded(static_cast<std::size_t>(pad_front) + fs.samples.size() + static_cast<std::size_t>(pad_tail), cf{});
        std::copy(fs.samples.begin(), fs.samples.end(), padded.begin() + pad_front);
        add_noise_and_write(padded);

        json fr;
        fr["seq"] = L >= 4 ? json(static_cast<uint32_t>(i)) : json(nullptr);
        fr["offset"]        = offsets[static_cast<std::size_t>(i)];
        fr["length"]        = L;
        fr["sample_offset"] = sample_offset;
        fr["samples"]       = padded.size();
        jframes.push_back(fr);
        sample_offset += padded.size();
        total_pure += fs.samples.size();
        payload_bytes += L;
    }
    std::fclose(fst);
    if (ftx) {
        std::fclose(ftx);
    }

    // ---- manifest.json: the GR3 layout, the keys rx_latency4 and the scripts read
    const double duration_s = static_cast<double>(sample_offset) / bandwidth;
    json m;
    m["schema_version"] = 1;
    json gen;
    gen["tool"] = "gen4";
    gen["duration_s"] = nullptr;
    gen["frames"] = frames;
    gen["bandwidth_hz"] = bandwidth;
    gen["occupancy"] = nullptr;
    gen["mcs"] = encoding_name(mcs);
    gen["mcs_index"] = static_cast<int>(mcs);
    if (range_min >= 0) {
        gen["payload_bytes"]   = range_min == range_max ? json(range_min) : json(nullptr);
        gen["payload_lengths"] = per_frame;
        gen["payload_range"]   = json::array({range_min, range_max});
    } else {
        gen["payload_bytes"]   = lengths.size() == 1 ? json(lengths[0]) : json(nullptr);
        gen["payload_lengths"] = lengths;
    }
    gen["seed"] = seed;
    gen["preset"] = nullptr;
    gen["cycle_period_s"] = nullptr;
    gen["cycle_sets"] = nullptr;
    gen["cycle_intervals"] = json::array({json{{"set", 0}, {"first_frame", 0}, {"last_frame", frames - 1}}});
    m["generator"] = gen;
    m["phy"] = {{"frequency_hz", 5.89e9}, {"bandwidth_hz", bandwidth}, {"sensitivity", 0.56}, {"sync_length", 320}, {"min_plateau", 2}, {"chan_est", "LS"}, {"encoding", encoding_name(mcs)}, {"pad_front", pad_front}, {"pad_tail", pad_tail}, {"noise_voltage", noise_voltage}, {"noise_seed", noise_seed}, {"deviations_from_upstream", json::array({"frequency 5.9e6 -> 5.89e9 (0011)", "pad_tail 0 -> N (0012)", "RX stimulus carries noise_source_c + add_cc at noise_voltage/noise_seed (0016)"})}};
    m["prediction"] = {{"frames", frames}, {"total_samples", sample_offset}, {"sample_file_bytes", sample_offset * 8}, {"pure_samples", total_pure}, {"pure_file_bytes", total_pure * 8}, {"payload_bytes", payload_bytes}, {"duration_s", duration_s}, {"frames_per_s", frames / duration_s}, {"payload_mbps", payload_bytes * 8.0 / duration_s / 1e6}, {"realised_occupancy", static_cast<double>(total_pure) / static_cast<double>(sample_offset)}};
    m["frames"] = jframes;
    m["environment"] = nullptr;
    m["runs"] = json::array();
    {
        std::ofstream mf(out + "/manifest.json");
        mf << m.dump(2) << "\n";
    }
    std::println(stderr, "gen4: {} frames, {}, payload {}, pad_front {}, pad_tail {}", frames, encoding_name(mcs), range_min >= 0 ? std::format("uniform in [{},{}]", range_min, range_max) : std::format("{} entries", lengths.size()), pad_front, pad_tail);
    std::println(stderr, "gen4: {} pure samples, {} padded samples ({:.3f} GB), {:.4f} s of airtime at {} Hz", total_pure, sample_offset, sample_offset * 8.0 / 1e9, duration_s, bandwidth);
    std::println(stderr, "gen4: wrote {}/payloads.bin, manifest.json, rx_stimulus.cf32{}", out, tx_samples ? ", tx_samples.cf32" : "");
    return 0;
}
