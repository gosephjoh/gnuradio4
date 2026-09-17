/*
 * trace_export -- flatten a GR4 `.gr4trace` capture into CSV tables the
 * offline analysis (scripts/trace-batch-rt.py) reads with pandas.
 *
 *   trace_export CAPTURE --out-dir DIR [--meta trace_meta.json] [--all-events]
 *
 * Uses GR4's own reader (gr::trace::load, core/src/Trace.cpp), so every
 * format check the tool `gr4-trace` applies -- magic, endianness, version,
 * record size, truncation -- applies here too.  With --meta, the block roles
 * rx_latency4 wrote (unique name -> chain, role) are joined onto every row.
 *
 * Files written (all CSV with a header row; nanoseconds are absolute
 * steady_clock = CLOCK_MONOTONIC values):
 *   trace_header.json      the capture header (mask, lost, rings, anchors)
 *   trace_kinds.csv        kind,count -- the record census
 *   trace_entities.csv     id,worker,n_in,n_out,unique_name,type_name,chain,role
 *   trace_invocations.csv  workExact: entity,chain,role,start_ns,dur_ns,in,out,pos,status
 *   trace_work.csv         workEnd:   entity,chain,role,worker,start_ns,dur_ns,requested,performed,status,flags
 *   trace_releases.csv     jobRelease: entity,chain,role,start_ns,batch,rel_deadline_ns,unassigned,flags,queue_depth
 *   trace_misses.csv       deadlineMiss: entity,chain,role,start_ns,late_ns,response_ns,batch,flags
 *   trace_sync.csv         stateSync: worker,start_ns,dur_ns,list_size,discarded,flags
 *   trace_probes.csv       workProbe: entity,chain,role,worker,start_ns,count,total_ns  (the unproductive
 *                          work() attempts, aggregated per block per sweep -- real CPU the busy fraction
 *                          would otherwise miss, large under RM/RR whose loops use work() as the oracle)
 *   trace_sweeps.csv       sweep: worker,start_ns,dur_ns,blocks,performed,status   (only with --all-events)
 *   trace_events.csv       every record: kind,entity,worker,start_ns,dur_ns,p0,p1,p2,status,flags (only with --all-events)
 */
#include <gnuradio-4.0/Trace.hpp>
#include <gnuradio-4.0/TraceCatapult.hpp> // kindName()
#include <gnuradio-4.0/TraceFile.hpp>

#include <nlohmann/json.hpp>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <print>
#include <string>
#include <unordered_map>
#include <vector>

using json = nlohmann::ordered_json;

namespace {
struct Role {
    int         chain = -1;
    std::string role;
};

std::FILE* openCsv(const std::string& path, const char* header) {
    std::FILE* f = std::fopen(path.c_str(), "w");
    if (!f) {
        std::println(stderr, "trace_export: cannot open {}", path);
        std::exit(1);
    }
    std::fputs(header, f);
    std::fputc('\n', f);
    return f;
}
} // namespace

int main(int argc, char** argv) {
    std::string capturePath, outDir, metaPath;
    bool        allEvents = false;
    for (int i = 1; i < argc; i++) {
        const std::string a = argv[i];
        if (a == "--out-dir" && i + 1 < argc) {
            outDir = argv[++i];
        } else if (a == "--meta" && i + 1 < argc) {
            metaPath = argv[++i];
        } else if (a == "--all-events") {
            allEvents = true;
        } else if (a.starts_with("--")) {
            std::println(stderr, "usage: trace_export CAPTURE --out-dir DIR [--meta trace_meta.json] [--all-events]");
            return 2;
        } else {
            capturePath = a;
        }
    }
    if (capturePath.empty() || outDir.empty()) {
        std::println(stderr, "usage: trace_export CAPTURE --out-dir DIR [--meta trace_meta.json] [--all-events]");
        return 2;
    }

    const auto loaded = gr::trace::load(capturePath);
    if (!loaded) {
        std::println(stderr, "trace_export: {}", loaded.error().message);
        return 1;
    }
    const gr::trace::Capture& cap = *loaded;

    // roles from rx_latency4's sidecar
    std::unordered_map<std::string, Role> roles;
    if (!metaPath.empty()) {
        std::ifstream f(metaPath);
        json          meta;
        f >> meta;
        for (const auto& ch : meta.at("chains")) {
            const int k = ch.at("chain").get<int>();
            for (const auto& b : ch.at("blocks")) {
                roles[b.at("unique_name").get<std::string>()] = Role{k, b.at("role").get<std::string>()};
            }
        }
    }
    std::unordered_map<std::uint16_t, Role> roleOf;
    for (const auto& e : cap.entities) {
        if (auto it = roles.find(e.uniqueName); it != roles.end()) {
            roleOf[e.id] = it->second;
        }
    }
    const auto chainOf = [&](std::uint16_t id) { auto it = roleOf.find(id); return it == roleOf.end() ? -1 : it->second.chain; };
    const auto nameOf  = [&](std::uint16_t id) -> const std::string& { static const std::string none; auto it = roleOf.find(id); return it == roleOf.end() ? none : it->second.role; };

    // header
    {
        const auto& h = cap.header;
        json        j = {{"capture", capturePath}, {"format_version", h.formatVersion}, {"clock_cost_ns", h.clockCostNs}, {"steady_anchor_ns", h.steadyAnchorNs}, {"monotonic_anchor_ns", h.monotonicAnchorNs}, {"clock_domain", h.clockDomain}, {"category_mask", h.categoryMask}, {"wall_clock_ns", h.wallClockNs}, {"process_id", h.processId}, {"entity_count", h.entityCount}, {"event_count", h.eventCount}, {"lost_count", h.lostCount}, {"ring_count", h.ringCount}};
        std::ofstream f(outDir + "/trace_header.json");
        f << j.dump(2) << "\n";
    }
    // entities
    {
        std::FILE* f = openCsv(outDir + "/trace_entities.csv", "id,worker,n_in,n_out,unique_name,type_name,chain,role");
        for (const auto& e : cap.entities) {
            std::fprintf(f, "%u,%u,%u,%u,%s,%s,%d,%s\n", e.id, e.workerId, e.nInputPorts, e.nOutputPorts, e.uniqueName.c_str(), e.typeName.c_str(), chainOf(e.id), nameOf(e.id).c_str());
        }
        std::fclose(f);
    }

    std::FILE* inv  = openCsv(outDir + "/trace_invocations.csv", "entity,chain,role,start_ns,dur_ns,in,out,pos,status");
    std::FILE* work = openCsv(outDir + "/trace_work.csv", "entity,chain,role,worker,start_ns,dur_ns,requested,performed,status,flags");
    std::FILE* rel  = openCsv(outDir + "/trace_releases.csv", "entity,chain,role,start_ns,batch,rel_deadline_ns,unassigned,flags,queue_depth");
    std::FILE* miss = openCsv(outDir + "/trace_misses.csv", "entity,chain,role,start_ns,late_ns,response_ns,batch,flags");
    std::FILE* sync = openCsv(outDir + "/trace_sync.csv", "worker,start_ns,dur_ns,list_size,discarded,flags");
    std::FILE* prb  = openCsv(outDir + "/trace_probes.csv", "entity,chain,role,worker,start_ns,count,total_ns");
    std::FILE* swp  = allEvents ? openCsv(outDir + "/trace_sweeps.csv", "worker,start_ns,dur_ns,blocks,performed,status") : nullptr;
    std::FILE* all  = allEvents ? openCsv(outDir + "/trace_events.csv", "kind,entity,worker,start_ns,dur_ns,p0,p1,p2,status,flags") : nullptr;

    std::map<std::string, std::uint64_t> census;
    using gr::trace::Kind;
    for (const gr::trace::Event& e : cap.events) {
        const Kind k = static_cast<Kind>(e.kind);
        census[std::string(gr::trace::detail::kindName(k))]++;
        const int   ch = chainOf(e.entity);
        const char* rn = nameOf(e.entity).c_str();
        switch (k) {
        case Kind::workExact: std::fprintf(inv, "%u,%d,%s,%llu,%u,%u,%u,%u,%d\n", e.entity, ch, rn, static_cast<unsigned long long>(e.startNs), e.durationNs, e.payload0, e.payload1, e.payload2, static_cast<int>(e.status)); break;
        case Kind::workEnd: std::fprintf(work, "%u,%d,%s,%u,%llu,%u,%u,%u,%d,%u\n", e.entity, ch, rn, e.workerId, static_cast<unsigned long long>(e.startNs), e.durationNs, e.payload0, e.payload1, static_cast<int>(e.status), e.flags); break;
        case Kind::jobRelease: std::fprintf(rel, "%u,%d,%s,%llu,%u,%u,%u,%u,%u\n", e.entity, ch, rn, static_cast<unsigned long long>(e.startNs), e.payload0, e.payload1, e.payload2, e.flags & 0x3u, e.flags >> 2); break;
        case Kind::deadlineMiss: std::fprintf(miss, "%u,%d,%s,%llu,%u,%u,%u,%u\n", e.entity, ch, rn, static_cast<unsigned long long>(e.startNs), e.payload0, e.payload1, e.payload2, e.flags); break;
        case Kind::stateSync: std::fprintf(sync, "%u,%llu,%u,%u,%u,%u\n", e.workerId, static_cast<unsigned long long>(e.startNs), e.durationNs, e.payload0, e.payload1, e.flags); break;
        case Kind::workProbe: std::fprintf(prb, "%u,%d,%s,%u,%llu,%u,%u\n", e.entity, ch, rn, e.workerId, static_cast<unsigned long long>(e.startNs), e.payload0, e.payload1); break;
        case Kind::sweep:
            if (swp) {
                std::fprintf(swp, "%u,%llu,%u,%u,%u,%d\n", e.workerId, static_cast<unsigned long long>(e.startNs), e.durationNs, e.payload0, e.payload1, static_cast<int>(e.status));
            }
            break;
        default: break;
        }
        if (all) {
            std::fprintf(all, "%s,%u,%u,%llu,%u,%u,%u,%u,%d,%u\n", gr::trace::detail::kindName(k).data(), e.entity, e.workerId, static_cast<unsigned long long>(e.startNs), e.durationNs, e.payload0, e.payload1, e.payload2, static_cast<int>(e.status), e.flags);
        }
    }
    for (std::FILE* f : {inv, work, rel, miss, sync, prb, swp, all}) {
        if (f) {
            std::fclose(f);
        }
    }
    {
        std::FILE* f = openCsv(outDir + "/trace_kinds.csv", "kind,count");
        for (const auto& [k, n] : census) {
            std::fprintf(f, "%s,%llu\n", k.c_str(), static_cast<unsigned long long>(n));
        }
        std::fclose(f);
    }
    std::println(stderr, "trace_export: {} records, {} entities, {} lost, {} rings, mask 0x{:x} -> {}", cap.events.size(), cap.entities.size(), cap.header.lostCount, cap.header.ringCount, cap.header.categoryMask, outDir);
    return 0;
}
