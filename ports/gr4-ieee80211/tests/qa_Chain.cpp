#include <boost/ut.hpp>

#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>
#include <gnuradio-4.0/SchedulingAnalysis.hpp>

#include "gr4ieee80211/chain.hpp"

#include <algorithm>
#include <map>
#include <string>

using namespace boost::ut;
using namespace gr4wifi;

namespace {

constexpr unsigned kBatch = 1024U;
constexpr double   kRate  = 5e6;

ChainConfig pacerConfig() {
    ChainConfig cfg;
    cfg.input       = "unused-in-pacer-mode.cf32";
    cfg.rate        = kRate;
    cfg.chunk       = kBatch;
    cfg.batch       = kBatch;
    cfg.fixed_batch = kBatch;
    cfg.tiny_period = 1e-6f;
    cfg.feed        = ChainConfig::Feed::pacer;
    return cfg;
}

// role -> what GR4's scheduling analysis derived for that block, after init
template<typename TSched>
std::map<std::string, gr::scheduler::DerivedAttributes> derive(TSched& sched, gr::Graph graph, const ChainBlocks& chain) {
    expect(sched.exchange(std::move(graph)).has_value());
    expect(sched.changeStateTo(gr::lifecycle::State::INITIALISED).has_value());
    std::map<std::string, gr::scheduler::DerivedAttributes> byRole;
    for (const auto& block : sched.graph().blocks()) {
        const auto role = std::ranges::find_if(chain.roles, [&](const auto& r) { return r.first == block->uniqueName(); });
        if (role != chain.roles.end()) {
            byRole[role->second] = *sched.schedulingAnalysis().find(*block);
        }
    }
    return byRole;
}

} // namespace

const boost::ut::suite<"pacer-mode receiver"> chainTests = [] {
    "an entry block replaces the file source, throttle and stamper"_test = [] {
        gr::Graph         graph;
        const ChainBlocks chain = buildChain(graph, pacerConfig());
        const auto        has   = [&](std::string_view role) { return std::ranges::any_of(chain.roles, [&](const auto& r) { return r.second == role; }); };
        expect(has("feed"));
        expect(!has("fsrc") && !has("throttle") && !has("stamp")) << "nothing of the in-graph clock remains";
        expect(eq(chain.block_count, 16)) << "18 blocks, three removed, one added";
        expect(chain.feed_in != nullptr) << "the pacer needs the entry block's input";
        expect(chain.stamper == nullptr) << "arrival is the pacer's log, not a stamper's";
    };

    "under EDF every period is exactly 0 and every deadline explicit"_test = [] {
        ChainConfig cfg = pacerConfig();
        cfg.zero_period = true;
        gr::Graph                                                                                                                      graph;
        const ChainBlocks                                                                                                              chain = buildChain(graph, cfg);
        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded, gr::profiling::null::Profiler, gr::scheduler::EdfPolicy> sched;
        const auto                                                                                                                     byRole = derive(sched, std::move(graph), chain);

        expect(eq(byRole.size(), 16UZ));
        for (const auto& [role, a] : byRole) {
            expect(eq(a.period, 0.f)) << role << ": a zero period, so no temporal gate";
            expect(a.periodOrigin == gr::scheduler::AttributeOrigin::userSet) << role << ": kept, not derived";
            expect(a.relativeDeadline > 0.f) << role << ": without its own deadline a block would sort last";
        }
        const float batchPeriod = static_cast<float>(kBatch / kRate);
        expect(approx(byRole.at("feed").relativeDeadline, batchPeriod, 1e-9f)) << "the entry block has a pre-gate block's deadline, not the throttle's tiny one";
        expect(eq(byRole.at("feed").relativeDeadline, byRole.at("mag2").relativeDeadline));
        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "under RM the entry block shares its receiver's true period and rank"_test = [] {
        ChainConfig cfg     = pacerConfig();
        cfg.rm_true_periods = true;
        gr::Graph                                                                                                                                graph;
        const ChainBlocks                                                                                                                        chain = buildChain(graph, cfg);
        gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::singleThreaded, gr::profiling::null::Profiler, gr::scheduler::RateMonotonicPolicy> sched;
        const auto                                                                                                                               byRole = derive(sched, std::move(graph), chain);

        expect(approx(byRole.at("feed").period, static_cast<float>(kBatch / kRate), 1e-9f));
        expect(eq(byRole.at("feed").period, byRole.at("mag2").period));
        expect(eq(byRole.at("feed").priority, byRole.at("mag2").priority)) << "one rate class: arrival is not ranked above the receiver it feeds";
        std::ignore = sched.changeStateTo(gr::lifecycle::State::STOPPED);
    };

    "a zero period is refused where a block has no deadline of its own"_test = [] {
        ChainConfig cfg = pacerConfig();
        cfg.feed        = ChainConfig::Feed::throttle; // the file source and throttle rely on the implicit deadline
        cfg.zero_period = true;
        gr::Graph graph;
        expect(throws([&] { std::ignore = buildChain(graph, cfg); })) << "a zero period with no deadline would sort the block last under EDF";
    };
};

int main() { /* not needed for UT */ }
