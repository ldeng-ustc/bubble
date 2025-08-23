#include "fmt/format.h"
#include "fmt/ranges.h"

#include "graph.h"
#include "importer.h"

#include "useful_configs.h"

using namespace dcsr;

int main() {
    SetAffinityThisThread(0);

    auto cname = ConfigName::LARGE; // Change this to test different dataset
    auto [dataset, config] = useful_configs[static_cast<size_t>(cname)];

    config.init_vertex_count = 72ul * 1024 * 1024;
    config.partition_size = 4ul * 1024 * 1024;
    // config.partition_size = 16ul * 1024 * 1024;
    config.buffer_count = 2;

    // config.sort_batch_size = 1024 * 128;

    
    constexpr size_t REPEAT = 4;
    double rt_all = 0.0;
    double pt_all = 0.0;
    double rt, pt;

    auto g = std::make_unique<Graph<void>>("./data/tmpdb2", config);
    size_t total_edges = 0;
    
    for(size_t i = 0; i < REPEAT; i++){
        std::tie(rt, pt) = ScanLargeFile<RawEdge64<void>, 8*1024*1024>(dataset, [&](RawEdge64<void> e) {
            g->AddEdge(e);
            total_edges++;
        });
        rt_all += rt;
        pt_all += pt;
    }

    double speed_process = total_edges / pt_all / 1e6;
    double speed_end2end = total_edges / (rt_all + pt_all) / 1e6;
    fmt::println("Imported {} edges", total_edges);
    fmt::println("Speed (process): {:.2f}M edges/s;  Speed (end2end): {:.2f}M edges/s", speed_process, speed_end2end);


    return 0;
}