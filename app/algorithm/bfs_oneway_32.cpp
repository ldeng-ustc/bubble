#include <omp.h>
#include "fmt/format.h"
#include "fmt/ranges.h"

#include "graph.h"
#include "importer.h"
#include "useful_configs.h"
#include "naive_memgraph.h"
#include "algorithms/bfs.h"
using namespace dcsr;

int main() {

    SetAffinityThisThread(0);
    
    auto cname = ConfigName::LARGE; // Change this to test different dataset
    auto [dataset, config] = useful_configs[static_cast<size_t>(cname)];
    config.buffer_size = 1024 * 1024 * 1024;
    // config.sort_batch_size = 64 * 1024;
    config.merge_multiplier = 2.0;

    if(config.init_vertex_count < 128 * 1024 * 1024) {
        // Run vector based graph for comparison
        auto mg = dcsr::LoadInMemoryOneWay(dataset, config.init_vertex_count);
        fmt::println("{}", mg[1]);
        mem_bfs_oneway(&mg, 1);
    }

    auto g = std::make_unique<Graph32<void>>("./data/tmp_graph/", config);

    // auto graph_mem = LoadInMemory(dataset);
    // bfs_mem(&graph_mem, 1);

    auto [rt, pt] = ScanLargeFile<RawEdge64<void>, 8*1024*1024>(dataset, [&](RawEdge64<void> e) {
        g->AddEdge({e.from, e.to});
    });

    fmt::println("Total sleep time: {}ms", g->TotalSleepMillis());

    auto lt = TimeIt([&] {
        g->WaitSortingAndPrepareAnalysis();
    });

    fmt::println("{}", g->GetNeighborsVectorInMemory(1));

    fmt::println("Read time: {:.2f}s, Process time: {:.2f}s", rt, pt);
    fmt::println("Lock wait time: {:.2f}s", lt);

    UnsetAffinityThisThread();
    // bfs_oneway_omp(g.get(), 1);

    std::vector<double> times;
    size_t BFS_TIMES = 100;
    for(size_t i = 0; i < BFS_TIMES; i++) {
        auto t = TimeIt([&] {
            bfs_oneway(g.get(), 1);
        });
        times.push_back(t);
    }
    g->FinishAlgorithm();
    
    fmt::println("BFS time: {::.2f}", times);
    fmt::println("Average BFS time: {:.2f}s", std::accumulate(times.begin(), times.end(), 0.0) / BFS_TIMES);
    return 0;
}