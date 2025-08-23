#include <omp.h>
#include "fmt/format.h"
#include "fmt/ranges.h"

#include "env.h"
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

    auto g = std::make_unique<TGraph32<void>>("./data/tmp_graph/", config);

    auto [rt, pt] = ScanLargeFile<RawEdge64<void>, 8*1024*1024>(dataset, [&](RawEdge64<void> e) {
        g->AddEdge(RawEdge32<void>{e.from, e.to});
    });

    fmt::println("Total sleep time: {}ms", g->TotalSleepMillis());

    auto lt = TimeIt([&] {
        g->WaitSortingAndPrepareAnalysis();
    });

    fmt::println("Read time: {:.2f}s, Process time: {:.2f}s", rt, pt);
    fmt::println("Lock wait time: {:.2f}s", lt);

    UnsetAffinityThisThread();

    auto t = TimeIt([&] {
        bfs(g.get(), 1);
        // bfs_oneway_reverse(g.get(), 1);  // compare with LSGraph dense, cannot get correct result
    });

    g->FinishAlgorithm();
    
    fmt::println("BFS time: {:.2f}s", t);
    return 0;
}
