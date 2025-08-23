#include <omp.h>
#include "fmt/format.h"
#include "fmt/ranges.h"

#include "graph.h"
#include "importer.h"
#include "useful_configs.h"
#include "naive_memgraph.h"
using namespace dcsr;

template<typename Weight>
void check_oneway(Graph<Weight>* graph, MemGraph* mem_graph, size_t vertex_count) {
    for(VID i=0; i < vertex_count; i++) {
        auto edges = graph->GetNeighborsVectorInMemory(i);
        std::vector<VID> gn;
        for(auto& e : edges) {
            gn.push_back(e.to);
        }

        auto mgn = (*mem_graph)[i];

        std::sort(gn.begin(), gn.end());
        std::sort(mgn.begin(), mgn.end());
        if(gn != mgn) {
            fmt::println("Vertex {} not equal: ", i);
            fmt::println(" gn: {}", gn);
            fmt::println("mgn: {}", mgn);
            exit(1);
        }

    }

    return;
}

int main() {
    SetAffinityThisThread(0);
    
    auto cname = ConfigName::MEDIUM; // Change this to test different dataset
    auto [dataset, config] = useful_configs[static_cast<size_t>(cname)];
    config.buffer_size = 1024 * 1024 * 1024;
    config.buffer_count = 1;
    config.sort_batch_size = 128;

    auto mg = dcsr::LoadInMemoryOneWay(dataset, config.init_vertex_count);
    fmt::println("{}", mg[1]);
    // mem_bfs_oneway(&mg, 1);

    auto g = std::make_unique<Graph<void>>("./data/tmp_graph/", config);

    // auto graph_mem = LoadInMemory(dataset);
    // bfs_mem(&graph_mem, 1);

    auto [rt, pt] = ScanLargeFile<RawEdge64<void>, 8*1024*1024>(dataset, [&](RawEdge64<void> e) {
        g->AddEdge(e);
    });

    auto lt = TimeIt([&] {
        g->WaitSortingAndPrepareAnalysis();
    });

    fmt::println("{}", g->GetNeighborsVectorInMemory(1));

    fmt::println("Read time: {:.2f}s, Process time: {:.2f}s", rt, pt);
    fmt::println("Lock wait time: {:.2f}s", lt);
    
    check_oneway(g.get(), &mg, config.init_vertex_count);

    g->FinishAlgorithm();

    return 0;
}