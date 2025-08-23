#ifndef __DCSR_NAIVE_MEMGRAPH_H__
#define __DCSR_NAIVE_MEMGRAPH_H__

#include <filesystem>
#include <vector>
#include "fmt/format.h"
#include "datatype.h"
#include "importer.h"


namespace dcsr {

using MemGraph = std::vector<std::vector<VID>>;
using MemTGraph = std::pair<MemGraph, MemGraph>;

MemGraph LoadInMemoryOneWay(std::filesystem::path dataset, size_t vertex_count) {
    MemGraph graph_out;
    graph_out.resize(vertex_count);
    auto [rt, pt] = ScanLargeFile<RawEdge64<void>, 8*1024*1024>(dataset, [&](RawEdge64<void> e) {
        graph_out[e.from].push_back(e.to);
    });
    fmt::println("Read time: {:.2f}s", rt);
    fmt::println("Process time: {:.2f}s", pt);
    return graph_out;
}

MemTGraph LoadInMemoryTwoWay(std::filesystem::path dataset, size_t vertex_count) {
    MemGraph graph_in;
    MemGraph graph_out;
    graph_in.resize(vertex_count);
    graph_out.resize(vertex_count);
    auto [rt, pt] = ScanLargeFile<RawEdge64<void>, 8*1024*1024>(dataset, [&](RawEdge64<void> e) {
        graph_out[e.from].push_back(e.to);
        graph_in[e.to].push_back(e.from);
    });
    fmt::println("Read time: {:.2f}s", rt);
    fmt::println("Process time: {:.2f}s", pt);
    return std::make_pair(std::move(graph_in), std::move(graph_out));
}



void mem_bfs_oneway(MemGraph* graph, VID root) {
    int level = 1;
    int64_t frontier = 0;
    size_t v_count = graph->size();

    auto& graph_out = *graph;

    auto levels = std::make_unique_for_overwrite<uint16_t[]>(v_count);
    std::fill(levels.get(), levels.get() + v_count, 0);

    SimpleTimer timer;
    levels[root] = level;

    do {
        frontier = 0;
        SimpleTimer level_timer;

        //#pragma omp parallel reduction(+:frontier)
        {
            //#pragma omp for nowait
            for (VID v = 0; v < v_count; v++) {
                if (levels[v] != level) {
                    continue;
                }

                for(auto to: graph_out[v]) {
                    if(levels[to] == 0) {
                        levels[to] = level + 1;
                        frontier++;
                    }
                }
            }
        }

        double level_time = level_timer.Stop();
        fmt::println("Level = {}, Frontier Count = {}, Time = {:.2f}s", level, frontier, level_time);

        level ++;
    } while (frontier);

    fmt::println("BFS root = {}, Time = {:.2f}s", root, timer.Stop());
}

} // namespace dcsr

#endif // __DCSR_NAIVE_MEMGRAPH_H__