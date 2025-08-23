/**
 * @file graph_insertion_test.cpp
 * @author Long Deng (ldeng@mail.ustc.edu.cn)
 * @brief 简单测试基于插入的动态图ingesting性能上限。
 * @version 0.1
 * @date 2024-10-08
 * 
 * @copyright Copyright (c) 2024
 * 
 */
#include <bits/stdc++.h>
#include <omp.h>
#include <numa.h>
#include "importer.h"
#include "common.h"
#include "metrics.h"
using namespace std;
namespace fs = std::filesystem;

using Vertex = uint32_t;
// using Vertex = uint64_t;
using RawEdge = std::pair<uint32_t, uint32_t>;
using Edge = std::pair<Vertex, Vertex>;
using VertexIndexElem = std::array<uint32_t, 1>;

const fs::path PATH = "./data/bin32/shuffled/friendster/friendster.bin";
const size_t FILE_EDGES = 2586147869;
const size_t E = 64ull * 1024 * 1024;
const size_t V = 68349466;

const fs::path PERF_FIFO = "/tmp/bubble_perf_fifo";

bool perf_enable = false;

#define EXPOUT "[EXPOUT]"


void do_insertion(std::span<VertexIndexElem> space, std::span<Edge> edges) {
    size_t n = edges.size();
    for(size_t i = 0; i < n; i++) {
        auto& buf = space[edges[i].first];
        buf[0] ++;
        // buf[1] = edges[i].second;
    }
}

void test_multi_thread_insertion(size_t thread_count) {
    auto edge_array = make_unique_for_overwrite<Edge[]>(FILE_EDGES);
    
    size_t n_edges = 0;
    dcsr::ScanLargeFileSegmentSilent<RawEdge, 1 << 20>(PATH, 0, FILE_EDGES, [&](Edge e){
        edge_array[n_edges++] = {e.first, e.second};
    });

    std::span<Edge> edge(edge_array.get(), n_edges);
    size_t n = E;
    size_t group_v = V / thread_count;
    // size_t group_v = V / 80;
    // size_t group_v = 16 * 1024 * 1024;

    auto times = std::make_unique<double[]>(thread_count);

    // use openmp
    SimpleTimer tm;
    #pragma omp parallel num_threads(thread_count)
    {
        // prepare data
        // prepare data
        size_t total_cores = omp_get_num_procs();
        size_t numa_cores = total_cores / numa_num_configured_nodes();

        size_t i = omp_get_thread_num();
        size_t core_group = i / 4;  // each group has 4 cores, 2 for each numa node
        size_t hyper = i % 4 / 2;
        size_t node = i % 2;
        size_t core_id = core_group * 2 + hyper + node * numa_cores;
        // fmt::println("[{:3}T]Thread {}: core group {}, node {}, hyper {}, core id {}", thread_count, i, core_group, node, hyper, core_id);

        (void)core_id;
        dcsr::SetAffinityThisThread(core_id);

        // size_t groups = V / group_v;
        // size_t vl = (i % groups) * group_v;
        // size_t vr = (i % groups + 1) * group_v;
        size_t vl = i * group_v;
        size_t vr = (i + 1) * group_v;
        
        // fmt::println("[{:3}T]Thread {}: [{}, {})", thread_count, i, vl, vr);
        
        // auto index = make_unique<VertexIndexElem[]>(group_v);
        // auto thread_test_edge = std::make_unique_for_overwrite<Edge[]>(n);
        auto index = (VertexIndexElem*)numa_alloc_local(sizeof(VertexIndexElem) * group_v);
        auto thread_test_edge = (Edge*)numa_alloc_local(sizeof(Edge) * n);
        std::fill(index, index + group_v, VertexIndexElem{0});

        size_t off = 0;
        for(size_t j = 0; j < n_edges && off < n; j++) {
            if(edge_array[j].first >= vl && edge_array[j].first < vr) {
                thread_test_edge[off] = edge_array[j];
                thread_test_edge[off].first -= vl;
                off++;
            }
        }
        for(size_t k = 0; off < n;) {
            thread_test_edge[off++] = thread_test_edge[k++];
        }
        // fmt::println("[{:3}T]Thread {}: {} edges", thread_count, i, off);
        #pragma omp barrier
        #pragma omp single
        {
            fmt::println("Timer start, prepare cost: {:.5f}s", tm.Lap());
            if(perf_enable)
                system(fmt::format("echo enable > {}", PERF_FIFO.string()).c_str());
            tm.Lap();
        }
        #pragma omp barrier

        times[i] = TimeIt([&]{
            // auto index_span = std::span<VertexIndexElem>(index.get(), group_v);
            // auto edges_span = std::span<Edge>(thread_test_edge.get(), n);
            // do_insertion(index_span, edges_span);
            do_insertion({index, group_v}, {thread_test_edge, n});
        });
    }
    double total = tm.Stop();
    double total_mops = n * thread_count / total / 1'000'000;
    double total_bw_mbytes = n * thread_count * sizeof(uint32_t) / total / 1024 / 1024;

    fmt::println("[{:3}T]Total: {:.5f}s  {:.2f}M Edges/s, {:.2f}MB/s", thread_count, total, total_mops, total_bw_mbytes);
    for(size_t i = 0; i < thread_count; i++) {
        fmt::println("[{:3}T]Thread {}: {:.5f}s", thread_count, i, times[i]);
    }

    fmt::println(EXPOUT "throughput: {}", total_mops);
}

int main(int argc, char** argv) {
    if(argc < 2) {
        fmt::print("Usage: {} <thread_count>\n", argv[0]);
        return 1;
    }

    if(argc == 3) {
        perf_enable = true;
    }

    size_t t = std::atoi(argv[1]);
    fmt::println("Start testing with {} threads", t);
    test_multi_thread_insertion(t);
    
    // test_multi_thread_insertion(edges_span, 1);
    // test_multi_thread_insertion(edges_span, 2);
    // test_multi_thread_insertion(edges_span, 4);
    // test_multi_thread_insertion(edges_span, 8);
    // test_multi_thread_insertion(edges_span, 16);
    // test_multi_thread_insertion(edges_span, 32);
    // test_multi_thread_insertion(edges_span, 64);


    return 0;
}

