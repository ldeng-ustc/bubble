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
#include "third_party/pdqsort.h"
using namespace std;
namespace fs = std::filesystem;

using Vertex = uint32_t;
// using Vertex = uint64_t;
using RawEdge = std::pair<uint32_t, uint32_t>;
using Edge = std::pair<Vertex, Vertex>;

const fs::path PATH = "./data/bin32/shuffled/friendster/friendster.bin";
const size_t FILE_EDGES = 2586147869;
const size_t E = 64ull * 1024 * 1024;
const size_t V = 68349466;
size_t BATCH = 65536;

const fs::path PERF_FIFO = "/tmp/bubble_perf_fifo";
bool perf_enable = false;


// 为了随机数生成速度，使用自然溢出的线性同余发生器
// x[i+1] = (a*x[i] + 1) mod 2^64, a = 0xf9b25d65
// using FastRandomEngine = std::linear_congruential_engine<uint64_t, 0xf9b25d65, 1, 0>;
// 或更快的乘法同余发生器
using FastRandomEngine = std::linear_congruential_engine<uint64_t, 0xe817fb2d, 0, 0>;
// 两者的低位都有明显规律，最好抛弃低位，或使用下面较慢的 Mersenne Twister （一定程度上影响测试准确性）
// using FastRandomEngine = std::mt19937_64;

#define EXPOUT "[EXPOUT]"


void do_insertion(/*std::span<Edge> space,*/ std::span<Edge> edges) {
    // (void)space;
    size_t n = edges.size();
    // for(size_t i = 0; i < n; i++) {
    //     if(i != 0 && i % BATCH == 0) {
    //         // std::sort(space.begin() + i - BATCH, space.begin() + i, [](const Edge& a, const Edge& b){
    //         //     return a.first < b.first;
    //         // });
    //         pdqsort_branchless(edges.begin() + i - BATCH, edges.begin() + i, [](const Edge& a, const Edge& b){
    //             return a.first < b.first;
    //         });
    //         // std::sort(edges.begin() + i - BATCH, edges.begin() + i, [](const Edge& a, const Edge& b){
    //         //     return a.first < b.first;
    //         // });
    //         // pdqsort(edges.begin() + i - BATCH, edges.begin() + i, [](const Edge& a, const Edge& b){
    //         //     return a.first < b.first;
    //         // });
    //     }
    //     space[i] = edges[i];
    // }

    for(size_t i = 0; i < n; i += BATCH) {
        size_t len = std::min(BATCH, n - i);
        // pdqsort_branchless(edges.data() + i, edges.data() + i + len, [](const Edge& a, const Edge& b){
        //     return a.first < b.first;
        // });
        std::shuffle(edges.data() + i, edges.data() + i + len, FastRandomEngine{1});
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
    // size_t group_v = V / thread_count;
    size_t group_v = 16 * 1024 * 1024;
    auto times = std::make_unique<double[]>(thread_count);

    // use openmp
    SimpleTimer tm;
    #pragma omp parallel num_threads(thread_count)
    {
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
        // fmt::println("[{:3}T]Thread {}: node {}, ret={} (errno={})", thread_count, i, node, ret, strerror(errno));
        // fmt::println("[{:3}T]Thread {}: physical core: {}", thread_count, i, sched_getcpu());
        // fmt::println("[{:3}T]Thread {}: numa available: {}", thread_count, i, numa_available());


        size_t groups = V / group_v;
        size_t vl = (i % groups) * group_v;
        size_t vr = (i % groups + 1) * group_v;
        auto thread_test_edge = (Edge*)numa_alloc_local(sizeof(Edge) * n);

        // FastRandomEngine gen{1};
        // std::uniform_int_distribution<size_t> dist(0, group_v-1);
        // for(size_t j = 0; j < n; j++) {
        //     thread_test_edge[j] = {dist(gen), dist(gen)};
        // }

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

        // fmt::println("[{:3}T]Thread {}: start insertion, physical core: {}", thread_count, i, sched_getcpu());

        times[i] = TimeIt([&]{
            do_insertion(/*{buffer, n},*/ {thread_test_edge, n});
        });
    }
    double total = tm.Stop();
    double total_mops = n * thread_count / total / 1'000'000;
    double total_bw_mbytes = n * thread_count * sizeof(Edge) / total / 1024 / 1024;

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

    size_t t = std::atoi(argv[1]);
    fmt::println("Start testing with {} threads", t);
    if(argc >= 3) {
        BATCH = std::atoi(argv[2]);
    }

    if(argc >= 4) {
        perf_enable = true;
    }
    
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

