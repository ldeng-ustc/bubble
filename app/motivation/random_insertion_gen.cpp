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

using IndexT = uint32_t;

const size_t E = 128ull * 1024 * 1024;
const size_t V = (1ull << 26);

const fs::path PERF_FIFO = "/tmp/bubble_perf_fifo";

bool perf_enable = false;

#define EXPOUT "[EXPOUT]"

// 为了随机数生成速度，使用自然溢出的线性同余发生器
// x[i+1] = (a*x[i] + 1) mod 2^64, a = 0xf9b25d65
// using FastRandomEngine = std::linear_congruential_engine<uint64_t, 0xf9b25d65, 1, 0>;
// 或更快的乘法同余发生器
using FastRandomEngine = std::linear_congruential_engine<uint64_t, 0xe817fb2d, 0, 0>;
// 两者的低位都有明显规律，最好抛弃低位，或使用下面较慢的 Mersenne Twister （一定程度上影响测试准确性）
// using FastRandomEngine = std::mt19937_64;

void do_insertion(std::span<IndexT> index, size_t n) {
    FastRandomEngine gen{1};
    std::uniform_int_distribution<size_t> dist(0, (index.size() - 1)*4);

    for(size_t i = 0; i < n; i++) {
        size_t off = (dist(gen)>>2);
        index[off]++;
    }
}

void test_multi_thread_insertion(size_t thread_count) {
    size_t group_v = V / thread_count;
    // size_t group_v = V / 80;
    // size_t group_v = 16 * 1024 * 1024;

    auto times = std::make_unique<double[]>(thread_count);

    // use openmp
    SimpleTimer tm;
    #pragma omp parallel num_threads(thread_count)
    {
        // prepare data
        size_t i = omp_get_thread_num();
        size_t node = i % numa_num_configured_nodes();
        // bind core i to thread i
        numa_run_on_node(i % numa_num_configured_nodes());

        auto index = (IndexT*)numa_alloc_onnode(sizeof(IndexT) * group_v, node);
        std::fill(index, index + group_v, IndexT{0});   // warm up

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
            do_insertion({index, group_v}, E);
        });
    }
    double total = tm.Stop();
    double total_mops = E * thread_count / total / 1'000'000;
    double total_bw_mbytes = E * thread_count * sizeof(IndexT) / total / 1024 / 1024;

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

