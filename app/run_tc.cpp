#include <omp.h>

#include "cxxopts.hpp"
#include "fmt/format.h"
#include "fmt/ranges.h"

#include "env.h"
#include "graph.h"
#include "importer.h"
#include "algorithms.h"

using namespace dcsr;
using namespace std;
namespace fs = std::filesystem;

#define EXPOUT "[EXPOUT]"

template<typename RE>
size_t load_dataset(const fs::path& dataset, RawEdge32<void>* edge_buffer) {
    size_t vertex_count = 0;
    size_t i = 0;
    ScanLargeFile<RE, 8*1024*1024>(dataset, [&](RE e) {
        edge_buffer[i++] = RawEdge32<void>{e.from, e.to};
        if(e.from >= vertex_count) {
            vertex_count = e.from + 1;
        }
        if(e.to >= vertex_count) {
            vertex_count = e.to + 1;
        }
    });
    return vertex_count;
}

int main(int argc, char** argv) {
    cxxopts::Options options("benchmarks", "Benchmarks for DCSR");
    options.add_options()
        ("h,help", "Print help")
        ("f,input", "Dataset to use", cxxopts::value<string>())
        ("b32", "Load 32 bit dataset. (Graph32 is always 32 bit, but you can load 64 bit dataset)")
        ("b,batch_size", "Ingesting batch size, to speed up dispatch", cxxopts::value<size_t>()->default_value("65536"))
        ("t,thread", "Number of threads to use for ingesting", cxxopts::value<size_t>())
        ;

    auto result = options.parse(argc, argv);
    bool b32_dataset = result["b32"].as<bool>();
    fs::path dataset = result["input"].as<string>();
    size_t edge_size = b32_dataset ? sizeof(RawEdge32<void>) : sizeof(RawEdge64<void>);
    size_t edge_count = fs::file_size(dataset) / edge_size;


    SimpleTimer timer;

    // To fairly compare with other works, load dataset first
    auto edge_buffer = std::make_unique_for_overwrite<RawEdge32<void>[]>(edge_count);
    size_t vertex_count = 0;
    if(!b32_dataset) {
        vertex_count = load_dataset<RawEdge64<void>>(dataset, edge_buffer.get());
    } else {
        vertex_count = load_dataset<RawEdge32<void>>(dataset, edge_buffer.get());
    }

    size_t thread_count = result.count("thread") ? result["thread"].as<size_t>() : GetLogicalCoreCount();
    Config config = GenerateUGraphConfig(vertex_count, edge_count, thread_count);
    fmt::println("Config:\n{}", config);

    auto t_load = timer.Lap();

    // ingesting
    auto g = std::make_unique<UGraph32<void>>("./data/tmp_graph/", config);
    auto t_init = timer.Lap();

    size_t b = 0;
    size_t batch_size = result["batch_size"].as<size_t>();
    // size_t batch_size = 1024*1024;
    for(size_t i = 0; i < edge_count; i+=batch_size) {
        size_t len = std::min(batch_size, edge_count - i);
        std::span<const RawEdge32<void>> batch(edge_buffer.get() + i, len);
        // fmt::println("Batch {}, len: {}", i, len);
        g->AddEdgeBatch(batch);
        b++;
    }
    g->Collect();
    fmt::println("Batch count: {}", b);
    auto t_insert = timer.Lap();

    g->WaitSortingAndPrepareAnalysis();
    // UnsetAffinityThisThread();
    auto t_wait = timer.Lap();
    auto t_ingest = t_insert + t_wait;
    
    edge_buffer.reset();
    timer.Lap();


    // g->GraphView().IterateNeighborsInOrder(1, [](VID32 v) {
    //     fmt::println("{}", v);
    // });
    // exit(0);
    
    // size_t count = tc_gapbs(g.get());
    size_t count = tc_gapbs_cached(g.get());
    // size_t count = tc_lsgraph(g.get());
    auto t_tc = timer.Lap();

    fmt::println("Triange count: {}", count);

    g->FinishAlgorithm();

    fmt::println(EXPOUT "Dataset: {}", dataset.string());
    fmt::println(EXPOUT "Vertex count: {}", vertex_count);
    fmt::println(EXPOUT "Load: {:.3f}s", t_load);
    fmt::println(EXPOUT "Ingest: {:.3f}s", t_ingest);
    fmt::println("\t{:.3f}s (init) + {:.3f}s (insert) + {:.3f}s (wait)", t_init, t_insert, t_wait);
    fmt::println(EXPOUT "TC: {:.3f}s", t_tc);

    return 0;
}