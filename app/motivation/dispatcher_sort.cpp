#include <bits/stdc++.h>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "cxxopts.hpp"
#include "parlay/primitives.h"

#include "metrics.h"

using namespace std;
namespace fs = std::filesystem;

using Vertex = uint32_t;
using Edge = std::pair<uint32_t, uint32_t>;
using Slice = decltype(parlay::make_slice(std::declval<Edge*>(), std::declval<Edge*>()));

// LSGraph dispatcher
void sort_updates(Slice edges, size_t nn = std::numeric_limits<size_t>::max()) {
    using edge = std::tuple<uint32_t, uint32_t>;
    size_t vtx_bits = parlay::log2_up(nn);

    size_t m = edges.size();

    auto edge_to_long = [vtx_bits] (edge e) -> size_t {
        return (static_cast<size_t>(std::get<0>(e)) << vtx_bits) | static_cast<size_t>(std::get<1>(e));
    };

    // Only apply integer sort if it will be work-efficient
    if (nn <= (m * parlay::log2_up(m))) {
        parlay::stable_integer_sort_inplace(edges, edge_to_long);
    } else {
        // fmt::println("Sorting with std::less");
        parlay::stable_sort_inplace(edges, std::less<edge>());
    }
}

auto read_dataset(const string& dataset_path) {
    auto file_size = fs::file_size(dataset_path);
    size_t edge_count = file_size / sizeof(Edge);
    edge_count = std::min(edge_count, (size_t)256*1024*1024);

    auto buffer = make_unique_for_overwrite<Edge[]>(edge_count);

    FILE* f = fopen(dataset_path.c_str(), "rb");
    if (f == nullptr) {
        fmt::print("Failed to open {}\n", dataset_path);
        exit(1);
    }

    int ret = fread(buffer.get(), sizeof(Edge), edge_count, f);
    if(ret != (int)edge_count) {
        fmt::print("Failed to read edges, expected {}, got {}\n", edge_count, ret);
        fclose(f);
        exit(1);
    }
    fclose(f);

    return std::make_pair(std::move(buffer), edge_count);
}

int main(int argc, char** argv) {
    cxxopts::Options options("summarize", "Summarize the distribution of a dataset");
    options.add_options()
        ("i,input", "Input file path", cxxopts::value<string>())
        ("v", "Vertex count", cxxopts::value<size_t>())
        ("b,batch", "Batch size", cxxopts::value<size_t>()->default_value("65536"))
        // ("t,threads", "Number of threads", cxxopts::value<size_t>()->default_value("1"))
        // set env: PARLAY_NUM_THREADS=t to set number of threads
        ("h,help", "Print usage");
    auto result = options.parse(argc, argv);
    if (result.count("help") || !result.count("input")) {
        cout << options.help() << endl;
        return 0;
    }

    std::string dataset_path = result["input"].as<string>();
    size_t batch = result["batch"].as<size_t>();

    auto [edges, n] = read_dataset(dataset_path);

    parlay::sequence<Edge> edges_seq(edges.get(), edges.get() + n);
    size_t nn = result["v"].as<size_t>();

    SimpleTimer tm;

    for(size_t i = 0; i < edges_seq.size(); i += batch) {
        size_t r = std::min(i + batch, edges_seq.size());
        auto slice = parlay::make_slice(edges_seq.begin() + i, edges_seq.begin() + r);
        sort_updates(slice, nn);

        parlay::sequence<Edge> edges_seq2(slice.size());
        for(size_t j = 0; j < slice.size(); j++) {
            edges_seq2[j] = Edge(slice[j].second, slice[j].first);
        }
        auto slice2 = parlay::make_slice(edges_seq2.begin(), edges_seq2.end());
        sort_updates(slice2, nn);
    }
    double sort_time = tm.Stop();

    fmt::println("Sorted edges in {:.5f}s", sort_time);
    fmt::println("Throughput: {:.2f} MEPS", n / 1e6 / sort_time);
    fflush(stdout);
    return 0;
}