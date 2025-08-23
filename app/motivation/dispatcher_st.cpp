#include <bits/stdc++.h>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "cxxopts.hpp"

#include "metrics.h"

using namespace std;
namespace fs = std::filesystem;

using Vertex = uint32_t;
using Edge = std::pair<uint32_t, uint32_t>;
using Slice = std::span<Edge>;

static std::vector<Edge> local_buffers[512];
static size_t off[512] = {0};
static std::vector<Edge> local_buffers_r[512];
static size_t off_r[512] = {0};

const size_t BATCH = 32*1024;

// GraphOne dispatcher
void sort_updates(Slice edges, size_t pwidth_bits, size_t threads) {
    (void) threads;
    (void) pwidth_bits;
    // for(size_t i = 0; i < threads; i++) {
    //     local_buffers[i].clear();
    //     local_buffers_r[i].clear();
    // }

    for(Edge e: edges) {
        auto [from, to] = e;
        size_t partition_id = (from >> pwidth_bits);
        size_t partition_id_r = to >> pwidth_bits;
        partition_id = partition_id >= threads ? partition_id - threads : partition_id;
        partition_id_r = partition_id_r >= threads ? partition_id_r - threads : partition_id_r;
        // size_t partition_id = from & (threads - 1);
        // size_t partition_id_r = to & (threads - 1);

        // mt::println("Edge: {} -> {}, partition_id: {}, partition_id_r: {}", from, to, partition_id, partition_id_r);
        // local_buffers[partition_id].push_back(e);
        // local_buffers_r[partition_id_r].push_back(e);
        local_buffers[partition_id][off[partition_id]%BATCH] = e;
        off[partition_id]++;
        local_buffers_r[partition_id_r][off_r[partition_id_r]%BATCH] = e;
        off_r[partition_id_r]++;
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
        ("t,threads", "Number of threads", cxxopts::value<size_t>()->default_value("1"))
        ("h,help", "Print usage");
    auto result = options.parse(argc, argv);
    if (result.count("help") || !result.count("input")) {
        cout << options.help() << endl;
        return 0;
    }

    std::string dataset_path = result["input"].as<string>();
    size_t batch = result["batch"].as<size_t>();
    size_t threads = result["threads"].as<size_t>();
    threads *= 4; // 4 partitionos per thread

    auto [edges, n] = read_dataset(dataset_path);
    size_t nn = result["v"].as<size_t>();

    size_t pwidth = (nn + threads - 1) / threads;
    size_t pwidth_bits = std::bit_width(pwidth) - 1;

    fmt::println("Partition width: {}", pwidth);

    for(size_t i = 0; i < threads; i++) {
        local_buffers[i].resize(BATCH);
        // local_buffers[i].clear();
        local_buffers_r[i].resize(BATCH);
        // local_buffers_r[i].clear();
        // local_buffers[i].reserve(n);
        // local_buffers_r[i].reserve(n);
    }

    SimpleTimer tm;

    for(size_t i = 0; i < n; i += batch) {
        size_t r = std::min(i + batch, n);
        auto slice = Slice(edges.get() + i, edges.get() + r);
        sort_updates(slice, pwidth_bits, threads);
    }
    double sort_time = tm.Stop();

    // for(size_t i = 0; i < threads; i++) {
    //     fmt::println("Thread {}: {} edges", i, local_buffers[i].size());
    //     fmt::println("Thread {}: {} edges(r)", i, local_buffers_r[i].size());
    // }

    fmt::println("Dispatch edges in {:.5f}s", sort_time);
    fmt::println("Throughput: {:.2f} MEPS", n / 1e6 / sort_time);
    fflush(stdout);
    exit(0);
    return 0;
}