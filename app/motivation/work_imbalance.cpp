#include <bits/stdc++.h>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "cxxopts.hpp"

using namespace std;
namespace fs = std::filesystem;

std::vector<size_t> simulate(std::span<size_t> partitions, size_t threads) {
    using PQE = std::pair<size_t, size_t>;
    std::priority_queue<PQE, std::vector<PQE>, std::greater<PQE>> pq;
    for (size_t i = 0; i < threads; i++) {
        pq.push({0, i});
    }

    for(size_t psize: partitions) {
        auto [cur, id] = pq.top();
        pq.pop();
        cur += psize;
        pq.push({cur, id});
    }

    std::vector<size_t> result(threads);
    while (!pq.empty()) {
        auto [cur, id] = pq.top();
        pq.pop();
        result[id] = cur;
    }

    return result;
}

std::pair<double,double> imbalance_ratio(std::span<size_t> partitions, size_t threads) {
    auto result = simulate(partitions, threads);
    size_t maxe = *std::max_element(result.begin(), result.end());
    double avge = std::accumulate(result.begin(), result.end(), 0.0) / threads;
    double ratio = static_cast<double>(maxe) / avge;

    double utilization = 0;
    for(size_t i = 0; i < threads; i++) {
        utilization += static_cast<double>(result[i]) / maxe;
    }
    utilization /= threads;
    return {ratio, utilization};
}

using Vertex = uint32_t;
using Edge = std::pair<Vertex, Vertex>;

auto read_dataset(const string& dataset_path) {
    auto file_size = fs::file_size(dataset_path);
    size_t edge_count = file_size / sizeof(Edge);

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

std::pair<double,double> hist(std::span<Edge> edges, size_t vcount, size_t batch, size_t threads, size_t partitions) {
    std::vector<size_t> out_degree(partitions, 0);
    std::vector<size_t> result(threads, 0);

    size_t pwidth = (vcount + partitions - 1) / partitions;

    double ratio_sum = 0;
    double utilization_sum = 0;
    size_t cnt = 0;
    for(size_t l = 0; l < edges.size(); l += batch) {
        std::fill(out_degree.begin(), out_degree.end(), 0);
        size_t r = std::min(l + batch, edges.size());
        for(size_t i = l; i < r; i++) {
            auto [from, to] = edges[i];
            size_t partition_id = from / pwidth;
            out_degree[partition_id]++;
        }
        auto [ratio, utilization] = imbalance_ratio(out_degree, threads);
        ratio_sum += ratio;
        utilization_sum += utilization;
        cnt++;
    }
    double avg_ratio = ratio_sum / cnt;
    double utilization = utilization_sum / cnt;
    return {avg_ratio, utilization};
}

int main(int argc, char** argv) {
    cxxopts::Options options("summarize", "Summarize the distribution of a dataset");
    options.add_options()
        ("i,input", "Input file path", cxxopts::value<string>())
        ("v", "Vertex count", cxxopts::value<size_t>())
        ("p,partitions", "Number of partitions", cxxopts::value<vector<size_t>>())
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

    std::vector<size_t> partitions;
    partitions = result["partitions"].as<vector<size_t>>();

    auto [edges, n] = read_dataset(dataset_path);
    auto espan = std::span<Edge>(edges.get(), n);
    size_t vcount = result["v"].as<size_t>();


    std::vector<double> res_ratio;
    std::vector<double> res_utilization;
    for(size_t p: partitions) {
        auto [ratio, u] = hist(espan, vcount, batch, threads, p);
        res_ratio.push_back(ratio);
        res_utilization.push_back(u);
    }

    fmt::println("[EXPOUT] partitions: [{}]", fmt::join(partitions, ","));
    fmt::println("[EXPOUT] imbalance_ratio: [{}]", fmt::join(res_ratio, ","));
    fmt::println("[EXPOUT] utilization: [{}]", fmt::join(res_utilization, ","));

    return 0;
}