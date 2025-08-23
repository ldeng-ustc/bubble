#include <bits/stdc++.h>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "cxxopts.hpp"

using namespace std;
namespace fs = std::filesystem;

string dataset_path;
size_t max_vertex_count = 1ull << 31;
size_t batch_size = 10 * 1024 * 1024;

template<typename T>
void print_hist(const string& dataset_path) {
    auto buffer = make_unique<T[]>(batch_size);
    auto out_degree = make_unique<size_t[]>(max_vertex_count);
    
    FILE* f = fopen(dataset_path.c_str(), "rb");
    if (f == nullptr) {
        fmt::print("Failed to open {}\n", dataset_path);
        exit(1);
    }

    auto file_size = fs::file_size(dataset_path);
    auto blocks = file_size / sizeof(T) / batch_size;

    size_t read = 0;
    T maxv = 0;
    size_t b = 0;
    while ((read = fread(buffer.get(), sizeof(T), batch_size, f)) > 0) {
        for (size_t i = 0; i < read; i += 2) {
            out_degree[buffer[i]]++;
            maxv = std::max(maxv, buffer[i]);
        }
        b++;
        fmt::println("Read block {}/{}", b, blocks);
    }
    fclose(f);

    // Edges count ratio of top 1% vertices
    size_t top_1_percent = static_cast<size_t>(maxv * 0.01);
    size_t top_1_percent_edges = 0;
    size_t total = std::accumulate(out_degree.get(), out_degree.get() + maxv + 1, 0ull);

    std::sort(out_degree.get(), out_degree.get() + maxv + 1, std::greater<size_t>());
    for (size_t i = 0; i < top_1_percent; i++) {
        top_1_percent_edges += out_degree[i];
    }

    size_t top_10_percent = static_cast<size_t>(maxv * 0.1);
    size_t top_10_percent_edges = 0;
    for (size_t i = 0; i < top_10_percent; i++) {
        top_10_percent_edges += out_degree[i];
    }


    fmt::println("Top 1% vertices count: {}", top_1_percent);
    fmt::println("Top 1% edges count: {}", top_1_percent_edges);
    fmt::println("Top 1% edges ratio: {:.2f}%", static_cast<double>(top_1_percent_edges) / total * 100);

    fmt::println("Top 10% vertices count: {}", top_10_percent);
    fmt::println("Top 10% edges count: {}", top_10_percent_edges);
    fmt::println("Top 10% edges ratio: {:.2f}%", static_cast<double>(top_10_percent_edges) / total * 100);

}

int main(int argc, char** argv) {
    cxxopts::Options options("summarize", "Summarize the distribution of a dataset");
    options.add_options()
        ("i,input", "Input file path", cxxopts::value<string>())
        ("h,help", "Print usage");
    auto result = options.parse(argc, argv);
    if (result.count("help") || !result.count("input")) {
        cout << options.help() << endl;
        return 0;
    }

    dataset_path = result["input"].as<string>();

    fmt::print("Input: {}\n", dataset_path);
    print_hist<uint32_t>(dataset_path);
    return 0;
}