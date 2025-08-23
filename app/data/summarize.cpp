#include <bits/stdc++.h>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "cxxopts.hpp"

using namespace std;
namespace fs = std::filesystem;

string dataset_path;
string output_path;
size_t max_vertex_count = 1ull << 31;
size_t batch_size = 10 * 1024 * 1024;

const size_t max_cnt_details = 1024;
uint64_t degree_cnt[max_cnt_details];
uint64_t degree_cnt_log[64];
double degree_percent[max_cnt_details];

template<typename T>
auto hist(T* data, size_t size) {
    auto details = make_unique<T[]>(max_cnt_details);
    auto log = make_unique<T[]>(64);
    for(size_t i = 0; i < size; i++) {
        if(data[i] < max_cnt_details) {
            details[data[i]]++;
        }
        log[bit_width(data[i])]++;
    }

    auto percent = make_unique<double[]>(max_cnt_details);
    for(size_t i = 0; i < max_cnt_details; i++) {
        percent[i] = 1.0 * details[i] / size;
    }
    return make_tuple(move(details), move(log), move(percent));
}

template<typename T>
void print_hist(const string& dataset_path) {
    auto buffer = make_unique<T[]>(batch_size);
    auto out_degree = make_unique<size_t[]>(max_vertex_count);
    auto in_degree = make_unique<size_t[]>(max_vertex_count);
    
    FILE* f = fopen(dataset_path.c_str(), "rb");
    if(f == nullptr) {
        fmt::print("Failed to open {}\n", dataset_path);
        exit(1);
    }

    auto file_size = fs::file_size(dataset_path);
    auto blocks = file_size / sizeof(T) / batch_size;

    size_t read = 0;
    T maxv = 0;
    size_t b = 0;
    while((read = fread(buffer.get(), sizeof(T), batch_size, f)) > 0) {
        for(size_t i = 0; i < read; i+=2) {
            out_degree[buffer[i]]++;
            in_degree[buffer[i+1]]++;
            maxv = std::max(maxv, buffer[i]);
        }
        b++;
        fmt::println("Read block {}/{}", b, blocks);
    }

    auto [out_degree_details, out_degree_log, out_degree_percent] = hist(out_degree.get(), maxv + 1);
    auto [in_degree_details, in_degree_log, in_degree_percent] = hist(in_degree.get(), maxv + 1);
    for(size_t i = 0; i < maxv + 1; i++) {
        out_degree[i] = out_degree[i] + in_degree[i];
    }
    auto [all_degree_details, all_degree_log, all_degree_percent] = hist(out_degree.get(), maxv + 1);

    fmt::println("Out degree details: {}", fmt::join(out_degree_details.get(), out_degree_details.get() + max_cnt_details, ", "));
    fmt::println("Out degree log: {}", fmt::join(out_degree_log.get(), out_degree_log.get() + 64, ", "));
    fmt::println("Out degree percent: {:.2g}", fmt::join(out_degree_percent.get(), out_degree_percent.get() + max_cnt_details, ", "));

    fmt::println("In degree details: {}", fmt::join(in_degree_details.get(), in_degree_details.get() + max_cnt_details, ", "));
    fmt::println("In degree log: {}", fmt::join(in_degree_log.get(), in_degree_log.get() + 64, ", "));
    fmt::println("In degree percent: {:.2g}", fmt::join(in_degree_percent.get(), in_degree_percent.get() + max_cnt_details, ", "));

    fmt::println("All degree details: {}", fmt::join(all_degree_details.get(), all_degree_details.get() + max_cnt_details, ", "));
    fmt::println("All degree log: {}", fmt::join(all_degree_log.get(), all_degree_log.get() + 64, ", "));
    fmt::println("All degree percent: {:.2g}", fmt::join(all_degree_percent.get(), all_degree_percent.get() + max_cnt_details, ", "));
}

int main(int argc, char** argv) {
    cxxopts::Options options("summarize", "Summarize the distribution of a dataset");
    options.add_options()
        ("i,input", "Input file path", cxxopts::value<string>())
        ("o,output", "Output file path", cxxopts::value<string>())
        ("s,short", "Use 32bit integer as vertex ID")
        ("h,help", "Print usage");
    auto result = options.parse(argc, argv);
    if(result.count("help") || !result.count("input")) {
        cout << options.help() << endl;
        return 0;
    }

    dataset_path = result["input"].as<string>();
    if(result.count("output")) {
        output_path = result["output"].as<string>();
    } else {
        output_path = fs::path("./data") / (fs::path(dataset_path).stem().string() + ".dis");
    }

    fmt::print("Input: {}\n", dataset_path);
    fmt::print("Output: {}\n", output_path);


    if(result.count("short")) {
        print_hist<uint32_t>(dataset_path);
    } else {
        print_hist<uint64_t>(dataset_path);
    }


    return 0;
}