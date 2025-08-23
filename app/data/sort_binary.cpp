#include <cstdio>
#include <string>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <utility>
#include <queue>

#include "cxxopts.hpp"
#include "fmt/format.h"

#include "importer.h"
#include "common.h"
#include "third_party/pdqsort.h"

namespace fs = std::filesystem;
using std::string;
using std::ifstream;
using std::ofstream;

const string PROG_NAME = "sort_binary";

/**
 * @brief Sort the binary edge list file, with VID as vertex ID type.
 *        Edges are sorted first by source vertex, then by target vertex.

 * @param in input file
 * @param out output file
 * @param buffer_size_gb buffer size for each run in GB (larger is better)
 * @param output_txt whether to output as text format (each edge per line, space separated)
 */
template<typename VID>
void sort_edges(fs::path in, fs::path out, bool output_txt=false, size_t buffer_size_gb=100) {
    // using TmpEdge = std::pair<uint64_t, uint64_t>;
    using TmpEdge = std::pair<VID, VID>;
    size_t file_size = fs::file_size(in);
    size_t total_edges = file_size / sizeof(TmpEdge);
    
    // 将 buffer_size_gb 从 GB 转换为字节
    size_t buffer_size = static_cast<size_t>(buffer_size_gb) * 1024 * 1024 * 1024;
    
    fmt::println("File size: {:.2f} GB, Buffer size: {:.2f} GB", 
                file_size / (1024.0 * 1024 * 1024), buffer_size / (1024.0 * 1024 * 1024));
    
    // If file is small enough, sort in memory directly
    if (file_size > buffer_size) {
        fmt::println("File is too large, failed to sort.");
        return;
    }

    std::vector<TmpEdge> all_edges;
    all_edges.reserve(total_edges);
    
    dcsr::ScanLargeFile<TmpEdge, 8*1024*1024>(in, [&](TmpEdge e) {
        all_edges.push_back(e);
    });
    
    fmt::println("Sorting {} edges in memory", all_edges.size());
    auto t = TimeIt([&] {
        pdqsort_branchless(
            all_edges.begin(), all_edges.end(),
            [](const TmpEdge& a, const TmpEdge& b) {
                return (a.first < b.first) | ((a.first == b.first) & (a.second < b.second));
            }
        );
    });
    fmt::println("Sorted in {:.2f}s", t);
    
    double write_time = 0;
    if (output_txt) {
        std::ofstream out_file(out);
        if (!out_file) {
            fmt::println(stderr, "Error: Cannot open output file {}", out.string());
            return;
        }
        
        t = TimeIt([&] {
            for (const auto& edge : all_edges) {
                out_file << edge.first << " " << edge.second << "\n";
            }
        });
        write_time = t;
        out_file.close();
    } else {
        FILE* out_fp = fopen(out.c_str(), "wb");
        if (!out_fp) {
            fmt::println(stderr, "Error: Cannot open output file {}", out.string());
            return;
        }
        t = TimeIt([&] {
            fwrite(all_edges.data(), sizeof(TmpEdge), all_edges.size(), out_fp);
        });
        write_time = t;
        fclose(out_fp);
    }
    
    fmt::println("Edges written to {} in {:.2f}s", out.string(), write_time);
    return;
}


int main(int argc, char** argv) {
    cxxopts::Options options(PROG_NAME, "Sort graph dataset in binary format by source and target vertices.");
    options.add_options()
        ("f,file", "Input file name", cxxopts::value<string>())
        ("o,out", "Output file name", cxxopts::value<string>())
        ("short", "Use VID32 as vertex ID")
        ("txt", "Output as text format (each edge per line, space separated)")
        ;
    auto opt = options.parse(argc, argv);

    if(!opt.count("file") || !opt.count("out")) {
        std::cout << options.help() << std::endl;
        exit(0);
    }

    string filename = opt["file"].as<string>();
    string outname = opt["out"].as<string>();
    bool use_vid32 = opt.count("short");
    bool output_txt = opt.count("txt");

    auto outpath = fs::path(outname);
    auto outdir = outpath.remove_filename();
    fs::create_directories(outdir);

    if(use_vid32) {
        sort_edges<uint32_t>(filename, outname, output_txt);
    } else {
        sort_edges<uint64_t>(filename, outname, output_txt);
    }

    return 0;
}

