#include <cstdio>
#include <string>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <utility>

#include "cxxopts.hpp"
#include "fmt/format.h"

#include "third_party/pdqsort.h"
#include "importer.h"
#include "common.h"

namespace fs = std::filesystem;
using std::string;
using std::ifstream;
using std::ofstream;

const string PROG_NAME = "shuffle_binary";

/**
 * @brief 

 * @param in input file
 * @param out output file
 * @param buffer_size buffer size for each run (larger is better)
 */
template<typename VID>
void make_undireted(fs::path in, fs::path out, bool shuffle=true) {
    using TmpEdge = std::pair<VID, VID>;

    size_t edge_count = fs::file_size(in) / sizeof(TmpEdge);
    TmpEdge* edges = new TmpEdge[edge_count];

    ifstream in_fp(in, std::ios::binary);
    in_fp.read(reinterpret_cast<char*>(edges), edge_count * sizeof(TmpEdge));
    in_fp.close();
    fmt::println("Read {} edges", edge_count);

    // direct edges & sort
    size_t swap_count = 0;
    for(size_t i=0; i<edge_count; i++) {
        if(edges[i].first > edges[i].second) {
            std::swap(edges[i].first, edges[i].second);
            swap_count++;
        }
    }
    fmt::println("Swapped {} edges", swap_count);
    pdqsort(edges, edges + edge_count, [](const TmpEdge& a, const TmpEdge& b) {
        return a.first < b.first || (a.first == b.first && a.second < b.second);
    });
    fmt::println("Sorted");

    // remove duplicate & self-loop
    size_t j = 0;
    for(size_t i=1; i<edge_count; i++) {
        if(edges[i] != edges[j] && edges[i].first != edges[i].second) {
            j++;
            edges[j] = edges[i];
        }
    }
    size_t new_edge_count = j + 1;
    TmpEdge* last = edges + new_edge_count;
    fmt::println("Removed {} self-loop edges", edge_count - new_edge_count);

    // shuffle
    if(shuffle) {
        std::mt19937_64 shuffle_engine(0);
        std::shuffle(edges, last, shuffle_engine);
    }
    fmt::println("Shuffled");

    ofstream out_fp(out, std::ios::binary);
    out_fp.write(reinterpret_cast<char*>(edges), new_edge_count * sizeof(TmpEdge));
    out_fp.close();
}


int main(int argc, char** argv) {
    cxxopts::Options options(PROG_NAME, "Convert graph dataset in text format to binary.");
    options.add_options()
        ("f,file", "Input file name", cxxopts::value<string>())
        ("o,out", "Output file name", cxxopts::value<string>())
        ("short", "Use VID32 as vertex ID")
        ;
    auto opt = options.parse(argc, argv);

    if(!opt.count("file") || !opt.count("out")) {
        std::cout << options.help() << std::endl;
        exit(0);
    }

    string filename = opt["file"].as<string>();
    string outname = opt["out"].as<string>();
    bool use_vid32 = opt.count("short");

    auto outpath = fs::path(outname);
    auto outdir = outpath.remove_filename();
    fs::create_directories(outdir);

    if(use_vid32) {
        make_undireted<uint32_t>(filename, outname);
    } else {
        make_undireted<uint64_t>(filename, outname);
    }

    return 0;
}

