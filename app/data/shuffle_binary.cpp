#include <cstdio>
#include <string>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <utility>

#include "cxxopts.hpp"
#include "fmt/format.h"

#include "importer.h"
#include "common.h"

namespace fs = std::filesystem;
using std::string;
using std::ifstream;
using std::ofstream;

const string PROG_NAME = "shuffle_binary";

/**
 * @brief Shuffle the binary edge list file, with 64 bits VID (16 bytes per edge).
 *        For large file, scan file n runs, each run shuffle 100GB edges. (with same seed.) 

 * @param in input file
 * @param out output file
 * @param buffer_size buffer size for each run (larger is better)
 */
template<typename VID>
void shuffle(fs::path in, fs::path out, size_t buffer_size_gb=100) {
    // using TmpEdge = std::pair<uint64_t, uint64_t>;
    using TmpEdge = std::pair<VID, VID>;
    size_t buffer_size = buffer_size_gb * 1024 * 1024 * 1024;
    size_t runs = dcsr::div_up(fs::file_size(in), buffer_size);
    size_t avg_item_count = buffer_size / sizeof(TmpEdge);
    std::vector<TmpEdge> buffer;
    buffer.reserve(avg_item_count + 10*1024*1024);  // larger than 10 stddev for 1000B edges, grantee no reallocation

    FILE* out_fp = fopen(out.c_str(), "wb");

    std::mt19937_64 shuffle_engine(0);

    for(size_t i=0u; i<runs; i++) {
        dcsr::set_random_seed(0);
        
        buffer.clear();
        dcsr::ScanLargeFile<TmpEdge, 8*1024*1024>(in, [&](TmpEdge e) {
            uint64_t r = dcsr::random_int<uint64_t>(0u, runs);
            if(r == i) {
                buffer.push_back(e);
            }
        });
        fmt::println("Run {} read {} edges", i, buffer.size());
        auto t = TimeIt([&] {
            std::shuffle(buffer.begin(), buffer.end(), shuffle_engine);
        });
        fmt::println("Shuffled {}th run, time: {:.2f}s", i, t);

        fwrite(buffer.data(), sizeof(TmpEdge), buffer.size(), out_fp);
    }

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
        shuffle<uint32_t>(filename, outname);
    } else {
        shuffle<uint64_t>(filename, outname);
    }

    return 0;
}

