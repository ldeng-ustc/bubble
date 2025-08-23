#include <cstdio>
#include <string>
#include <fstream>
#include <filesystem>
#include <iostream>

#include "datatype.h"
#include "importer.h"
#include "cxxopts.hpp"
#include "fmt/format.h"

using namespace dcsr;
namespace fs = std::filesystem;
using std::string;
using std::ifstream;
using std::ofstream;

const string PROG_NAME = "convert_to_binary";

void convert(const string& inname, const string& outname) {
    ofstream fout(outname, std::ofstream::binary);
    ScanLargeFile<RawEdge64<void>, 8*1024*1024>(fs::path(inname), [&](const RawEdge64<void>& edge) {
        uint32_t data[2];
        data[0] = static_cast<uint32_t>(edge.from);
        data[1] = static_cast<uint32_t>(edge.to);
        fout.write(reinterpret_cast<char*>(data), sizeof(data));
    });
}

int main(int argc, char** argv) {
    cxxopts::Options options(PROG_NAME, "Convert graph dataset in text format to binary.");
    options.add_options()
        ("f,file", "Input file name", cxxopts::value<string>())
        ("o,out", "Output file name", cxxopts::value<string>())
        ;
    auto opt = options.parse(argc, argv);

    if(!opt.count("file") || !opt.count("out")) {
        std::cout << options.help() << std::endl;
        exit(0);
    }

    string filename = opt["file"].as<string>();
    string outname = opt["out"].as<string>();

    auto outpath = fs::path(outname);
    auto outdir = outpath.remove_filename();
    fs::create_directories(outpath);
    
    convert(filename, outname);

    return 0;
}

