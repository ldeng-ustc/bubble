#include <cstdio>
#include <string>
#include <fstream>
#include <filesystem>
#include <iostream>

#include "datatype.h"
#include "cxxopts.hpp"
#include "fmt/format.h"

namespace fs = std::filesystem;
using std::string;
using std::ifstream;
using std::ofstream;

const string PROG_NAME = "convert_to_binary";

template<typename VTYPE>
void convert(const string& inname, const string& outname, bool skip_rest, size_t skip_header) {
    ifstream f(inname, std::ifstream::in);
    FILE *fout = fopen(outname.c_str(), "wb");
    VTYPE data[2];
    char buffer[1024];
    size_t count = 0;

    if(skip_header > 0) {
        for(size_t i = 0; i < skip_header; i++) {
            f.getline(buffer, 1024);
            fmt::print("Skipped header: \"{}\"\n", buffer);
        }
    }

    while (f.good()) {
        if(!(f >> data[0] >> data[1])) {
            f.clear();
            fmt::println("Wrong format, skip line.");
            f.getline(buffer, 1024);
            fmt::print("Skipped content: \"{}\"\n", buffer);
            continue;
        }
        fwrite(data, sizeof(VTYPE), 2, fout);
        count++;

        if(skip_rest) {
            f.getline(buffer, 1024);
        }
    }
    fmt::print("Total {} edges written.\n", count);
}

int main(int argc, char** argv) {
    cxxopts::Options options(PROG_NAME, "Convert graph dataset in text format to binary.");
    options.add_options()
        ("f,file", "Input file name", cxxopts::value<string>())
        ("o,out", "Output file name", cxxopts::value<string>())
        ("short", "Use VID32 as vertex ID")
        ("skip-rest", "Skip the rest of the line, useful for skipping comments or weights")
        ("skip-header", "Skip the lines before the first edge", cxxopts::value<size_t>()->default_value("0"))
        ;
    auto opt = options.parse(argc, argv);

    if(!opt.count("file") || !opt.count("out")) {
        std::cout << options.help() << std::endl;
        exit(0);
    }

    string filename = opt["file"].as<string>();
    string outname = opt["out"].as<string>();
    bool skip_rest = opt["skip-rest"].as<bool>();
    size_t skip_header = opt["skip-header"].as<size_t>();

    auto outpath = fs::path(outname);
    auto outdir = outpath.remove_filename();
    fs::create_directories(outpath);
    
    using VID32 = uint32_t;
    using VID64 = uint64_t;
    if(opt["short"].as<bool>()) {
        convert<VID32>(filename, outname, skip_rest, skip_header);
    } else {
        convert<VID64>(filename, outname, skip_rest, skip_header);
    }

    return 0;
}

