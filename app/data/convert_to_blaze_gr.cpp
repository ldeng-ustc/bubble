/*
 * @Author: Long Deng
 * @Date: 2024-01-07
 * @LastEditors: Long Deng
 * @Description: convert binary edge list to blaze graph format (gr.index and gr.adj).
 */
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <string>
#include <fstream>
#include <filesystem>
#include <numeric>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "cxxopts.hpp"
#include "fmt/format.h"

using namespace std;
namespace fs = std::filesystem;


const string PROG_NAME = "convert_to_binary";

template<typename Func>
void iterate_over_edges(const uint64_t *buf, size_t total_edges, bool weighted, Func func) {
    auto st = chrono::steady_clock::now();
    auto last = st;
    const uint64_t *ptr = buf;
    for(size_t i = 0; i < total_edges; i++) {
        uint64_t u = *ptr++;
        uint64_t v = *ptr++;
        uint64_t w = weighted ? *ptr++ : 0;
        func(u, v, w);
        if(i % (1024 * 1024) == 0) {
            auto tmp = chrono::steady_clock::now();
            auto duration = chrono::duration<double>(tmp - st).count();
            if(tmp - last > chrono::seconds(1)) {
                last = tmp;
                auto cnt_m = i / 1024.0 / 1024.0;
                auto total_m = total_edges / 1024.0 / 1024.0;
                auto speed = cnt_m / duration;
                fmt::println("Edges: {:.2f}M / {:.2f}M , speed: {:.2f}M edges/s", cnt_m, total_m, speed);
            }
        }
    }
    auto tmp = chrono::steady_clock::now();
    auto duration = chrono::duration<double>(tmp - st).count();
    fmt::println("File scan speed: {:.2f} M edges/s", total_edges / duration / 1024 / 1024);
}


uint64_t align_upto(size_t size, size_t align) {
    return (size + align -1u) &~ (align - 1u);
}

char *create_and_mmap(const string& outname, size_t size) {
    int fd = open(outname.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if(fd < 0) {
        fmt::println("Cannot open file: {}", outname);
        exit(1);
    }
    int ret = ftruncate(fd, size);
    if(ret < 0) {
        fmt::println("Cannot truncate file: {}", outname);
        exit(1);
    }
    char* base = (char*)mmap(NULL, size, PROT_WRITE, MAP_SHARED, fd, 0);
    if(base == MAP_FAILED) {
        fmt::println("Cannot mmap file: {}", outname);
        exit(1);
    }
    ret = close(fd);
    if(ret < 0) {
        fmt::println("Cannot close file: {}", outname);
        exit(1);
    }
    return base;
}


void write_index_file(const string& outname, const vector<uint64_t>& edge_count, size_t total_edges) {
    const size_t CACHE_LINE = 64;
    size_t num_nodes = edge_count.size();
    
    size_t num_offsets = ((num_nodes - 1) / 16) + 1;
    size_t len_header =  (num_offsets + 4 /*header size*/) * sizeof(uint64_t);
    size_t len_header_aligned = align_upto(len_header, CACHE_LINE);
    size_t new_len = len_header_aligned + num_offsets * CACHE_LINE;

    fmt::println("# nodes: {}", num_nodes);
    fmt::println("[compact]");
    fmt::println("  header size : {}", len_header_aligned);
    fmt::println("    header size  : {}", sizeof(uint64_t) * 4);
    fmt::println("    offset size  : {}", num_offsets * sizeof(uint64_t));
    fmt::println("    before align : {}", len_header);
    fmt::println("+ degree size : {}", num_offsets * CACHE_LINE);
    fmt::println("= index size  : {}", new_len);

    char* new_base = create_and_mmap(outname, new_len);


    const auto& index = edge_count;
    uint64_t offset;

    uint64_t *np = (uint64_t *)new_base;
    *np++ = 0;
    *np++ = 0;
    *np++ = num_nodes;
    *np++ = total_edges;

    uint64_t *new_index = (uint64_t *)np;
    uint32_t *degrees = (uint32_t*)(new_base + len_header_aligned);

    for (uint64_t node = 0; node < num_nodes; node++) {
        if (node == 0) {
            offset = 0;
        }
        auto degree = index[node];

        if (node % 16 == 0) {
            new_index[node / 16] = offset;
        }
        degrees[node] = degree;
        offset += degree;
        // fmt::println("node: {}, degree: {}, offset: {}", node, degree, offset);
    }

    msync(new_base, new_len, MS_SYNC);
    munmap(new_base, new_len);

}


size_t n_threads = 1;

void parallel_set_dst(const uint64_t* buffer, size_t total_edges, uint32_t* dst, size_t* offset, bool weighted) {
    std::vector<std::thread> threads;
    size_t chunk_size = total_edges / n_threads;
    chunk_size = align_upto(chunk_size, 4096);
    fmt::println("n_threads: {}", n_threads);
    fmt::println("chunk_size: {}", chunk_size);

    for(size_t i = 0; i < n_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i + 1) * chunk_size;
        end = std::min(end, total_edges);
        if(start >= total_edges) {
            break;
        }

        threads.emplace_back([=]() {
            iterate_over_edges(buffer, total_edges, weighted, [&](uint64_t u, uint64_t v, uint64_t w) {
                (void)w;
                if(start <= u && u < end) {
                    dst[offset[u]++] = v;
                }
            });
        });
    }

    for(auto& t : threads) {
        t.join();
    }

}

void write_adj_file(const string& outname, const uint64_t *buffer, const vector<uint64_t>& edge_count, size_t total_edges, bool weighted) {
    const size_t PAGE_SIZE = 4096;
    size_t num_nodes = edge_count.size();
    size_t dst_size = total_edges * sizeof(uint32_t);
    size_t align_size = align_upto(dst_size, PAGE_SIZE);

    fmt::println("dst_size: {}, align_size: {}", dst_size, align_size);
    
    auto offset = make_unique_for_overwrite<size_t[]>(num_nodes + 1);
    std::partial_sum(edge_count.begin(), edge_count.end(), offset.get() + 1);
    offset[0] = 0;

    char* base = create_and_mmap(outname, align_size);
    uint32_t* dst = (uint32_t*)base;
    // uint64_t* ptr = (uint64_t*)buffer;

    parallel_set_dst(buffer, total_edges, dst, offset.get(), weighted);


    // // test code
    // auto dst2 = make_unique_for_overwrite<uint32_t[]>(align_size / sizeof(uint32_t));
    // auto offset2 = make_unique_for_overwrite<size_t[]>(num_nodes + 1);
    // std::partial_sum(edge_count.begin(), edge_count.end(), offset2.get() + 1);
    // offset2[0] = 0;
    // iterate_over_edges(buffer, total_edges, weighted, [&](uint64_t u, uint64_t v, uint64_t w) {
    //     (void)w;
    //     dst2[offset2[u]++] = v;
    // });
    // // compare
    // for(size_t i = 0; i < num_nodes + 1; i++) {
    //     if(offset[i] != offset2[i]) {
    //         fmt::println("offset[{}] = {}, offset2[{}] = {}", i, offset[i], i, offset2[i]);
    //     }
    // }
    // for(size_t i = 0; i < total_edges; i++) {
    //     if(dst[i] != dst2[i]) {
    //         fmt::println("dst[{}] = {}, dst2[{}] = {}", i, dst[i], i, dst2[i]);
    //     }
    // }

    fmt::println("dst_size: {}, align_size: {}", dst_size, align_size);

    memset(base + dst_size, 0xFF, align_size - dst_size);
    msync(base, align_size, MS_SYNC);
    munmap(base, align_size);
}

void convert(const string& inname, const string& outname, bool weighted) {
    const size_t N = 1024 * 1024 * 1024;
    vector<uint64_t> edge_count;
    edge_count.reserve(N);

    int fd = open(inname.c_str(), O_RDONLY);
    if(fd < 0) {
        fmt::println("Cannot open file: {}", inname);
        exit(1);
    }

    size_t edge_size = weighted ? sizeof(uint64_t) * 3 : sizeof(uint64_t) * 2;
    size_t file_size = fs::file_size(inname);
    size_t total_edges = file_size / edge_size;

    uint64_t *buf = (uint64_t*)mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if(buf == MAP_FAILED) {
        fmt::println("Cannot mmap file: {}", inname);
        exit(1);
    }

    iterate_over_edges(buf, total_edges, weighted, [&](uint64_t u, uint64_t v, uint64_t w) {
        (void)w;
        (void)v;
        if(u >= edge_count.size()) {
            edge_count.resize(u + 1);
        }
        edge_count[u]++;
    });

    const string index_name = outname + ".index";
    const string adj_name = outname + ".adj.0";
    write_index_file(index_name, edge_count, total_edges);
    write_adj_file(adj_name, buf, edge_count, total_edges, weighted);
}

int main(int argc, char** argv) {
    cxxopts::Options options(PROG_NAME, "Convert graph dataset in text format to binary.");
    options.add_options()
        ("f,file", "Input file name", cxxopts::value<string>())
        ("o,out", "Output file name", cxxopts::value<string>())
        ("weighted", "Weighted Graph", cxxopts::value<bool>()->default_value("false"))
        ("t,threads", "Number of threads", cxxopts::value<size_t>()->default_value("1"))
        ("h,help", "Print help")
        ;
    auto opt = options.parse(argc, argv);

    if(!opt.count("file") || !opt.count("out")) {
        std::cout << options.help() << std::endl;
        exit(0);
    }

    string filename = opt["file"].as<string>();
    string outname = opt["out"].as<string>();
    n_threads = opt["threads"].as<size_t>();

    auto outpath = fs::path(outname);
    auto outdir = outpath.remove_filename();
    fs::create_directories(outpath);

    bool weighted = opt.count("weighted");
    convert(filename, outname, weighted);

    return 0;
}

