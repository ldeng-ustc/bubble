#include "common.h"
#include "vec.h"
#include "graph.h"
#include "importer.h"

using namespace dcsr;
using namespace std;

int main() {

    std::unique_ptr<uint64_t[]> data = nullptr;

    size_t V = 1 << 30;
    size_t E = 8 * V;

    auto t = TimeIt([&] {
        data = make_unique_with_random<uint64_t>(E*2, 0, V, 0);
    });
    fmt::println("Data generation time: {:.2f}s", t);

    FILE* f = fopen("./dataset/random_edge_list.bin", "wb");

    size_t Block = 1 << 24;
    size_t blocks = E * 2 / Block;
    for(size_t i = 0; i < blocks; i++) {
        fwrite(data.get() + i * Block, sizeof(uint64_t), Block, f);
        fmt::println("Write block {}/{}", i, blocks);
    }

    return 0;
}