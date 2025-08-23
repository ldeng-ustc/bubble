#include <bits/stdc++.h>

using namespace std;

const char* PATH = "./data/Kron30-24/block-00.bin";
const size_t V = 1 << 30;
const size_t BATCH = 10 * 1024 * 1024;

int main() {
    // 打开 ./data/Kron30-24/block-00.bin ，每次读取 10M 个 uint64_t，每两个数一组，统计第一个数出现次数
    FILE* f = fopen("./data/Kron30-24/block-00.bin", "rb");
    auto buffer = make_unique<uint64_t[]>(BATCH);
    auto cnt = make_unique<size_t[]>(V);

    size_t read = 0;
    uint64_t maxv = 0;
    size_t b = 0;
    while((read = fread(buffer.get(), sizeof(uint64_t), BATCH, f)) > 0) {
        for(size_t i = 0; i < read; i+=2) {
            cnt[buffer[i]]++;
            maxv = std::max(maxv, buffer[i]);
        }
        b++;
        printf("Read block %zu\n", b);
    }

    // 将统计结果写入 ./data/dis_kron30-24.bin

    FILE* f2 = fopen("./data/dis_kron30-24.bin", "wb");
    fwrite(cnt.get(), sizeof(size_t), maxv + 1, f2);
    return 0;
}