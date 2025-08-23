#include <bits/stdc++.h>
#include "fmt/format.h"
#include "fmt/ranges.h"
using namespace std;

const auto PATH = "../data/dis_kron30-24.bin";
const size_t V = 1 << 30;

const uint64_t CNT_DETAILS = 1024;
uint64_t cnt[CNT_DETAILS];
uint64_t cnt_log[bit_width(V) + 1];
double cnt_percent[CNT_DETAILS];

int main() {
    auto arr = make_unique<size_t[]>(V);
    FILE* f = fopen(PATH, "rb");
    int ret = fread(arr.get(), sizeof(uint64_t), V, f);
    if(ret != V) {
        fmt::print("Read {} elements, expected {}\n", ret, V);
        return 1;
    }
    fclose(f);

    memset(cnt, 0, sizeof(cnt));
    for(size_t i = 0; i < V; i++) {
        if(arr[i] < CNT_DETAILS) {
            cnt[arr[i]]++;
        } else {
            cnt_log[bit_width(arr[i])]++;
        }
    }

    for(size_t i = 0; i < CNT_DETAILS; i++) {
        cnt_percent[i] = 1.0 * cnt[i] / V;
    }

    fmt::println("cnt: {}", cnt);
    fmt::println("cnt_log: {}", cnt_log);
    fmt::println("cnt_percent: {:.2f}", fmt::join(cnt_percent, cnt_percent + CNT_DETAILS, ", "));
    return 0;
}