/**
 * @file sort.h
 * @author Long Deng (ldeng@mail.ustc.edu.cn)
 * @brief Custom sort algorithms
 * @version 0.1
 * @date 2024-10-25
 * 
 * @copyright Copyright (c) 2024
 * 
 */
#ifndef __DCSR_SORT_H__
#define __DCSR_SORT_H__

#include <span>
#include <queue>
#include <datatype.h>
#include <env.h>
#include "third_party/pdqsort.h"
#include "third_party/timsort.hpp"

namespace dcsr {

template<typename EdgeType>
    requires requires(EdgeType e) { e.from; }
class BucketIdGetter {
private:
    using VertexType = decltype(EdgeType::from);
    uint64_t vstart_;
    size_t shift_bits_;

public:
    BucketIdGetter(size_t bucket_count, uint64_t vstart, uint64_t vcount) {
        dcsr_assert(std::has_single_bit(bucket_count), "bucket_count should be power of 2");
        // dcsr_assert(std::has_single_bit(vcount), "vcount should be power of 2");
        // dcsr_assert(vcount >= bucket_count, "vcount should be larger than bucket_count");
        size_t bucket_bits = std::bit_width(bucket_count - 1);
        size_t vbits = std::bit_width(std::bit_ceil(vcount) - 1);
        shift_bits_ = (vbits > bucket_bits ? vbits - bucket_bits : 0);
        vstart_ = vstart;
        //fmt::println("BucketIdGetter: vstart: {}, vcount: {}, bucket_count: {}, shift_bits: {}", vstart, vcount, bucket_count, shift_bits_);
    }

    size_t operator()(EdgeType e) const {
        //fmt::println("Edge: {}, Bucket: {}", e.from, (e.from - vstart_) >> shift_bits_);
        return (e.from - vstart_) >> shift_bits_;
    }

    size_t operator()(VertexType v) const {
        return (v - vstart_) >> shift_bits_;
    }

    size_t BucketSize() const {
        return 1 << shift_bits_;
    }

    bool IsPerVertexBucket() const {
        return shift_bits_ == 0;
    }

    size_t BucketWidth() const {
        return 1 << shift_bits_;
    }
};

/**
 * @brief 
 * @param edges edges array
 * @param n number of edges
 * @param bucket_count number of buckets
 * @param key key(e) returns bucket id of edge e
 * @return count of edges in each bucket
 */
template<typename EdgeType, typename Key>
    requires requires(EdgeType e) { e.from; }  &&
             requires(const Key& k, const EdgeType& e) { {k(e)} -> std::convertible_to<uint64_t>; }
std::unique_ptr<uint32_t[]> count_bucket_size(EdgeType*edges, size_t n, size_t bucket_count, const Key& key) {
    auto c = std::make_unique<uint32_t[]>(bucket_count);
    for(size_t i = 0; i < n; i++) {
        size_t idx = key(edges[i]);
        // fmt::println("Edge: {}, Bucket: {}", edges[i].from, idx);
        c[idx]++;
    }
    return c;
}

void count2offset(uint32_t* c, size_t bucket_count) {
    uint32_t sum = 0;
    for(uint32_t i = 0; i < bucket_count; i++) {
        uint32_t tmp = c[i];
        c[i] = sum;
        sum += tmp;
    }
}

/**
 * @brief Move edges to buffer by bucket, c will be updated to end offset of each bucket (instead of start offset)
 */
template<typename EdgeType, typename Key>
    requires requires(EdgeType e) { e.from; }  &&
             requires(const Key& k, const EdgeType& e) { {k(e)} -> std::convertible_to<uint64_t>; }
void move_by_bucket(EdgeType* edges, size_t n, EdgeType* buffer, uint32_t* c, size_t _bucket_count, const Key& key) {
    (void)_bucket_count;
    for(size_t i = 0; i < n; i++) {
        size_t idx = key(edges[i]);
        buffer[c[idx]] = edges[i];
        c[idx]++;
    }
}

/**
 * @brief sort each bucket by end offset of each bucket
 */
template<typename EdgeType, typename Cmp, bool StdSort=false>
void sort_each_bucket(EdgeType* buffer, uint32_t* c, size_t bucket_count, const Cmp& cmp) {
    size_t st = 0;
    for(size_t i = 0; i < bucket_count; i++) {
        size_t ed = c[i];
        if(ed - st > 1) {
            if(StdSort) {
                std::sort(buffer + st, buffer + ed, cmp);
            } else {
                pdqsort_branchless(buffer + st, buffer + ed, cmp);
            }
        }
        st = ed;
    }
}

/**
 * @brief L2 efficient sort
 *        1. Bucket sort to split edges into buckets (average bucket size should smaller than L2 cache size)
 *        2. Sort each bucket by local cache aware sort (pdqsort now)
 * @param edges     array of edges
 * @param n         number of edges
 */
template<typename EdgeType, bool StdSort=false>
void l2_efficient_sort_to(EdgeType* edges, size_t n, EdgeType* target, size_t vstart, size_t vcount) {
    constexpr size_t L2_EDGES = L2_CACHE_SIZE / sizeof(EdgeType);
    constexpr size_t MAX_BUCKET_COUNT = L2_CACHE_SIZE / sizeof(uint32_t) / 2;

    // Determine bucket count, making the average bucket size is maximized without exceeding the L2 Cache 
    size_t bucket_count = std::bit_ceil(n / L2_EDGES);
    bucket_count = std::min(bucket_count, MAX_BUCKET_COUNT);

    //fmt::println("Bucket Count: {}", bucket_count);

    // Construct comparator and bucket id getter
    const CmpFrom<EdgeType> cmp;
    BucketIdGetter<EdgeType> key(bucket_count, vstart, vcount);

    auto c = count_bucket_size(edges, n, bucket_count, key);

    //fmt::println("Count bucket size done");
    
    count2offset(c.get(), bucket_count);

    //fmt::println("Count to offset done");

    move_by_bucket(edges, n, target, c.get(), bucket_count, key);

    //fmt::println("Move by bucket done");

    sort_each_bucket<EdgeType, decltype(cmp), StdSort>(target, c.get(), bucket_count, cmp);

    //fmt::println("Sort each bucket done");
}

/**
 * @brief Same as l2_efficient_sort_to, but copy the result back to edges
 */
template<typename EdgeType, bool StdSort=false>
void l2_efficient_sort_inplace(EdgeType* edges, size_t n, size_t vstart, size_t vcount, EdgeType* buffer=nullptr) {
    if(buffer == nullptr) {
        buffer = new EdgeType[n];
    }

    [[maybe_unused]] VID min_vid = vstart + vcount;
    [[maybe_unused]] VID max_vid = vstart;
    RUN_IN_DEBUG {
        for(size_t i = 0; i < n; i++) {
            min_vid = std::min(min_vid, edges[i].from);
            max_vid = std::max(max_vid, edges[i].from);
        }
    }

    l2_efficient_sort_to<EdgeType, StdSort>(edges, n, buffer, vstart, vcount);

    std::copy(buffer, buffer + n, edges);

    RUN_IN_DEBUG {
        dcsr_assert(min_vid == edges[0].from, "Unexpected min_vid");
        dcsr_assert(max_vid == edges[n-1].from, "Unexpected max_vid");
    }

    delete[] buffer;

}

template<typename T, typename Cmp>
void naive_merge_to(T* arr, size_t n, T* target, size_t* ranges, size_t r, const Cmp& cmp) {
    // using PQItem = std::pair<T, size_t>;
    // auto cmp_pq = [&cmp](const PQItem& a, const PQItem& b) {
    //     return cmp(a.first, b.first);
    // };
    // using PriorityQueue = std::priority_queue<PQItem, std::vector<PQItem>, decltype(cmp_pq)>;
    // std::vector<PQItem> pc_cont;
    // pq.reserve(r);
    // PriorityQueue pq(cmp_pq, std::move(pq_cont));
}

/**
 * @brief Give a sorted array, and function key, build a group index
 * index[i] indicates the end offset of elements with key = i.
 * For example:
 * arr = [1, 1, 2, 2, 2, 4, 4, 4, 4], key(x) = x
 * index = [2, 5, 5, 9, 9]
 */
template<typename T, typename Key>
    requires requires(const Key& k, const T& e) { {k(e)} -> std::convertible_to<uint32_t>; }
void build_group_index(std::span<T> arr, std::span<uint32_t> index, const Key& key) {
    size_t current_key = 0;
    for(size_t i = 0; i < arr.size(); i++) {
        size_t k = key(arr[i]);
        while(current_key < k) {
            index[current_key] = i;
            current_key++;
        }
    }
    for(; current_key < index.size(); current_key++) {
        index[current_key] = arr.size();
    }
}

} // namespace dcsr

#endif // __DCSR_SORT_H__