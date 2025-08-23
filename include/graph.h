#ifndef __DCSR_GRAPH_H__
#define __DCSR_GRAPH_H__

#include <mutex>
#include <omp.h>
#include <unistd.h>
#include <fcntl.h>

#include <boost/container/static_vector.hpp>
#include <boost/dynamic_bitset.hpp>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "readerwriterqueue.h"

#include "third_party/pdqsort.h"
#include "third_party/sb_lower_bound.h"
#include "checker.h"
#include "common.h"
#include "config.h"
#include "datatype.h"
#include "env.h"
#include "filename.h"
#include "formatter.h"
#include "mergeable_ranges.h"
#include "metrics.h"
#include "ring_buffer.h"
#include "sort.h"
#include "vec.h"

namespace dcsr {

template<typename EdgeType>
    requires requires(EdgeType e) { e.from; }
class IndexKeyFunc {
private:
    using VertexType = decltype(EdgeType::from);
    uint64_t vstart_;
    size_t shift_bits_;

public:
    IndexKeyFunc(size_t bucket_count, uint64_t vstart, uint64_t vcount) {
        // fmt::println("IndexKeyFunc: bucket_count: {}, vstart: {}, vcount: {}", bucket_count, vstart, vcount);
        size_t vertex_count_per_bucket = div_up(vcount, bucket_count);
        shift_bits_ = std::bit_width(vertex_count_per_bucket - 1);
        vstart_ = vstart;
    }

    size_t operator()(EdgeType e) const {
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

template<typename KeyFunc>
class BucketIndexWrapper {
public:
    using OffType = uint32_t;
private:
    const OffType* index_;
    const size_t size_;
    const KeyFunc key_func_;

public:
    BucketIndexWrapper(const OffType* index, size_t size, const KeyFunc& key_func)
    : index_(index), size_(size), key_func_(key_func) { }

    template<typename E>
    std::span<const E> GetBucket(E* edges, VID v) const {
        size_t idx = key_func_(v);
        size_t st = idx == 0 ? 0 : index_[idx - 1];
        size_t ed = index_[idx];
        return std::span<const E>(edges + st, ed - st);
    }

    bool IsPerVertexBucket() const {
        return key_func_.IsPerVertexBucket();
    }

    size_t BucketWidth() const {
        return key_func_.BucketWidth();
    }

    const KeyFunc& GetKeyFunc() const {
        return key_func_;
    }

    std::span<const OffType> GetOffset() const {
        return std::span<const OffType>(index_, size_);
    }
};

template<typename E, bool NeighborsOrder=false, bool StdSort=false>
class SortBasedMemPartition {
public:
    using VertexType = E::VertexType;
    using EdgeType = E;
    using MutexType = SpinMutex;
    using BinarySemaphore = SpinBinarySemaphore;
    using IndexRange = std::span<uint32_t>;
    using ConstIndexRange = std::span<const uint32_t>;
    // using KeyFunc = BucketIdGetter<EdgeType>;
    using KeyFunc = IndexKeyFunc<EdgeType>;
    using BitSet = boost::dynamic_bitset<uint64_t>;
    using EdgeSortComparator = std::conditional_t<NeighborsOrder, CmpFromTo<EdgeType>, CmpFrom<EdgeType>>;


    static const size_t MAX_WRITE_THREADS = 16;
    static const size_t MAX_SORT_LEVEL = 16;
    static const size_t MAX_RANGES_COUNT = 64;
    static const size_t L2_EDGES = L2_CACHE_SIZE / sizeof(EdgeType) / 2;    // div 2 to leverage hyper-threading
    static const size_t ENABLE_STEAL_THRESHOLD = 8 * 1024;
    // static const size_t ENABLE_STEAL_THRESHOLD = 1024ull * 1024ull * 1024;
    static const size_t MAX_STEAL_SIZE = 32 * 1024;
    static const size_t MIN_STEAL_SIZE = 512;
private:
    // Meta Infomation
    const size_t pid_;
    const VID vid_start_;
    const size_t width_;

    // Configuration
    const size_t minimum_sort_batch_;
    const size_t l2_mini_batch_count_;
    const double merge_multiplier_;
    const size_t flush_batch_size_;
    const size_t index_ratio_;
    const size_t index_ratio_bits_;
    const int numa_node_;

    // Buffer
    // BatchRingBuffer<EdgeType> ring_buffer_;
    // BatchNumaBuffer<EdgeType> ring_buffer_;
    MultiWritableBatchNumaBuffer<EdgeType, MAX_WRITE_THREADS> ring_buffer_;
    size_t sort_times_[MAX_SORT_LEVEL];     // sort_times_[i] indicates how many times the level-i sort has been executed for this batch
    size_t sorted_count_;       // how many edges have been sorted
    EdgeType* current_batch_;

    // Work stealing  // 可能要加锁？明天想想
    BinarySemaphore steal_semaphore_;   // 1 for stealable, 0 for not stealable
    size_t steal_sorted_count_;

    // Index
    mergeable_ranges<MAX_RANGES_COUNT> sorted_ranges_;
    uint32_t* current_batch_index_;
    uint32_t* first_level_index_;
    BitSet nonempty_bitset_;
    bool bitset_valid_;
    

    // Mutex
    MutexType reading_mutex_;
    std::atomic_flag initialized_;

    // Metrics
    // inline static size_t edges_count_ = 0;
    // inline static double search_unsorted_time_ = 0.0;

public:

    //(size_t pid, VID vstart, size_t vcount, const Config& config, std::atomic<size_t>* full_buffers_count)

    SortBasedMemPartition(size_t pid, VID vstart, size_t vcount, int numa_node, const Config& c)
    : pid_(pid), vid_start_(vstart), width_(vcount),
      minimum_sort_batch_(c.sort_batch_size),
      l2_mini_batch_count_(L2_EDGES / c.sort_batch_size),
      merge_multiplier_(c.merge_multiplier),
      flush_batch_size_(c.buffer_size),
      index_ratio_(c.index_ratio),
      index_ratio_bits_(std::bit_width(c.index_ratio - 1)),
      numa_node_(numa_node),
      // ring_buffer_(c.buffer_size * c.buffer_count, c.sort_batch_size, c.buffer_size),
      ring_buffer_(c.buffer_size * c.buffer_count, c.sort_batch_size, c.dispatch_thread_count, numa_node),
      sort_times_{0}, sorted_count_{0},
      current_batch_(ring_buffer_.VisibleBatchPointer()),
      steal_semaphore_{0},
      steal_sorted_count_{0},
      nonempty_bitset_{},
      bitset_valid_{false},
      reading_mutex_{},
      initialized_{}
    {
        dcsr_assert((flush_batch_size_ % index_ratio_) == 0, "Flush batch size must be multiple of index ratio");
        current_batch_index_ = new uint32_t[flush_batch_size_ / index_ratio_];
        first_level_index_ = new uint32_t[width_];
    }

    ~SortBasedMemPartition() {
        delete[] current_batch_index_;
        delete[] first_level_index_;
        // sfmt::println("~MemPartition[{}]: edges: {:L}, sorted ranges: {}", pid_, sorted_count_, sorted_ranges_.size());
        // size_t unsorted_edges = ring_buffer_.ReadyData().size();
        // fmt::println("~MemPartition[{}]: edges: {:L}, search_unsorted_time: {:.2f}s ({} Edges)", 
        //             pid_, edges_count_, search_unsorted_time_, unsorted_edges);
    }

    void AddEdge(EdgeType e) {
        // fmt::println("[{}] Add edge: {}", pid_, e);
        // ring_buffer_.PushBack(e);
        ring_buffer_.PushBackInto(e, 0);
        // edges_count_++;
    }

    void AddEdgeMultiThread(EdgeType e, size_t thread_id) {
        ring_buffer_.PushBackInto(e, thread_id);
    }

    void Collect() {
        ring_buffer_.Collect();
    }

    // 如果当前可见的 batch 大小足够，就排序一个 mini batch
    bool SortVisible() {
        size_t visible_size = ring_buffer_.VisibleBatchSize();
        size_t new_edges_size = visible_size - sorted_count_;
        if(new_edges_size >= minimum_sort_batch_) {
            // SortNextMiniBatch();

            size_t batch_count = new_edges_size / minimum_sort_batch_;
            SortNextMultipleMiniBatchs(batch_count);
            return true;
        }
        return false;
    }

    // Call by writer thread of other partition
    bool TrySteal() {
        bool can_steal = steal_semaphore_.try_acquire();
        if(!can_steal) {
            return false;
        }

        bool success = false;
        size_t visible_size = ring_buffer_.VisibleBatchSize();
        size_t new_edges_size = visible_size - steal_sorted_count_;
        if(new_edges_size >= MIN_STEAL_SIZE) {
            size_t steal_len = std::min<size_t>(MAX_STEAL_SIZE, new_edges_size);
            EdgeType *st = current_batch_ + steal_sorted_count_;
            EdgeType *ed = current_batch_ + steal_sorted_count_ + steal_len;
            SmallRangeSort(st, ed);
            steal_sorted_count_ += steal_len;
            success = true;
        }
        steal_semaphore_.release();
        return success;
    }

    // 当前 Batch 所有部分均已排过序，即为多个有序区间，粒度至少为 minimum_sort_batch_
    bool BatchPartialSorted() const {
        return sorted_count_ >= flush_batch_size_;
    }

    // 当前可见的部分均已排过序，即为多个有序区间，粒度至少为 minimum_sort_batch_
    bool VisiblePartialSorted() {
        // fmt::println("VisiblePartialSorted: sorted_count={}, visible_size={}", sorted_count_, ring_buffer_.VisibleBatchSize());
        return ring_buffer_.VisibleBatchSize() == sorted_count_;
    }

    // 当前 batch
    std::span<EdgeType> GetCurrentBatch() {
        return std::span<EdgeType>(current_batch_, std::min(flush_batch_size_, sorted_count_));
    }

    void WaitInitialized() {
        initialized_.wait(false);
    }

    void Initialized() {
        initialized_.test_and_set();
        initialized_.notify_all();
    }

    MutexType& GetReadingMutex() {
        return reading_mutex_;
    }

    // 获取顶点邻居，包括未排序的部分，并非线程安全，性能较差
    // Not for performance, only for test
    std::vector<EdgeType> GetNeighborsVector(VID v) const {
        std::vector<EdgeType> neighbors;

        // std::lock_guard<MutexType> lock(reading_mutex_);
        // fmt::println("Sorted count: {}", sorted_count_);

        // SimpleTimer timer;
        // Unsorted part
        auto unsorted = ring_buffer_.ReadyData();
        for(const auto& e: unsorted) {
            if(e.from == v) {
                neighbors.push_back(e);
            }
        }
        // search_unsorted_time_ += timer.Stop();

        RUN_IN_DEBUG {
            fmt::println("Unsorted part size: {}", unsorted.size());
            fmt::println("neigh: {::t}", neighbors);
            fmt::println("Ranges: {}", sorted_ranges_.to_string());
        }

        // fmt::println("Ranges: {}", sorted_ranges_.to_string());

        for(const auto& r: sorted_ranges_) {
            const EdgeType* st = current_batch_ + r.first;
            const EdgeType* ed = current_batch_ + r.second;

            RUN_IN_DEBUG {
                fmt::println("Range: [{}, {})", r.first, r.second);
                // fmt::println("Ranges: {}", sorted_ranges_.to_string());
            }

            // fmt::println("Range: [{}, {})", r.first, r.second);
            // fmt::println("Ranges: {}", sorted_ranges_.to_string());

            auto index = GetRelatedIndexWrapper(st, ed);
            auto range = index.GetBucket(st, v);
            const EdgeType* rst = range.data();
            const EdgeType* red = range.data() + range.size();

            RUN_IN_DEBUG {
                fmt::println("Search vertex: {}", v);
                fmt::println("search len: {}", ed - st);
                fmt::println("After index: [{}, {}) (len: {})", rst - st, red - st, red - rst);
            }

            if(rst == red) {
                continue;
            }

            auto it = BinarySearchVertexInRange(v, rst, red);
            while(it != ed && it->from == v) {
                neighbors.push_back(*it);
                it++;
            }

            // fmt::println("neigh: {::t}", neighbors);
        }
        // fmt::println("==============");
        return neighbors;
    }
    
    template<typename Func>
        requires std::invocable<Func, VID>
    void IterateNeighbors(VID v, const Func& func) const {
        if(bitset_valid_ && !nonempty_bitset_[v - vid_start_]) {
            return;
        }

        for(const auto& r: sorted_ranges_) {
            const EdgeType* st = current_batch_ + r.first;
            const EdgeType* ed = current_batch_ + r.second;

            RUN_IN_DEBUG {
                fmt::println("Range: [{}, {})", r.first, r.second);
                // fmt::println("Ranges: {}", sorted_ranges_.to_string());
            }

            auto index = GetRelatedIndexWrapper(st, ed);
            auto range = index.GetBucket(st, v);
            const EdgeType* rst = range.data();
            const EdgeType* red = range.data() + range.size();

            RUN_IN_DEBUG {
                fmt::println("Search vertex: {}", v);
                fmt::println("search len: {}", ed - st);
                fmt::println("After index: [{}, {}) (len: {})", rst - st, red - st, red - rst);
            }

            if(rst == red) {
                continue;
            }

            auto it = BinarySearchVertexInRange(v, rst, red);
            while(it != ed && it->from == v) {
                if constexpr (std::is_same_v<std::invoke_result_t<Func, VID>, bool>) {
                    bool cont = func(it->to);
                    if(!cont) {
                        return;
                    }
                } else {
                    func(it->to);
                }
                it++;
            }
            // fmt::println("neigh: {::t}", neighbors);
        }
        // fmt::println("==============");

        // // SimpleTimer timer;
        // // Unsorted part
        auto unsorted = ring_buffer_.ReadyData();
        for(const auto& e: unsorted) {
            if(e.from == v) {
                // Support early break
                if constexpr (std::is_same_v<std::invoke_result_t<Func, VID>, bool>) {
                    bool cont = func(e.to);
                    if(!cont) {
                        return;
                    }
                } else {
                    func(e.to);
                }
            }
        }
        // // search_unsorted_time_ += timer.Stop();

        // RUN_IN_DEBUG {
        //     fmt::println("Unsorted part size: {}", unsorted.size());
        //     fmt::println("Ranges: {}", sorted_ranges_.to_string());
        // }

        // fmt::println("Ranges: {}", sorted_ranges_.to_string());

    }


    size_t GetDegree(VID v) const {
        if(bitset_valid_ && !nonempty_bitset_[v - vid_start_]) {
            return 0;
        }

        size_t degree = 0;

        // Unsorted part
        auto unsorted = ring_buffer_.ReadyData();
        for(const auto& e: unsorted) {
            if(e.from == v) {
                degree++;
            }
        }

        for(const auto& r: sorted_ranges_) {
            const EdgeType* st = current_batch_ + r.first;
            const EdgeType* ed = current_batch_ + r.second;

            auto index = GetRelatedIndexWrapper(st, ed);
            auto range = index.GetBucket(st, v);

            if(range.empty()) {
                continue;
            }

            if(index.IsPerVertexBucket()) {
                degree += range.size();
                continue;
            }

            const EdgeType* rst = range.data();
            const EdgeType* red = range.data() + range.size();

            degree += BinarySearchVertexCountInRange(v, rst, red);
        }
        return degree;
    }

    /**
     * @brief Iterate neighbors of v in the range [v1, v2)
     */
    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsRangeInLevel(VID v1, VID v2, size_t level, const Func& func) const {
        if(level >= sorted_ranges_.size()) {
            return;
        }
        v1 = std::max(v1, vid_start_);
        v2 = std::min(v2, vid_start_ + width_);

        const auto& r = sorted_ranges_[level];
        const EdgeType* range_st = current_batch_ + r.first;
        const EdgeType* range_ed = current_batch_ + r.second;
        auto index = GetRelatedIndexWrapper(range_st, range_ed);
        auto v1_bucket = index.GetBucket(range_st, v1);

        auto bucket_st = v1_bucket.data();
        auto bucket_ed = v1_bucket.data() + v1_bucket.size();
        const EdgeType* v1_st = BinarySearchVertexInRange(v1, bucket_st, bucket_ed);

        auto it = v1_st;
        while(it != range_ed && it->from < v2) {
            using FuncRet = std::invoke_result_t<Func, VID, VID>;
            if constexpr (std::is_same_v<FuncRet, bool>) {
                // Breakable API
                bool cont = func(it->from, it->to);
                if(!cont) {
                    return;
                }
                it++;
            } else if constexpr (std::is_same_v<FuncRet, IterateOperator>) {
                // Breakable & Skipable API
                // fmt::println("Skipable API is supported in this function");
                auto ret = func(it->from, it->to);
                if(ret == IterateOperator::CONTINUE) {
                    it++;
                } else if(ret == IterateOperator::BREAK) {
                    return;
                } else if(ret == IterateOperator::SKIP_TO_NEXT_VERTEX) {
                    // Most of the time, next vertex is near to current vertex
                    // So use exponential search to speed up
                    auto next_it = ExponentialSearchVertex(it->from + 1, it, range_ed);
                    it = next_it;
                }
            } else if constexpr (std::is_integral_v<FuncRet>) {
                // Multi-step skipable (i.e. a "long jump") API
                // fmt::println("Iterate {} to {}", it->from, it->to);
                size_t jump = func(it->from, it->to);
                if(jump == 0) {
                    it++;
                } else {
                    // fmt::println("Jump to: {}", it->from + jump);
                    it = ExponentialSearchVertex(it->from + jump, it, range_ed);
                }
            } else {
                func(it->from, it->to);
                it++;
            }
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsRange(VID v1, VID v2, const Func& func) const {
        v1 = std::max(v1, vid_start_);
        v2 = std::min(v2, vid_start_ + width_);

        for(size_t i = 0; i < sorted_ranges_.size(); i++) {
            IterateNeighborsRangeInLevel(v1, v2, i, func);
        }

        // fmt::println("Unsorted part");
        for(EdgeType e: ring_buffer_.ReadyData()) {
            if(e.from >= v1 && e.from < v2) {
                func(e.from, e.to);
            }
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRangeInLevel(VID v1, VID v2, size_t sample_count, size_t level, const Func& func) const {
        VID v = v1;
        size_t count = 0;
        
        IterateNeighborsRangeInLevel(v1, v2, level, [&](VID from, VID to) {
            if(from != v) {
                v = from;
                count = 0;
            }
            func(from, to, count);
            count++;
            // fmt::println("From: {}, Count: {}/{}", from, count, sample_count);
            if(count == sample_count) {
                // fmt::println("Skip to next vertex: {}", v);
                return IterateOperator::SKIP_TO_NEXT_VERTEX;
            }
            return IterateOperator::CONTINUE;
        });
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRange(VID v1, VID v2, size_t sample_count, const Func& func) const {
        v1 = std::max(v1, vid_start_);
        v2 = std::min(v2, vid_start_ + width_);

        thread_local size_t csize = v2 - v1;
        thread_local auto count = std::make_unique_for_overwrite<uint8_t[]>(csize);
        if(csize < (v2 - v1)) { // If count array is enough, just reuse it
            csize = v2 - v1;
            count = std::make_unique_for_overwrite<uint8_t[]>(csize);
        }
        std::fill(count.get(), count.get() + (v2 - v1), 0);

        for(size_t i = 0; i < sorted_ranges_.size(); i++) {
            IterateNeighborsRangeInLevel(v1, v2, i, [&](VID from, VID to) {
                // if(count[from - v1] == sample_count) {
                //     return IterateOperator::SKIP_TO_NEXT_VERTEX;
                // }
                // func(from, to, count[from - v1]);
                // count[from - v1]++;
                // if(count[from - v1] == sample_count) {
                //     return IterateOperator::SKIP_TO_NEXT_VERTEX;
                // }
                // return IterateOperator::CONTINUE;

                func(from, to, count[from - v1]);
                count[from - v1]++;
                size_t jump = 1;
                while(from + jump < v2 && count[from + jump - v1] == sample_count) {
                    jump++;
                }
                return jump;
            });
        }

        // fmt::println("Unsorted part");
        for(EdgeType e: ring_buffer_.ReadyData()) {
            if(e.from >= v1 && e.from < v2) {
                if(count[e.from - v1] == sample_count) {
                    continue;
                }
                func(e.from, e.to, count[e.from - v1]);
                count[e.from - v1]++;
            }
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRange2(VID v1, VID v2, size_t sample_count, const Func& func) {
        // Build bit map at the same time
        v1 = std::max(v1, vid_start_);
        v2 = std::min(v2, vid_start_ + width_);

        // if(nonempty_bitset_.size() < width_) {
        //     nonempty_bitset_.resize(width_);
        //     nonempty_bitset_.reset();
        // }

        thread_local size_t csize = v2 - v1;
        thread_local auto count = std::make_unique_for_overwrite<uint8_t[]>(csize);
        if(csize < (v2 - v1)) { // If count array is enough, just reuse it
            csize = v2 - v1;
            count = std::make_unique_for_overwrite<uint8_t[]>(csize);
        }
        std::fill(count.get(), count.get() + (v2 - v1), 0);

        for(size_t i = 0; i < sorted_ranges_.size(); i++) {
            IterateNeighborsRangeInLevel(v1, v2, i, [&](VID from, VID to) {
                if(count[from - v1] == sample_count) {
                    return IterateOperator::SKIP_TO_NEXT_VERTEX;
                }
                if(sample_count == 1) {
                    nonempty_bitset_.set(from - vid_start_);
                }
                func(from, to, count[from - v1]);
                count[from - v1]++;
                if(count[from - v1] == sample_count) {
                    return IterateOperator::SKIP_TO_NEXT_VERTEX;
                }
                return IterateOperator::CONTINUE;
            });
        }

        // fmt::println("Unsorted part");
        for(EdgeType e: ring_buffer_.ReadyData()) {
            if(e.from >= v1 && e.from < v2) {
                if(count[e.from - v1] == sample_count) {
                    continue;
                }
                if(sample_count == 1) {
                    nonempty_bitset_.set(e.from - vid_start_);
                }
                func(e.from, e.to, count[e.from - v1]);
                count[e.from - v1]++;
            }
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRangeFast(VID v1, VID v2, size_t sample_count, const Func& func) const {
        v1 = std::max(v1, vid_start_);
        v2 = std::min(v2, vid_start_ + width_);


        std::vector<EdgeType> unsort_neighbors;

        // Unsorted part
        auto unsorted = ring_buffer_.ReadyData();
        for(const auto& e: unsorted) {
            if(e.from >= v1 && e.from < v2) {
                unsort_neighbors.push_back(e);
            }
        }

        // Sort unsorted part, by from vertex
        pdqsort_branchless(unsort_neighbors.begin(), unsort_neighbors.end(), CmpFrom<EdgeType>());

        // Insert all ranges into a vector, sort ranges by first element's target vertex
        using Range = std::pair<const EdgeType*, const EdgeType*>;
        std::vector<Range> ranges;
        for(const auto& r: sorted_ranges_) {
            const EdgeType* st = current_batch_ + r.first;
            const EdgeType* ed = current_batch_ + r.second;

            auto index = GetRelatedIndexWrapper(st, ed);
            auto range = index.GetBucket(st, v1);
            const EdgeType* rst1 = range.data();
            const EdgeType* red1 = range.data() + range.size();

            auto range2 = index.GetBucket(st, v2-1);
            const EdgeType* rst2 = range2.data();
            const EdgeType* red2 = range2.data() + range2.size();

            if(rst1 == red2) {
                continue;
            }

            auto it = BinarySearchVertexInRange(v1, rst1, red1);
            auto it2 = BinarySearchVertexInRange(v2, rst2, red2);
            if(it != it2 && it->from < v2) {
                ranges.push_back({it, it2});
            }
        }
        if(!unsort_neighbors.empty()) {
            ranges.push_back({unsort_neighbors.data(), unsort_neighbors.data() + unsort_neighbors.size()});
        }

        // auto cmp_range_first = [&](Range a, Range b) {
        //     return CmpFrom<EdgeType>()(*a.first, *b.first);
        // };

        // pdqsort_branchless(ranges.begin(), ranges.end(), cmp_range_first);

        std::span<Range> sp(ranges);

        // We try to sample from first range (level), until the vertex has not enough edges in this level
        
        // size_t z = 0;
        // size_t p1 = 0, po = 0;
        size_t cnt = 0;
        for(VID v = v1; v < v2;) {
            // fmt::println("v: {}", v);
            VID v_next = std::numeric_limits<VID>::max();
            for(size_t i = 0; i < sp.size(); i++) {
                // fmt::println("i: {}", i);

                auto& r = sp[i];
                if(r.first->from < v) {
                    r.first = ExponentialSearchVertex2(v, r.first, r.second);
                }
                while(r.first < r.second && r.first->from == v) {
                    func(r.first->from, r.first->to, cnt);
                    r.first++;
                    cnt++;
                    // if(r.first == sp[0].first) {
                    //     p1++;
                    // } else {
                    //     po++;
                    // }
                    if(cnt == sample_count) {   // 如果当前顶点抽样完毕，直接原地尝试下一个顶点。
                        cnt = 0;
                        v++;
                        if(v == v2) [[unlikely]] {
                            return;
                        }
                        // r.first = BinarySearchVertexInRange(v, r.first, r.second);
                        r.first = ExponentialSearchVertex2(v, r.first, r.second);
                    }
                }

                // 当前层没找到，要么当前层找完了，要么当前层没有这个顶点
                // 如果没找完，维护当前层的下一个顶点
                if(r.first != r.second) {
                    v_next = std::min<VID>(v_next, r.first->from);
                }
            }

            // 所有层找完了，如果所有层都没有之后要找的几个顶点，直接跳过多个顶点。
            if(v_next > v) {
                v = v_next;
                cnt = 0;
            }
        }

        // fmt::println("z: {}", z);
        // fmt::println("p1: {}, po: {}", p1, po);
    }


    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRangeFast2(VID v1, VID v2, size_t sample_count, const Func& func) const {
        v1 = std::max(v1, vid_start_);
        v2 = std::min(v2, vid_start_ + width_);
        if(v1 >= v2) {
            return;
        }

        std::vector<EdgeType> unsort_neighbors;

        // Unsorted part
        auto unsorted = ring_buffer_.ReadyData();
        for(const auto& e: unsorted) {
            if(e.from >= v1 && e.from < v2) {
                unsort_neighbors.push_back(e);
            }
        }

        // Sort unsorted part, by from vertex
        pdqsort_branchless(unsort_neighbors.begin(), unsort_neighbors.end(), CmpFrom<EdgeType>());

        // Insert all ranges into a vector, sort ranges by first element's target vertex
        using Range = std::pair<const EdgeType*, const EdgeType*>;
        std::vector<Range> ranges;
        for(const auto& r: sorted_ranges_) {
            const EdgeType* st = current_batch_ + r.first;
            const EdgeType* ed = current_batch_ + r.second;

            auto index = GetRelatedIndexWrapper(st, ed);
            auto range = index.GetBucket(st, v1);
            const EdgeType* rst1 = range.data();
            const EdgeType* red1 = range.data() + range.size();

            auto range2 = index.GetBucket(st, v2);
            const EdgeType* rst2 = range2.data();
            const EdgeType* red2 = range2.data() + range2.size();

            if(rst1 == red2) {
                continue;
            }

            auto it = BinarySearchVertexInRange(v1, rst1, red1);
            auto it2 = BinarySearchVertexInRange(v2, rst2, red2);
            if(it != it2 && it->from < v2) {
                ranges.push_back({it, it2});
            }
        }
        if(!unsort_neighbors.empty()) {
            ranges.push_back({unsort_neighbors.data(), unsort_neighbors.data() + unsort_neighbors.size()});
        }

        [[maybe_unused]] auto cmp_range_first = [&](Range a, Range b) {
            return a.first->from < b.first->from;
        };

        if(ranges.empty()) {
            return;
        }

        Range r0 = ranges[0];
        std::span<Range> sp(ranges.data() + 1, ranges.size() - 1);
        // pdqsort_branchless(sp.begin(), sp.end(), cmp_range_first);

        // auto r0_index = GetRelatedIndexWrapper(r0.first, r0.second);

        // fmt::println("R0 length: {}", r0.second - r0.first);
        // fmt::println("Sample width: {}", v2 - v1);
        // fmt::println("Density: {:.4f}", (r0.second - r0.first) / (double)(v2 - v1));
        // fmt::println("R0 has per-vertext bucket: {}", r0_index.IsPerVertexBucket());


        // Most of samples are from ranges[0], so we iterate it independently (in main loop)
        // Then, we keep the order of other ranges, by their first element's source vertex
        // If we cannot find the vertex in r0, compare sp[0].first->from to

        VID v = v1;
        size_t cnt = 0;

        while(r0.first < r0.second) {
            if(r0.first->from < v) {
                r0.first = ExponentialSearchVertex(v, r0.first, r0.second);
            }

            while(r0.first < r0.second && r0.first->from == v) {
                func(r0.first->from, r0.first->to, cnt);
                r0.first++;
                cnt++;
                if(cnt == sample_count) {   // 如果当前顶点抽样完毕，直接原地尝试下一个顶点。
                    cnt = 0;
                    v++;
                    if(v == v2) [[unlikely]] {
                        return;
                    }
                    r0.first = ExponentialSearchVertex2(v, r0.first, r0.second);
                }
            }

            // Cannot be found in r0, try to find in other ranges
            for(auto& r: sp) {
                if(r.first < r.second && r.first->from < v) {
                    r.first = ExponentialSearchVertex(v, r.first, r.second);
                }
                while(r.first < r.second && r.first->from == v) {
                    func(r.first->from, r.first->to, cnt);
                    r.first++;
                    cnt++;
                    if(cnt == sample_count) {
                        cnt = 0;
                        v++;
                        if(v == v2) [[unlikely]] {
                            return;
                        }
                        goto range_search_end;
                    }
                }
            }

            // cannot find:
            v++;
            if(v == v2) {
                return;
            }
            // found:
            range_search_end:
            ;
        }
    }


    // template<typename Func>
    //     requires std::invocable<Func, VID, VID, size_t>
    // static void SampleFromRanges()

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRangeDensityAware(VID v1, VID v2, size_t sample_count, const Func& func) const {
        v1 = std::max(v1, vid_start_);
        v2 = std::min(v2, vid_start_ + width_);

        std::vector<EdgeType> unsort_neighbors;

        // Unsorted part
        auto unsorted = ring_buffer_.ReadyData();
        for(const auto& e: unsorted) {
            if(e.from >= v1 && e.from < v2) {
                unsort_neighbors.push_back(e);
            }
        }

        // Sort unsorted part, by from vertex
        pdqsort_branchless(unsort_neighbors.begin(), unsort_neighbors.end(), CmpFrom<EdgeType>());

        // Insert all ranges into a vector, sort ranges by first element's target vertex
        using Range = std::pair<const EdgeType*, const EdgeType*>;
        std::vector<Range> ranges;
        for(const auto& r: sorted_ranges_) {
            const EdgeType* st = current_batch_ + r.first;
            const EdgeType* ed = current_batch_ + r.second;

            auto index = GetRelatedIndexWrapper(st, ed);
            auto range = index.GetBucket(st, v1);
            const EdgeType* rst1 = range.data();
            const EdgeType* red1 = range.data() + range.size();

            auto range2 = index.GetBucket(st, v2-1);
            const EdgeType* rst2 = range2.data();
            const EdgeType* red2 = range2.data() + range2.size();

            if(rst1 == red2) {
                continue;
            }

            auto it = BinarySearchVertexInRange(v1, rst1, red1);
            auto it2 = BinarySearchVertexInRange(v2, rst2, red2);
            if(it != it2 && it->from < v2) {
                ranges.push_back({it, it2});
            }
        }
        if(!unsort_neighbors.empty()) {
            ranges.push_back({unsort_neighbors.data(), unsort_neighbors.data() + unsort_neighbors.size()});
        }

        // auto cmp_range_first = [&](Range a, Range b) {
        //     return CmpFrom<EdgeType>()(*a.first, *b.first);
        // };

        // pdqsort_branchless(ranges.begin(), ranges.end(), cmp_range_first);
        if(ranges.empty()) {
            return;
        }
        std::span<Range> sp(ranges.data() + 1, ranges.size() - 1);

        auto index0 = GetIndexWrapperOf(0);
        dcsr_assert(index0.IsPerVertexBucket(), "First level must be per-vertex index");
        dcsr_assert(index0.GetOffset().data() == first_level_index_, "First level must be standalone");
        // VID other_next_v = v1;        
        for(VID v = v1; v < v2; v++) {
            auto range = index0.GetBucket(current_batch_, v);
            if(range.size() >= sample_count) {
                for(size_t i = 0; i < sample_count; i++) {
                    // dcsr_assert(range[i].from == v, "Invalid vertex");
                    func(range[i].from, range[i].to, i);
                }
            } else {
                for(size_t i = 0; i < range.size(); i++) {
                    // dcsr_assert(range[i].from == v, "Invalid vertex");
                    func(range[i].from, range[i].to, i);
                }

                // Search in other ranges
                size_t rest_cnt = sample_count - range.size();
                for(auto& r: sp) {
                    if(r.first < r.second && r.first->from < v) {
                        r.first = ExponentialSearchVertex(v, r.first, r.second);
                    }
                    while(r.first < r.second && r.first->from == v) {
                        func(r.first->from, r.first->to, sample_count - rest_cnt);
                        r.first++;
                        rest_cnt--;
                        if(rest_cnt == 0) {
                            break;
                        }
                    }
                    if(rest_cnt == 0) {
                        break;
                    }
                }
            }
        } 
        return;


    }


    template<typename Func>
        requires std::invocable<Func, VID>
    void IterateNeighborsInOrder(VID v, const Func& func) const {
        if constexpr (!NeighborsOrder) {
            dcsr_assert(false, "NeighborsOrder is disable, IterateNeighborsInOrder is not supported.");
            return;
        }

        std::vector<EdgeType> unsort_neighbors;

        // Unsorted part
        auto unsorted = ring_buffer_.ReadyData();
        for(const auto& e: unsorted) {
            if(e.from == v) {
                unsort_neighbors.push_back(e);
            }
        }

        // Sort unsorted part in vector
        pdqsort_branchless(unsort_neighbors.begin(), unsort_neighbors.end(), CmpTo<EdgeType>());

        // Insert all ranges into a vector, sort ranges by first element's target vertex
        using Range = std::pair<const EdgeType*, const EdgeType*>;
        std::vector<Range> ranges;
        for(const auto& r: sorted_ranges_) {
            const EdgeType* st = current_batch_ + r.first;
            const EdgeType* ed = current_batch_ + r.second;

            auto index = GetRelatedIndexWrapper(st, ed);
            auto range = index.GetBucket(st, v);
            const EdgeType* rst = range.data();
            const EdgeType* red = range.data() + range.size();

            if(rst == red) {
                continue;
            }

            auto it = BinarySearchVertexInRange(v, rst, red);
            if(it != red && it->from == v) {
                ranges.push_back({it, red});
            }
        }
        if(!unsort_neighbors.empty()) {
            ranges.push_back({unsort_neighbors.data(), unsort_neighbors.data() + unsort_neighbors.size()});
        }

        auto cmp_range_first = [&](Range a, Range b) {
            return CmpTo<EdgeType>()(*a.first, *b.first);
        };
    
        pdqsort_branchless(ranges.begin(), ranges.end(), cmp_range_first);

        // Now we have all ranges sorted by target vertex
        std::span<Range> sp(ranges);

        // First element is the edges with the smallest target vertex, iterate it
        // Then use insertion sort to keep the order
        VID last = 0;
        while(!sp.empty()) {
            Range r = sp[0];
            if(r.first->to < last) {
                dcsr_assert(false, "Not sorted");
            }
            if(r.first->from != v) {
                dcsr_assert(false, "Not the same vertex");
            }

            if constexpr (std::is_same_v<std::invoke_result_t<Func, VID>, bool>) {
                bool cont = func(r.first->to);
                if(!cont) {
                    return;
                }
            } else {
                func(r.first->to);
            }

            if(r.first + 1 != r.second && (r.first + 1)->from == v) {
                // insert to correct position
                r.first++;
                auto it = LowerBound(sp.begin() + 1, sp.end(), r, cmp_range_first);
                std::move(sp.begin() + 1, it, sp.begin());
                // for(auto it2 = sp.begin() + 1; it2 != it; it2++) {
                //     *(it2 - 1) = *it2;
                // }
                *(it - 1) = r;
            } else {
                sp = sp.subspan(1);
            }
        }
    }

    // void BuildBitmap() {
    //     nonempty_bitset_.resize(width_);
    //     nonempty_bitset_.reset();
    //     SampleNeighborsRange(vid_start_, vid_start_ + width_, 1, [&](VID from, VID to, size_t count){
    //         nonempty_bitset_.set(from - vid_start_);
    //         (void)to;
    //         (void)count;
    //     });
    //     bitset_valid_ = true;
    // }

    void BuildBitmap() {
        nonempty_bitset_.resize(width_);
        nonempty_bitset_.reset();
        VID v1 = vid_start_;
        VID v2 = vid_start_ + width_;
        for(size_t i = 0; i < sorted_ranges_.size(); i++) {
            IterateNeighborsRangeInLevel(v1, v2, i, [&](VID from, VID to) {
                nonempty_bitset_.set(from - vid_start_);
                (void)to;
                return IterateOperator::SKIP_TO_NEXT_VERTEX;
            });
        }

        // fmt::println("Unsorted part");
        for(EdgeType e: ring_buffer_.ReadyData()) {
            nonempty_bitset_.set(e.from - vid_start_);
        }

        bitset_valid_ = true;
    }

    void InvalidateBitmap() {
        bitset_valid_ = false;
    }

    void ValidateBitmap() {
        nonempty_bitset_.resize(width_);
        nonempty_bitset_.reset();
        bitset_valid_ = true;
    }

private:
    template <class ForwardIt, class T, class Compare>
    static constexpr ForwardIt LowerBound(ForwardIt first, ForwardIt last, const T& value, const Compare& comp) {
        return std::lower_bound(first, last, value, comp);
        // return sb_lower_bound(first, last, value, comp);
        // return sbm_lower_bound(first, last, value, comp);
        // return sbpm_lower_bound(first, last, value, comp);
        // return branchless_lower_bound(first, last, value, comp);
        // return asm_lower_bound(first, last, value, comp);
    }


    static const EdgeType* BinarySearchVertexInRange(VID v, const EdgeType* st, const EdgeType* ed) {
        return LowerBound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        // return std::lower_bound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        // return sbm_lower_bound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        // return sbpm_lower_bound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
    }

    static size_t BinarySearchVertexCountInRange(VID v, const EdgeType* st, const EdgeType* ed) {
        const EdgeType* rst = LowerBound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        const EdgeType* red = LowerBound(st, ed, EdgeType(v+1, 0), CmpFrom<EdgeType>());
        // EdgeType* rst = std::lower_bound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        // EdgeType* red = std::upper_bound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        // EdgeType* rst = sbm_lower_bound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        // EdgeType* red = sbm_lower_bound(st, ed, EdgeType(v+1, 0), CmpFrom<EdgeType>());
        // EdgeType* rst = sbpm_lower_bound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        // EdgeType* red = sbpm_lower_bound(st, ed, EdgeType(v+1, 0), CmpFrom<EdgeType>());
        return red - rst;
    }

    static const EdgeType* ExponentialSearchVertex(VID v, const EdgeType* st, const EdgeType* ed) {
        size_t len = ed - st;
        size_t i = 1;
        size_t last = 0;
        while(i < len && st[i].from < v) {
            // fmt::println("Exponential search: i={}, v={}, st[i]={}", i, v, st[i].from);
            last = i;
            i *= 2;
        }
        return LowerBound(st + last, std::min(st + i, ed), EdgeType(v, 0), CmpFrom<EdgeType>());
    }

    template<size_t Scan, size_t FirstStep, size_t Multiplier>
    static const EdgeType* ExponentialSearchVertexTemp(VID v, const EdgeType* st, const EdgeType* ed) {
        const EdgeType* scan_ed = st + std::min<size_t>(Scan, ed - st);
        while(st < scan_ed) {
            if(st->from >= v) {
                return st;
            }
            st++;
        }

        size_t i = FirstStep;
        size_t last = 0;
        while(st + i < ed) {
            if((st + i)->from < v) {
                last = i;
                i *= Multiplier;
            } else {
                break;
            }
        }
        return LowerBound(st + last, std::min(st + i, ed), EdgeType(v, 0), CmpFrom<EdgeType>());
    }

    template<size_t Scan, size_t FirstStep, size_t Multiplier>
    static const EdgeType* ExponentialSearchVertexTemp2(VID v, const EdgeType* st, const EdgeType* ed) {
        const EdgeType* scan_ed = st + std::min<size_t>(Scan, ed - st);
        while(st < scan_ed) {
            if(st->from >= v) {
                return st;
            }
            st++;
        }

        size_t i = FirstStep;
        size_t last = 0;
        while(st + i < ed) {
            if((st + i)->from < v) {
                last = i;
                i *= Multiplier;
            } else {
                break;
            }
        }
        return LowerBound(st + last, std::min(st + i, ed), EdgeType(v, 0), CmpFrom<EdgeType>());
    }

    // inline static size_t i_count[257] = {0};
    static const EdgeType* ExponentialSearchVertex2(VID v, const EdgeType* st, const EdgeType* ed) {
        if(st >= ed || st->from >= v) [[unlikely]] {
            // i_count[0]++;
            return st;
        }

        size_t last = 4;
        size_t len = ed - st;
        if(len <= last) [[unlikely]] {
            return LowerBound(st, ed, EdgeType(v, 0), CmpFrom<EdgeType>());
        }

        for(size_t j = 1; j <= last; j++) {
            if(st[j].from >= v) {
                return st + j;
            }
        }
        
        constexpr size_t Multipliers = 8;
        size_t i = last * Multipliers;
        while(i < len && st[i].from < v) {
            // fmt::println("Exponential search: i={}, v={}, st[i]={}", i, v, st[i].from);
            // if(i < 257) {
            //     i_count[i]++;
            //     if(i == 1 && i_count[i] == 10000000) {
            //         fmt::print("i=0: {} |", i_count[0]);
            //         for(size_t j = 1; j < 257; j*=2) {
            //             fmt::print("i={}: {} |", j, i_count[j]);
            //         }
            //         fmt::println("");
            //     }
            // }
            last = i;
            i *= Multipliers;
        }
        auto it = LowerBound(st + last + 1, std::min(st + i, ed), EdgeType(v, 0), CmpFrom<EdgeType>());
        dcsr_assert(it == ed || it->from >= v, "Exponential search failed");
        dcsr_assert((it-1)->from < v, "Exponential search failed, skip unexpected vertex");
        return it;
    }

    /**
     * @brief Internal only, sort a small range which can contain by L2 cache
     */
    static void SmallRangeSort(EdgeType* begin, EdgeType* end) {
        if constexpr (StdSort) {
            std::sort(begin, end, EdgeSortComparator());
        } else {
            pdqsort_branchless(begin, end, EdgeSortComparator());
        }
    }

    void LargeRangeSort(EdgeType* begin, EdgeType* end) {
        if constexpr (StdSort) {
            std::sort(begin, end, EdgeSortComparator());
        } else {
            [[maybe_unused]] size_t len = end - begin;
            // l2_efficient_sort_inplace<EdgeType, StdSort>(begin, len, vid_start_, width_);
            // l2_efficient_sort_inplace<EdgeType, false>(begin, len, vid_start_, width_);
            // l2_efficient_sort_inplace<EdgeType, true>(begin, len, vid_start_, width_);
            pdqsort_branchless(begin, end, EdgeSortComparator());
            // gfx::timsort(begin, end, EdgeSortComparator());
            // std::sort(begin, end, EdgeSortComparator());
        }

    }

    void AdaptiveRangeSort(EdgeType* begin, EdgeType* end) {
        size_t len = end - begin;
        if(len <= L2_EDGES) {
            SmallRangeSort(begin, end);
        } else {
            LargeRangeSort(begin, end);
        }
    }

    // Internal only, sort [begin, end) range, which consist of several sorted ranges, except [unsorted_begin, end)
    void MergeRange(EdgeType* begin, EdgeType* unsorted_begin, EdgeType* end) {
        AdaptiveRangeSort(unsorted_begin, end);
        gfx::timsort(begin, end, EdgeSortComparator());
        // l2_efficient_sort_inplace<EdgeType, true>(begin, end-begin, vid_start_, width_);
    }

    IndexRange GetRelatedIndexRange(EdgeType* st, EdgeType* ed) {
        if(st == current_batch_) {
            return IndexRange(first_level_index_, first_level_index_ + width_);
        }
        size_t st_off = st - current_batch_;
        size_t ed_off = ed - current_batch_;
        size_t st_index_off = (st_off >> index_ratio_bits_);    // perf shows div is slow, use shift instead
        size_t ed_index_off = (ed_off >> index_ratio_bits_);
        return IndexRange(current_batch_index_ + st_index_off, current_batch_index_ + ed_index_off);
    }

    ConstIndexRange GetRelatedIndexRangeConst(const EdgeType* st, const EdgeType* ed) const {
        if(st == current_batch_) {
            return ConstIndexRange(first_level_index_, first_level_index_ + width_);
        }
        size_t st_off = st - current_batch_;
        size_t ed_off = ed - current_batch_;
        size_t st_index_off = (st_off >> index_ratio_bits_);    // perf shows div is slow, use shift instead
        size_t ed_index_off = (ed_off >> index_ratio_bits_);
        return ConstIndexRange(current_batch_index_ + st_index_off, current_batch_index_ + ed_index_off);
    }

    KeyFunc GetIndexKeyFunc(size_t len) const {
        return KeyFunc(len, vid_start_, width_);
    }

    BucketIndexWrapper<KeyFunc> GetRelatedIndexWrapper(const EdgeType* st, const EdgeType* ed) const {
        auto index = GetRelatedIndexRangeConst(st, ed);
        // size_t index_len = std::bit_floor(index.size());
        size_t index_len = index.size();
        return BucketIndexWrapper<KeyFunc>(index.data(), index_len, GetIndexKeyFunc(index_len));
    }

    BucketIndexWrapper<KeyFunc> GetIndexWrapperOf(size_t idx) const {
        const EdgeType* st = current_batch_ + sorted_ranges_[idx].first;
        const EdgeType* ed = current_batch_ + sorted_ranges_[idx].second;
        return GetRelatedIndexWrapper(st, ed);
    }

    /**
     * @brief Internal only, build group index for a sorted range
     */
    void BuildGroupIndex(EdgeType* begin, EdgeType* end) {
        auto index = GetRelatedIndexRange(begin, end);
        size_t range_len = end - begin;
        // size_t index_len = std::bit_floor(index.size());
        size_t index_len = index.size();
        auto key = GetIndexKeyFunc(index_len);
        build_group_index(std::span<EdgeType>(begin, range_len), index, key);
    }

    /**
     * @brief Determine best merge range.
     * invariant: sorted_ranges size is decreasing (but new_edges_count can larger than last range)
     * @return EdgeType* for best start, size_t for merged range count (exclude new edges)
     */
    std::pair<EdgeType*, size_t> OptimizeMergeRangeStart(size_t new_edges_count) {
        EdgeType* st = current_batch_;
        size_t total = sorted_count_ + new_edges_count;
        size_t count = sorted_ranges_.size();
        for(auto& r: sorted_ranges_) {
            size_t rsize = (r.second - r.first);
            size_t max_rsize = std::max(rsize, new_edges_count);
            if(max_rsize * merge_multiplier_ <= total) {
                return std::make_pair(st, count);
            }
            st += rsize;
            total -= rsize;
            count--;
        }
        return std::make_pair(nullptr, count);
    }

    /**
     * @brief Internal only, sort multiple mini batches, a fast way to keep order
     */
    void SortNextMultipleMiniBatchs(size_t count) {

        // fmt::println("[{:2}]Sort batch: {} ({})", pid_, count, count*minimum_sort_batch_);
        // Decide merge or only sort new mini batchs
        auto [best_st, merged_ranges] = OptimizeMergeRangeStart(count * minimum_sort_batch_);
        size_t new_sorted_count = sorted_count_ + count * minimum_sort_batch_;
        EdgeType* ed = current_batch_ + new_sorted_count;
        if(merged_ranges == 0) {
            // Only sort new mini batchs
            EdgeType* st = current_batch_ + sorted_count_;
            EdgeType* steal_sorted = current_batch_ + steal_sorted_count_;
            size_t len = ed - st;
            bool need_steal = (len > ENABLE_STEAL_THRESHOLD);
            if(need_steal){
                steal_sorted_count_ = new_sorted_count;
                steal_semaphore_.release();
            }
            if(steal_sorted > st) {
                MergeRange(st, steal_sorted, ed);
            } else {
                AdaptiveRangeSort(st, ed);
            }
            sorted_ranges_.append(new_sorted_count);
            BuildGroupIndex(st, ed);
            if(need_steal) {
                steal_semaphore_.acquire();
            }
        } else {
            // Merge
            [[maybe_unused]] size_t len = ed - best_st;
            [[maybe_unused]] EdgeType* unsorted_st = current_batch_ + sorted_count_;
            [[maybe_unused]] EdgeType* steal_sorted = current_batch_ + steal_sorted_count_;
            if(steal_sorted > unsorted_st) {
                unsorted_st = steal_sorted;
            }

            bool need_steal = (len > ENABLE_STEAL_THRESHOLD);
            if(need_steal){
                steal_sorted_count_ = new_sorted_count;
                steal_semaphore_.release();
            }
            
            MergeRange(best_st, unsorted_st, ed);

            sorted_ranges_.append(new_sorted_count);
            sorted_ranges_.merge_end(merged_ranges + 1);
            BuildGroupIndex(best_st, ed);

            if(need_steal) {
                steal_semaphore_.acquire();
            }
        }
        sorted_count_ += count * minimum_sort_batch_;
        
        RUN_IN_DEBUG{
            if(merged_ranges > 0) {
                fmt::println("[{}] Merged {} mini batchs, new sorted count: {}", pid_, count, sorted_count_);
            }
        }
    }

    /**
     * @brief Internal only, sort next minimum sort batch
     */
    void SortNextMiniBatch() {
        // std::lock_guard<MutexType> lock(reading_mutex_);        // 无需锁定，因为只有一个线程会调用

        RUN_IN_DEBUG{
            if(pid_ == 0) {
            fmt::println("[{}] SortNextMiniBatch (sorted: {})", pid_, sorted_count_);
            fmt::println("Sorted count: {}", sorted_count_);
            fmt::println("Visible size: {}", ring_buffer_.VisibleBatchSize());
            }
            // std::span<EdgeType> batch_data(current_batch_ + sorted_count_, minimum_sort_batch_);
            // fmt::println("Batch data: {}", batch_data);
        }


        size_t new_sorted_count = sorted_count_ + minimum_sort_batch_;
        EdgeType* batch_end = current_batch_ + new_sorted_count;
        size_t len = minimum_sort_batch_;

        // First level
        EdgeType* st = batch_end - len;
        SmallRangeSort(st, batch_end);
        sort_times_[0]++;
        sorted_ranges_.append(new_sorted_count);


        // If mini batchs are enough, merge them

        for(size_t level=0; sort_times_[level] == merge_multiplier_; level++) {
            len *= merge_multiplier_;
            st = batch_end - len;

            if(len*sizeof(EdgeType) > L2_CACHE_SIZE * 64) {
                // fmt::println("[{}] Level {} sort: size={}", pid_, level, len);
                [[maybe_unused]] auto t = TimeIt([&](){
                    l2_efficient_sort_inplace(st, len, vid_start_, width_);
                });
                //fmt::println("[{}] Level {} sort time: {:.2f}s", pid_, level, t);
            } else {
                SmallRangeSort(st, batch_end);
            }

            sort_times_[level] = 0;
            sort_times_[level + 1]++;
            sorted_ranges_.merge_end(merge_multiplier_);

            RUN_IN_DEBUG{
                check_sorted(st, len, CmpFrom<EdgeType>());
                check_from_in_range(st, len, vid_start_, vid_start_ + width_);
                // if(pid_ == 0) {
                //     fmt::println("Neighbors of 0:");
                //     for(size_t i = 0; i < len; i++) {
                //         if(st[i].from == 0) {
                //             fmt::print("{}", (VID)st[i].to);
                //         }
                //     }
                //     fmt::println("");
                // }
            }

        }

        sorted_count_ = new_sorted_count;
        BuildGroupIndex(st, batch_end);


        RUN_IN_DEBUG{
            // fmt::println("[{}] Checking, sorted count: {}", pid_, sorted_count_);
            // check_sorted(current_batch_, sorted_count_, CmpFrom<EdgeType>());
            // check_from_in_range(current_batch_, sorted_count_, vid_start_, vid_start_ + width_);
            dcsr_assert(sorted_ranges_.back().second == sorted_count_, "Unexpected sorted count");
            if(pid_ == 0) {
                std::vector<size_t> tmp;
                bool has6 = false;
                // for(size_t i = 0; i < len; i++) {
                //     if(st[i].from == 0) {
                //         tmp.push_back(st[i].to);
                //         if(st[i].to == 6) {
                //             has6 = true;
                //         }
                //     }
                // }
                for(size_t i = 0; i < sorted_count_; i++) {
                    if(current_batch_[i].from == 0) {
                        tmp.push_back(current_batch_[i].to);
                        if(current_batch_[i].to == 6) {
                            has6 = true;
                        }
                    }
                }
                if(tmp.size() > 0u && has6 != 0) {
                    fmt::println("[Merged] Neighbors of 0: {}", tmp.size());
                    fmt::println("{}", tmp);
                    fmt::println("Sorted: ");
                    std::sort(tmp.begin(), tmp.end());
                    fmt::println("{}", tmp);
                }
            }
        }
    }

};


template<typename Weight, typename VType=VID64, bool NeighborsOrder=false, bool StdSort=false, size_t MAX_MEM_PARTS_CNT=128, size_t MAX_PARTS_CNT=128>
class Graph {
public:
    using WeightType = Weight;
    using VertexType = VType;
    using VID = VType;
    using EdgeType = RawEdge<WeightType, VType>;
    using TargetType = CompactTarget<WeightType>;
    // using MemPartType = MemPartition<EdgeType>;
    using MemPartType = SortBasedMemPartition<EdgeType, NeighborsOrder, StdSort>;
    // using PartitionType = Partition<EdgeType>;
    using MutexType = SpinMutex;

    template<typename T, size_t N>
    using StaticVector = boost::container::static_vector<T, N>;
private:
    // Memory components
    // std::array<MemPartType, MAX_MEM_PARTS_CNT> mem_parts_;
    // size_t mem_parts_count_;
    StaticVector<MemPartType, MAX_MEM_PARTS_CNT> mem_parts_;


    size_t max_vertex_count_;       // 仅通过 AddMemPartition() 修改以上三个成员
    size_t vertex_count_;
    size_t edge_count_;
    const size_t part_width_;
    // const size_t bits_per_partition_;
    const size_t buffer_size_;
    const size_t buffer_count_;
    const size_t sort_batch_;
    const size_t graph_id_;

    // Disk components
    // std::array<PartitionType, MAX_PARTS_CNT> partitions_;
    // size_t parts_count_;

    // Synchonization
    // std::atomic<size_t> enqueue_buffers_count_;
    // std::atomic<size_t> flushed_buffers_count_;
    // MutexType mutex_;  // for partition compaction and visit
    std::atomic_flag read_flag_;
    StaticVector<std::scoped_lock<MutexType>, MAX_MEM_PARTS_CNT> read_locks_;

    // Threads
    std::vector<std::jthread> writer_threads_;
    CoreSet available_cores_;


    // Global config
    const Config config_;               // config backup
    const bool auto_scale_;
    // const size_t compact_threshold_;
    const fs::path path_;

    // Metrics
    std::atomic<size_t> total_sleep_millis_;

private:
    size_t mem_parts_count() const {
        return mem_parts_.size();
    }

public:
    Graph(const fs::path& path, Config config, size_t graph_id=1)
        :   max_vertex_count_{0},
            vertex_count_{config.init_vertex_count},
            edge_count_{0},
            part_width_{config.partition_size},
            // part_width_{std::bit_ceil(config.partition_size)},
            // bits_per_partition_{std::bit_width(part_width_ - 1)},
            buffer_size_{std::bit_ceil(config.buffer_size)},
            buffer_count_{config.buffer_count},
            sort_batch_{config.sort_batch_size},
            graph_id_{graph_id},
            read_flag_{},
            read_locks_{},
            config_{config},
            auto_scale_{config.auto_extend},
            // compact_threshold_{config.compaction_threshold},
            path_{path}
    {
        fmt::println("{}", config);
        if(!fs::exists(path)) {
            fs::create_directories(path);
        }
        if(!fs::is_directory(path)) {
            throw std::runtime_error("Path is not a directory");
        }

        if(config.bind_numa) {
            available_cores_ = GetLogicalCoresOnNumaNode(graph_id_);
        } else {
            available_cores_ = GetAllLogicalCores();
        }
        
        AllocateCore(); // Allocate one core for main thread
        // fmt::println("Init available cores: {}", available_cores_.to_string());

        // Initial memory partitions = ceil(vertex_count / part_width)
        auto req_parts = div_up(config.init_vertex_count, part_width_);
        ExtendBlocks(req_parts);
        // fmt::println("part_width_={}, bits_per_partition_={}, buffer_size_={}, mem_parts_count()={}", part_width_, bits_per_partition_, buffer_size_, mem_parts_count());

        // Wait all writer got lock
        for(size_t i = 0; i < mem_parts_count(); i++) {
            mem_parts_[i].WaitInitialized();
        }
    }

    ~Graph() {
        fmt::println("Total sleep millis: {}", total_sleep_millis_.load());
    }

    void AddMemPartition() {
        size_t pid = mem_parts_count();
        int numa_node = (pid % GetNumaNodeCount()) ^ graph_id_; // interleave numa node
        mem_parts_.emplace_back(
            pid,                  // Memory Partition ID
            pid * part_width_,    // Memory Partition Start Vertex ID
            part_width_,          // Memory Partition Vertex Count
            numa_node,            // Memory Partition Numa Node
            config_               // Config
        );
    }

    void AddBlock() {
        fmt::println("Adding block");
        
        StopWatch t;
        this->AddMemPartition();
        auto t1 = t.Lap();
        // this->AddPartition();
        auto t2 = t.Lap();
        int core = AllocateCore();
        max_vertex_count_ = mem_parts_count() * part_width_;
        this->writer_threads_.emplace_back(std::bind_front(&Graph::WriterLoop, this), mem_parts_count() - 1, core);
        auto t3 = t.Lap();
        fmt::println("AddBlock: AddMemPartition: {:.2f}s, AddPartition: {:.2f}s, WriterLoop: {:.2f}s", t1, t2, t3);

    }

    void ExtendBlocks(size_t required_parts) {
        for(size_t i = mem_parts_count(); i < required_parts; i++) {
            AddBlock();
        }
    }

    // Update API

    void AddEdgeMultiThread(EdgeType e, size_t thread_id) {
        if(auto_scale_) {
            size_t max_vid = std::max(e.from, e.to);
            if(max_vid >= vertex_count_) [[unlikely]] {
                vertex_count_ = max_vid + 1;
            }
            if(max_vid >= max_vertex_count_) [[unlikely]] {
                // auto need_parts = (max_vid >> bits_per_partition_) + 1;
                auto need_parts = GetPid(max_vid) + 1;
                ExtendBlocks(need_parts);
            }
        }
        // edge_count_ ++;
        // if(e.from < 50 && e.to < 50) {
        //     fmt::println("AddEdge: {} -> {}", e.from, e.to);
        // }
        mem_parts_[GetPid(e.from)].AddEdgeMultiThread(e, thread_id);
    }

    void AddEdge(EdgeType e) {
        AddEdgeMultiThread(e, 0);
    }

    void Collect() {
        for(size_t i = 0; i < mem_parts_count(); i++) {
            mem_parts_[i].Collect();
        }
    }

    // Qurey API

    void WaitSortingAndPrepareAnalysisNoWait() {
        read_flag_.test_and_set(std::memory_order_acquire);
    }

    void WaitToPrepared() {
        for(size_t i = 0; i < mem_parts_count(); i++) {
            auto& part = mem_parts_[i];
            read_locks_.emplace_back(part.GetReadingMutex());
        }
    }

    void WaitSortingAndPrepareAnalysis() {
        read_flag_.test_and_set(std::memory_order_acquire);
        for(size_t i = 0; i < mem_parts_count(); i++) {
            auto& part = mem_parts_[i];
            read_locks_.emplace_back(part.GetReadingMutex());
        }
    }

    void BuildBitmapParallel() {
        std::vector<std::thread> threads;
        for(size_t i = 0; i < mem_parts_count(); i++) {
            auto& part = mem_parts_[i];
            threads.emplace_back([&part](){
                part.BuildBitmap();
            });
        }
        for(auto& t: threads) {
            t.join();
        }
    }

    void FinishAlgorithm() {
        read_flag_.clear(std::memory_order_seq_cst);
        read_flag_.notify_all();
        read_locks_.clear();
        for(size_t i = 0; i < mem_parts_count(); i++) {
            auto& part = mem_parts_[i];
            part.InvalidateBitmap();
        }
    }

    // Metrics
    size_t TotalSleepMillis() const {
        return total_sleep_millis_.load();
    }

    // Meta Infomation

    size_t VertexCount() const {
        return vertex_count_;
    }

    size_t EdgeCount() const {
        return edge_count_;
    }

    std::vector<EdgeType> GetNeighborsVectorInMemory(VID v) {
        // auto pid = v >> bits_per_partition_;
        auto pid = GetPid(v);
        return mem_parts_[pid].GetNeighborsVector(v);
    }

    template<typename Func>
        requires std::invocable<Func, VID>
    void IterateNeighborsInMemory(VID v, const Func& func) const {
        // auto pid = v >> bits_per_partition_;
        auto pid = GetPid(v);
        mem_parts_[pid].IterateNeighbors(v, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID>
    void IterateNeighbors(VID v, const Func& func) const {
        IterateNeighborsInMemory(v, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsRangeInLevel(VID v1, VID v2, size_t level, const Func& func) const {
        // auto pid1 = v1 >> bits_per_partition_;
        // auto pid2 = (v2-1) >> bits_per_partition_;
        auto pid1 = GetPid(v1);
        auto pid2 = GetPid(v2 - 1);
        for(size_t pid = pid1; pid <= pid2; pid++) {
            mem_parts_[pid].IterateNeighborsRangeInLevel(v1, v2, level, func);
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsRange(VID v1, VID v2, const Func& func) const {
        // auto pid1 = v1 >> bits_per_partition_;
        // auto pid2 = (v2-1) >> bits_per_partition_;
        auto pid1 = GetPid(v1);
        auto pid2 = GetPid(v2 - 1);
        for(size_t pid = pid1; pid <= pid2; pid++) {
            mem_parts_[pid].IterateNeighborsRange(v1, v2, func);
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRangeInLevel(VID v1, VID v2, size_t sample_count, size_t level, const Func& func) const {
        // auto pid1 = v1 >> bits_per_partition_;
        // auto pid2 = (v2-1) >> bits_per_partition_;
        auto pid1 = GetPid(v1);
        auto pid2 = GetPid(v2 - 1);
        for(size_t pid = pid1; pid <= pid2; pid++) {
            mem_parts_[pid].SampleNeighborsRangeInLevel(v1, v2, sample_count, level, func);
        }
    }


    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRange(VID v1, VID v2, size_t sample_count, const Func& func) const {
        // auto pid1 = v1 >> bits_per_partition_;
        // auto pid2 = (v2-1) >> bits_per_partition_;
        auto pid1 = GetPid(v1);
        auto pid2 = GetPid(v2 - 1);
        for(size_t pid = pid1; pid <= pid2; pid++) {
            mem_parts_[pid].SampleNeighborsRange(v1, v2, sample_count, func);
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRange2(VID v1, VID v2, size_t sample_count, const Func& func) {
        // auto pid1 = v1 >> bits_per_partition_;
        // auto pid2 = (v2-1) >> bits_per_partition_;
        auto pid1 = GetPid(v1);
        auto pid2 = GetPid(v2 - 1);
        for(size_t pid = pid1; pid <= pid2; pid++) {
            mem_parts_[pid].SampleNeighborsRange2(v1, v2, sample_count, func);
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRangeFast(VID v1, VID v2, size_t sample_count, const Func& func) const {
        // auto pid1 = v1 >> bits_per_partition_;
        // auto pid2 = (v2-1) >> bits_per_partition_;
        auto pid1 = GetPid(v1);
        auto pid2 = GetPid(v2 - 1);
        for(size_t pid = pid1; pid <= pid2; pid++) {
            mem_parts_[pid].SampleNeighborsRangeFast(v1, v2, sample_count, func);
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsRangeDensityAware(VID v1, VID v2, size_t sample_count, const Func& func) const {
        auto pid1 = GetPid(v1);
        auto pid2 = GetPid(v2 - 1);
        for(size_t pid = pid1; pid <= pid2; pid++) {
            mem_parts_[pid].SampleNeighborsRangeDensityAware(v1, v2, sample_count, func);
        }
    }

    template<typename Func>
        requires std::invocable<Func, VID>
    void IterateNeighborsInOrder(VID v, const Func& func) const {
        // auto pid = v >> bits_per_partition_;
        auto pid = GetPid(v);
        mem_parts_[pid].IterateNeighborsInOrder(v, func);
    } 

    void ValidateBitmap() {
        for(size_t i = 0; i < mem_parts_count(); i++) {
            mem_parts_[i].ValidateBitmap();
        }
    }

    size_t GetDegreeInMemory(VID v) const {
        // auto pid = v >> bits_per_partition_;
        auto pid = GetPid(v);
        return mem_parts_[pid].GetDegree(v);
    }

    size_t GetDegree(VID v) const {
        return GetDegreeInMemory(v);
    }

private:
    size_t GetPid(VID v) const {
        return v / part_width_;
        // const static size_t bits_per_partition_ = std::bit_width(part_width_ - 1);
        // return v >> bits_per_partition_;
    }

    int AllocateCore() {
        // fmt::println("Available cores: {}", available_cores_.to_string());
        for(size_t i = 0; i < available_cores_.size(); i++) {
            if(available_cores_.test(i)) {
                available_cores_.reset(i);
                return i;
            }
        }
        dcsr_assert(false, "No available core");
        return -1;
    }

    void WriterLoop(std::stop_token stop_token, size_t worker_id, int core) {
        // SetAffinityThisThread((worker_id + start_physical_core_) * 2);
        if(config_.bind_core) {
            SetAffinityThisThread(core);
        }
        const size_t mem_part_id = worker_id;
        MemPartType& mem_part = mem_parts_[mem_part_id];
        fmt::println("[Worker {}:{:2}] Start writer loop on core {}", graph_id_, worker_id, core);

        bool initialized = false;
        size_t idle = 0;
        size_t sleep_millis = 0;
        size_t consecutive_sleep = 0;

        size_t stealing_part_id = (mem_part_id + 1) % mem_parts_count();
        while(!stop_token.stop_requested()) {
            read_flag_.wait(true);  // wait untial read_flag_ is not true
            std::lock_guard<MutexType> lock(mem_part.GetReadingMutex());
            if(!initialized) {
                mem_part.Initialized();
                initialized = true;
            }
            // Internal loop to avoid repeated lock/unlock
            while(!stop_token.stop_requested()) {
                if(read_flag_.test() && mem_part.VisiblePartialSorted()) {
                    break;  // release read lock of mem partition
                }

                bool run_sort = mem_part.SortVisible();
                if(run_sort) {
                    idle = 0;
                    consecutive_sleep = 0;
                } else {
                    idle++;
                }

                bool steal = false;
                if(consecutive_sleep > 2) {
                    // fmt::println("[Worker {}:{:2}] Start stealing", graph_id_, worker_id);
                    for(size_t i = 0; i < mem_parts_count(); i++) {
                        if(stealing_part_id == mem_part_id) {
                            stealing_part_id = (stealing_part_id + 1) % mem_parts_count();
                            break;  // sleep one round
                        }

                        auto& steal_part = mem_parts_[stealing_part_id];
                        steal = steal_part.TrySteal(); //这里要改，如果steal了，就不应该休眠了
                        if(steal) {
                            // fmt::println("[Worker {}:{:2}] Steal from {}", graph_id_, worker_id, stealing_part_id);
                            break;
                        }
                        stealing_part_id = (stealing_part_id + 1) % mem_parts_count();
                    }
                }

                if(idle > 1 && !steal) {
                    constexpr size_t SleepTime = 5;
                    total_sleep_millis_.fetch_add(SleepTime, std::memory_order_relaxed);
                    sleep_millis += SleepTime;
                    idle = 0;
                    consecutive_sleep++;
                    std::this_thread::sleep_for(std::chrono::milliseconds(SleepTime));
                }

            }
            fmt::println("[Worker {}:{:2}] Release reading lock, sleep: {}", graph_id_, worker_id, sleep_millis);
        }
        // fmt::println("[Worker {}:{:2}] Stop writer loop, sleep: {}", graph_id_, worker_id, sleep_millis);
    }

};

template<typename Weight, size_t MAX_MEM_PARTS_CNT=128, size_t MAX_PARTS_CNT=128>
using Graph32 = Graph<Weight, VID32, false, false, MAX_MEM_PARTS_CNT, MAX_PARTS_CNT>;



// 这里要写一个UGraph，来载入无向图，并实现TC，要考虑自动配置。
template<typename Weight, typename VType=VID64, bool NeighborsOrder=true, bool StdSort=false, size_t MAX_MEM_PARTS_CNT=128, size_t MAX_PARTS_CNT=128>
class UGraph {
public:
    using VertexType = VType;
    using VID = VType;
    using GraphType = Graph<Weight, VType, NeighborsOrder, StdSort, MAX_MEM_PARTS_CNT, MAX_PARTS_CNT>;
    using EdgeType = GraphType::EdgeType;
private:
    GraphType g_;

    size_t edge_count_;
    size_t new_edge_count_;
    const size_t dispatch_thread_count_;

public:
    UGraph(const fs::path& path, Config config)
        :   g_(path, config, 0),
            edge_count_{0},
            new_edge_count_{0},
            dispatch_thread_count_{config.dispatch_thread_count}
    { }

    void AddEdgeBatch(std::span<const EdgeType> edges) {
        size_t sz = edges.size();
        edge_count_ += sz;
        new_edge_count_ += sz;
        #pragma omp parallel num_threads(dispatch_thread_count_)
        {
            int tid = omp_get_thread_num();
            #pragma omp for schedule(dynamic, 4096)
            for(size_t i = 0; i < sz; i++) {
                auto e = edges[i];
                g_.AddEdgeMultiThread(e, tid);
                g_.AddEdgeMultiThread(e.Reverse(), tid);
            }
        }
    }

    void Collect() {
        g_.Collect();
    }

    void WaitSortingAndPrepareAnalysis() {
        g_.WaitSortingAndPrepareAnalysis();
    }

    void FinishAlgorithm() {
        g_.FinishAlgorithm();
    }


    const GraphType& GraphView() const {
        return g_;
    }

};

template<typename Weight>
using UGraph32 = UGraph<Weight, VID32, true, false>;


/**
 * @brief Two way graph, store edges in both directions
 */
template<typename Weight, typename VType=VID64, bool NeighborsOrder=false, bool StdSort=false, size_t MAX_MEM_PARTS_CNT=128, size_t MAX_PARTS_CNT=128>
class TGraph {
public:
    using VertexType = VType;
    using VID = VType;
    using GraphType = Graph<Weight, VType, NeighborsOrder, StdSort, MAX_MEM_PARTS_CNT, MAX_PARTS_CNT>;
    using VersionType = std::pair<size_t, size_t>;
    using TargetType = GraphType::TargetType;
    using EdgeType = GraphType::EdgeType;
    using DispatchQueue = moodycamel::BlockingReaderWriterQueue<std::span<const EdgeType>>;
private:
    GraphType gin_;
    GraphType gout_;


    size_t edge_count_;
    size_t new_edge_count_;
    const size_t dispatch_thread_count_;

    // DispatchQueue qin_;
    // std::jthread din_;

    // DispatchQueue qout_;
    // std::jthread dout_;

    // template<bool IsIn>
    // static void Ingest(std::stop_token st, GraphType& g, DispatchQueue& q) {
    //     size_t cnt = 0;
    //     // size_t ecnt = 0;
    //     while(true) {
    //         bool stop = st.stop_requested();
    //         // process all available batches even got stop signal
    //         std::span<const EdgeType> edges;
    //         while(q.wait_dequeue_timed(edges, std::chrono::milliseconds(10))) {
    //             for(auto& e: edges) {
    //                 if constexpr(IsIn) {
    //                     g.AddEdge(e.Reverse());
    //                 } else {
    //                     g.AddEdge(e);
    //                 }
    //                 // ecnt ++;
    //             }
    //             cnt ++;
    //         }

    //         if(stop) {
    //             break;
    //         }
    //     }
    //     if(cnt!=0) {
    //         fmt::println("Ingest {} batches, in: {}", cnt, IsIn);
    //     }
    // }

public:
    TGraph(const fs::path& path, Config config)
        :   gin_(path / "in", config, 0),
            gout_(path / "out", config, 1),
            edge_count_{0},
            new_edge_count_{0},
            dispatch_thread_count_{config.dispatch_thread_count}
    {

        // din_ = std::jthread(Ingest<true>, std::ref(gin_), std::ref(qin_));
        // dout_ = std::jthread(Ingest<false>, std::ref(gout_), std::ref(qout_));
    }

    void AddEdge(EdgeType e) {
        gin_.AddEdge(e.Reverse());
        gout_.AddEdge(e);
    }

    void AddEdgeIn(EdgeType e) {
        gin_.AddEdge(e.Reverse());
    }

    void AddEdgeOut(EdgeType e) {
        gout_.AddEdge(e);
    }

    // void AddEdgeBatch(std::span<const EdgeType> edges) {
    //     qin_.enqueue(edges);
    //     qout_.enqueue(edges);
    //     while(qin_.size_approx() > 500) {
    //         std::this_thread::sleep_for(std::chrono::milliseconds(50));
    //     }
    // }

    void AddEdgeMultiThread(EdgeType e, size_t thread_id) {
        // fmt::println("Add({}): {}", thread_id, e);
        gin_.AddEdgeMultiThread(e.Reverse(), thread_id);
        gout_.AddEdgeMultiThread(e, thread_id);
    }

    void AddEdgeBatch(std::span<const EdgeType> edges) {
        size_t sz = edges.size();
        edge_count_ += sz;
        new_edge_count_ += sz;
        #pragma omp parallel num_threads(dispatch_thread_count_)
        {
            int tid = omp_get_thread_num();
            #pragma omp for schedule(dynamic, 4096)
            for(size_t i = 0; i < sz; i++) {
                gin_.AddEdgeMultiThread(edges[i].Reverse(), tid);
                gout_.AddEdgeMultiThread(edges[i], tid);
            }
        }
        // if(new_edge_count_ > 64 * 1024 * 1024) {
        //     gin_.Collect();
        //     gout_.Collect();
        //     new_edge_count_ = 0;
        // }
    }

    // Deprecated
    VersionType MakeVersion() {
        dcsr_assert(false, "Deprecated");
        return std::make_pair(gin_.MakeVersion(), gout_.MakeVersion());
    }

    // Deprecated
    void WaitVersion(VersionType version) {
        dcsr_assert(false, "Deprecated");
        gin_.WaitVersion(version.first);
        gout_.WaitVersion(version.second);
    }

    // Meta Infomation

    size_t VertexCount() const {
        return gin_.VertexCount();
    }

    size_t EdgeCount() const {
        return gin_.EdgeCount();
    }

    // Deprecated
    std::span<TargetType> GetNeighborsIn(VID v, VersionType version) {
        dcsr_assert(false, "Deprecated");
        gin_.WaitVersion(version.first);
        return gin_.GetNeighbors(v, version.first);
    }

    // Deprecated
    std::span<TargetType> GetNeighborsOut(VID v, VersionType version) {
        dcsr_assert(false, "Deprecated");
        gout_.WaitVersion(version.second);
        return gout_.GetNeighbors(v, version.second);
    }

    double TotalSleepMillis() const {
        return gin_.TotalSleepMillis() + gout_.TotalSleepMillis();
    }

    void Collect() {
        gin_.Collect();
        gout_.Collect();
    }

    void WaitSortingAndPrepareAnalysis() {
        auto st = std::chrono::steady_clock::now();
        gin_.Collect();
        gout_.Collect();
        auto et = std::chrono::steady_clock::now();
        fmt::println("Collect time: {:.2f}s", std::chrono::duration<double>(et - st).count());
        // din_.request_stop();
        // dout_.request_stop();
        // din_.join();
        // dout_.join();
        gin_.WaitSortingAndPrepareAnalysisNoWait();
        gout_.WaitSortingAndPrepareAnalysisNoWait();
        gin_.WaitToPrepared();
        gout_.WaitToPrepared();

        // dcsr_assert(qin_.size_approx() == 0, "qin_ not empty");
        // dcsr_assert(qout_.size_approx() == 0, "qout_ not empty");
    }

    void BuildBitmapParallel() {
        gin_.BuildBitmapParallel();
        gout_.BuildBitmapParallel();
    }

    void FinishAlgorithm() {
        gin_.FinishAlgorithm();
        gout_.FinishAlgorithm();
        // din_ = std::jthread(Ingest<true>, std::ref(gin_), std::ref(qin_));
        // dout_ = std::jthread(Ingest<false>, std::ref(gout_), std::ref(qout_));
    }

    const GraphType& InGraphView() const {
        return gin_;
    }

    const GraphType& OutGraphView() const {
        return gout_;
    }

    size_t GetDegreeIn(VID v) const {
        return gin_.GetDegreeInMemory(v);
    }

    size_t GetDegreeOut(VID v) const {
        return gout_.GetDegreeInMemory(v);
    }

    template<typename Func>
        requires std::invocable<Func, VID>
    void IterateNeighborsIn(VID v, const Func& func) const {
        gin_.IterateNeighborsInMemory(v, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID>
    void IterateNeighborsOut(VID v, const Func& func) const {
        gout_.IterateNeighborsInMemory(v, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsInRangeInLevel(VID v1, VID v2, size_t level, const Func& func) const {
        gin_.IterateNeighborsRangeInLevel(v1, v2, level, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsOutRangeInLevel(VID v1, VID v2, size_t level, const Func& func) const {
        gout_.IterateNeighborsRangeInLevel(v1, v2, level, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsOutRange(VID v1, VID v2, const Func& func) const {
        gout_.IterateNeighborsRange(v1, v2, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID>
    void IterateNeighborsInRange(VID v1, VID v2, const Func& func) const {
        gin_.IterateNeighborsRange(v1, v2, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsInRangesInLevel(VID v1, VID v2, size_t sample_count, size_t level, const Func& func) const {
        gin_.SampleNeighborsRangeInLevel(v1, v2, sample_count, level, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsOutRangesInLevel(VID v1, VID v2, size_t sample_count, size_t level, const Func& func) const {
        gout_.SampleNeighborsRangeInLevel(v1, v2, sample_count, level, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsInRanges(VID v1, VID v2, size_t sample_count, const Func& func) const {
        gin_.SampleNeighborsRange(v1, v2, sample_count, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsOutRanges(VID v1, VID v2, size_t sample_count, const Func& func) const {
        gout_.SampleNeighborsRange(v1, v2, sample_count, func);
    }

    template<typename Func>
        requires std::invocable<Func, VID, VID, size_t>
    void SampleNeighborsOutRanges2(VID v1, VID v2, size_t sample_count, const Func& func) {
        gout_.SampleNeighborsRange2(v1, v2, sample_count, func);
    }

    void ValidateBitmapOut() {
        gout_.ValidateBitmap();
    }
};

template<typename Weight>
using TGraph32 = TGraph<Weight, VID32, false, false, 128, 0>;

template<typename Weight>
using TOGraph32 = TGraph<Weight, VID32, true, false, 128, 0>;

}

#endif // __DCSR_GRAPH_H__