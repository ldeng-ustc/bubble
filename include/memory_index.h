#ifndef __DCSR_MEMORY_INDEX_H__
#define __DCSR_MEMORY_INDEX_H__

#include <array>
#include <vector>
#include "fmt/format.h"

#include "common.h"
#include "config.h"
#include "datatype.h"
#include "small_bitset.h"

namespace dcsr {

using CacheLine = std::array<std::byte, 64>;


class CacheLinePool {
private:
    std::unique_ptr<CacheLine[]> pool_;
    size_t pool_size_;
    size_t used_count_;
    std::vector<CacheLine> extents_;

public:
    CacheLinePool() : pool_{nullptr}, pool_size_{0}, used_count_{0}, extents_{} {}

    void Init(size_t size) {
        pool_size_ = size;
        pool_ = std::make_unique<CacheLine[]>(pool_size_);
        used_count_ = 0;
    }

    size_t Alloc(size_t count) {
        if (used_count_ + count <= pool_size_) {
            size_t start = used_count_;
            used_count_ += count;
            return start;
        } else {
            size_t start = extents_.size();
            extents_.resize(extents_.size() + count);
            return start;
        }
    }

    size_t AllocBytes(size_t bytes) {
        size_t count = div_up(bytes, sizeof(CacheLine));
        return Alloc(count);
    }

    template<typename T>
    T* GetAs(size_t idx) {
        if (idx < pool_size_) {
            return reinterpret_cast<T*>(&pool_[idx]);
        } else {
            return reinterpret_cast<T*>(&extents_[idx - pool_size_]);
        }
    }

};

template<typename Weight, size_t ELEM_SIZE>
struct alignas(64) 
UnitIndexElement
{
    using CompactEdgeType = CompactEdge<Weight>;
    using WeightType = Weight;
    using VertexType = VID;

    union EdgeOrPointer {
        CompactEdgeType edge;
        UnitIndexElement<Weight, ELEM_SIZE> *same_ptr;
        UnitIndexElement<Weight, ELEM_SIZE*2> *double_ptr;
        UnitIndexElement<Weight, ELEM_SIZE*4> *quad_ptr;
    };

    constexpr static size_t MAX_EDGES = (ELEM_SIZE - 8) / sizeof(CompactEdgeType);

    uint16_t edge_count;
    uint16_t _pad;
    SmallBitset<uint32_t> has_next_level;

    std::array<EdgeOrPointer, MAX_EDGES> elems;

    UnitIndexElement() = default;

    void Init() {
        edge_count = 0;
        is_next_level.reset();
    }

    bool TryAddEdge(uint16_t from, VertexType to) {
        static_assert(std::is_void_v<Weight>, "Only void edge can call this function");
        // fmt::println("Adding edge from {} to {}", from, to);
        // fmt::println("Edge count: {}", edge_count);
        if(edge_count == MAX_EDGES) {
            return false;
        }
        elems[edge_count].edge.from = from;
        elems[edge_count].edge.to = to;
        edge_count ++;
        return true;
    }

    template<typename W=Weight>
    bool TryAddEdge(uint16_t from, VertexType to, W w) {
        static_assert(!std::is_void_v<Weight>, "Only non-void edge can call this function");
        static_assert(std::is_same_v<W, Weight>, "Only non-void edge can call this function");
        
        if(edge_count == MAX_EDGES) {
            return false;
        }
        elems[edge_count].edge.from = from;
        elems[edge_count].edge.to = to;
        elems[edge_count].edge.weight = w;
        edge_count ++;
        return true;
    }


};
static_assert(std::is_standard_layout_v<UnitIndexElement<void, 64>>);

struct alignas(8)
LargeNode {
    uint16_t src;
    uint16_t count;
    uint32_t next;
};

template<typename Weight>
class MemoryIndexPartition {
public:
    using WeightType = Weight;
    using InsertEdgeType = RawEdge64<WeightType>;
    using CompactEdgeType = CompactEdge<WeightType>;
    using CompactTargetType = CompactTarget<WeightType>;

    using IndexElementType = UnitIndexElement<WeightType>;
    
    size_t _first_count = 0;
    size_t _large_count = 0;
    size_t _full_count = 0;
    size_t _large_v = 0;
    size_t _full_v = 0;
    
private:
    VID vertex_start_;
    size_t vertex_count_;
    size_t pid_;

    size_t unit_width_;
    size_t unit_width_bits_;
    size_t unit_count_;
    std::unique_ptr<IndexElementType[]> index_elements_;    // First level index elements

    CacheLinePool cache_line_pool_;

private:

    struct UnitIdPart {
        size_t unit_id;
        uint16_t in_unit_vertex_id;
    };

    UnitIdPart VertexIdSplit(uint64_t vid, uint64_t unit_width_bits) const {
        dcsr_assume(unit_width_bits < 16);
        vid = vid - vertex_start_;
        uint64_t unit_width = 1ull << unit_width_bits;
        return {vid/unit_width, static_cast<uint16_t>(vid%unit_width)};
    }

    size_t UnitId(VID vid, size_t unit_width) const {
        return VertexIdSplit(vid, unit_width).unit_id;
    }

    uint16_t UnitVertexId(VID vid, size_t unit_width) const {
        return VertexIdSplit(vid, unit_width).in_unit_vertex_id;
    }


public:
    MemoryIndexPartition() = default;

    MemoryIndexPartition(size_t pid, VID vstart, size_t vcount, size_t unit_width) {
        Init(pid, vstart, vcount, unit_width);
    }

    ~MemoryIndexPartition() {
        fmt::println("Level 1 count: {}", _first_count);
        fmt::println("Large count: {}", _large_count);
        fmt::println("Full count: {}", _full_count);
        fmt::println("Large unit: {}", _large_v);
        fmt::println("Full unit: {}", _full_v);

        double l1_gb = sizeof(IndexElementType) * unit_count_ / 1024.0 / 1024.0 / 1024.0;
        fmt::println("L1: {:.2f}GiB", l1_gb);

        // std::vector<size_t> hist;
        // hist.resize(unit_width_+1, 0);

        // for(size_t i = 0; i < unit_count_; i++) {
        //     hist[index_elements_[i].edge_count] ++;
        // }

        // for(size_t i = 0; i < hist.size(); i++) {
        //     fmt::println("Degree: {}, edges: {}", i, hist[i]);
        // }

    }

    void Init(size_t pid, VID vstart, size_t vcount, size_t unit_width) {
        dcsr_assert(std::has_single_bit(vcount), "Vertex count must be aligned to 2^k");
        dcsr_assert(std::has_single_bit(unit_width), "Unit width must be aligned to 2^k");

        fmt::println("First level unit width: {}, max edges: {}", unit_width, IndexElementType::MAX_EDGES);

        vertex_start_ = vstart;
        vertex_count_ = vcount;
        pid_ = pid;

        unit_width_ = unit_width;
        unit_width_bits_ = std::bit_width(unit_width - 1);
        unit_count_ = div_up(vertex_count_, unit_width);
        index_elements_ = std::make_unique<IndexElementType[]>(unit_count_);

    }

    void AddEdge(InsertEdgeType e) {
        auto [unit_id, index_vid] = VertexIdSplit(e.from, unit_width_bits_);
        auto& elem = index_elements_[unit_id];

        bool ok = false;
        if constexpr (std::is_void_v<WeightType>) {
            ok = elem.TryAddEdge(index_vid, e.to);
        } else {
            ok = elem.TryAddEdge(index_vid, e.to, e.weight);
        }

        if(ok) {
            _first_count ++;
            return;
        }

    }

    const IndexElementType& GetUnit(VID vid) const {
        // fmt::println("Unit ID is {} (VID {})", UnitId(vid), UnitVid(vid));
        return index_elements_[UnitId(vid)];
    }
    

};



} // namespace dcsr

#endif // __DCSR_MEMORY_INDEX_H__