#ifndef __DCSR_DATATYPE_H__
#define __DCSR_DATATYPE_H__

#include <climits>
#include <cstddef>
#include <cstdint>
#include <bit>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <functional>

namespace dcsr {

static_assert(CHAR_BIT == 8);
static_assert(std::endian::native == std::endian::little);

using VID32 = uint32_t;
using VID64 = uint64_t;

using VID = uint64_t; // type of vertex id
using EID = uint64_t; // type of edge id
using PID = uint32_t; // type of partition id

enum class IterateOperator {
    CONTINUE,
    BREAK,
    SKIP_TO_NEXT_VERTEX,
};

//typedef std::shared_ptr<spdlog::logger> LoggerPtr;


/// @brief Untagged edge with weight, if no weight, using RawEdge<void>. Only for preprocessing or data importing.
template<typename Weight, typename Vertex>
struct RawEdge {
    using VertexType = Vertex;
    using WeightType = Weight;
    VertexType from;
    VertexType to;
    WeightType weight;

    RawEdge() = default;
    RawEdge(VertexType src, VertexType dst, WeightType w)
        : from(src), to(dst), weight(w) {}

    RawEdge<Weight, Vertex> Reverse() const {
        return {to, from, weight};
    }
};

template<typename Vertex>
struct RawEdge<void, Vertex> {
    using VertexType = Vertex;
    using WeightType = void;
    VertexType from;
    VertexType to;

    RawEdge() = default;
    RawEdge(VID src, VID dst)
        : from(src), to(dst) {}
    
    RawEdge<void, Vertex> Reverse() const {
        return {to, from};
    }
};

template<typename Weight>
using RawEdge32 = RawEdge<Weight, VID32>;
template<typename Weight>
using RawEdge64 = RawEdge<Weight, VID>;
static_assert(sizeof(RawEdge32<void>) == 8);
static_assert(sizeof(RawEdge64<void>) == 16);


struct Tag {
    bool is_del: 1;
    uint16_t _pad:15;
    Tag() = default;
    Tag(bool is_del): is_del(is_del) {}
    Tag(uint16_t t): is_del(t&1) {}
    operator uint16_t() const {
        return *reinterpret_cast<const uint16_t*>(this);
    }
};

template<typename T>
struct CompactTarget;

/// @brief Edge with weight of type T, if no weight, using Edge<void>
/// @tparam T Edge weight type
template<typename T>
struct alignas(8)
Edge 
{
    using WeightType = T;
    VID from;
    VID to;
    WeightType weight;

    uint16_t tag:16;

    Edge() = default;
    Edge(VID src, VID dst, WeightType w)
        : from(src), to(dst), weight(w), tag(0) {}
    Edge(VID src, VID dst, WeightType w, Tag t)
        : from(src), to(dst), weight(w), tag(t) {}
    Edge(VID src, CompactTarget<T> dst);
    
    template<typename OtherEdge>
    Edge(OtherEdge e) {
        if constexpr (sizeof(OtherEdge) == sizeof(Edge<T>)) {
            memcpy(this, &e, sizeof(Edge<T>));
        } else {
            from = e.from;
            to = e.to;
            weight = e.weight;
            tag = 0;
        }
    }

    Edge<T> Reverse() const {
        return {to, from, weight, tag};
    }

};


template<>
struct alignas(8)
Edge<void>
{
    using WeightType = void;
    VID from;
    VID to: 48;
    uint16_t tag:16;

    Edge() = default;
    Edge(VID src, VID dst)
        : from(src), to(dst), tag(0) {}
    Edge(VID src, VID dst, Tag t)
        : from(src), to(dst), tag(t) {}
    Edge(VID src, CompactTarget<void> dst);

    template<typename OtherEdge>
    Edge(OtherEdge e) {
        if constexpr (sizeof(OtherEdge) == sizeof(Edge<void>)) {
            memcpy(this, &e, sizeof(Edge<void>));
        } else {
            from = e.from;
            to = e.to;
            tag = 0;
        }
    }

    Edge<void> Reverse() const {
        return {to, from, tag};
    }
};


/// @brief Target (CSR item) with weight of type T, if no weight, using CompactTarget<void>
/// @tparam T Weight type
template<typename T>
struct alignas(8)
CompactTarget
{
    using WeightType = T;
    VID vid: 48;
    uint16_t tag: 16;
    WeightType weight;

    CompactTarget() = default;
    CompactTarget(VID v, WeightType w): vid(v), tag(0), weight(w) {}
    CompactTarget(VID v, WeightType w, Tag t): vid(v), tag(t), weight(w) {}
    explicit CompactTarget(Edge<T> e): vid(e.to), tag(e.tag), weight(e.weight) {}

    void SetFromEdge(Edge<T> e) {
        vid = e.to;
        tag = e.tag;
        weight = e.weight;
    }
};

template<>
struct alignas(8)
CompactTarget<void>
{
    using WeightType = void;
    VID vid: 48;
    uint16_t tag: 16;

    CompactTarget() = default;
    CompactTarget(VID v): vid(v), tag(0) {}
    CompactTarget(VID vid, Tag t): vid(vid), tag(t) {}
    explicit CompactTarget(Edge<void> e): vid(e.to), tag(e.tag) {}

    void SetFromEdge(Edge<void> e) {
        auto p = reinterpret_cast<VID*>(this);
        auto q = reinterpret_cast<const VID *>(&e) + 1;
        *p = *q;
    }
};


/// @brief Compact edge (in a unit) with weight of type T, if no weight, using CompactEdge<void>
/// @tparam T Weight type
template<typename T>
struct alignas(8)
CompactEdge {
    using WeightType = T;
    VID to: 48;
    uint16_t from: 16;
    WeightType weight;

    CompactEdge() = default;
    CompactEdge(uint16_t from, VID to): to(to), from(from), weight(0) {}
    CompactEdge(uint16_t from, VID to, WeightType w): to(to), from(from), weight(w) {}
};


template<>
struct alignas(8)
CompactEdge<void>
{
    using WeightType = void;
    VID to: 48;
    uint16_t from: 16;

    CompactEdge() = default;
    CompactEdge(uint16_t from, VID to): to(to), from(from) {}
};


template<typename T>
Edge<T>::Edge(VID src, CompactTarget<T> dst)
        : from(src), to(dst.vid), weight(dst.weight), tag(dst.tag) {}


inline Edge<void>::Edge(VID src, CompactTarget<void> dst)
        : from(src), to(dst.vid), tag(dst.tag) {}


template<typename E>
struct CmpFrom
{
    bool operator()(const E& a, const E& b) const {
        return a.from < b.from;
    }
};

template<typename E>
struct CmpTo
{
    bool operator()(const E& a, const E& b) const {
        return a.to < b.to;
    }
};

template<typename E>
struct CmpFromTo
{
    bool operator()(const E& a, const E& b) const {
        if constexpr(sizeof(E) == 8) {
            uint64_t keya = (static_cast<uint64_t>(a.from) << 32) | a.to;
            uint64_t keyb = (static_cast<uint64_t>(b.from) << 32) | b.to;
            return keya < keyb;
        } else {
            return (a.from < b.from) | ((a.from == b.from) & (a.to < b.to));
        }

    }

};

} // namespace dcsr

#endif