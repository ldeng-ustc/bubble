#ifndef __DCSR_CONCEPTS_H_
#define __DCSR_CONCEPTS_H_

#include <concepts>
#include <type_traits>
#include <cstdint>
#include <cstddef>

namespace dcsr {

template<typename GraphType>
concept GraphMetaInfo = requires(const GraphType& g) {
    // Has VertexType
    requires std::convertible_to<typename GraphType::VertexType, size_t>;
    
    // Can get total vertex count
    { g.VertexCount() } -> std::convertible_to<size_t>;

    // Can get total edge count
    { g.EdgeCount() } -> std::convertible_to<size_t>;
};

// A concept for a graph can iterator edge
template<typename GraphType>
concept BasicIterableGraph = requires(const GraphType& g, int& output) {
    // Can get meta info
    requires GraphMetaInfo<GraphType>;
    
    // Can iterate neighbors
    { g.IterateNeighbors(0, [](GraphType::VertexType v){ (void)v; }) };

    // Can get vertex degree
    { g.GetDegree(0) } -> std::convertible_to<size_t>;
};

template<typename GraphType>
concept BasicIterableTwoWayGraph = requires(const GraphType& g, int& output) {
    // Can get meta info
    requires GraphMetaInfo<GraphType>;
    
    // Can iterate neighbors in two way (in/out)
    { g.IterateNeighborsIn(0, [](GraphType::VertexType v){ (void)v; }) };
    { g.IterateNeighborsOut(0, [](GraphType::VertexType v){ (void)v; }) };

    // Can get vertex degree
    { g.GetDegreeIn(0) } -> std::convertible_to<size_t>;
    { g.GetDegreeOut(0) } -> std::convertible_to<size_t>;
};

template<typename GraphType>
concept ConditionalStopIterableTwoWayGraph = requires(const GraphType& g, int& output) {
    // Can get meta info
    requires GraphMetaInfo<GraphType>;
    
    // Can iterate neighbors in two way (in/out)
    { g.IterateNeighborsIn(0, [](GraphType::VertexType v){ (void)v; }) };
    { g.IterateNeighborsOut(0, [](GraphType::VertexType v){ (void)v; }) };
    
    // Can stop iteration by condition
    { g.IterateNeighborsIn(0, [](GraphType::VertexType v){ return v == 5u;}) };
    { g.IterateNeighborsOut(0, [](GraphType::VertexType v){ return v == 5u;}) };

    // Can get vertex degree
    { g.GetDegreeIn(0) } -> std::convertible_to<size_t>;
    { g.GetDegreeOut(0) } -> std::convertible_to<size_t>;
};

template<typename GraphType>
concept UndirectedGraph = requires(const GraphType& g, int& output) {
    // { g.GraphView() } -> BasicIterableGraph;
    
    { g.GraphView().IterateNeighborsInOrder(0, [](GraphType::VertexType v){ (void)v; }) };
};

} // namespace dcsr


#endif // __DCSR_CONCEPTS_H_