#include <algorithm>
#include <iostream>
#include <vector>
#include <cctype>

#include "concepts.h"

template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
std::vector<std::vector<typename UGraph::VertexType>> PrepareGraph(const UGraph &g) {
    using Vector = std::vector<typename UGraph::VertexType>;
    std::vector<Vector> graph(g.GraphView().VertexCount());
    size_t vcount = g.GraphView().VertexCount();
    #pragma omp parallel for schedule(dynamic, 64)
    for(size_t i = 0; i < vcount; i++) {
        Vector neigh;
        g.GraphView().IterateNeighborsInOrder(i, [&](UGraph::VertexType v) {
            if(v >= i)
                return false;
            if(neigh.size() > 0 && v < neigh.back()) {
                fmt::println("v: {}, neigh.back(): {}", v, neigh.back());
                dcsr::dcsr_assert(false, "Not sorted");
            }
            neigh.push_back(v);
            return true;
        });
        std::swap(graph[i], neigh);
    }
    return graph;
}

//assumes sorted neighbor lists
template <typename V>
long countCommon(uint32_t a, uint32_t b, const std::vector<std::vector<V>>& mp) { 
    long ans=0;
    auto& nei_a = mp[a];
    auto& nei_b = mp[b];
    uint32_t i = 0, j = 0, size_a = nei_a.size(), size_b = nei_b.size();
    if(size_a == 0 || size_b == 0) return 0;

    uint32_t a_v = nei_a[i], b_v = nei_b[j];
    while (i < size_a && j < size_b) { //count "directed" triangles
        a_v = nei_a[i];
        b_v = nei_b[j];
        if (a_v == b_v) {
            ++i;
            ++j;
            ans++;
        }
        else if (a_v < b_v){
            ++i;
        }
        else{
            ++j;
        }
    }
    return ans;
}

template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
uint64_t TC_LSGraph(UGraph& G, const std::vector<std::vector<uint32_t>>& mp) {
    using NodeID = typename UGraph::VertexType;
    uint32_t n = G.GraphView().VertexCount();

    size_t count = 0;
    #pragma omp parallel for reduction(+:count) schedule(dynamic, 64)
    for (uint32_t i = 0; i < n; i++) {
        for (uint32_t j : mp[i]) {
            if (j < i) {
                count += countCommon<NodeID>(i, j, mp);
            } else {
                break;
            }
        }
    }
    fmt::println("count: {}", count);
    return count;
}


template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
uint64_t tc_lsgraph(UGraph* G) {
    SimpleTimer timer;
    auto mp = PrepareGraph<UGraph>(*G);
    double t_prepare = timer.Lap();
    size_t count = TC_LSGraph(*G, mp);
    double t_tc = timer.Lap();
    fmt::println("Triange count: {}", count);
    fmt::println("Prepare: {:.3f}s, TC: {:.3f}s", t_prepare, t_tc);
    return count;
}

