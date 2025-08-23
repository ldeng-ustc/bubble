// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details

// Encourage use of gcc's parallel algorithms (for sort for relabeling)
#ifdef _OPENMP
  #define _GLIBCXX_PARALLEL
#endif

#include <algorithm>
#include <iostream>
#include <vector>
#include <cctype>

/*
GAP Benchmark Suite
Kernel: Triangle Counting (TC)
Author: Scott Beamer

Will count the number of triangles (cliques of size 3)

Input graph requirements:
  - undirected
  - has no duplicate edges (or else will be counted as multiple triangles)
  - neighborhoods are sorted by vertex identifiers

Other than symmetrizing, the rest of the requirements are done by SquishCSR
during graph building.

This implementation reduces the search space by counting each triangle only
once. A naive implementation will count the same triangle six times because
each of the three vertices (u, v, w) will count it in both ways. To count
a triangle only once, this implementation only counts a triangle if u > v > w.
Once the remaining unexamined neighbors identifiers get too big, it can break
out of the loop, but this requires that the neighbors are sorted.

This implementation relabels the vertices by degree. This optimization is
beneficial if the average degree is sufficiently high and if the degree
distribution is sufficiently non-uniform. To decide whether to relabel the
graph, we use the heuristic in WorthRelabelling.
*/


using namespace std;

template <class UGraph>
    requires dcsr::UndirectedGraph<UGraph>
size_t OrderedCount(const UGraph &g) {
    using NodeID = typename UGraph::VertexType;
    size_t num_nodes = g.GraphView().VertexCount();
    size_t total = 0;

    #pragma omp parallel reduction(+ : total)
    {
        std::vector<NodeID> u_neighbors;
        #pragma omp for schedule(dynamic, 64)
        for (NodeID u=0; u < num_nodes; u++) {
            u_neighbors.clear();
            g.GraphView().IterateNeighborsInOrder(u, [&](NodeID v) {
                if(v > u)
                    return false;
                u_neighbors.push_back(v);
                return true;
            });

            // fmt::println("u: {}, neighbors: {}", u, u_neighbors);

            for (NodeID v : u_neighbors) {
                auto it = u_neighbors.begin();
                g.GraphView().IterateNeighborsInOrder(v, [&](NodeID w) {
                    if (w > v) {
                        return false;
                    }
                    while (it < u_neighbors.end() && *it < w)
                        it++;
                    if(it == u_neighbors.end())
                        return false;
                    if (w == *it) {
                        total++;
                        // return false;
                    }
                    return true;
                });
            }
        }
    }

    return total;
}

template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
std::vector<std::vector<typename UGraph::VertexType>> PrepareGraphGABPS(const UGraph &g) {
    using Vector = std::vector<typename UGraph::VertexType>;
    std::vector<Vector> graph(g.GraphView().VertexCount());
    size_t vcount = g.GraphView().VertexCount();
    #pragma omp parallel for schedule(dynamic, 64)
    for(size_t i = 0; i < vcount; i++) {
        auto &neigh = graph[i];
        g.GraphView().IterateNeighborsInOrder(i, [&](UGraph::VertexType v) {
            if(v >= i)
                return false;
            // if(i < 50)
            //     fmt::println("i: {}, v: {}", i, v);
            // if(neigh.size() > 0 && v < neigh.back()) {
            //     fmt::println("v: {}, neigh.back(): {}", v, neigh.back());
            //     dcsr::dcsr_assert(false, "Not sorted");
            // }
            neigh.push_back(v);
            return true;
        });
    }
    return graph;
}

template <typename NodeID>
static std::vector<size_t> ParallelPrefixSum(const std::vector<NodeID>& degrees) {
    const size_t block_size = 1<<20;
    const size_t num_blocks = (degrees.size() + block_size - 1) / block_size;
    std::vector<size_t> local_sums(num_blocks);
    #pragma omp parallel for
    for (size_t block=0; block < num_blocks; block++) {
        size_t lsum = 0;
        size_t block_end = std::min((block + 1) * block_size, degrees.size());
        for (size_t i=block * block_size; i < block_end; i++)
            lsum += degrees[i];
        local_sums[block] = lsum;
    }
    std::vector<size_t> bulk_prefix(num_blocks+1);
    size_t total = 0;
    for (size_t block=0; block < num_blocks; block++) {
        bulk_prefix[block] = total;
        total += local_sums[block];
    }
    bulk_prefix[num_blocks] = total;
    std::vector<size_t> prefix(degrees.size() + 1);
    #pragma omp parallel for
    for (size_t block=0; block < num_blocks; block++) {
        size_t local_total = bulk_prefix[block];
        size_t block_end = std::min((block + 1) * block_size, degrees.size());
        for (size_t i=block * block_size; i < block_end; i++) {
            prefix[i] = local_total;
            local_total += degrees[i];
        }
    }
    prefix[degrees.size()] = bulk_prefix[num_blocks];
    return prefix;
}

// static DestID_** GenIndex(const pvector<SGOffset> &offsets, DestID_* neighs) {
//     NodeID_ length = offsets.size();
//     DestID_** index = new DestID_*[length];
//     #pragma omp parallel for
//     for (NodeID_ n=0; n < length; n++)
//       index[n] = neighs + offsets[n];
//     return index;
//   }

template <typename NodeID>
using SimpleCSR = std::pair<std::vector<std::span<NodeID>>, std::unique_ptr<NodeID[]>>;

template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
SimpleCSR<typename UGraph::VertexType> PrepareGraphGABPSNew(const UGraph &g) {
    using NodeID = typename UGraph::VertexType;
    using Vector = std::vector<typename UGraph::VertexType>;
    std::vector<Vector> graph(g.GraphView().VertexCount());
    size_t vcount = g.GraphView().VertexCount();
    size_t ecount = 0;

    #pragma omp parallel for schedule(dynamic, 64) reduction(+ : ecount)
    for(size_t i = 0; i < vcount; i++) {
        omp_get_thread_num();
        Vector neigh;
        g.GraphView().IterateNeighborsInOrder(i, [&](UGraph::VertexType v) {
            // if(v >= i)
            //     return false;
            // if(i < 50)
            //     fmt::println("i: {}, v: {}", i, v);
            // if(neigh.size() > 0 && v < neigh.back()) {
            //     fmt::println("v: {}, neigh.back(): {}", v, neigh.back());
            //     dcsr::dcsr_assert(false, "Not sorted");
            // }
            neigh.push_back(v);
            ecount++;
            return true;
        });
        std::swap(graph[i], neigh);
    }

    size_t num_nodes = vcount;
    typedef std::pair<int64_t, NodeID> DegreeNodePair;
    std::vector<DegreeNodePair> degree_id_pairs(num_nodes);
    #pragma omp parallel for
    for(NodeID n = 0; n < num_nodes; n++) {
        degree_id_pairs[n] = DegreeNodePair(graph[n].size(), n);
    }
    std::sort(degree_id_pairs.begin(), degree_id_pairs.end(), std::greater<DegreeNodePair>());
    std::vector<NodeID> degrees(num_nodes);
    std::vector<NodeID> new_ids(num_nodes);

    #pragma omp parallel for
    for(NodeID n = 0; n < num_nodes; n++) {
        degrees[n] = degree_id_pairs[n].first;
        new_ids[degree_id_pairs[n].second] = n;
    }

    std::vector<size_t> offsets = ParallelPrefixSum(degrees);
    auto neighs = std::make_unique<NodeID[]>(offsets[num_nodes]);

    #pragma omp parallel for
    for(NodeID u=0; u < num_nodes; u++) {
        size_t off = offsets[new_ids[u]];
        for(NodeID v: graph[u]) {
            neighs[off++] = new_ids[v];
        }
        std::sort(neighs.get() + offsets[new_ids[u]], neighs.get() + off);
    }

    SimpleCSR<NodeID> csr;
    csr.first.resize(num_nodes);
    csr.second = std::move(neighs);
    #pragma omp parallel for
    for(NodeID n = 0; n < num_nodes; n++) {
        size_t off_st = offsets[n];
        size_t off_ed = n == num_nodes - 1 ? ecount : offsets[n + 1];
        size_t len = off_ed - off_st;
        csr.first[n] = std::span<NodeID>(csr.second.get() + off_st, len);
    }
    return csr;
}

template <class UGraph>
    requires dcsr::UndirectedGraph<UGraph>
size_t OrderedCountPrepared(const UGraph &g) {
    using NodeID = typename UGraph::VertexType;

    SimpleTimer timer;
    const auto& neighbors = PrepareGraphGABPS(g);
    // const auto& [neighbors, edges] = PrepareGraphGABPSNew(g);
    fmt::println("PrepareGraph: {:.3f}s", timer.Lap());

    size_t num_nodes = g.GraphView().VertexCount();
    size_t total = 0;

    // std::vector<NodeID> no_empty;
    // for (NodeID i = 0; i < num_nodes; i++) {
    //     if (neighbors[i].size() > 0) {
    //         no_empty.push_back(i);
    //     }
    // }

    fmt::println("Remove empty: {:.3f}s", timer.Lap());

    #pragma omp parallel for reduction(+ : total) schedule(dynamic, 64)
    for (NodeID u=0; u < num_nodes; u++) {
    // for (NodeID u : no_empty) {
        for (NodeID v : neighbors[u]) {
            if(v >= u) {
                break;
            }
            auto it = neighbors[u].begin();
            for (NodeID w : neighbors[v]) {
                if (w >= v) {
                    break;
                }
                while (it < neighbors[u].end() && *it < w)
                    it++;
                if(it == neighbors[u].end())
                    break;
                if (w == *it) {
                    total++;
                }
            }
        }
    }
    fmt::println("Count: {:.3f}s", timer.Lap());

    return total;
}

// Uses heuristic to see if worth relabeling
template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
size_t Hybrid(const UGraph &g) {
    return OrderedCount(g);
}

template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
size_t tc_gapbs(const UGraph *g) {
    size_t triangles_count = Hybrid(*g);
    return triangles_count;
}

template <typename UGraph>
    requires dcsr::UndirectedGraph<UGraph>
size_t tc_gapbs_cached(const UGraph *g) {
    size_t triangles_count = OrderedCountPrepared(*g);
    return triangles_count;
}
