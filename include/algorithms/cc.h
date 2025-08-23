#ifndef __DCSR_CC_H_
#define __DCSR_CC_H_

#include <span>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <boost/dynamic_bitset.hpp>
#include "concepts.h"
#include "metrics.h"

namespace dcsr {

template<typename T>
bool compare_and_swap(T &x, const T &old_val, const T &new_val) {
    return __sync_bool_compare_and_swap(&x, old_val, new_val);
}

// Place nodes u and v in same component of lower component ID
template<typename NodeID>
void Link(NodeID u, NodeID v, NodeID* comp) {
    NodeID p1 = comp[u];
    NodeID p2 = comp[v];
    while (p1 != p2) {
        NodeID high = p1 > p2 ? p1 : p2;
        NodeID low = p1 + (p2 - high);
        NodeID p_high = comp[high];
        // Was already 'low' or succeeded in writing 'low'
        if ((p_high == low) || (p_high == high && compare_and_swap(comp[high], high, low)))
            break;
        p1 = comp[comp[high]]; // Union-Find ? path compression?
        p2 = comp[low];
    }
}

// Reduce depth of tree for each component to 1 by crawling up parents
template<typename NodeID>
void Compress(std::span<NodeID> comp) {
  #pragma omp parallel for schedule(dynamic, 16384)
  for (NodeID n = 0; n < comp.size(); n++) {
    while (comp[n] != comp[comp[n]]) {
      comp[n] = comp[comp[n]];
    }
  }
}

template<typename NodeID>
NodeID SampleFrequentElement(const NodeID* comp, NodeID v_count, bool logging=false, size_t num_samples=1024) {
    std::unordered_map<NodeID, int> sample_counts(32);
    using kvp_type = std::unordered_map<NodeID, int>::value_type;
    // Sample elements from 'comp'
    std::mt19937 gen;
    std::uniform_int_distribution<NodeID> distribution(0, v_count - 1);
    for (NodeID i = 0; i < num_samples; i++) {
        NodeID n = distribution(gen);
        sample_counts[comp[n]]++;
    }
    // Find most frequent element in samples (estimate of most frequent overall)
    auto most_frequent = std::max_element(
        sample_counts.begin(), sample_counts.end(),
        [](const kvp_type& a, const kvp_type& b) { return a.second < b.second; });
    float frac_of_graph = static_cast<float>(most_frequent->second) / num_samples;
    if(logging) {
        fmt::println("Skipping largest intermediate component (ID: {}, approx. {:.0f}% of the graph)", most_frequent->first, frac_of_graph * 100);
    }
    return most_frequent->first;
}

// Returns k pairs with the largest values from list of key-value pairs
template<typename KeyT, typename ValT>
std::vector<std::pair<ValT, KeyT>> TopK(const std::vector<std::pair<KeyT, ValT>> &to_sort, size_t k) {
    std::vector<std::pair<ValT, KeyT>> top_k;
    ValT min_so_far = 0;
    for (auto kvp : to_sort) {
        if ((top_k.size() < k) || (kvp.second > min_so_far)) {
            top_k.push_back(std::make_pair(kvp.second, kvp.first));
            std::sort(top_k.begin(), top_k.end(), std::greater<std::pair<ValT, KeyT>>());
            if (top_k.size() > k)
                top_k.resize(k);
            min_so_far = top_k.back().first;
        }
    }
    return top_k;
}

template<typename NodeID>
void PrintCompStats(std::span<const NodeID> comp) {
    fmt::println("");
    std::unordered_map<NodeID, NodeID> count;
    for (NodeID comp_i : comp)
        count[comp_i] += 1;
    int k = 5;
    std::vector<std::pair<NodeID, NodeID>> count_vector;
    count_vector.reserve(count.size());
    for (auto kvp : count)
        count_vector.push_back(kvp);
    std::vector<std::pair<NodeID, NodeID>> top_k = TopK(count_vector, k);
    k = std::min(k, static_cast<int>(top_k.size()));
    fmt::println("{} biggest clusters", k);
    for(auto kvp : top_k)
        fmt::println("{}:{}", kvp.second, kvp.first);
    fmt::println("There are {} components", count.size());
}


// Some old sampling methods
template<typename GraphType, typename NodeID, size_t VBATCH=16384>
void LinkSampleNeighborsBackup(GraphType* graph, std::span<NodeID> comp_span, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    size_t v_count = graph->VertexCount();

    for (size_t r = 0; r < neighbor_rounds; ++r) {
        // Batched sampling
        // #pragma omp parallel for schedule(dynamic, 1)
        // for (NodeID u1 = 0; u1 < v_count; u1 += VBATCH) {
        //     NodeID u2 = std::min(u1 + VBATCH, v_count);
        //     graph->SampleNeighborsOutRanges(u1, u2, r+1, [&](NodeID u, NodeID v, [[maybe_unused]]size_t count) {
        //         if(count == r) {
        //             Link<NodeID>(u, v, comp);
        //         }
        //     });
        // }

        // Batched sampling, enable bitmap
        // if(r == 0) {
        //     graph->ValidateBitmapOut();
        // }
        // #pragma omp parallel for schedule(dynamic, 1)
        // for (NodeID u1 = 0; u1 < v_count; u1 += VBATCH) {
        //     NodeID u2 = std::min(u1 + VBATCH, v_count);
        //     graph->SampleNeighborsOutRanges2(u1, u2, r+1, [&](NodeID u, NodeID v, [[maybe_unused]]size_t count) {
        //         if(count == r) {
        //             Link<NodeID>(u, v, comp);
        //         }
        //     });
        // }
        Compress(comp_span);
    }
}


/**
 * @brief Sample neighbor_rounds neighbors for each node in the graph, and link them together.
 *        Simplest version, just call IterateNeighborsOut for each node.
 */
template<typename GraphType, typename NodeID, size_t VBATCH=16384>
void LinkSampleNeighborsSimple(GraphType* graph, std::span<NodeID> comp_span, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    size_t v_count = graph->VertexCount();
    for (size_t r = 0; r < neighbor_rounds; ++r) {
        // Simple per-node sampling
        #pragma omp parallel for schedule(dynamic, VBATCH)
        for (NodeID u = 0; u < v_count; u++) {
            size_t k = 0;
            graph->IterateNeighborsOut(u, [&](NodeID v) {
                if(k == r) { // The r-th neighbor
                    Link<NodeID>(u, v, comp);
                    return false; // Break, only process one neighbor (sampling)
                } else {
                    k++;
                    return true;
                }
            });
        }
        Compress(comp_span);
    }
}

using Bitset = boost::dynamic_bitset<uint64_t>;
/**
 * @brief Sample neighbor_rounds neighbors for each node in the graph, and link them together.
 *        Sample & set bitset in first round, then use bitset to skip nodes in later rounds.
 *        VBATCH % 64 == 0 to avoid conflicts in bitmap, % 256 == 0 for better performance
 */
template<typename GraphType, typename NodeID, size_t VBATCH=16384>
void LinkSampleNeighborsBitmap(GraphType* graph, std::span<NodeID> comp_span, Bitset& visited, size_t neighbor_rounds = 2) {
    if(neighbor_rounds == 0) {
        return;
    }

    NodeID* comp = comp_span.data();
    size_t v_count = graph->VertexCount();

    // first round, sample and set bitset
    #pragma omp parallel for schedule(dynamic, VBATCH)
    for (NodeID u = 0; u < v_count; u++) {
        graph->IterateNeighborsOut(u, [&](NodeID v) {
            visited.set(u);
            Link<NodeID>(u, v, comp);
            return false; // Break, only process one neighbor (sampling)
        });
    }
    Compress(comp_span);

    // later rounds, use bitset to skip nodes
    for (size_t r = 1; r < neighbor_rounds; ++r) {
        // Simple per-node sampling
        #pragma omp parallel for schedule(dynamic, VBATCH)
        for (NodeID u = 0; u < v_count; u++) {
            if(!visited[u]) {
                continue;
            }

            size_t k = 0;
            graph->IterateNeighborsOut(u, [&](NodeID v) {
                if(k == r) { // The r-th neighbor
                    Link<NodeID>(u, v, comp);
                    return false; // Break, only process one neighbor (sampling)
                } else {
                    k++;
                    return true;
                }
            });
        }
        Compress(comp_span);
    }
}

template<typename GraphType, typename NodeID, size_t VBATCH=16384>
void LinkSampleNeighborsInOnce(GraphType* graph, std::span<NodeID> comp_span, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    size_t v_count = graph->VertexCount();

    size_t r = neighbor_rounds;
    // Simple per-node sampling, access neighbors_rounds edges in one round
    #pragma omp parallel for schedule(dynamic, VBATCH)
    for (NodeID u = 0; u < v_count; u++) {
        size_t k = 0;
        graph->IterateNeighborsOut(u, [&](NodeID v) {
            Link<NodeID>(u, v, comp);
            k++;
            if(k == r) {
                return false;
            }
            return true;
        });
    }
    Compress(comp_span);
}

template<typename GraphType, typename NodeID, size_t VBATCH_SET=16384>
void LinkSampleNeighborsBatchFastOnce(GraphType* graph, std::span<NodeID> comp_span, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    size_t v_count = graph->VertexCount();
    size_t r = neighbor_rounds;

    // size_t total_samples = 0;
    // size_t cnt = 0;
    // size_t cur = 0;

    size_t VBATCH = VBATCH_SET;
    while(v_count / VBATCH < 1024) {
        VBATCH /= 2; 
    }

    // Simple per-node sampling, access neighbors_rounds edges in one round
    #pragma omp parallel for schedule(dynamic, 1) // reduction(+:total_samples)
    for (NodeID u1 = 0; u1 < v_count; u1+=VBATCH) {
        NodeID u2 = std::min(u1 + VBATCH, v_count);

        graph->OutGraphView().SampleNeighborsRangeFast(u1, u2, r, [&](NodeID u, NodeID v, [[maybe_unused]]size_t count) {
            Link<NodeID>(u, v, comp);
            // total_samples++;
            // if(cur != u) {
            //     cur = u;
            //     cnt = 0;
            // }
            // cnt++;
            // fmt::println("Sampled: {} -> {}, count: {}", u, v, cnt);
            // if(cnt > neighbor_rounds) {
            //     fmt::println("Error: {} -> {}, count: {}", u, v, cnt);
            //     exit(1);
            // }

        });
    }
    Compress(comp_span);
    // fmt::println("Total samples: {}", total_samples);
}

template<typename GraphType, typename NodeID, size_t VBATCH=16384>
void LinkSampleNeighborsOneLevel(GraphType* graph, std::span<NodeID> comp_span, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    size_t v_count = graph->VertexCount();

    size_t r = neighbor_rounds;
    // Simple per-node sampling, access neighbors_rounds edges in one round
    #pragma omp parallel for schedule(dynamic, 1)
    for (NodeID u1 = 0; u1 < v_count; u1+=VBATCH) {
        NodeID u2 = std::min(u1 + VBATCH, v_count);

        graph->OutGraphView().SampleNeighborsRangeInLevel(u1, u2, r, 0, [&](NodeID u, NodeID v, [[maybe_unused]]size_t count) {
            Link<NodeID>(u, v, comp);
        });
    }
    Compress(comp_span);
}

template<typename GraphType, typename NodeID, size_t VBATCH_SET=16384>
void LinkSampleNeighborsBatchDensityAwareOnce(GraphType* graph, std::span<NodeID> comp_span, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    size_t v_count = graph->VertexCount();
    size_t r = neighbor_rounds;

    // size_t total_samples = 0;
    // size_t cnt = 0;
    // size_t cur = 0;

    size_t VBATCH = std::min<size_t>(VBATCH_SET, std::bit_floor(v_count / 1024));
    // fmt::println("VBATCH: {}, blocks: {}", VBATCH, v_count / VBATCH);

    // Simple per-node sampling, access neighbors_rounds edges in one round
    #pragma omp parallel for schedule(dynamic, 1) // reduction(+:total_samples)
    for (NodeID u1 = 0; u1 < v_count; u1+=VBATCH) {
        NodeID u2 = std::min(u1 + VBATCH, v_count);

        graph->OutGraphView().SampleNeighborsRangeDensityAware(u1, u2, r, [&](NodeID u, NodeID v, [[maybe_unused]]size_t count) {
            Link<NodeID>(u, v, comp);
            // total_samples++;
            // if(cur != u) {
            //     cur = u;
            //     cnt = 0;
            // }
            // cnt++;
            // // fmt::println("Sampled: {} -> {}, count: {}", u, v, cnt);
            // if(cnt > neighbor_rounds) {
            //     fmt::println("Error: {} -> {}, count: {}", u, v, cnt);
            //     exit(1);
            // }

        });
    }
    Compress(comp_span);
    // fmt::println("Total samples: {}", total_samples);
}


/**
 * @brief Link rest of the neighbors for each node in the graph, after sampling.
 *        Ignore node in max_comp component, which is the largest intermediate component.
 */
template<typename GraphType, typename NodeID, size_t VBATCH_SET=16384>
void FinalizeSimple(GraphType* graph, std::span<NodeID> comp_span, NodeID max_comp, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    NodeID c = max_comp;
    size_t v_count = graph->VertexCount();
    (void) neighbor_rounds;

    size_t VBATCH = std::min<size_t>(VBATCH_SET, std::bit_floor(v_count / 2048));

    // Directed graph, so we need to process both directions
    #pragma omp parallel for schedule(dynamic, VBATCH)
    for (NodeID u = 0; u < v_count; u++) {
        if (comp[u] == c)
            continue;

        // size_t k = neighbor_rounds;
        // graph->IterateNeighborsOut(u, [&](NodeID v) {
        //     if(k>0) {   // Skip neighbors in the first few rounds (which were sampled)
        //         k--;
        //     } else {
        //         Link<NodeID>(u, v, comp);
        //     }
        // });
        graph->IterateNeighborsOut(u, [&](NodeID v) {
            Link<NodeID>(u, v, comp);
        });
        // To support directed graphs, process reverse graph completely
        graph->IterateNeighborsIn(u, [&](NodeID v) {
            Link<NodeID>(u, v, comp);
        });
    }
}

/**
 * @brief Batched version of FinalizeSimple, too slow
 */
template<typename GraphType, typename NodeID, size_t VBATCH=16384>
void FinalizeBatched(GraphType* graph, std::span<NodeID> comp_span, NodeID max_comp, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    NodeID c = max_comp;
    size_t v_count = graph->VertexCount();
    (void) neighbor_rounds;

    // Batched processing, too slow
    #pragma omp parallel for schedule(dynamic, 1)
    for (NodeID u1 = 0; u1 < v_count; u1 += VBATCH) {
        NodeID u2 = std::min(u1 + VBATCH, v_count);

        graph->IterateNeighborsOutRange(u1, u2, [&](NodeID u, NodeID v) {
            if(comp[u] == c) {
                int skip = 0;
                while(u + skip < u2 && comp[u + skip] == c) {
                    skip++;
                }
                return skip;
            }
            Link<NodeID>(u, v, comp);
            return 0;
        });

        graph->IterateNeighborsInRange(u1, u2, [&](NodeID u, NodeID v) {
            if(comp[u] == c) {
                int skip = 0;
                while(u + skip < u2 && comp[u + skip] == c) {
                    skip++;
                }
                return skip;
            }
            Link<NodeID>(u, v, comp);
            return 0;
        });
    }
}

/**
 * @brief Bitmap version of FinalizeSimple
 */
template<typename GraphType, typename NodeID, size_t VBATCH=16384>
void FinalizeBitmap(GraphType* graph, std::span<NodeID> comp_span, NodeID max_comp, const Bitset& visited, size_t neighbor_rounds = 2) {
    NodeID* comp = comp_span.data();
    NodeID c = max_comp;
    size_t v_count = graph->VertexCount();
    (void) neighbor_rounds;

    #pragma omp parallel for schedule(dynamic, 1)
    for (NodeID u1 = 0; u1 < v_count; u1 += VBATCH) {
        NodeID u2 = std::min(u1 + VBATCH, v_count);

        for(NodeID u = u1; u < u2;) {
            if(comp[u] == c || !visited[u]) {
                u++;
                continue;
            }

            NodeID u_st = u;
            NodeID u_ed = u+1;
            while(u_ed < u2 && comp[u_ed] != c && visited[u_ed]) {
                u_ed++;
            }

            graph->IterateNeighborsOutRange(u_st, u_ed, [&](NodeID u, NodeID v) {
                Link<NodeID>(u, v, comp);
            });

            u = u_ed;
        }


        for(NodeID u = u1; u < u2;) {
            if(comp[u] == c) {
                u++;
                continue;
            }

            NodeID u_st = u;
            NodeID u_ed = u+1;
            while(u_ed < u2 && comp[u_ed] != c) {
                u_ed++;
            }

            graph->IterateNeighborsInRange(u_st, u_ed, [&](NodeID u, NodeID v) {
                Link<NodeID>(u, v, comp);
            });

            u = u_ed;
        }
    }
}



template<ConditionalStopIterableTwoWayGraph GraphType>
auto connected_components(GraphType* graph, bool logging, size_t neighbor_rounds = 2) {
    using NodeID = GraphType::VertexType;

    SimpleTimer timer;
    size_t v_count = graph->VertexCount();
    auto comp_uptr = std::make_unique_for_overwrite<NodeID[]>(v_count);
    NodeID* comp = comp_uptr.get();
    std::span<NodeID> comp_span(comp, v_count);

    // using Bitset = boost::dynamic_bitset<>;
    // Bitset visited(v_count);

    // Initialize each node to a single-node self-pointing tree
    #pragma omp parallel for schedule(static, 16384)
    for (NodeID v = 0; v < v_count; v++) {
        comp[v] = v;
    }

    // graph->BuildBitmapParallel();
    fmt::println("Initialization time: {:.4f}s", timer.Lap());

    // Process a sparse sampled subgraph first for approximating components.
    // Sample by processing a fixed number of neighbors for each node (see paper)

    // LinkSampleNeighborsSimple(graph, comp_span, neighbor_rounds);
    // LinkSampleNeighborsBitmap(graph, comp_span, visited, neighbor_rounds);
    // LinkSampleNeighborsInOnce(graph, comp_span, neighbor_rounds);
    // LinkSampleNeighborsOneLevel(graph, comp_span, neighbor_rounds);
    // LinkSampleNeighborsBatchFastOnce<GraphType, NodeID, 65536>(graph, comp_span, neighbor_rounds);
    LinkSampleNeighborsBatchDensityAwareOnce<GraphType, NodeID, 65536>(graph, comp_span, neighbor_rounds);

    fmt::println("Sampling time: {:.4f}s", timer.Lap());
    
    NodeID c = SampleFrequentElement<NodeID>(comp, v_count, true);


    FinalizeSimple<GraphType, NodeID, 16384>(graph, comp_span, c, neighbor_rounds);
    // FinalizeBatched(graph, comp_span, c, neighbor_rounds);
    // FinalizeBitmap(graph, comp_span, c, visited, neighbor_rounds);

    // Finally, 'compress' for final convergence
    Compress(comp_span);

    fmt::println("Finalization time: {:.4f}s", timer.Lap());

    if(logging) {
        PrintCompStats(std::span<const NodeID>(comp, v_count));
    }
    return comp_uptr;
}

template<ConditionalStopIterableTwoWayGraph GraphType>
auto cc_gapbs(GraphType* graph, size_t neighbor_rounds = 2) {
    return connected_components(graph, false, neighbor_rounds);
} 

} // namespace dcsr

#endif // __DCSR_CC_H_