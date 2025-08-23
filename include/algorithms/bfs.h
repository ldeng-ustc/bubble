#ifndef __DCSR_BFS_H__
#define __DCSR_BFS_H__

#include <cstdint>
#include <boost/dynamic_bitset.hpp>
#include "concepts.h"
#include "metrics.h"

namespace dcsr {

template<BasicIterableGraph GraphType>
void bfs_oneway(const GraphType* graph, uint64_t root) {
    using VID = typename GraphType::VertexType;

    int level = 1;
    int64_t frontier = 0;
    size_t v_count = graph->VertexCount();

    auto levels = std::make_unique_for_overwrite<uint16_t[]>(v_count);
    std::fill(levels.get(), levels.get() + v_count, 0);

    levels[root] = level;
    do {
        frontier = 0;
        SimpleTimer level_timer;

        #pragma omp parallel for reduction(+:frontier) schedule(dynamic, 16384)
        for (VID v = 0; v < v_count; v++) {
            if (levels[v] != level) {
                continue;
            }

            graph->IterateNeighbors(v, [&](VID to) {
                if(levels[to] == 0) {
                    levels[to] = level + 1;
                    frontier++;
                }
            });

        }

        double level_time = level_timer.Stop();
        fmt::println("Level = {}, Frontier Count = {}, Time = {:.2f}s", level, frontier, level_time);

        level ++;
    } while (frontier);
}

template<BasicIterableGraph GraphType>
void bfs_oneway_serial(const GraphType* graph, uint64_t root) {
    using VID = typename GraphType::VertexType;

    int level = 1;
    int64_t frontier = 0;
    size_t v_count = graph->VertexCount();

    auto levels = std::make_unique_for_overwrite<uint16_t[]>(v_count);
    std::fill(levels.get(), levels.get() + v_count, 0);

    levels[root] = level;
    do {
        frontier = 0;
        SimpleTimer level_timer;

        for (VID v = 0; v < v_count; v++) {
            if (levels[v] != level) {
                continue;
            }

            graph->IterateNeighbors(v, [&](VID to) {
                if(levels[to] == 0) {
                    levels[to] = level + 1;
                    frontier++;
                }
            });

        }

        [[maybe_unused]] double level_time = level_timer.Stop();
        fmt::println("Level = {}, Frontier Count = {}, Time = {:.2f}s", level, frontier, level_time);

        level ++;
    } while (frontier);
}

template<ConditionalStopIterableTwoWayGraph GraphType>
void bfs(const GraphType* graph, VID root) {
    using VID = typename GraphType::VertexType;
    fmt::println("BFS from root = {}", root);

    int level = 1;
    int64_t frontier = 0;
    size_t v_count = graph->VertexCount();
    // fmt::println("Vertex Count = {}", v_count);
    auto levels = std::make_unique_for_overwrite<uint16_t[]>(v_count);
    std::fill(levels.get(), levels.get() + v_count, 0);

    levels[root] = level;
    int	top_down = 1;
    do {
        frontier = 0;
        SimpleTimer level_timer;

        if (top_down) {
            #pragma omp parallel for reduction(+:frontier) schedule(dynamic, 16384)
            for (VID v = 0; v < v_count; v++) {
                if (levels[v] != level) {
                    continue;
                }

                graph->IterateNeighborsOut(v, [&](VID to) {
                    if(levels[to] == 0) {
                        levels[to] = level + 1;
                        frontier++;
                    }
                });

            }
        } else { //bottom up
            #pragma omp parallel for reduction(+:frontier) schedule(dynamic, 16384)
            for (VID v = 0; v < v_count; v++) {
                if (levels[v] != 0) {
                    continue;
                }

                graph->IterateNeighborsIn(v, [&](VID from) {
                    if(levels[from] == level) {
                        levels[v] = level + 1;
                        frontier++;
                        return false;
                    }
                    return true;
                });
            }
        }

        [[maybe_unused]] double level_time = level_timer.Stop();
        if(level_time > 0.1) {
            fmt::println("Top down = {}, Level = {}, Frontier Count = {}, Time = {:.2f}s", top_down, level, frontier, level_time);
        }
        //Point is to simulate bottom up bfs, and measure the trade-off    
        if (frontier >= 0.002 * v_count) { // same as GraphOne, XPGraph
        // if (20ull * frontier >= v_count) { // better
            top_down = false;
        } else {
            top_down = true;
        }
        level ++;
    } while (frontier);
}

template<ConditionalStopIterableTwoWayGraph GraphType>
void bfs_oneway_reverse(const GraphType* graph, uint64_t root) {
    using VID = typename GraphType::VertexType;

    int level = 1;
    int64_t frontier = 0;
    size_t v_count = graph->VertexCount();

    
    auto levels = std::make_unique_for_overwrite<uint16_t[]>(v_count);
    std::fill(levels.get(), levels.get() + v_count, 0);
    boost::dynamic_bitset<> cur_frontier(v_count);
    boost::dynamic_bitset<> next_frontier(v_count);

    cur_frontier[root] = 1;
    levels[root] = level;
    do {
        frontier = 0;
        SimpleTimer level_timer;

        #pragma omp parallel for reduction(+:frontier) schedule(dynamic, 16384)
        for (VID v = 0; v < v_count; v++) {
            if (levels[v] != 0) {
                continue;
            }

            graph->IterateNeighborsOut(v, [&](VID to) {
                if(cur_frontier[to]) {
                    levels[v] = level + 1;
                    frontier++;
                    next_frontier[v] = 1;
                    return false;
                }
                return true;
            });

        }

        cur_frontier.swap(next_frontier);

        double level_time = level_timer.Stop();
        fmt::println("Level = {}, Frontier Count = {}, Time = {:.2f}s", level, frontier, level_time);

        level ++;
    } while (frontier);
}

} // namespace dcsr


#endif // __DCSR_BFS_H__