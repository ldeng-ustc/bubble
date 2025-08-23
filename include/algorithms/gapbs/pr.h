// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details


#include <memory>

#include "concepts.h"

/*
GAP Benchmark Suite
Kernel: PageRank (PR)
Author: Scott Beamer

Will return pagerank scores for all vertices once total change < epsilon

This PR implementation uses the traditional iterative approach. It performs
updates in the pull direction to remove the need for atomics, and it allows
new values to be immediately visible (like Gauss-Seidel method). The prior PR
implementation is still available in src/pr_spmv.cc.
*/

using ScoreT = float;
const float kDamp = 0.85;

template <typename TGraph>
    requires dcsr::BasicIterableTwoWayGraph<TGraph>
ScoreT* PageRankPullGS(const TGraph &g, int max_iters, double epsilon=0, bool logging_enabled = false) {
    using NodeID = typename TGraph::VertexType;
    const size_t v_count = g.VertexCount();
    const ScoreT init_score = 1.0f / v_count;
    const ScoreT base_score = (1.0f - kDamp) / v_count;
    if(logging_enabled) {
        fmt::println("init_score={}, base_score={}", init_score, base_score);
        fmt::println("v_count={}, max_iters={}, epsilon={}", v_count, max_iters, epsilon);
    }


    auto scores = std::make_unique<ScoreT[]>(v_count);
    auto outgoing_contrib = std::make_unique<ScoreT[]>(v_count);
    auto degree_cache = std::make_unique<uint32_t[]>(v_count);
    // std::fill_n(scores.get(), v_count, init_score);
    #pragma omp parallel for
    for (NodeID n=0; n < v_count; n++)
        scores[n] = init_score;

    #pragma omp parallel for
    for (NodeID n=0; n < v_count; n++) {
        degree_cache[n] = g.GetDegreeOut(n);
        // outgoing_contrib[n] = init_score / g.GetDegreeOut(n);
        outgoing_contrib[n] = init_score / degree_cache[n];
    }

    for (int iter=0; iter < max_iters; iter++) {
        SimpleTimer t;
        double error = 0;

        constexpr size_t VBATCH = 16384;
        // #pragma omp parallel for reduction(+ : error) schedule(dynamic, VBATCH)
        // for (NodeID u=0; u < v_count; u++) {
        //     ScoreT incoming_total = 0;

        //     g.IterateNeighborsIn(u, [&](NodeID v) {
        //         incoming_total += outgoing_contrib[v];
        //     });

        //     ScoreT old_score = scores[u];
        //     scores[u] = base_score + kDamp * incoming_total;
            
        //     error += fabs(scores[u] - old_score);
        //     // outgoing_contrib[u] = scores[u] / g.GetDegreeOut(u);
        //     outgoing_contrib[u] = scores[u] / degree_cache[u];
        // }

        #pragma omp parallel for reduction(+ : error) schedule(dynamic, 1)
        for (NodeID u1 = 0; u1 < v_count; u1 += VBATCH) {
            NodeID u2 = std::min(u1 + VBATCH, v_count);
            auto incoming_total = std::make_unique<ScoreT[]>(u2 - u1);
            memset(incoming_total.get(), 0, sizeof(ScoreT) * (u2 - u1));

            g.IterateNeighborsInRange(u1, u2, [&](NodeID u, NodeID v) {
                incoming_total[u - u1] += outgoing_contrib[v];
            });

            for(NodeID u = u1; u < u2; u++) {
                ScoreT old_score = scores[u];
                scores[u] = base_score + kDamp * incoming_total[u - u1];
                error += fabs(scores[u] - old_score);
                // outgoing_contrib[u] = scores[u] / g.GetDegreeOut(u);
                outgoing_contrib[u] = scores[u] / degree_cache[u];
            }
        }

        if (error < epsilon)
            break;
        if (logging_enabled)
            fmt::println("PR Iteration {} (error={:.9f}, time={:.2f}s)", iter, error, t.Stop());
    }
    return scores.release();
}


template <typename TGraph>
    requires dcsr::BasicIterableTwoWayGraph<TGraph>
auto pr_gapbs(const TGraph *g, int max_iters) {
    auto scores = PageRankPullGS(*g, max_iters, 0, true);
    return std::unique_ptr<ScoreT[]>(scores);
}

template<typename T=float>
void PrintScores(T* scores, int64_t N) {
    T max_score = 0;
    int64_t idx = 0;
    for (int64_t n=0; n < N; n++) {
        if (scores[n] > max_score) {
            idx = n;
            max_score = scores[n];
        }
    }

    for (int64_t n=0; n < 5; n++)
        fmt::println("Score[{}] = {:.9f}", n, scores[n]);
    fmt::println("Score[{}] = {:.9f} (Max)", idx, max_score);

}
