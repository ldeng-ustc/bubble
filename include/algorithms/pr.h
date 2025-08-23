#ifndef __DCSR_PR_H__
#define __DCSR_PR_H__

#include "concepts.h"

namespace dcsr {

// Credits to :
// http://www.memoryhole.net/kyle/2012/06/a_use_for_volatile_in_multithr.html
typedef float rank_t; 
float qthread_dincr(float *operand, float incr)
{
    //*operand = *operand + incr;
    //return incr;
    
    union {
       rank_t   d;
       uint32_t i;
    } oldval, newval, retval;
    do {
         oldval.d = *(volatile rank_t *)operand;
         newval.d = oldval.d + incr;
         //__asm__ __volatile__ ("lock; cmpxchgq %1, (%2)"
         __asm__ __volatile__ ("lock; cmpxchg %1, (%2)"
                                : "=a" (retval.i)
                                : "r" (newval.i), "r" (operand),
                                 "0" (oldval.i)
                                : "memory");
    } while (retval.i != oldval.i);
    return oldval.d;
}

template<BasicIterableTwoWayGraph GraphType>
void pagerank_pull(const GraphType* graph, size_t iteration_count) {

    size_t v_count = graph->VertexCount();
    auto rank_array = make_huge_for_overwrite<float[]>(v_count);
    auto prior_rank_array = make_huge_for_overwrite<float[]>(v_count);
    auto dset = make_huge_for_overwrite<float[]>(v_count);
    
	//initialize the rank, and get the degree information
    const float inv_v_count = 0.15;//1.0f/vert_count;
    #pragma omp parallel for schedule(dynamic, 65536)
    for (size_t v = 0; v < v_count; ++v) {
        size_t degree = graph->GetDegreeOut(v);
        if (degree != 0) {
            dset[v] = 1.0f / degree;
            prior_rank_array[v] = inv_v_count;//XXX
        } else {
            dset[v] = 0;
            prior_rank_array[v] = 0;
        }
    }

    // Run pagerank
	for (size_t iter_count = 0; iter_count < iteration_count; ++iter_count) {
        SimpleTimer iter_timer;

        #pragma omp parallel 
        {
            float rank = 0.0f; 
         
            #pragma omp for schedule (dynamic, 65536) nowait 
            for (VID v = 0; v < v_count; v++) {
                graph->IterateNeighborsIn(v, [&](VID to) {
                    rank += prior_rank_array[to];
                });
                rank_array[v] += rank;  // for pull, no need to synchronize
            }

            if (iter_count != iteration_count - 1) {
                #pragma omp for
                for (VID v = 0; v < v_count; v++) {
                    rank_array[v] = (0.15 + 0.85 * rank_array[v]) * dset[v];
                    prior_rank_array[v] = 0;
                }
            } else { 
                #pragma omp for
                for (VID v = 0; v < v_count; v++) {
                    rank_array[v] = (0.15 + 0.85 * rank_array[v]);
                    prior_rank_array[v] = 0;
                }
            }
        }
        std::swap(prior_rank_array, rank_array);

        fmt::println("Iteration {} time: {:.2f}s", iter_count, iter_timer.Stop());
    }
}

template<BasicIterableGraph GraphType>
void pagerank_push(const GraphType* graph, size_t iteration_count) {

    size_t v_count = graph->VertexCount();
    auto rank_array = make_huge_for_overwrite<float[]>(v_count);
    auto prior_rank_array = make_huge_for_overwrite<float[]>(v_count);
    auto dset = make_huge_for_overwrite<float[]>(v_count);

	//initialize the rank, and get the degree information
    const float inv_v_count = 0.15;//1.0f/vert_count;
    #pragma omp parallel for schedule(dynamic, 65536)
    for (size_t v = 0; v < v_count; ++v) {
        size_t degree = graph->GetDegree(v);
        if (degree != 0) {
            dset[v] = 1.0f / degree;
            prior_rank_array[v] = inv_v_count;//XXX
        } else {
            dset[v] = 0;
            prior_rank_array[v] = 0;
        }
    }


    // Run pagerank
	for (size_t iter_count = 0; iter_count < iteration_count; ++iter_count) {
        SimpleTimer iter_timer;

        #pragma omp parallel 
        {
            float rank = 0.0f; 
         
            #pragma omp for schedule(dynamic, 16384)
            for (VID v = 0; v < v_count; v++) {
                rank = prior_rank_array[v];
                graph->IterateNeighbors(v, [&](VID to) {
                    qthread_dincr(&rank_array[to], rank);
                });
            }

            if (iter_count != iteration_count - 1) {
                #pragma omp for schedule(dynamic, 16384)
                for (VID v = 0; v < v_count; v++) {
                    rank_array[v] = (0.15 + 0.85 * rank_array[v]) * dset[v];
                    prior_rank_array[v] = 0;
                }
            } else { 
                #pragma omp for schedule(dynamic, 16384)
                for (VID v = 0; v < v_count; v++) {
                    rank_array[v] = (0.15 + 0.85 * rank_array[v]);
                    prior_rank_array[v] = 0;
                }
            }
        }
        std::swap(prior_rank_array, rank_array);
        fmt::println("Iteration {} time: {:.2f}s", iter_count, iter_timer.Stop());
    }	
}

} // namespace dcsr

#endif // __DCSR_PR_H__