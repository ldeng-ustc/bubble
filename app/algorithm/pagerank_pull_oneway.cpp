#include <omp.h>
#include "fmt/format.h"
#include "fmt/ranges.h"

#include "env.h"
#include "graph.h"
#include "importer.h"
#include "useful_configs.h"
#include "naive_memgraph.h"
using namespace dcsr;

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



double pagerank_pull(Graph<void>* graph, size_t iteration_count) {

    size_t v_count = graph->VertexCount();
    auto rank_array = make_huge_for_overwrite<float[]>(v_count);
    auto prior_rank_array = make_huge_for_overwrite<float[]>(v_count);
    auto dset = make_huge_for_overwrite<float[]>(v_count);

    SimpleTimer timer;
    
	//initialize the rank, and get the degree information
    

    const float inv_v_count = 0.15;//1.0f/vert_count;
    #pragma omp parallel for schedule(dynamic, 65536)
    for (size_t v = 0; v < v_count; ++v) {
        // Wrong implementation, should be GetDegreeOut, But cannot implement in the one-way graph
        size_t degree = graph->GetDegreeInMemory(v);
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
                graph->IterateNeighborsInMemory(v, [&](VID to) {
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
        // double end1 = mywtime();
        // std::string statistic_filename = "xpgraph_query.csv";
        // std::ofstream ofs;
        // ofs.open(statistic_filename.c_str(), std::ofstream::out | std::ofstream::app );
        // ofs << "Iteration Time = " << end1 - start1 << std::endl;
        // ofs.close();
        // // std::cout << "Iteration Time = " << end1 - start1 << std::endl;
    }	
    // double end = mywtime();

    // std::string statistic_filename = "xpgraph_query.csv";
    // std::ofstream ofs;
    // ofs.open(statistic_filename.c_str(), std::ofstream::out | std::ofstream::app );
    // ofs << "PR Time = " << end - start << std::endl;
    // ofs << std::endl;
    // ofs.close();
	// // std::cout << "PR Time = " << end - start << std::endl;
	// // std::cout << std::endl;
    // return end - start;

    return timer.Stop();
}



int main() {

    SetAffinityThisThread(0);
    
    auto cname = ConfigName::LARGE; // Change this to test different dataset
    auto [dataset, config] = useful_configs[static_cast<size_t>(cname)];
    config.buffer_size = 1024 * 1024 * 1024;
    // config.sort_batch_size = 64 * 1024;
    config.merge_multiplier = 2.0;

    if(config.init_vertex_count < 128 * 1024 * 1024) {
        // Run vector based graph for comparison
        auto mg = dcsr::LoadInMemoryOneWay(dataset, config.init_vertex_count);
        fmt::println("{}", mg[1]);
        mem_bfs_oneway(&mg, 1);
    }

    auto g = std::make_unique<Graph<void>>("./data/tmp_graph/", config);

    // auto graph_mem = LoadInMemory(dataset);
    // bfs_mem(&graph_mem, 1);

    // Load reversed graph
    auto [rt, pt] = ScanLargeFile<RawEdge64<void>, 8*1024*1024>(dataset, [&](RawEdge64<void> e) {
        g->AddEdge(RawEdge64<void>{e.to, e.from});
    });

    fmt::println("Total sleep time: {}ms", g->TotalSleepMillis());

    auto lt = TimeIt([&] {
        g->WaitSortingAndPrepareAnalysis();
    });

    fmt::println("{}", g->GetNeighborsVectorInMemory(1));

    fmt::println("Read time: {:.2f}s, Process time: {:.2f}s", rt, pt);
    fmt::println("Lock wait time: {:.2f}s", lt);

    UnsetAffinityThisThread();
    // bfs_oneway_omp(g.get(), 1);

    auto t = pagerank_pull(g.get(), 10);
    g->FinishAlgorithm();
    
    fmt::println("PageRank time: {:.2f}s", t);
    return 0;
}
