#ifndef __CONFIG_H__
#define __CONFIG_H__

#include <cstddef>

namespace dcsr
{

struct Config
{
    /**
     * @brief 当插入的边中顶点编号超出当前图的范围时，是否自动扩展图的范围。
     * 
     */
    bool auto_extend = true;

    bool bind_core = false;

    bool bind_numa = true;

    // number of buffers in buffers pool per memory partition
    size_t buffer_count = 1;

    // max number of edges in memory buffer per partition
    size_t buffer_size = 1024 * 1024;

    size_t compaction_threshold = 4;

    size_t dispatch_thread_count = 4;

    size_t index_ratio = 8;     // index_size ~= edges count / index_ratio
    
    size_t init_vertex_count = 0;

    double merge_multiplier = 2.0;

    size_t min_csr_num_to_compact = 2;

    // number of vertices per partition
    size_t partition_size = 128 * 1024;

    // min batch size for sorting
    size_t sort_batch_size = 1024;


    /**
     * @brief max number of eddges stored in single WAL file.
     *        64M edges (or 1GiB WAL file) by default.
     */
    // size_t wal_file_size = 64 * 1024 * 1024;

    // size_t max_wal_file_num = 3;

};

/**
 * @brief Generate a configuration for a undirected graph, which has only one graph, and ingesting edges in both directions.
 * 
 * @param vertex_count 
 * @param thread_count 
 * @return Config 
 */
Config GenerateUGraphConfig(size_t vertex_count, size_t edge_count, size_t thread_count) {
    Config config;
    size_t dispatch_thread_count = std::min<size_t>(16, div_up(thread_count, 8));
    size_t partition_count = thread_count - dispatch_thread_count;
    size_t partition_width = div_up(vertex_count, partition_count);

    config.sort_batch_size = 128;
    config.buffer_size = bit_ceil(edge_count + dispatch_thread_count * config.sort_batch_size);
    config.init_vertex_count = vertex_count;
    config.partition_size = partition_width;
    config.buffer_count = 1;
    
    config.auto_extend = false;
    config.bind_core = false;
    config.bind_numa = false;
    config.dispatch_thread_count = dispatch_thread_count;
    
    
    return config;
}

Config GenerateTGraphConfig(size_t vertex_count, size_t edge_count, size_t thread_count) {
    size_t dispatch_thread_count = 0;
    if(thread_count < 4) {  // should bind cores by taskset manually, otherwise it will use one extra core for dispatching
        dispatch_thread_count = 1;
    } else {    // thread_count >= 4, auto allocate cores for dispatching and ingesting
        dispatch_thread_count = std::min<size_t>(16, div_up(thread_count, 10) * 2);
        thread_count -= dispatch_thread_count;
    }

    size_t partition_count = thread_count / 2;    // In/Out graph got half
    partition_count = std::max<size_t>(1, partition_count);
    size_t partition_width = div_up(vertex_count, partition_count);

    Config config;
    config.sort_batch_size = 128;
    config.buffer_size = bit_ceil(edge_count + config.sort_batch_size * dispatch_thread_count);
    config.init_vertex_count = vertex_count;
    config.partition_size = partition_width;

    // don't modify these configs, just for compatibility
    config.buffer_count = 1;    
    config.auto_extend = false;
    config.bind_core = false;
    config.dispatch_thread_count = dispatch_thread_count;
    
    return config;
}

}   // namespace dcsr
#endif