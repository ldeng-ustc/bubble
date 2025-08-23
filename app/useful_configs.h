#ifndef __DEFAULT_CONFIG_H__
#define __DEFAULT_CONFIG_H__

#include <filesystem>
#include "config.h"

[[maybe_unused]] dcsr::Config config_tiny{
    .auto_extend = true,
    .buffer_size = 64,
    .init_vertex_count = 1024,
    .partition_size = 64,
};

[[maybe_unused]] dcsr::Config config_small{
    .auto_extend = true,
    .buffer_size = 1024,
    .init_vertex_count = 1024,
    .partition_size = 1024,
};

[[maybe_unused]] dcsr::Config config_medium{
    .auto_extend = false,
    .buffer_size = 1024 * 1024 * 1024,
    .init_vertex_count = 5 * 1024 * 1024,
    .partition_size = 256 * 1024,
};

[[maybe_unused]] dcsr::Config config_large{
    .auto_extend = false,
    .buffer_size = 128 * 1024 * 1024,
    .init_vertex_count = 128 * 1024 * 1024,
    .partition_size = 8 * 1024 * 1024,
};

[[maybe_unused]] dcsr::Config config_exlarge{
    .auto_extend = true,
    .buffer_size = 8 * 1024 * 1024,
    .init_vertex_count = 1024 * 1024 * 1024,
    .partition_size = 128 * 1024 * 1024,
};

std::filesystem::path dataset_tiny = "./data/toy_graphs/selfloop64-32/edgelist.bin";
std::filesystem::path dataset_small = "./data/toy_graphs/selfloop1024-32/edgelist.bin";
std::filesystem::path dataset_medium = "./data/bin/shuffle/livejournal/livejournal-shuffled.bin";
std::filesystem::path dataset_large = "./data/bin/shuffle/friendster/friendster-shuffled.bin";
std::filesystem::path dataset_exlarge = "./data/Kron30-16/block-00.bin";

std::pair<std::filesystem::path, dcsr::Config> useful_configs[] = {
    {dataset_tiny, config_tiny},
    {dataset_small, config_small},
    {dataset_medium, config_medium},
    {dataset_large, config_large},
    {dataset_exlarge, config_exlarge},
};

enum class ConfigName {
    TINY,
    SMALL,
    MEDIUM,
    LARGE,
    EXLARGE,
};


#endif // DEFAULT_CONFIG_H