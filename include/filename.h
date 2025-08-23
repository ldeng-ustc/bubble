#ifndef __FILENAME_H__
#define __FILENAME_H__

#include <string>
#include <cstdint>
#include <filesystem>
#include "fmt/format.h"

namespace dcsr {

inline std::string PartitionFileName(const std::filesystem::path& dbpath, uint64_t id) {
    std::string filename = fmt::format("{}_{:04}", "Partition", id);
    return (dbpath / filename).string();
}

inline std::string MetaFileName(const std::filesystem::path& dbpath) {
    return (dbpath / "CONTEXT").string();
}

inline std::string WalFileName(const std::filesystem::path& dbpath, uint64_t id) {
    return (dbpath / fmt::format("{}_{:04}", "WAL", id)).string();
}

}

#endif