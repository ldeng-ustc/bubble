#ifndef __DCSR_ENV_BASE_H__
#define __DCSR_ENV_BASE_H__

#include <cerrno>
#include <algorithm>
#include <array>
#include <filesystem>
#include <bitset>
#include "hwloc.h"
#include "fmt/format.h"

#include "common.h"


namespace dcsr {

namespace fs = std::filesystem;

constexpr size_t CACHE_LINE_SIZE = 64;
constexpr size_t PAGE_SIZE = 4096;
constexpr size_t HUGE_PAGE_SIZE = 2 * 1024 * 1024;

constexpr size_t PHYSICAL_CORE_PER_CPU = 20;
constexpr size_t HYPER_THREAD_PER_PCORE = 2;
constexpr size_t LOGICAL_CORE_PER_CPU = PHYSICAL_CORE_PER_CPU * HYPER_THREAD_PER_PCORE;
constexpr size_t NUMA_NODE = 2;
constexpr size_t TOTAL_CORE = LOGICAL_CORE_PER_CPU * NUMA_NODE;

constexpr size_t L1I_CACHE_SIZE = 32 * 1024;
constexpr size_t L1D_CACHE_SIZE = 32 * 1024;
constexpr size_t L2_CACHE_SIZE = 1024 * 1024;
constexpr size_t L3_CACHE_SIZE = 28 * 1024 * 1024;  // Shared by all cores 

constexpr size_t MAX_LOGICAL_CORES = 512;
// using CoreSet = std::bitset<MAX_LOGICAL_CORES>;
class CoreSet {
public:
    using BitSet = std::bitset<MAX_LOGICAL_CORES>;
private:
    BitSet bits_;
    size_t size_ = 0;
public:
    CoreSet(): bits_(), size_(0) { }
    CoreSet(size_t n): bits_(), size_(n) { }

    CoreSet(const CoreSet& other) = default;
    CoreSet(CoreSet&& other) = default;

    CoreSet& operator=(const CoreSet& other) = default;

    size_t size() const {
        return size_;
    }

    void set(size_t i) {
        bits_.set(i);
    }

    void reset(size_t i) {
        bits_.reset(i);
    }

    bool test(size_t i) const {
        return bits_.test(i);
    }

    void reset() {
        bits_.reset();
    }

    std::string to_string() const {
        std::string res = bits_.to_string();
        std::reverse(res.begin(), res.end());
        res = res.substr(0, size_);
        return res;
    }
};


struct LogicalCoreInfo {
    int id;
    int numa_node_id;
    int l3_cache_id;
    int l2_cache_id;
    int l1_cache_id;
    int physical_core_id;
};

struct MachineInfo {
    LogicalCoreInfo logical_cores[MAX_LOGICAL_CORES];
    int logical_core_count;
    int numa_node_count;
    int l3_cache_count;
    int l2_cache_count;
    int l1_cache_count;
    int physical_core_count;
};

MachineInfo GetMachineInfo() {
    MachineInfo machine_info;
    machine_info.numa_node_count = 0;
    machine_info.l3_cache_count = 0;
    machine_info.l2_cache_count = 0;
    machine_info.l1_cache_count = 0;
    machine_info.physical_core_count = 0;
    machine_info.logical_core_count = 0;

    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);

    int num_cores = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
    machine_info.logical_core_count = num_cores;

    machine_info.numa_node_count = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PACKAGE);
    machine_info.l3_cache_count = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_L3CACHE);
    machine_info.l2_cache_count = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_L2CACHE);
    machine_info.l1_cache_count = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_L1CACHE);
    machine_info.physical_core_count = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_CORE);

    for(int i = 0; i < num_cores; i++) {
        hwloc_obj_t pu = hwloc_get_obj_by_type(topology, HWLOC_OBJ_PU, i);
        LogicalCoreInfo& info = machine_info.logical_cores[i];
        info.id = pu->logical_index;

        hwloc_obj_t obj = hwloc_get_ancestor_obj_by_type(topology, HWLOC_OBJ_PACKAGE, pu);
        info.numa_node_id = obj ? obj->logical_index : -1;

        obj = hwloc_get_ancestor_obj_by_type(topology, HWLOC_OBJ_L3CACHE, pu);
        info.l3_cache_id = obj ? obj->logical_index : -1;

        obj = hwloc_get_ancestor_obj_by_type(topology, HWLOC_OBJ_L2CACHE, pu);
        info.l2_cache_id = obj ? obj->logical_index : -1;

        obj = hwloc_get_ancestor_obj_by_type(topology, HWLOC_OBJ_L1CACHE, pu);
        info.l1_cache_id = obj ? obj->logical_index : -1;

        obj = hwloc_get_ancestor_obj_by_type(topology, HWLOC_OBJ_CORE, pu);
        info.physical_core_id = obj ? obj->logical_index : -1;
    }

    hwloc_topology_destroy(topology);

    return machine_info;
}

static MachineInfo _machine_info = GetMachineInfo();

int GetLogicalCoreCount() {
    return _machine_info.logical_core_count;
}

int GetPhysicalCoreCount() {
    return _machine_info.physical_core_count;
}

int GetNumaNodeCount() {
    return _machine_info.numa_node_count;
}

CoreSet GetLogicalCoresOnNumaNode(int numa_node_id) {
    CoreSet cores(_machine_info.logical_core_count);
    cores.reset();
    for(int i = 0; i < _machine_info.logical_core_count; i++) {
        if(_machine_info.logical_cores[i].numa_node_id == numa_node_id) {
            cores.set(i);
        }
    }
    return cores;
}

CoreSet GetAllLogicalCores() {
    CoreSet cores(_machine_info.logical_core_count);
    cores.reset();
    for(int i = 0; i < _machine_info.logical_core_count; i++) {
        cores.set(i);
    }
    return cores;
}

inline void PosixAssert(bool condition, int posix_errno=0) {
    if(!condition) {
        auto eno = posix_errno ? posix_errno : errno;
        const auto& err_msg = std::strerror(eno);
        // fmt::print(stderr, "Posix Error({}): {}\n", eno, err_msg);
        auto msg = fmt::format("Posix Error({}): {}", eno, err_msg);
        throw RuntimeError(msg);
    }
}

struct Page {
    std::array<std::byte, PAGE_SIZE> data;
};
static_assert(sizeof(Page) == PAGE_SIZE);

} // namespace dcsr

#endif // __DCSR_ENV_BASE_H__