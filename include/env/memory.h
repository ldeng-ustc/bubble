/**
 * @file memory.h
 * @author Long Deng (ldeng@mail.ustc.edu.cn)
 * @brief memory management utilities, defined some smart pointers and memory allocation functions
 *          mmap_uptr: a smart pointer for mmaped memory
 *          aligned_uptr: a smart pointer for aligned memory
 *          huge_uptr: a smart pointer for the memory (be advised to) use huge page
 * @version 0.1
 * @date 2024-03-21
 * 
 * @copyright Copyright (c) 2024
 * 
 */

#ifndef __DCSR_ENV_MEMORY_H___
#define __DCSR_ENV_MEMORY_H___

#include <memory>
#include <cstdlib>
#include <malloc.h>
#include <sys/mman.h>

#include "numa.h"
#include "common.h"

namespace dcsr {

// Wrapper of numa.h interface

void* NumaAllocOnNode(size_t size, int node) {
    void* ptr = numa_alloc_onnode(size, node);
    dcsr_assert(ptr != nullptr, "numa_alloc_onnode failed");
    return ptr;
}

void NumaFree(void* ptr, size_t size) {
    numa_free(ptr, size);
}

template<typename T>
T* NumaAllocArrayOnNode(size_t size, int node) {
    return static_cast<T*>(NumaAllocOnNode(sizeof(T) * size, node));
}

template<typename T>
void NumaFreeArray(T* ptr, size_t size) {
    NumaFree(ptr, sizeof(T) * size);
}

// mmap_uptr

template<typename T>
struct mmap_deleter {
    inline void operator() (T* pointer) {
        auto ret = munmap(pointer, sizeof(T));
        PosixAssert(ret == 0);
    }
};

template<typename T>
struct mmap_deleter<T[]> {
    size_t size;
    mmap_deleter(size_t size) noexcept: size(size) {}
    mmap_deleter() = default;    // default construct use to init empty unique_ptr

    inline void operator() (T* pointer) {
        // fmt::println("munmap ptr={}, size={}", fmt::ptr(pointer), size);
        auto ret = munmap(pointer, size);
        // fmt::println("munmap ret: {}", ret);
        PosixAssert(ret == 0);
        // fmt::println("munmap done");
    }
};

template<typename T>
using mmap_uptr = std::unique_ptr<T, mmap_deleter<T>>;


// aligned_uptr

template<typename T>
class aligned_deleter {
public:
    void operator()(T* ptr) {
        std::free(ptr);
    }
};

template<typename T>
    requires std::is_unbounded_array_v<T>
class aligned_deleter<T> {
public:
    void operator()(std::remove_extent_t<T>* ptr) {
        std::free(ptr);
    }
};

template<typename T>
using aligned_uptr = std::unique_ptr<T, aligned_deleter<T>>;

template<typename T, typename... Args>
    requires (!std::is_array_v<T>)
aligned_uptr<T> make_aligned(size_t aligned, Args&&... args) {
    void* ptr = std::aligned_alloc(aligned, sizeof(T));
    if(ptr == nullptr) {
        throw std::bad_alloc();
    }
    new (ptr) T(std::forward<Args>(args)...);
    return aligned_uptr<T>(static_cast<T*>(ptr));
}

template<typename T>
    requires std::is_unbounded_array_v<T>
aligned_uptr<T> make_aligned(size_t aligned, size_t size) {
    void* ptr = std::aligned_alloc(aligned, sizeof(std::remove_extent_t<T>) * size);
    if(ptr == nullptr) {
        throw std::bad_alloc();
    }
    new (ptr) T[size]();
    return aligned_uptr<T>(static_cast<std::remove_extent_t<T>*>(ptr));
}

template<typename T, typename... Args>
    requires std::is_bounded_array_v<T>
aligned_uptr<T> make_aligned(Args&&...) = delete;

template<typename T>
    requires (!std::is_array_v<T>)
aligned_uptr<T> make_aligned_for_overwrite(size_t aligned) {
    void* ptr = std::aligned_alloc(aligned, sizeof(T));
    if(ptr == nullptr) {
        throw std::bad_alloc();
    }
    return aligned_uptr<T>(static_cast<T*>(ptr));
}

template<typename T>
    requires std::is_unbounded_array_v<T>
aligned_uptr<T> make_aligned_for_overwrite(size_t size, size_t aligned) {
    void* ptr = std::aligned_alloc(aligned, sizeof(std::remove_extent_t<T>) * size);
    if(ptr == nullptr) {
        throw std::bad_alloc();
    }
    return aligned_uptr<T>(static_cast<std::remove_extent_t<T>*>(ptr));
}

template<class T, class... Args>
    requires std::is_bounded_array_v<T>
aligned_uptr<T> make_aligned_for_overwrite(Args&&...) = delete;


// huge_uptr

template<typename T>
using huge_uptr = mmap_uptr<T>;

void* mmap_huge(size_t size) {
    void* ptr = mmap(nullptr, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB, -1, 0);
    if(ptr == MAP_FAILED) {
        return nullptr;
    }
    int ret = madvise(ptr, size, MADV_HUGEPAGE);
    if(ret != 0) {
        return nullptr;
    }
    return ptr;
}

std::tuple<void*, bool> mmap_huge_first(size_t size) {
    bool huge = true;
    void* ptr = mmap_huge(size);
    if(ptr == nullptr) {
        // Fall back to normal page
        ptr = mmap(nullptr, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        PosixAssert(ptr != MAP_FAILED);
        huge = false;
    }
    return {ptr, huge};
}

template<typename T>
    requires std::is_unbounded_array_v<T>
huge_uptr<T> make_huge(size_t size) {
    size_t real_size = sizeof(std::remove_extent_t<T>) * size;
    real_size = round_up(real_size, HUGE_PAGE_SIZE);
    auto [ptr, is_huge] = mmap_huge_first(real_size);
    if(!is_huge) {
        fmt::println("Warning: huge page allocation failed, fallback to normal page");
    }
    new (ptr) T[size]();
    return huge_uptr<T>(static_cast<std::remove_extent_t<T>*>(ptr), mmap_deleter<T>(real_size));
}

template<typename T, typename... Args>
    requires std::is_unbounded_array_v<T>
huge_uptr<T> make_huge_for_overwrite(size_t size) {
    size_t real_size = sizeof(std::remove_extent_t<T>) * size;
    real_size = round_up(real_size, HUGE_PAGE_SIZE);
    auto [ptr, is_huge] = mmap_huge_first(real_size);
    if(!is_huge) {
        fmt::println("Warning: huge page allocation failed, fallback to normal page");
    }
    return huge_uptr<T>(static_cast<std::remove_extent_t<T>*>(ptr), mmap_deleter<T>(real_size));
}

template<typename T, typename... Args>
    requires (!std::is_unbounded_array_v<T>)
huge_uptr<T> make_huge(Args&&...) = delete;

template<typename T, typename... Args>
    requires (!std::is_unbounded_array_v<T>)
huge_uptr<T> make_huge_for_overwrite(Args&&...) = delete;

} // namespace dcsr
#endif // __DCSR_ENV_MEMORY_H___
