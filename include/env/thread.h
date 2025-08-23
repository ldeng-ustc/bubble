#ifndef __DCSR_ENV_THREAD_H__
#define __DCSR_ENV_THREAD_H__

#include <thread>
#include "base.h"


namespace dcsr {

// Thread

inline void SetAffinity(pthread_t thread_id, size_t core_id) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(core_id, &mask);
    int ret = pthread_setaffinity_np(thread_id, sizeof(mask), &mask);
    PosixAssert(ret == 0);
}

inline void SetAffinityThisThread(size_t core_id) {
    (void) core_id;
    SetAffinity(pthread_self(), core_id);
}

inline void SetAffinityMultiCores(pthread_t thread_id, const std::vector<size_t>& core_ids) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    for(auto& core_id : core_ids) {
        CPU_SET(core_id, &mask);
    }
    int ret = pthread_setaffinity_np(thread_id, sizeof(mask), &mask);
    PosixAssert(ret == 0);
}

inline void SetAffinityThisThreadMultiCores(const std::vector<size_t>& core_ids) {
    SetAffinityMultiCores(pthread_self(), core_ids);
}

inline void UnsetAffinity(pthread_t thread_id) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    memset(&mask, -1, sizeof(mask));
    int ret = pthread_setaffinity_np(thread_id, sizeof(mask), &mask);
    PosixAssert(ret == 0);
}

inline void UnsetAffinityThisThread() {
    UnsetAffinity(pthread_self());
}

/**
 * @brief A lightweight mutex implement BasicLockable with binary semaphore.
 * (Deprecated) Because it is not efficient, still switch into kernel.
 */
class SemaMutex {
private:
    std::binary_semaphore sema_;
public:
    SemaMutex(): sema_(1) {}
    void lock() { sema_.acquire(); }
    void unlock() { sema_.release(); }
};

/**
 * @brief Maybe useful spin lock, do note that spinlock mutexes are extremely dubious in practice.
 * https://www.realworldtech.com/forum/?threadid=189711&curpostid=189723
 * 
 */
class SpinMutex {
    std::atomic_flag m_;
public:
    SpinMutex() noexcept : m_{} {}

    void lock() noexcept {
        while (m_.test_and_set(std::memory_order_acquire)) {
            // Since C++20, locks can be acquired only after notification in the unlock,
            // avoiding any unnecessary spinning.
            // Note that even though wait gurantees it returns only after the value has
            // changed, the lock is acquired after the next condition check.
            m_.wait(true, std::memory_order_relaxed);
        }
    }

    bool try_lock() noexcept {
        return !m_.test_and_set(std::memory_order_acquire);
    }

    void unlock() noexcept
    {
        m_.clear(std::memory_order_release);
        m_.notify_one();
    }
};

void WaitFlag(const std::atomic_flag& flag) {
    while(!flag.test(std::memory_order_acquire)) {
        flag.wait(true, std::memory_order_relaxed);
    }
}

class SpinBinarySemaphore {
    std::atomic_flag flag_;
public:
    SpinBinarySemaphore(): flag_{} {}

    SpinBinarySemaphore(bool init_state): flag_{} {
        if(init_state == 0) {
            flag_.test_and_set(std::memory_order_acquire);
        } else {
            flag_.clear(std::memory_order_release);
        }
    }

    void acquire() {
        while(flag_.test_and_set(std::memory_order_acquire)) {
            flag_.wait(true, std::memory_order_relaxed);
        }
    }

    bool try_acquire() {
        return !flag_.test_and_set(std::memory_order_acquire);
    }

    void release() {
        flag_.clear(std::memory_order_release);
        flag_.notify_one();
    }
};


} // namespace dcsr

#endif // __DCSR_ENV_THREAD_H__