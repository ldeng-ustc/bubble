#ifndef __DCSR_ENV_FS_H__
#define __DCSR_ENV_FS_H__

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "base.h"
#include "memory.h"

namespace dcsr {

// File Operations

inline int Open(const fs::path& path, int flags=O_RDONLY) {
    auto cpath = path.c_str();
    auto fd = open(cpath, flags);
    PosixAssert(fd >= 0);
    return fd;
}

inline int OpenRead(const fs::path& path, int flags=O_RDONLY) {
    return Open(path, O_RDONLY | flags);
}

inline int Create(const fs::path& path, mode_t mode=0644) {
    auto cpath = path.c_str();
    auto fd = open(cpath, O_RDWR | O_CREAT | O_TRUNC, mode);
    PosixAssert(fd >= 0);
    return fd;
}

inline void Close(int fd) {
    auto ret = close(fd);
    PosixAssert(ret == 0);
}

inline size_t FileSize(int fd) {
    struct stat st;
    auto ret = fstat(fd, &st);
    PosixAssert(ret == 0);
    return st.st_size;
}

inline size_t FTell(int fd) {
    auto ret = lseek(fd, 0, SEEK_CUR);
    PosixAssert(ret >= 0);
    return ret;
}

inline void LSeek(int fd, size_t offset) {
    auto ret = lseek(fd, offset, SEEK_SET);
    PosixAssert(ret >= 0);
}

inline void Truncate(int fd, size_t length) {
    auto ret = ftruncate(fd, length);
    PosixAssert(ret == 0);
}

template<typename T>
inline size_t ReadArray(int fd, T* buf, size_t count) {
    // fmt::println("[Read] Read {} items", count);
    auto ret = read(fd, buf, count * sizeof(T));
    PosixAssert(ret >= 0);
    return ret / sizeof(T);
}

template<typename T>
inline size_t PRead(int fd, T* buf, size_t count, size_t offset) {
    auto ret = pread(fd, buf, count * sizeof(T), offset);
    PosixAssert(ret >= 0);
    return ret / sizeof(T);
}

template<typename T>
inline size_t WriteArray(int fd, const T* buf, size_t count) {
    auto ret = write(fd, buf, count * sizeof(T));
    PosixAssert(ret >= 0);
    return ret / sizeof(T);
}

template<typename T>
inline mmap_uptr<T> Mmap(int fd, void* addr, size_t byte_size, int prot, int flags, size_t offset) {
    void* ptr = mmap(addr, byte_size, prot, flags, fd, offset);
    PosixAssert(ptr != MAP_FAILED);
    if constexpr (std::is_array_v<T>) {
        return mmap_uptr<T>(static_cast<std::remove_extent_t<T>*>(ptr), byte_size);
    } else {
        return mmap_uptr<T>(static_cast<T*>(ptr));
    }
}

template<typename T>
inline mmap_uptr<T> MmapObject(int fd, size_t offset=0, int prot=PROT_READ|PROT_WRITE, int flags=MAP_SHARED) {
    return Mmap<T>(fd, nullptr, sizeof(T), prot, flags, offset);
}

template<typename T>
inline mmap_uptr<T[]> MmapArray(int fd, size_t array_size, size_t offset=0, int prot=PROT_READ|PROT_WRITE, int flags=MAP_SHARED) {
    return Mmap<T[]>(fd, nullptr, array_size * sizeof(T), prot, flags, offset);
}

} // namespace dcsr

#endif // __DCSR_ENV_H__