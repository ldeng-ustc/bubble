#ifndef __DCSR_CHECKER_H__
#define __DCSR_CHECKER_H__

#include <cstddef>
#include "common.h"

namespace dcsr {

template<typename T, typename Cmp>
void check_sorted(T* arr, size_t n, const Cmp& cmp) {
    for(size_t i = 1; i < n; i++) {
        dcsr_assert(!cmp(arr[i], arr[i - 1]), "Array is not sorted");
    }
}

template<typename Edge>
    requires requires(Edge e) { e.from; }
void check_from_in_range(Edge* edges, size_t n, size_t l, size_t r) {
    for(size_t i = 0; i < n; i++) {
        dcsr_assert(edges[i].from >= l && edges[i].from < r, "From out of range");
    }
}

}

#endif // __DCSR_CHECKER_H__