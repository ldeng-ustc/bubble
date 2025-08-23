// Copyright 2023 Mihail Dumitrescu mhdm.dev
// Provided under "MIT Licence" terms.
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>
#include <bit>
#include <functional>

template <class ForwardIt, class T, class Compare>
constexpr ForwardIt sb_lower_bound(ForwardIt first, ForwardIt last, const T& value, Compare comp) {
   auto length = last - first;
   while (length > 0) {
      auto half = length / 2;
      if (comp(first[half], value)) {
         // length - half equals half + length % 2
         first += length - half;
      }
      length = half;
   }
   return first;
}

template <class ForwardIt, class T>
constexpr ForwardIt sb_lower_bound(ForwardIt first, ForwardIt last, const T& value) {
   return sb_lower_bound(first, last, value, std::less<>{});
}

template <class ForwardIt, class T, class Compare>
constexpr ForwardIt sbm_lower_bound(ForwardIt first, ForwardIt last, const T& value, Compare comp) {
   auto length = last - first;
   while (length > 0) {
      auto half = length / 2;
      // gcc generates a cmov with a multiply instead of a ternary conditional
      first += comp(first[half], value) * (length - half);
      length = half;
   }
   return first;
}

template <class ForwardIt, class T>
constexpr ForwardIt sbm_lower_bound(ForwardIt first, ForwardIt last, const T& value) {
   return sbm_lower_bound(first, last, value, std::less<>{});
}

// Copyright 2023 Mihail Dumitrescu mhdm.dev
// Provided under "MIT Licence" terms.
// SPDX-License-Identifier: MIT

template <class ForwardIt, class T, class Compare>
constexpr ForwardIt sbpm_lower_bound(
      ForwardIt first, ForwardIt last, const T& value, Compare comp) {
   auto length = last - first;
   // Sized to roughly fit in L2 cache
   constexpr int entries_per_256KB = 256 * 1024 / sizeof(T);
   if (length >= entries_per_256KB) {
      constexpr int num_per_cache_line = std::max(64 / int(sizeof(T)), 1);
      while (length >= 3 * num_per_cache_line) {
         auto half = length / 2;
         __builtin_prefetch(&first[half / 2]);
         // length - half equals half + length % 2
         auto first_half1 = first + (length - half);
         __builtin_prefetch(&first_half1[half / 2]);
         first += comp(first[half], value) * (length - half);
         length = half;
      }
   }
   while (length > 0) {
      auto half = length / 2;
      // gcc generates a cmov with a multiply instead of a ternary conditional
      first += comp(first[half], value) * (length - half);
      length = half;
   }
   return first;
}

template <class ForwardIt, class T>
constexpr ForwardIt sbpm_lower_bound(ForwardIt first, ForwardIt last, const T& value) {
   return sbpm_lower_bound(first, last, value, std::less<>{});
}

inline size_t bit_floor(size_t i)
{
    constexpr int num_bits = sizeof(i) * 8;
    return size_t(1) << (num_bits - std::countl_zero(i) - 1);
}
inline size_t bit_ceil(size_t i)
{
    constexpr int num_bits = sizeof(i) * 8;
    return size_t(1) << (num_bits - std::countl_zero(i - 1));
}

template<typename It, typename T, typename Cmp>
It branchless_lower_bound(It begin, It end, const T & value, Cmp && compare)
{
    std::size_t length = end - begin;
    if (length == 0)
        return end;
    std::size_t step = bit_floor(length);
    if (step != length && compare(begin[step], value))
    {
        length -= step + 1;
        if (length == 0)
            return end;
        step = bit_ceil(length);
        begin = end - step;
    }
    for (step /= 2; step != 0; step /= 2)
    {
        if (compare(begin[step], value))
            begin += step;
    }
    return begin + compare(*begin, value);
}

template<typename It, typename T>
It branchless_lower_bound(It begin, It end, const T & value)
{
    return branchless_lower_bound(begin, end, value, std::less<>{});
}

template <class ForwardIt, class T, class Compare>
constexpr ForwardIt asm_lower_bound(ForwardIt first, ForwardIt last, const T& value, Compare comp) {
   // Works for x86 only
   auto length = last - first;
   while (length > 0) {
      auto rem = length % 2;
      length /= 2;
      auto firstplus = first + length + rem;
      // Does a comparison which sets some x86 FLAGS like CF or ZF
      bool compare = comp(first[length], value);
      // Inline assembly doesn't support passing bools straight into FLAGS
      // so we ask the compiler to copy it from FLAGS into a register
      __asm__(
            // Then we test the register, which sets ZF=!compare and CF=0
            // Reference: https://www.felixcloutier.com/x86/test
            "test %[compare],%[compare]\n"
            // cmova copies firstplus into first if ZF=0 and CF=0
            // Reference: https://www.felixcloutier.com/x86/cmovv
            "cmova %[firstplus],%[first]\n"
            : [first] "+r"(first)
            : [firstplus] "r"(firstplus), [compare] "r"(compare));
   }
   return first;
}

template <class ForwardIt, class T>
constexpr ForwardIt asm_lower_bound(ForwardIt first, ForwardIt last, const T& value) {
   return asm_lower_bound(first, last, value, std::less<>{});
}
