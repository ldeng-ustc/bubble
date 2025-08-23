#ifndef __DCSR_COMMON_H__
#define __DCSR_COMMON_H__

#include <algorithm>
#include <memory>
#include <random>
#include <optional>
#include <string_view>
#include <source_location>
#include "fmt/format.h"

namespace dcsr {

#ifndef DCSR_DEBUG
#define DCSR_DEBUG 0
#endif

#define RUN_EXPR_IN_DEBUG(expr) do { if constexpr (DCSR_DEBUG) { expr; } } while (0)
#define RUN_IN_DEBUG if constexpr (DCSR_DEBUG)


#define dcsr_assume(cond) do { if (!(cond)) __builtin_unreachable(); } while (0)

class RuntimeError : public std::runtime_error {
public:
    RuntimeError(const std::string& msg) : std::runtime_error(msg) {}
    RuntimeError(const char* msg) : std::runtime_error(msg) {}
};

void dcsr_assert(bool cond, std::string_view msg="", std::source_location loc=std::source_location::current()) {
    if(!cond) {
        fmt::print(stderr, "Assertion failed: {} at {}:{}\n", msg, loc.file_name(), loc.line());
        throw RuntimeError("Assertion failed");
    }
}

template<typename T, typename U>
    requires std::convertible_to<U, T>
constexpr T round_up(T num, U multiple) {
    T multiple_ = static_cast<T>(multiple);
    return ((num + multiple_ - 1) / multiple_) * multiple_;
}

template<typename T, typename U>
    requires std::convertible_to<U, T>
constexpr T div_up(T num, U den) {
    T den_ = static_cast<T>(den);
    return (num + den_ - 1) / den_;
}

// choose from https://doi.org/10.1002/spe.3030 , TABLE 7
// x[i+1] = (a*x[i]) mod 2^64, a = 0xe817fb2d
// 永远是奇数，不太好
//using fastest_random_engine = std::linear_congruential_engine<uint64_t, 0xe817fb2d, 0, 0>;

// choose from https://doi.org/10.1002/spe.3030 , TABLE 6
// 奇偶循环，其实也有问题，最好不用低位
using fastest_random_engine = std::linear_congruential_engine<uint64_t, 0xf9b25d65, 1, 0>;

thread_local std::mt19937 _thread_local_random_generator(std::random_device{}());

void set_random_seed(uint32_t seed) {
    _thread_local_random_generator.seed(seed);
}

/**
 * @brief Random integer in the range [l, r)
 */
template<typename T>
    requires std::integral<T>
T random_int(T l, T r) {
    std::uniform_int_distribution<T> dis(l, r - 1);
    return dis(_thread_local_random_generator);
}

template<typename T, typename R=std::mt19937>
void generate_random(T* arr, size_t n, T l, T r, std::optional<uint32_t> seed=std::nullopt) {
    uint_fast32_t real_seed = (seed.has_value()) ? seed.value() : std::random_device{}();
    R gen(real_seed);
    std::uniform_int_distribution<T> dis(l, r - 1);
    for(size_t i = 0; i < n; i++) {
        arr[i] = dis(gen);
    }
}

template<typename T, typename R>
void generate_random_with_engine(T* arr, size_t n, T l, T r, R& gen) {
    std::uniform_int_distribution<T> dis(l, r - 1);
    for(size_t i = 0; i < n; i++) {
        arr[i] = dis(gen);
    }
}


/**
 * @brief generate a unique pointer with the given size, 
 *        and initialize it with random values in the range [l, r)
 * 
 * @tparam T 
 * @param n 
 * @param l 
 * @param r 
 * @param seed 
 * @return std::unique_ptr<T[]> 
 */
template<typename T>
std::unique_ptr<T[]> make_unique_with_random(size_t n, T l, T r, std::optional<uint32_t> seed=std::nullopt) {
    auto arr = std::make_unique_for_overwrite<T[]>(n);
    generate_random(arr.get(), n, l, r, seed);
    return arr;
}

template<typename T, typename R=std::mt19937>
std::unique_ptr<T[]> make_combination(T l, T r, std::optional<uint32_t> seed=std::nullopt) {
    dcsr_assert(l < r, "l should be less than r");
    size_t n = static_cast<size_t>(r - l);
    auto arr = std::make_unique_for_overwrite<T[]>(n);
    for(size_t i = 0; i < n; i++) {
        arr[i] = l + i;
    }
    uint_fast32_t real_seed = (seed.has_value()) ? seed.value() : std::random_device{}();
    R gen(real_seed);
    std::shuffle(arr.get(), arr.get() + n, gen);
    return arr;
}

template<typename T, typename R=std::mt19937>
std::unique_ptr<T[]> make_combination(T n) {
    return make_combination<T, R>(0, n);
}

} // namespace dcsr

#endif // __DCSR_COMMON_H__