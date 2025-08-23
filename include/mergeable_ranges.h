#ifndef __DCSR_MERGEABLE_RANGES_H__
#define __DCSR_MERGEABLE_RANGES_H__

#include <boost/container/static_vector.hpp>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "env.h"

namespace dcsr {

class mergeable_ranges_iterator {
public:
    using value_type = std::pair<size_t, size_t>;
    using reference = value_type&;
    using pointer = value_type*;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::random_access_iterator_tag;
private:
    size_t* ptr_;
public:
    mergeable_ranges_iterator(size_t* ptr) : ptr_(ptr) {}

    reference operator*() {
        return *reinterpret_cast<pointer>(ptr_);
    }

    pointer operator->() {
        return reinterpret_cast<pointer>(ptr_);
    }

    mergeable_ranges_iterator& operator++() {
        ptr_ += 1;
        return *this;
    }

    mergeable_ranges_iterator operator++(int) {
        mergeable_ranges_iterator tmp = *this;
        ptr_ += 1;
        return tmp;
    }

    mergeable_ranges_iterator& operator--() {
        ptr_ -= 1;
        return *this;
    }

    mergeable_ranges_iterator operator--(int) {
        mergeable_ranges_iterator tmp = *this;
        ptr_ -= 1;
        return tmp;
    }

    mergeable_ranges_iterator& operator+=(difference_type n) {
        ptr_ += n;
        return *this;
    }

    mergeable_ranges_iterator operator+(difference_type n) {
        return mergeable_ranges_iterator(ptr_ + n);
    }

    mergeable_ranges_iterator& operator-=(difference_type n) {
        ptr_ -= n;
        return *this;
    }

    mergeable_ranges_iterator operator-(difference_type n) {
        return mergeable_ranges_iterator(ptr_ - n);
    }

    difference_type operator-(const mergeable_ranges_iterator& other) {
        return ptr_ - other.ptr_;
    }

    bool operator==(const mergeable_ranges_iterator& other) {
        return ptr_ == other.ptr_;
    }

    bool operator!=(const mergeable_ranges_iterator& other) {
        return ptr_ != other.ptr_;
    }

    bool operator<(const mergeable_ranges_iterator& other) {
        return ptr_ < other.ptr_;
    }

    bool operator>(const mergeable_ranges_iterator& other) {
        return ptr_ > other.ptr_;
    }

    bool operator<=(const mergeable_ranges_iterator& other) {
        return ptr_ <= other.ptr_;
    }

    bool operator>=(const mergeable_ranges_iterator& other) {
        return ptr_ >= other.ptr_;
    }
};

class mergeable_ranges_const_iterator {
public:
    using value_type = std::pair<size_t, size_t>;
    using reference = const value_type&;
    using pointer = const value_type*;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::random_access_iterator_tag;
private:
    const size_t* ptr_;
public:
    mergeable_ranges_const_iterator(const size_t* ptr) : ptr_(ptr) {}

    reference operator*() {
        return *reinterpret_cast<pointer>(ptr_);
    }

    pointer operator->() {
        return reinterpret_cast<pointer>(ptr_);
    }

    mergeable_ranges_const_iterator& operator++() {
        ptr_ += 1;
        return *this;
    }

    mergeable_ranges_const_iterator operator++(int) {
        mergeable_ranges_const_iterator tmp = *this;
        ptr_ += 1;
        return tmp;
    }

    mergeable_ranges_const_iterator& operator--() {
        ptr_ -= 1;
        return *this;
    }

    mergeable_ranges_const_iterator operator--(int) {
        mergeable_ranges_const_iterator tmp = *this;
        ptr_ -= 1;
        return tmp;
    }

    mergeable_ranges_const_iterator& operator+=(difference_type n) {
        ptr_ += n;
        return *this;
    }

    mergeable_ranges_const_iterator operator+(difference_type n) {
        return mergeable_ranges_const_iterator(ptr_ + n);
    }

    mergeable_ranges_const_iterator& operator-=(difference_type n) {
        ptr_ -= n;
        return *this;
    }

    mergeable_ranges_const_iterator operator-(difference_type n) {
        return mergeable_ranges_const_iterator(ptr_ - n);
    }

    difference_type operator-(const mergeable_ranges_const_iterator& other) {
        return ptr_ - other.ptr_;
    }

    bool operator==(const mergeable_ranges_const_iterator& other) {
        return ptr_ == other.ptr_;
    }

    bool operator!=(const mergeable_ranges_const_iterator& other) {
        return ptr_ != other.ptr_;
    }

    bool operator<(const mergeable_ranges_const_iterator& other) {
        return ptr_ < other.ptr_;
    }

    bool operator>(const mergeable_ranges_const_iterator& other) {
        return ptr_ > other.ptr_;
    }

    bool operator<=(const mergeable_ranges_const_iterator& other) {
        return ptr_ <= other.ptr_;
    }

    bool operator>=(const mergeable_ranges_const_iterator& other) {
        return ptr_ >= other.ptr_;
    }

};


template<size_t MAX_RANGES=64>
class mergeable_ranges {
public:
    using offset_vector = boost::container::static_vector<size_t, MAX_RANGES>;
    using range = std::pair<size_t, size_t>;
    using iterator = mergeable_ranges_iterator;
    using const_iterator = mergeable_ranges_const_iterator;
private:
    offset_vector starts_;      // [0, 3, 9] indicates 2 ranges [0, 3) and [3, 9)

public:
    mergeable_ranges() {
        starts_.push_back(0);
    }

// Iterators

    const_iterator begin() const {
        return const_iterator(&starts_[0]);
    }

    const_iterator end() const {
        return const_iterator(&starts_[size()]);
    }

    iterator begin() {
        return iterator(&starts_[0]);
    }

    iterator end() {
        return iterator(&starts_[size()]);
    }

    size_t size() const {
        return starts_.size() - 1;
    }

    range operator[] (size_t idx) const {
        return {starts_[idx], starts_[idx + 1]};
    }

    range front() const {
        return {starts_[0], starts_[1]};
    }

    range back() const {
        return {starts_[size() - 1], starts_[size()]};
    }

    size_t& start_of(size_t idx) {
        return starts_[idx];
    }

    size_t& end_of(size_t idx) {
        return starts_[idx + 1];
    }

    void append(size_t range_end) {
        starts_.push_back(range_end);
    }

    void merge(size_t idx1, size_t idx2) {
        starts_.erase(&end_of(idx1), &end_of(idx2));
    }

    void merge_end(size_t count) {
        // fmt::println("Range: {}", starts_);
        size_t ed = back().second;
        starts_.resize(starts_.size() - count + 1);
        starts_.back() = ed;
        // fmt::println("After merge Range: {}", starts_);
    }

    std::string to_string() const {
        return fmt::format("{}", starts_);
    }
};

} // namespace dcsr

#endif // __DCSR_MERGEABLE_RANGES_H__