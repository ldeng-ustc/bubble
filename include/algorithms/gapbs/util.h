// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details

#ifndef UTIL_H_
#define UTIL_H_

#include <string>
#include "fmt/format.h"

#include "timer.h"


/*
GAP Benchmark Suite
Author: Scott Beamer

Miscellaneous helpers that don't fit into classes
*/


static const int64_t kRandSeed = 27491095;


void PrintLabel(const std::string &label, const std::string &val) {
    fmt::println("{:<21}{:>7}", label + ":", val);
}

void PrintTime(const std::string &s, double seconds) {
    fmt::println("{:<21}{:>3.5f}", s + ":", seconds);
}

void PrintStep(const std::string &s, int64_t count) {
    fmt::println("{:<14}{:>14}", s + ":", count);
}

void PrintStep(const std::string &s, double seconds, int64_t count = -1) {
    if (count != -1)
        fmt::println("{:5}{:11}{:10.5f}", s, count, seconds);
    else
        fmt::println("{:5}{:23.5f}", s, seconds);
}

void PrintStep(int step, double seconds, int64_t count = -1) {
  PrintStep(std::to_string(step), seconds, count);
}

// Runs op and prints the time it took to execute labelled by label
#define TIME_PRINT(label, op) {   \
  Timer t_;                       \
  t_.Start();                     \
  (op);                           \
  t_.Stop();                      \
  PrintTime(label, t_.Seconds()); \
}


template <typename T_>
class RangeIter {
  T_ x_;
 public:
  explicit RangeIter(T_ x) : x_(x) {}
  bool operator!=(RangeIter const& other) const { return x_ != other.x_; }
  T_ const& operator*() const { return x_; }
  RangeIter& operator++() {
    ++x_;
    return *this;
  }
};

template <typename T_>
class Range{
  T_ from_;
  T_ to_;
 public:
  explicit Range(T_ to) : from_(0), to_(to) {}
  Range(T_ from, T_ to) : from_(from), to_(to) {}
  RangeIter<T_> begin() const { return RangeIter<T_>(from_); }
  RangeIter<T_> end() const { return RangeIter<T_>(to_); }
};

#endif  // UTIL_H_