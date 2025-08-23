#ifndef __METRICS_H__
#define __METRICS_H__

#include <chrono>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>

int64_t GetRSS() {
    std::ifstream stat_file("/proc/self/stat");
    std::string line;
    std::getline(stat_file, line);
    std::istringstream iss(line);
    std::string token;

    // 跳过前 23 个字段
    for (int i = 0; i < 23; ++i) {
        iss >> token;
    }

    // 读取第 24 个字段（RSS）
    iss >> token;
    return std::stol(token) * sysconf(_SC_PAGESIZE);
}

class SimpleTimer {
public:
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = std::chrono::time_point<Clock>;
    using Duration = std::chrono::duration<double>;
private:
    TimePoint start_;

public:
    SimpleTimer() {
        start_ = Clock::now();
    }

    double Lap() {
        auto end = Clock::now();
        Duration duration = end - start_;
        start_ = end;
        return duration.count();
    }

    double Stop() {
        auto end = Clock::now();
        Duration duration = end - start_;
        return duration.count();
    }
};

class StopWatch {
public:
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = std::chrono::time_point<Clock>;
    using Duration = std::chrono::duration<double>;
private:
    std::vector<TimePoint> points_;

public:
    StopWatch() {
        points_.reserve(8);
        points_.emplace_back(Clock::now());
    }

    StopWatch(size_t max_laps) {
        points_.reserve(max_laps);
        points_.emplace_back(Clock::now());
    }

    double Start() {
        points_.clear();
        points_.emplace_back(Clock::now());
        return Duration(points_[1] - points_[0]).count();
    }

    double Lap() {
        points_.emplace_back(Clock::now());
        size_t n = points_.size();
        return Duration(points_[n-1] - points_[n-2]).count();
    }

    double Stop() {
        points_.emplace_back(Clock::now());
        return Duration(points_.back() - points_.front()).count();
    }

};


template<typename Func, typename... Args>
double TimeIt(Func&& func, Args&&... args) {
    SimpleTimer timer;
    func(std::forward<Args>(args)...);
    return timer.Stop();
}

#endif // __METRICS_H__
