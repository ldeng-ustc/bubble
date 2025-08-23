#ifndef __DCSR_TEST_IMPORTER_H__
#define __DCSR_TEST_IMPORTER_H__

#include <utility>
#include "indicators/progress_bar.hpp"

#include "env.h"
#include "metrics.h"

namespace dcsr {

template<typename ItemType, size_t BufferSize, typename Func>
std::pair<double, double> ScanFile(const std::filesystem::path& file, Func&& func) {
    static std::array<ItemType, BufferSize> buffer;    
    int fd = Open(file);
    double read_time = 0;
    double process_time = 0;
    while(true) {
        size_t cnt;
        read_time += TimeIt([&] {
            cnt = ReadArray<ItemType>(fd, buffer.data(), BufferSize);
        });
        // fmt::println("Read {} items", cnt);
        process_time += TimeIt([&] {
            for(size_t i = 0; i < cnt; i++) {
                func(buffer[i]);
            }
        });
        if(cnt < BufferSize) {
            break;
        }
    }
    Close(fd);
    return std::make_pair(read_time, process_time);
}

// void PrintScanProgress(size_t batch_cnt, size_t read_cnt, size_t total_cnt) {
//     double read_cnt_million = read_cnt / 1e6;
//     double total_cnt_million = total_cnt / 1e6;
//     double progress = read_cnt * 100.0 / total_cnt;
//     // fmt::println(
//     //     "[ScanLargeFile] ({:.2f}%) Read {:8} objects, total {:.2f}M/{:.2f}M objects",
//     //     progress, batch_cnt, read_cnt_million, total_cnt_million
//     // );
// }

// template<typename ItemType>
// void PrintScanSpeed(size_t batch_cnt, double batch_read_time, double batch_process_time) {
//     double batch_time = batch_read_time + batch_process_time;
//     double batch_speed_m = batch_cnt / batch_time / 1e6;
//     double batch_speed_mb = batch_speed_m * sizeof(ItemType);
//     fmt::println(
//         "      Time(R/P): {:4.2f}s/{:4.2f}s, Speed: {:5.2f}M/s ({:5.2f}MB/s)",
//         batch_read_time, batch_process_time, batch_speed_m, batch_speed_mb
//     );
// }

template<typename ItemType>
void PrintScanProgress(indicators::ProgressBar& bar, 
            size_t batch_cnt, size_t read_cnt, size_t total_cnt, 
            double batch_read_time, double batch_process_time
) {
    double progress = read_cnt * 100.0 / total_cnt;
    double read_cnt_million = read_cnt / 1e6;
    double total_cnt_million = total_cnt / 1e6;
    double batch_time = batch_read_time + batch_process_time;
    double batch_speed_m = batch_cnt / batch_time / 1e6;
    double batch_speed_mb = batch_speed_m * sizeof(ItemType);

    // Show progress bar on the top of the terminal
    // ANSI escape code:
    // \033[s: Save cursor position
    // \033[u: Restore cursor position
    // \033[H: Move cursor to top left
    auto postfix = fmt::format("{:.2f}% | {:.2f}M/{:.2f}M | R/P: {:.2f}s/{:.2f}s ({:.2f}MB/s) [ScanLargeFile]",
        progress, read_cnt_million, total_cnt_million, batch_read_time, batch_process_time, batch_speed_mb
    );
    //bar.set_option(indicators::option::PrefixText{"\033[s\033[H"});
    // bar.set_option(indicators::option::PostfixText{postfix + "\033[u"});
    bar.set_option(indicators::option::PrefixText{""});
    bar.set_option(indicators::option::PostfixText{postfix});
    bar.set_progress(progress);
}

struct ScanOptions {
    indicators::ProgressBar* bar;
    bool silent;
    ScanOptions(): bar(nullptr), silent(false) {}
};

/**
 * @brief Iterate over a large file with a buffer, and call the given function for each item.
 *        Only read the segment from `st_count` to `ed_count` items.
 * 
 * @tparam ItemType Element type of the file
 * @tparam BufferSize Buffer size 
 * @tparam Func Function type
 * @param file File path
 * @param st_offset Start file offset (in bytes)
 * @param count Read items count (in items number)
 * @param opts Scan options
 * @param func Function to call for each item
 * @return std::pair<double, double> Read time and process time
 */
template<typename ItemType, size_t BufferSize, typename Func>
std::pair<double, double> ScanLargeFileSegmentOptions(const std::filesystem::path& file, size_t st_offset, size_t count, ScanOptions opts, Func&& func) {
    static std::array<ItemType, BufferSize> buffer;    
    int fd = Open(file);
    size_t file_size = FileSize(fd);
    size_t total_cnt = (file_size - st_offset) / sizeof(ItemType);

    if(st_offset >= file_size) {
        fmt::println("ScanLargeFileSegment: start offset {} >= file size {}, read nothing.", st_offset, file_size);
        return std::make_pair(0, 0);
    }

    if(total_cnt < count) {
        fmt::println("ScanLargeFileSegment: total count {} < count {}, read all.", total_cnt, count);
    } else {
        total_cnt = count;
    }

    indicators::ProgressBar* bar = opts.bar;
    size_t read_cnt = 0;
    double read_time = 0;
    double process_time = 0;

    LSeek(fd, st_offset);
    while(read_cnt < total_cnt) {
        size_t cnt;
        double batch_read_time = TimeIt([&] {
            cnt = ReadArray<ItemType>(fd, buffer.data(), std::min(BufferSize, total_cnt - read_cnt));
        });

        bool stop = false;
        size_t i;
        double batch_process_time = TimeIt([&] {
            for(i = 0; i < cnt; i++) {
                if constexpr(std::is_same_v<std::invoke_result_t<Func, ItemType>, bool>) {
                    stop = !func(buffer[i]);
                    if(stop) {
                        break;
                    }
                } else {
                    func(buffer[i]);
                }
            }
        });

        read_cnt += i;
        read_time += batch_read_time;
        process_time += batch_process_time;

        if(bar != nullptr) {
            PrintScanProgress<ItemType>(*bar, cnt, read_cnt, total_cnt, batch_read_time, batch_process_time);
        }
        // PrintScanProgress(cnt, read_cnt, total_cnt);
        // PrintScanSpeed<ItemType>(cnt, batch_read_time, batch_process_time);

        if(cnt < BufferSize || stop) {
            break;
        }
    }
    Close(fd);

    if(! opts.silent) {
        fmt::println("ScanLargeFile end: read {}/{} items.", read_cnt, total_cnt);
        fmt::println("Read time: {:.3f}s, Process time: {:.3f}s", read_time, process_time);
    }
    return std::make_pair(read_time, process_time);
}


/**
 * @brief Iterate over a large file with a buffer, and call the given function for each item.
 *        Only read the segment from `st_count` to `ed_count` items.
 * 
 * @tparam ItemType Element type of the file
 * @tparam BufferSize Buffer size 
 * @tparam Func Function type
 * @param file File path
 * @param st_offset Start file offset (in bytes)
 * @param count Read items count (in items number)
 * @param func Function to call for each item
 * @return std::pair<double, double> Read time and process time
 */
template<typename ItemType, size_t BufferSize, typename Func>
std::pair<double, double> ScanLargeFileSegment(const std::filesystem::path& file, size_t st_offset, size_t count, Func&& func) {
    indicators::ProgressBar bar;
    ScanOptions opts;
    opts.bar = &bar;
    return ScanLargeFileSegmentOptions<ItemType, BufferSize>(file, st_offset, count, opts, func);
}


/**
 * @brief Same as ScanLargeFileSegment, but without progress bar and print.
 */
template<typename ItemType, size_t BufferSize, typename Func>
std::pair<double, double> ScanLargeFileSegmentSilent(const std::filesystem::path& file, size_t st_offset, size_t count, Func&& func) {
    ScanOptions opts;
    opts.silent = true;
    return ScanLargeFileSegmentOptions<ItemType, BufferSize>(file, st_offset, count, opts, func);
}


/**
 * @brief Iterate over a large file with a buffer, and call the given function for each item.
 *        Only read the first `count` items.
 * 
 * @tparam ItemType Element type of the file
 * @tparam BufferSize Buffer size 
 * @tparam Func Function type
 * @param file File path
 * @param count Number of items to read
 * @param func Function to call for each item
 * @return std::pair<double, double> Read time and process time
 */
template<typename ItemType, size_t BufferSize, typename Func>
std::pair<double, double> ScanLargeFileHead(const std::filesystem::path& file, size_t count, Func&& func) {
    indicators::ProgressBar bar;
    ScanOptions opts;
    opts.bar = &bar;
    return ScanLargeFileSegmentOptions<ItemType, BufferSize>(file, 0, count, opts, func);
}


/**
 * @brief Iterate over a large file with a buffer, and call the given function for each item.
 * 
 * @tparam ItemType Element type of the file
 * @tparam BufferSize Buffer size 
 * @tparam Func Function type
 * @param file File path
 * @param func Function to call for each item
 * @return std::pair<double, double> Read time and process time
 */
template<typename ItemType, size_t BufferSize, typename Func>
std::pair<double, double> ScanLargeFile(const std::filesystem::path& file, Func&& func) {
    size_t file_size = fs::file_size(file);
    size_t total_cnt = file_size / sizeof(ItemType);
    return ScanLargeFileHead<ItemType, BufferSize>(file, total_cnt, func);
}


} // namespace dcsr


#endif // __DCSR_TEST_IMPORTER_H__