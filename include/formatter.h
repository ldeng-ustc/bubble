#ifndef __DCSR_FORMATTER_H__
#define __DCSR_FORMATTER_H__

#include <locale>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "datatype.h"
#include "config.h"

template <typename T>
struct fmt::formatter<dcsr::CompactTarget<T>> {
    bool short_format = false;

    constexpr auto parse(fmt::format_parse_context& ctx) {
        auto it = ctx.begin(), end = ctx.end();
        if (it != end && *it == 's') {
            short_format = true;
            ++it;
        }
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const dcsr::CompactTarget<T>& ct, FormatContext& ctx) const {
        if (short_format){
            return fmt::format_to(ctx.out(), "{}", ct.vid);
        } else {
            if constexpr(std::is_same_v<T, void>)
                return fmt::format_to(ctx.out(), "<{}, tag={}>", ct.vid, ct.tag);
            else
                return fmt::format_to(ctx.out(), "<{}, w={}, tag={}>", ct.vid, ct.weight, ct.tag);
        }
    }
};

template <typename T>
struct fmt::formatter<dcsr::Edge<T>> {
    bool short_format = false;  // {:s} do not print tag
    bool only_from = false;     // {:f} only print from VID
    bool only_to = false;       // {:t} only print target(to) VID

    constexpr auto parse(fmt::format_parse_context& ctx) {
        auto it = ctx.begin(), end = ctx.end();
        if (it != end) {
            if (*it == 's') {
                short_format = true;
                ++it;
            } else if (*it == 'f') {
                only_from = true;
                ++it;
            } else if (*it == 't') {
                only_to = true;
                ++it;
            }
        }
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const dcsr::Edge<T>& e, FormatContext& ctx) const {
        if (short_format){
            if constexpr(std::is_same_v<T, void>)
                return fmt::format_to(ctx.out(), "<{}, {}>", e.from, e.to);
            else
                return fmt::format_to(ctx.out(), "<{}, {}, w={}>", e.from, e.to, e.weight);
        } else if (only_from) {
            return fmt::format_to(ctx.out(), "{}", e.from);
        } else if (only_to) {
            return fmt::format_to(ctx.out(), "{}", static_cast<dcsr::VID>(e.to));
        } else {
            if constexpr(std::is_same_v<T, void>)
                return fmt::format_to(ctx.out(), "<{}, {}, tag={}>", e.from, e.to, e.tag);
            else
                return fmt::format_to(ctx.out(), "<{}, {}, w={}, tag={}>", e.from, e.to, e.weight, e.tag);
        }
    }
};

template <typename T, typename V>
struct fmt::formatter<dcsr::RawEdge<T, V>> {
    bool only_from = false;     // {:f} only print from VID
    bool only_to = false;       // {:t} only print target(to) VID

    constexpr auto parse(fmt::format_parse_context& ctx) {
        auto it = ctx.begin(), end = ctx.end();
        if (it != end) {
            if (*it == 'f') {
                only_from = true;
                ++it;
            } else if (*it == 't') {
                only_to = true;
                ++it;
            }
        }
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const dcsr::Edge<T>& e, FormatContext& ctx) const {
        if (only_from) {
            return fmt::format_to(ctx.out(), "{}", e.from);
        } else if (only_to) {
            return fmt::format_to(ctx.out(), "{}", static_cast<dcsr::VID>(e.to));
        } else {
            if constexpr(std::is_same_v<T, void>)
                return fmt::format_to(ctx.out(), "<{}, {}>", e.from, e.to);
            else
                return fmt::format_to(ctx.out(), "<{}, {}, w={}>", e.from, e.to, e.weight);
        }
    }
};

template <>
struct fmt::formatter<dcsr::Config> {

    constexpr auto parse(fmt::format_parse_context& ctx) {
        auto it = ctx.begin(), end = ctx.end();
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const dcsr::Config& c, FormatContext& ctx) const {
        return fmt::format_to(
            ctx.out(),
            std::locale("en_US.UTF-8"),
            "==================== Graph Config ====================\n"
            "auto_extend = {}\n"
            "buffer_count = {:L}\n"
            "buffer_size = {:L}\n"
            "compaction_threshold = {:L}\n"
            "index_ratio = {:L}\n"
            "init_vertex_count = {:L}\n"
            "merge_multiplier = {:L}\n"
            "min_csr_num_to_compact = {:L}\n"
            "partition_size = {:L}\n"
            "sort_batch_size = {:L}\n"
            "======================================================\n",
            c.auto_extend,
            c.buffer_count,
            c.buffer_size,
            c.compaction_threshold,
            c.index_ratio,
            c.init_vertex_count,
            c.merge_multiplier,
            c.min_csr_num_to_compact,
            c.partition_size,
            c.sort_batch_size
        );
    }
};

#endif // __DCSR_FORMATTER_H__
