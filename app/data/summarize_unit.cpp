#include <bits/stdc++.h>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "cxxopts.hpp"

#include "datatype.h"
#include "importer.h"

using namespace std;
namespace fs = std::filesystem;

constexpr size_t MAX_DEGREE_HIST = 128;

size_t max_vertex_count;
size_t max_edge_count;


struct HistResult {
    size_t vertex_count;
    size_t edge_count;
    std::unique_ptr<size_t[]> out_degree;
    std::unique_ptr<size_t[]> in_degree;
    // std::unique_ptr<size_t[]> all_degree;
};

HistResult hist(const string& dataset_path, size_t start_bytes, size_t max_edge_count) {
    auto out_degree = make_unique<size_t[]>(max_vertex_count);
    auto in_degree = make_unique<size_t[]>(max_vertex_count);
    // auto all_degree = make_unique<size_t[]>(max_vertex_count);
    size_t real_vertex_count = 0;
    size_t real_edge_count = 0;
    // Count the degree of each vertex
    fmt::println("Start scanning large file: {} bytes, {} edges", start_bytes, max_edge_count);
    dcsr::ScanLargeFileSegment<dcsr::RawEdge64<void>, 8*1024*1024>(dataset_path, start_bytes, max_edge_count, [&](dcsr::RawEdge64<void> edge) {
        // fmt::println("Edge: {} -> {}", edge.from, edge.to);
        // exit(0);
        out_degree[edge.from]++;
        in_degree[edge.to]++;
        // all_degree[edge.from]++;
        // all_degree[edge.to]++;
        real_vertex_count = std::max(real_vertex_count, edge.from);
        real_vertex_count = std::max(real_vertex_count, edge.to);
        real_edge_count++;
    });
    real_vertex_count++;

    return {real_vertex_count, real_edge_count, std::move(out_degree), std::move(in_degree)/*, std::move(all_degree)*/};
}

struct UnitDistribution {
    size_t unit_vertex;
    size_t vertex_count;
    size_t edge_count;
    std::unordered_map<size_t, size_t> degree_hist_map;  // 每个unit对应的边数分布，degree_hist_map[i]表示有i条边的unit数
    std::vector<size_t> active_hist;      // 每个unit中非空的顶点数分布，active_hist[i]表示有i个非空顶点的unit数
    std::vector<size_t> active_hist_of_degrees[MAX_DEGREE_HIST];  // 每个degree下，active_hist的分布
    std::vector<size_t> hist_of_unit_max_degree[MAX_DEGREE_HIST];  // 每个unit中最大的degree分布
};

struct DegreeUnitDistribution {
    size_t unit_vertex;
    size_t vertex_count;
    size_t edge_count;
    std::unordered_map<size_t, size_t> degree_hist_map;  // 每个unit对应的边数分布，degree_hist_map[i]表示有i条边的unit数
    std::vector<size_t> active_hist;      // 每个unit中非空的顶点数分布，active_hist[i]表示有i个非空顶点的unit数
    std::vector<size_t> active_hist_of_degrees[MAX_DEGREE_HIST];  // 每个degree下，active_hist的分布
    std::vector<size_t> hist_of_unit_max_degree[MAX_DEGREE_HIST];  // 每个unit中最大的degree分布
};

DegreeUnitDistribution unit_distribution(std::span<size_t> degree, size_t vertex_count, size_t unit_vertex) {
    size_t edge_count = std::accumulate(degree.begin(), degree.end(), 0ull);

    DegreeUnitDistribution unit_dist;
    unit_dist.unit_vertex = unit_vertex;
    unit_dist.vertex_count = vertex_count;
    unit_dist.edge_count = edge_count;
    
    // constexpr size_t FastArraySize = 1024;
    // size_t degree_unit_count[FastArraySize];

    // 初始化
    auto& degree_unit_map = unit_dist.degree_hist_map;
    auto& active_hist = unit_dist.active_hist;
    // memset(degree_unit_count, 0, sizeof(degree_unit_count));
    active_hist.resize(unit_vertex + 1);
    for(auto& hist : unit_dist.active_hist_of_degrees) {
        hist.resize(unit_vertex + 1);
    }
    for(auto& hist : unit_dist.hist_of_unit_max_degree) {
        hist.resize(MAX_DEGREE_HIST + 1);
    }

    // 扫描每个 unit
    for(size_t i = 0; i < vertex_count; i+=unit_vertex) {
        size_t unit_degree = 0;
        size_t unit_nonempty = 0;
        size_t unit_max_degree = 0;
        for(size_t j = 0; j < unit_vertex && i+j < vertex_count; j++) {
            unit_degree += degree[i + j];   // unit的总degree
            if(degree[i + j] > 0) {
                unit_nonempty++;    // unit中非空顶点数
            }
            unit_max_degree = std::max(unit_max_degree, degree[i + j]); // unit中最高度顶点的degree
        }

        // if(unit_degree < FastArraySize) {   // 加快处理速度
        //     degree_unit_count[unit_degree]++;
        // } else {
        //     degree_unit_map[unit_degree]++;
        // }
        degree_unit_map[unit_degree]++;

        active_hist[unit_nonempty]++;
        if(unit_degree < MAX_DEGREE_HIST) {
            unit_dist.active_hist_of_degrees[unit_degree][unit_nonempty]++;
            unit_dist.hist_of_unit_max_degree[unit_degree][unit_max_degree]++;
        }

    }

    // for(size_t i = 0; i < FastArraySize; i++) {
    //     if(degree_unit_count[i] > 0) {
    //         degree_unit_map[i] = degree_unit_count[i];
    //     }
    // }

    return unit_dist;
}


struct SegmentHist {
    size_t vertex_count;
    size_t segment_size;
    size_t segment_count;
    std::vector<HistResult> segment_hist;
};

SegmentHist segment_hist(const string& dataset_path, size_t segment_size, size_t segment_count) {
    SegmentHist segment_hist;
    segment_hist.segment_size = segment_size;
    segment_hist.segment_count = segment_count;
    size_t vertex_count = 0;

    for(size_t i = 0; i < segment_count; i++) {
        size_t start_bytes = i * segment_size * sizeof(dcsr::RawEdge64<void>);
        auto hist_result = hist(dataset_path, start_bytes, segment_size);
        vertex_count = std::max(vertex_count, hist_result.vertex_count);
        segment_hist.segment_hist.emplace_back(std::move(hist_result));
    }
    segment_hist.vertex_count = vertex_count;
    return segment_hist;
}


struct SegmentUnitDistribution {
    size_t unit_vertex;
    size_t vertex_count;
    size_t edge_count;
    std::vector<DegreeUnitDistribution> unit_dist;
};

SegmentUnitDistribution segment_unit_distribution(const SegmentHist& segment_hist, size_t unit_vertex) {
    SegmentUnitDistribution segment_unit_dist;
    segment_unit_dist.unit_vertex = unit_vertex;
    segment_unit_dist.vertex_count = 0;
    segment_unit_dist.edge_count = 0;

    for(const auto& hist_result : segment_hist.segment_hist) {
        auto unit_dist = unit_distribution(
            std::span(hist_result.out_degree.get(), hist_result.vertex_count),
            hist_result.vertex_count,
            unit_vertex
        );
        segment_unit_dist.unit_dist.emplace_back(std::move(unit_dist));
        segment_unit_dist.vertex_count = std::max(segment_unit_dist.vertex_count, hist_result.vertex_count);
        segment_unit_dist.edge_count += hist_result.edge_count;
    }

    return segment_unit_dist;
}

const size_t MAX_SUMMARY_DEGREE = 32;

struct SegmentsSummary {
    size_t vertex_count;
    size_t segment_size;
    size_t segment_count;
    std::vector<size_t> units;
    std::vector<std::array<size_t, MAX_SUMMARY_DEGREE>> degree_hist;        // degree_hist[i][j]表示unit_size为units[i]时，degree为j的unit数
    std::vector<std::array<size_t, 64>> log_degree_hist;                    // log_degree_hist[i][j]表示unit_size为units[i]时，degree < 2^j 的 unit数
};

SegmentsSummary segments_summary(const SegmentHist& segment_hist, std::vector<size_t> units) {
    SegmentsSummary summary;
    summary.vertex_count = segment_hist.vertex_count;
    summary.segment_size = segment_hist.segment_size;
    summary.segment_count = segment_hist.segment_count;
    summary.units = units;

    for(size_t unit: units) {
        fmt::println("[Summary] Processing unit size: {}", unit);
        auto unit_dist = segment_unit_distribution(segment_hist, unit); 
        std::array<size_t, MAX_SUMMARY_DEGREE> degree_hist;
        std::array<size_t, 64> log_degree_hist;
        degree_hist.fill(0);
        log_degree_hist.fill(0);
        for(const auto& dist : unit_dist.unit_dist) {
            for(auto [degree, count] : dist.degree_hist_map) {
                if(degree < MAX_SUMMARY_DEGREE) {
                    degree_hist[degree] += count;
                }
                auto log_degree = std::countr_zero(std::bit_ceil(degree));
                log_degree_hist[log_degree] += count;
            }
        }
    }

    return summary;
}


void print_segments_summary_small_hist_csv(FILE* f, SegmentsSummary summary) {
    fmt::print(f, "segment");
    for(size_t i = 0; i < summary.segment_count; i++) {
        fmt::print(f, ",{}", i);
    }
    fmt::println(f, "");
    for(size_t i = 0; i < MAX_SUMMARY_DEGREE; i++) {
        fmt::print(f, "{}", i);
        for(size_t j = 0; j < summary.segment_count; j++) {
            fmt::print(f, ",{}", summary.degree_hist[j][i]);
        }
        fmt::println(f, "");
    }
}

void print_segments_summary_log_hist_csv(FILE* f, SegmentsSummary summary) {
    fmt::print(f, "segment");
    for(size_t i = 0; i < summary.segment_count; i++) {
        fmt::print(f, ",{}", i);
    }
    fmt::println(f, "");
    for(size_t i = 0; i < 64; i++) {
        fmt::print(f, "<=2^{}", i);
        for(size_t j = 0; j < summary.segment_count; j++) {
            fmt::print(f, ",{}", summary.log_degree_hist[j][i]);
        }
        fmt::println(f, "");
    }
}


void print_active_hist(FILE* f, const vector<size_t>& active_hist, size_t id) {
    size_t total_unit = std::accumulate(active_hist.begin(), active_hist.end(), 0);

    std::string ids = (id == 0 ? "ALL" : std::to_string(id));
    fmt::println(f, "{:4}: {::10}", ids, std::views::iota(0ull, active_hist.size()));
    fmt::println(f, "{:4}: {::10}", ids, active_hist);
    fmt::println(f, "{:4}: {::10.2f}%", ids, std::views::transform(active_hist, [&](size_t count) {return 100.0 * count / total_unit;}));
}

void print_max_degree_hist(FILE* f, const vector<size_t>& max_degree_hist, size_t id) {
    size_t total_unit = std::accumulate(max_degree_hist.begin(), max_degree_hist.end(), 0);

    std::string ids = (id == 0 ? "ALL" : std::to_string(id));
    fmt::println(f, "{:4}: {::10}", ids, std::views::iota(0ull, max_degree_hist.size()));
    fmt::println(f, "{:4}: {::10}", ids, max_degree_hist);
    // fmt::println(f, "{:4}: {::10.2f}%", ids, std::views::transform(max_degree_hist, [&](size_t count) {return 100.0 * count / total_unit;}));
    size_t sum = 0;
    fmt::print(f, "{:4}: [", ids);
    for(size_t i = 0; i < max_degree_hist.size(); i++) {
        sum += max_degree_hist[i];
        double acc_percent = 100.0 * sum / total_unit;
        fmt::print(f, "{:9.2f}%", acc_percent);
        if(i + 1 != max_degree_hist.size()) {
            fmt::print(f, ", ");
        }
    }
    fmt::println(f, "]");
}

void print_hist(const DegreeUnitDistribution& unit_dist, const string& output_path) {
    const auto& degree_map = unit_dist.degree_hist_map;
    const auto& active_hist = unit_dist.active_hist;

    size_t unit_vertex = unit_dist.unit_vertex;
    size_t vertex_count = unit_dist.vertex_count;
    size_t edge_count = unit_dist.edge_count;

    std::vector<std::pair<size_t, size_t>> degree_list(degree_map.begin(), degree_map.end());
    std::sort(degree_list.begin(), degree_list.end());

    size_t total_unit = (vertex_count + unit_vertex - 1) / unit_vertex;
    FILE* f = fopen(output_path.c_str(), "w");
    if(f == nullptr) {
        fmt::print("Failed to open {}\n", output_path);
        exit(1);
    }

    fmt::println(f, "Vertex count: {}", vertex_count);
    fmt::println(f, "Edge count: {}", edge_count);
    fmt::println(f, "Unit vertex: {}", unit_vertex);
    fmt::println(f, "=====================================================");

    fmt::println(f, "Active vertex count: ");
    print_active_hist(f, active_hist, 0);
    for(size_t i = 1; i < MAX_DEGREE_HIST; i*=2) {
        if(unit_dist.active_hist_of_degrees[i].empty()) {
            continue;
        }
        print_active_hist(f, unit_dist.active_hist_of_degrees[i], i);
    }

    fmt::println(f, "Max degree count: ");
    for(size_t i = 1; i < MAX_DEGREE_HIST; i*=2) {
        if(unit_dist.hist_of_unit_max_degree[i].empty()) {
            continue;
        }
        print_max_degree_hist(f, unit_dist.hist_of_unit_max_degree[i], i);
    }

    size_t acc_unit = 0;
    size_t acc_edge = 0;
    for(auto [degree, count] : degree_list) {
        acc_unit += count;
        acc_edge += count * degree;
        double percent_unit = 100.0 * acc_unit / total_unit;
        double percent_edge = 100.0 * acc_edge / edge_count;
        size_t contain_edges = acc_edge + (total_unit - acc_unit) * degree;
        double utilization = 100.0 * contain_edges / (total_unit * degree);
        double space_ratio = 100.0 * total_unit * degree / edge_count;
        fmt::println(
            f,
            "{} units have degree {}. (acc {:5.2f}% units, {:5.2f}% edges) (utilization: {:5.2f}%, space ratio: {:5.2f}%)",
            count, degree, percent_unit, percent_edge, utilization, space_ratio
        );
    }
}

int main(int argc, char** argv) {
    cxxopts::Options options("summarize", "Summarize the distribution of a dataset");
    options.add_options()
        ("i,input", "Input file path", cxxopts::value<string>())
        ("o,output", "Output directory", cxxopts::value<string>())
        ("u,unit", "Vertex count per unit", cxxopts::value<std::vector<size_t>>())
        ("e,edge", "Number of edges to analyze", cxxopts::value<size_t>())
        ("v,vertex", "Max Vertex count (vertices count in graph should less than it)", cxxopts::value<size_t>())
        ("start", "Start edges", cxxopts::value<size_t>()->default_value("0"))
        // ("s,segments", "Number of segments", cxxopts::value<size_t>())
        ("h,help", "Print usage");
    auto result = options.parse(argc, argv);
    if(result.count("help") || !result.count("input")) {
        cout << options.help() << endl;
        return 0;
    }

    string dataset_path;
    string output_path;
    dataset_path = result["input"].as<string>();
    output_path = result["output"].as<string>();

    fmt::print("Input: {}\n", dataset_path);
    fmt::print("Output: {}\n", output_path);

    std::filesystem::create_directories(output_path);

    max_vertex_count = result["vertex"].as<size_t>();
    max_edge_count = result["edge"].as<size_t>();

    auto units = result["unit"].as<std::vector<size_t>>();
    // size_t segments = result["segments"].as<size_t>();

    // auto segment_hist_result = segment_hist(dataset_path, max_edge_count, 1);

    size_t start_bytes = result["start"].as<size_t>() * sizeof(dcsr::RawEdge64<void>);
    const auto& hist_result = hist(dataset_path, start_bytes, max_edge_count);
    for(auto unit : units) {
        fmt::println("Processing unit size: {}", unit);
        auto s = std::span(hist_result.out_degree.get(), hist_result.vertex_count);
        auto unit_dist = unit_distribution(s, hist_result.vertex_count, unit);
        auto output_file = fs::path(output_path) / fmt::format("unit{}.txt", unit);
        print_hist(unit_dist, output_file);
    }

    // auto summary = segments_summary(segment_hist_result, units);
    // auto small_hist_file = fs::path(output_path) / "small_hist.csv";
    // auto log_hist_file = fs::path(output_path) / "log_hist.csv";
    // FILE* f = fopen(small_hist_file.c_str(), "w");
    // if(f == nullptr) {
    //     fmt::print("Failed to open {}\n", small_hist_file.c_str());
    //     exit(1);
    // }
    // print_segments_summary_small_hist_csv(f, summary);
    // fclose(f);

    // f = fopen(log_hist_file.c_str(), "w");
    // if(f == nullptr) {
    //     fmt::print("Failed to open {}\n", log_hist_file.c_str());
    //     exit(1);
    // }
    // print_segments_summary_log_hist_csv(f, summary);
    // fclose(f);
    
    return 0;
}