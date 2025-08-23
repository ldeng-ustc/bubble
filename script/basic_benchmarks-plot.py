import os
import argparse
import meta
import datasets

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from parse_output import get_latest_files, get_data_from_dir, extract_data

def plot_grouped_bar(
        df, ax, 
        title=None,
        xlabel=None,
        ylabel=None,
        font_of_tick=None,
        font_of_legend=None,
        font_of_label=None):
    """
    生成分组柱状图，每组为一个模型，柱子表示不同平台的数据，并在指定的 ax 上绘制，使用自定义字体。
    
    参数:
    df : pd.DataFrame
        需要绘制的数据，行是模型，列是平台。
    ax : matplotlib.axes.Axes
        要绘制图表的 Axes 对象。
    """

    # font_of_tick = {'family': 'Times New Roman', 'weight': 'normal', 'size': 20}
    # font_of_legend = {'family': 'Times New Roman', 'weight': 'normal', 'size': 18}
    # font_of_label = {'family': 'Times New Roman', 'weight': 'normal', 'size': 18}
    # font_of_tick = {'family': 'sans', 'weight': 'normal', 'size': 20}
    # font_of_legend = {'family': 'sans', 'weight': 'normal', 'size': 18}
    # font_of_label = {'family': 'sans', 'weight': 'normal', 'size': 18}

    # For paper: use DejaVu Sans
    # For PPT: use DejaVu Sans Display
    # Otherfonts: Arial, Courier New, Times New Roman, Verdana
    sans = ['DejaVu Sans Display', 'DejaVu Sans']
    font_of_tick = {'family': sans, 'weight': 'normal', 'size': 20}
    font_of_legend = {'family': sans, 'weight': 'normal', 'size': 18}
    font_of_label = {'family': sans, 'weight': 'normal', 'size': 18}

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

    # 在指定的 ax 上绘制柱状图
    left_margin = 0.3
    bar_width = 0.2
    gap_width = 0.3
    groups = len(df.columns)
    group_bars = len(df.index)
    group_width = bar_width * group_bars + gap_width
    group_xes = np.arange(groups) * group_width

    for i, row in enumerate(df.index):
        type_xes = group_xes + i * bar_width
        ax.bar(type_xes, df.loc[row], bar_width, label=row, align='edge', color=colors[i], edgecolor='black', )
    
    # X 轴刻度，刻度标签
    ax.set_xticks(group_xes + (group_width - gap_width) / 2)
    ax.set_xticklabels(df.columns, fontdict=font_of_tick)

    # 设置标题和标签
    # ax.set_title(title, fontdict=font_of_label)
    # ax.set_xlabel(xlabel, fontdict=font_of_label)
    ax.set_ylabel(ylabel, fontdict=font_of_label, fontsize=22)

    # 设置图例
    legend_bbox = (-0.05, 1.02, 1.05, 0.3)
    ax.legend(title=None, loc='lower left', mode="expand", ncol=4, bbox_to_anchor=legend_bbox, prop=font_of_legend)

    # 设置 tick 标签字体
    if font_of_tick:
        ax.tick_params(axis='x', labelsize=font_of_tick['size'])
        ax.tick_params(axis='y', labelsize=font_of_tick['size'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plot basic benchmarks')
    parser.add_argument('n_latest', type=int, help='Number of latest experiments to plot', nargs='?', default=1)
    args = parser.parse_args()

    exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'basic_benchmarks/raw')
    latest_dirs = get_latest_files(exp_dir, args.n_latest)
    # latest_dir = '/home/ldeng/graphtmp/data/experiments/basic_benchmarks/raw/2024-12-03-0658'

    print("Plot latest experiments:", latest_dirs)

    ingest_dataframes = []
    bfs_dataframes = []
    pr_dataframes = []
    cc_dataframes = []
    memory_ingest_dataframes = []
    memory_bfs_dataframes = []

    for dir in latest_dirs:
        print("Processing dir", dir)
        exp_data = get_data_from_dir(dir)
        ingest = extract_data(exp_data, 'work', 'dataset', 'ingest')
        bfs = extract_data(exp_data, 'work', 'dataset', 'bfs')
        pr = extract_data(exp_data, 'work', 'dataset', 'pr')
        cc = extract_data(exp_data, 'work', 'dataset', 'cc')
        memory_ingest = extract_data(exp_data, 'work', 'dataset', 'rss_ingest')
        memory_bfs = extract_data(exp_data, 'work', 'dataset', 'rss_bfs')

        ingest_dataframes.append(ingest)
        bfs_dataframes.append(bfs)
        pr_dataframes.append(pr)
        cc_dataframes.append(cc)
        memory_ingest_dataframes.append(memory_ingest)
        memory_bfs_dataframes.append(memory_bfs)

        print("Ingest:", ingest, sep='\n')
        print("BFS:", bfs, sep='\n')
        print("PR:", pr, sep='\n')
        print("CC:", cc, sep='\n')
        print("Memory Ingest:", memory_ingest, sep='\n')
        print("Memory BFS:", memory_bfs, sep='\n')
    
    ingest_avg = sum(ingest_dataframes) / len(ingest_dataframes)
    bfs_avg = sum(bfs_dataframes) / len(bfs_dataframes)
    pr_avg = sum(pr_dataframes) / len(pr_dataframes)
    cc_avg = sum(cc_dataframes) / len(cc_dataframes)
    memory_ingest_avg = sum(memory_ingest_dataframes) / len(memory_ingest_dataframes)
    memory_bfs_avg = sum(memory_bfs_dataframes) / len(memory_bfs_dataframes)

    memory_ingest_avg = memory_ingest_avg / 1024**3
    memory_bfs_avg = memory_bfs_avg / 1024**3

    print(f"Ingest average of {args.n_latest} experiments:", ingest_avg, sep='\n', end='\n\n')
    print(f"BFS average of {args.n_latest} experiments:", bfs_avg, sep='\n', end='\n\n')
    print(f"PR average of {args.n_latest} experiments:", pr_avg, sep='\n', end='\n\n')
    print(f"CC average of {args.n_latest} experiments:", cc_avg, sep='\n', end='\n\n')

    print(f"Memory Ingest (GB) average of {args.n_latest} experiments:", memory_ingest_avg, sep='\n', end='\n\n')
    print(f"Memory BFS (GB) average of {args.n_latest} experiments:", memory_bfs_avg, sep='\n', end='\n\n')

    

    
