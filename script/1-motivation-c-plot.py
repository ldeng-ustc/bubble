#!/usr/bin/env python
# coding: utf-8

# In[21]:


import os
import meta
import datasets
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from parse_output import get_latest_files, extract_multiple_data, get_data_from_dir_custom
from pathlib import Path

def plot_df_line(ax, df, linestyles: str|list = '-', markers: str|list = 'o'):
    """
    在指定的 Axes 对象上绘制 DataFrame 的每一列为折线图。

    参数:
    ax (matplotlib.axes.Axes): 用于绘图的 Axes 对象。
    df (pd.DataFrame): 输入的 DataFrame ，每一列对应一条线。
    linestyles (str|list): 线条样式，可以是单一字符串或列表。
    markers (str|list): 标记样式，可以是单一字符串或列表。
    """
    # 遍历 DataFrame 的每一列，并在指定的 Axes 上绘制折线图
    for i, col in enumerate(df.columns):
        # 选择线条样式和标记样式
        linestyle = linestyles if isinstance(linestyles, str) else linestyles[i % len(linestyles)]
        marker = markers if isinstance(markers, str) else markers[i % len(markers)]
        
        # 绘制折线图
        ax.plot(df.index, df[col], linestyle=linestyle, marker=marker, label=col, markersize=3, linewidth=1.6, clip_on=False, zorder=100+i)
    
    ax.set_xticks(df.index)

    # 添加图例
    # ax.legend()


def plot_bar_group(df: pd.DataFrame, ax, colors=None):
    """
    生成分组柱状图，每组为一个模型，柱子表示不同平台的数据，并在指定的 ax 上绘制，使用自定义字体。
    
    参数:
    df : pd.DataFrame
        需要绘制的数据，行是模型，列是平台。
    ax : matplotlib.axes.Axes
        要绘制图表的 Axes 对象。
    """

    if colors is None:
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
    ax.set_xticklabels(df.columns)


    # 设置图例
    legend_bbox = (-0.05, 1.02, 1.05, 0.3)
    ax.legend(title=None, loc='lower left', mode="expand", ncol=4, bbox_to_anchor=legend_bbox)

def num_to_bin_unit(num):
    unit = ['B', 'KB', 'MB', 'GB', 'TB']
    index = 0
    while num >= 1024 and index < len(unit) - 1:
        num /= 1024
        index += 1
    return f"{num:.0f}{unit[index]}"

if __name__ == '__main__':
    exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'motivation/raw')
    latest_dirs = get_latest_files(exp_dir, 6)

    print("Latest experiment:", latest_dirs)


    latest_datas = []
    for dir in latest_dirs:
        res = get_data_from_dir_custom(dir, title_args=['work', 'threads', 'batch'])
        latest_datas.append(res)
    
    df_mini_process_sort = extract_multiple_data(
        latest_datas, 
        row='threads', col='batch', value='throughput', 
        condition=lambda d: d['work'] == 'mini_process_sort'
    )

    df_mini_process_shuffle = extract_multiple_data(
        latest_datas,
        row='threads', col='batch', value='throughput',
        condition=lambda d: d['work'] == 'mini_process_shuffle'
    )

    def sort_index_columns(df):
        df.index = df.index.astype(int)
        df.sort_index(inplace=True)
        df.columns = df.columns.astype(int)
        df.sort_index(axis=1, inplace=True)

    sort_index_columns(df_mini_process_sort)
    sort_index_columns(df_mini_process_shuffle)

    df_shuffle = df_mini_process_shuffle.iloc[2:, 1:6]

    # Normalize, div first row
    df_shuffle_normalized = df_shuffle.div(df_shuffle.iloc[0], axis=1)

    pd.set_option('display.width', 1000)
    print(df_shuffle_normalized)

    fig_scale, ax_scale = plt.subplots(figsize=(3, 2.5), dpi=300)

    markers = ['s', 'o', '^', 'D', 'v']
    plot_df_line(ax_scale, df_shuffle_normalized, linestyles='-', markers=markers)

    legend_text = list(df_shuffle_normalized.columns)
    legend_text = [num_to_bin_unit(int(x) * 8) for x in legend_text]
    # ax_scale.legend(legend_text, loc='lower center', bbox_to_anchor=(0.5, 1.02), ncols=5, fontsize=6)
    ax_scale.legend(legend_text, loc='upper left', ncol=2, fontsize=8, labelspacing=0.3, columnspacing=1.0)
    ax_scale.yaxis.set_ticks_position('both')
    ax_scale.tick_params(axis='y', labelleft=True, labelright=True)
    ax_scale.set_xlim(xmin=df_shuffle_normalized.index[0], xmax=df_shuffle_normalized.index[-1])

    ax_scale.set_xlabel('#Threads')
    ax_scale.set_ylabel('Scalability', labelpad=0)


    fig_scale.savefig(meta.PROJECT_DIR + '/figs/batch-scale.pdf', bbox_inches='tight')
    # fig1.savefig(meta.PROJECT_DIR + '/figs/motivation-a1.pdf', bbox_inches='tight')
    # fig2.savefig(meta.PROJECT_DIR + '/figs/motivation-a2.pdf', bbox_inches='tight')


    fig_ratio, ax_ratio = plt.subplots(figsize=(3, 2.5), dpi=300)

    df_shuffle_batch = df_mini_process_shuffle.iloc[2:, 2:]
    df_sort_batch = df_mini_process_sort.iloc[2:, 2:]

    # ratio = df.loc[80]/df.loc[4]
    df_shuffle_ratio = df_shuffle_batch.loc[80] / df_shuffle_batch.loc[4]
    df_sort_ratio = df_sort_batch.loc[80] / df_sort_batch.loc[4]

    # Combine series to dataframe
    df_ratio = pd.DataFrame({
        'Shuffle': df_shuffle_ratio,
        'Sort': df_sort_ratio
    })
    df_ratio = df_ratio.T
    df_ratio.columns = [x * 8 // 2**20 for x in df_ratio.columns]
    print(df_ratio)

    plot_bar_group(df_ratio, ax_ratio)

    # 对于较大Batch上，Shuffle可拓展性提升的问题，经过测试，是因为4线程的性能降低了，而80线程性能降低没那么快。
    # 80线程已经一直是Cache miss了，而Batch size大时，4线程也受到Cache miss的影响。
    # 为什么不用Sort？Sort不同长度下计算量不一致。

    df_l3_stalls = extract_multiple_data(
        latest_datas,
        row='threads', col='batch', value='cycle_activity.stalls_l3_miss',
        condition=lambda d: d['work'] == 'mini_process_shuffle'
    )
    sort_index_columns(df_l3_stalls)
    
    df_l3_stalls_thread = df_l3_stalls.div(df_l3_stalls.index, axis=0)
    df_l3_stalls_thread = df_l3_stalls_thread.astype(int)
    df_l3_stalls_thread //= 2**20
    print(df_l3_stalls_thread)
    
    
    



