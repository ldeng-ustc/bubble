#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import meta
import datasets
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from parse_output import get_latest_files, parse_output, extract_data
from pathlib import Path

def plot_df_line(ax, df):
    """
    在指定的 Axes 对象上绘制 DataFrame 的每一列为折线图。

    参数:
    ax (matplotlib.axes.Axes): 用于绘图的 Axes 对象。
    df (pd.DataFrame): 输入的 DataFrame，每一列对应一条线。
    """
    # 遍历 DataFrame 的每一列，并在指定的 Axes 上绘制折线图
    for column in df.columns:
        ax.plot(df.index, df[column], label=column, marker='o', markersize=3)
    
    # 添加图例
    # ax.legend()



def plot_df_line_bar(ax, df, line_cols: list[str], bar_cols: list[str]):
    """
    在指定的 Axes 对象上绘制 DataFrame 的列，折线图用左侧y轴，柱状图用右侧y轴。

    参数:
    ax (matplotlib.axes.Axes): 用于绘图的 Axes 对象。
    df (pd.DataFrame): 输入的 DataFrame
    line_cols (list[str]): 需要绘制为折线图的列。
    bar_cols (list[str]): 需要绘制为柱状图的列。
    """
    # 生成统一的x轴位置
    x = np.arange(len(df.index))
    ax.set_xticks(x)
    ax.set_xticklabels(df.index)  # 设置x轴标签为原始索引


    # 绘制折线图（左侧y轴）
    for col in line_cols:
        ax.plot(x, df[col], label=col, marker='o', color='black', markersize=3)

    # 创建右侧y轴并绘制柱状图
    if bar_cols:
        ax2 = ax.twinx()
        bar_width = 0.5  # 柱状图总宽度
        n_bars = len(bar_cols)
        width_per_bar = bar_width / n_bars

        for i, col in enumerate(bar_cols):
            offset = i * width_per_bar
            ax2.bar(x + offset, df[col], width_per_bar, label=col)
    
    # 合并图例
    handles_line, labels_line = ax.get_legend_handles_labels()
    handles_bar, labels_bar = ax2.get_legend_handles_labels()
    # ax.legend(handles_line + handles_bar, labels_line + labels_bar, loc='upper right')
    # ax.legend(handles_line + handles_bar, labels_line + labels_bar)
    ax.legend(handles_line + handles_bar, labels_line + labels_bar, loc="lower center", bbox_to_anchor=(0.5, 1.02), ncols=2)

    # 将折线图放在最上层，并防止遮挡
    ax.set_zorder(2)
    ax2.set_zorder(1)
    ax.patch.set_visible(False)

    # 设置标签
    if df.index.name:
        ax.set_xlabel(df.index.name)
    if df.columns.name:
        ax.set_ylabel(df.columns.name)

    return ax, ax2

if __name__ == '__main__':
    exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'motivation/raw')
    latest_dirs = get_latest_files(exp_dir, 5)

    print("Latest experiment:", latest_dirs)

    dfs_dict = {}

    for dir in latest_dirs:
        raw_files = sorted(os.listdir(dir))
        exp_data = []
        for file in raw_files:
            # print("Processing", file)
            path = os.path.join(dir, file)
            work, threads, batch = Path(path).stem.split('-')
            threads = int(threads)
            res = parse_output(path)
            # res['throughput'] = res[str(threads)]
            res['bandwidth'] = res['throughput'] * 4
            # res.pop(str(threads))
            res['work'] = work
            res['threads'] = threads
            exp_data.append(res)
            print("Data:", res)

        row = 'threads'
        col = 'work'
        values_name = {
            'tp': 'throughput',
            'l2_total': 'l2_rqsts.references',
            'l2_miss': 'l2_rqsts.miss',
            'l2_demand_total': 'l2_rqsts.all_demand_references',
            'l2_demand_miss': 'l2_rqsts.all_demand_miss',
            'l3_total': 'cache-references',
            'l3_miss': 'cache-misses',
            'l3_miss_cycles': 'cycle_activity.cycles_l3_miss',
            'l3_miss_stalls': 'cycle_activity.stalls_l3_miss',
            'cycles': 'cycles'
        }

        for name, value in values_name.items():
            df = extract_data(exp_data, row=row, col=col, value=value, condition=lambda d: d['work'] == 'random_insertion_uniform')
            print("Dataframe:", df)
            dfs_dict.setdefault(name, []).append(df)

    for name, df in dfs_dict.items():
        tmp = sum(df) / len(df)
        if name not in ['tp', 'cycles', 'l3_miss_cycles', 'l3_miss_stalls']:
            tmp /= 1e6
        dfs_dict[name] = tmp
    
    dfs_dict['cycles'] = dfs_dict['cycles'] / (128 * 1024 * 1024)
    dfs_dict['l3_miss_cycles'] = dfs_dict['l3_miss_cycles'] / (128 * 1024 * 1024)
    dfs_dict['l3_miss_stalls'] = dfs_dict['l3_miss_stalls'] / (128 * 1024 * 1024)


    dfs_dict['l3_miss_rate'] = dfs_dict['l3_miss'] / dfs_dict['l3_total']
    dfs_dict['l2_miss_rate'] = dfs_dict['l2_miss'] / dfs_dict['l2_total']
    dfs_dict['l2_demand_miss_rate'] = dfs_dict['l2_demand_miss'] / dfs_dict['l2_demand_total']

    dfs_dict['tp_thread'] = dfs_dict['tp'].div(dfs_dict['tp'].index, axis=0)
    dfs_dict['l3_miss_thread'] = dfs_dict['l3_miss'].div(dfs_dict['l3_miss'].index, axis=0)
    dfs_dict['l2_miss_thread'] = dfs_dict['l2_miss'].div(dfs_dict['l2_miss'].index, axis=0)
    dfs_dict['l2_demand_miss_thread'] = dfs_dict['l2_demand_miss'].div(dfs_dict['l2_demand_miss'].index, axis=0)
    dfs_dict['l3_miss_cycles_thread'] = dfs_dict['l3_miss_cycles'].div(dfs_dict['l3_miss_cycles'].index, axis=0)
    dfs_dict['l3_miss_stalls_thread'] = dfs_dict['l3_miss_stalls'].div(dfs_dict['l3_miss_stalls'].index, axis=0)

    dfs_dict['latency_thread'] = 1 / dfs_dict['tp_thread']
    dfs_dict['cycles_thread'] = dfs_dict['cycles'].div(dfs_dict['cycles'].index, axis=0)

    # Combine all dataframes, each df as a column
    pd.set_option('display.width', 1000)
    final_df = pd.DataFrame(columns=dfs_dict.keys())
    for name, df in dfs_dict.items():
        final_df[name] = df
    print(final_df)
    final_df = final_df.loc[[4,8,16,32,48,64,80]]

    # fig, ax = plt.subplots(figsize=(10, 6))
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp_thread'], bar_cols=['l2_miss_rate', 'l2_demand_miss_rate', 'l3_miss_rate'])
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp_thread'], bar_cols=['l2_miss_thread', 'l3_miss_thread'])
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp'], bar_cols=['l2_miss_rate', 'l3_miss_rate'])


    # Two subplots, upper for scalability (shorter), lower for latency (higher)
    # fig, axes = plt.subplots(
    #     2, 1, 
    #     figsize=(4, 3), sharex=True,
    #     gridspec_kw={'height_ratios': [1, 2], 'hspace': 0.2}
    # )

    # Two subplots, left for scalability (shorter), right for latency (higher)
    # fig, axes = plt.subplots(
    #     1, 2, 
    #     figsize=(7, 3),
    #     gridspec_kw={'width_ratios': [1, 1], 'wspace': 0.06},
    #     layout="constrained",
    #     dpi=300
    # )



    # fig1, ax_scale = plt.subplots(figsize=(4, 1), dpi=300)
    # fig2, ax_latency = plt.subplots(figsize=(4, 1.6), dpi=300)
    fig1, ax_scale = plt.subplots(figsize=(2.5, 2), dpi=300)
    fig2, ax_latency = plt.subplots(figsize=(2.5, 2), dpi=300)

    # ax_scale = axes[0]
    final_df['Throughput'] = final_df['tp']
    tp_df = final_df[['Throughput']]
    tp_df.columns = ['Random Update']
    # Normalize to 4 threads
    tp_df = tp_df / tp_df.iloc[0].values[0]

    # For left-right plot
    xmin = tp_df.index.min()
    plot_df_line(ax_scale, tp_df)
    ax_scale.axline(xy1=(xmin, 1), xy2=(xmin*2,2), label='Ideal', color='grey', linestyle='--', linewidth=0.7, zorder=0)
    ax_scale.set_ylabel('Scalability', labelpad=-1)
    ax_scale.set_ylim(0, 12)
    ax_scale.set_xlim(xmin, 80)
    ax_scale.set_xticks(tp_df.index)
    ax_scale.set_xlabel('#Threads')
    ax_scale.legend(loc="lower center", bbox_to_anchor=(0.5, 1.02), ncols=2)
    # ax_scale.legend(loc="upper left", ncols=1)
    ax_scale2 = ax_scale.twinx()
    ax_scale2.set_ylim(0, 12)


    # ax_latency = axes[1]
    total_name = 'Total latency'
    l3_miss_name = 'L3 latency'
    final_df[total_name] = final_df['cycles_thread']
    final_df[l3_miss_name] = final_df['l3_miss_stalls_thread']
    ax1, ax2 = plot_df_line_bar(ax_latency, final_df, line_cols=[total_name], bar_cols=[l3_miss_name])

    # adjust y-axis limits
    ax1.set_ylim(0, 100)
    ax2.set_ylim(0, 100)

    ax1.set_xlabel('#Threads')
    ax1.set_ylabel('Latency (cycles)', labelpad=-5)

    fig1.savefig(meta.PROJECT_DIR + '/figs/motivation-a1.pdf', bbox_inches='tight', pad_inches=0)
    fig2.savefig(meta.PROJECT_DIR + '/figs/motivation-a2.pdf', bbox_inches='tight', pad_inches=0)



