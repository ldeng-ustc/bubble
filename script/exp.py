#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import meta
import datasets
from parse_output import get_latest_file, parse_output, extract_data

if __name__ == '__main__':
    exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'basic_benchmarks/raw')
    latest_dir = get_latest_file(exp_dir)
    # latest_dir = '/home/ldeng/graphtmp/data/experiments/basic_benchmarks/raw/2024-12-03-0658'

    print("Latest experiment:", latest_dir)

    raw_files = sorted(os.listdir(latest_dir))
    exp_data = []
    for file in raw_files:
        print("Processing", file)
        path = os.path.join(latest_dir, file)
        work, dataset_name = os.path.basename(path).split('.')[0].split('-')
        res = parse_output(path)
        res['dataset'] = dataset_name
        res['work'] = work
        exp_data.append(res)
        print("Data:", res)
    
    ingest = extract_data(exp_data, 'work', 'dataset', 'ingest', lambda d: d['dataset'] != 'Wikipedia')
    bfs = extract_data(exp_data, 'work', 'dataset', 'bfs', lambda d: d['dataset'] != 'Wikipedia')
    pr = extract_data(exp_data, 'work', 'dataset', 'pr', lambda d: d['dataset'] != 'Wikipedia')
    cc = extract_data(exp_data, 'work', 'dataset', 'cc', lambda d: d['dataset'] != 'Wikipedia')

    print("Ingest:", ingest, sep='\n')
    print("BFS:", bfs, sep='\n')
    print("PR:", pr, sep='\n')
    print("CC:", cc, sep='\n')


# In[3]:


import matplotlib.pyplot as plt
import matplotlib.font_manager
import numpy as np

def plot_grouped_bar(
        df, ax, 
        title=None,
        xlabel='Model',
        ylabel='Value',
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
    title : str
        图表标题，默认值为 'Grouped Bar Plot'。
    xlabel : str
        x轴标签，默认值为 'Model'。
    ylabel : str
        y轴标签，默认值为 'Value'。
    font_of_tick : dict
        设置 tick 标签的字体属性，默认值为 None。
    font_of_legend : dict
        设置图例字体属性，默认值为 None。
    font_of_label : dict
        设置坐标轴标签字体属性，默认值为 None。
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

fig, zx = plt.subplots(figsize=(10, 6))

# normalize_ingest = ingest.div(ingest.loc['bubble'], axis=1)

edges = np.asarray([datasets.dataset_by_name(name).ecount for name in ingest.columns], dtype=np.float32)
ingest_throughput = 1 / ingest
ingest_throughput = ingest_throughput.mul(edges, axis=1) * 1e-6
print(ingest_throughput)

# sorted columns by this
order = ['LiveJournal', 'Protein', 'Twitter', 'Friendster', 'UK2007', 'Protein2']
abbr = ['LJ', 'PR', 'TW', 'FR', 'UK', 'PR2']
normalize_ingest = ingest_throughput[order]
normalize_ingest.columns = abbr

speedup = 1 / normalize_ingest.div(normalize_ingest.loc['bubble'], axis=1)
avg_speedup = speedup.mean(axis=1)
print(avg_speedup)


plot_grouped_bar(normalize_ingest, zx, title='Ingest', xlabel='Work', ylabel='Ingesting Throughput (MOPS)')
fig.savefig(meta.PROJECT_DIR + '/figs/ingest.pdf')
fig.savefig(meta.PROJECT_DIR + '/figs/ingest.png')

fig, ax_bfs = plt.subplots(figsize=(10, 6))
normalize_bfs = bfs.div(bfs.loc['bubble'], axis=1)
normalize_bfs = normalize_bfs[order]
normalize_bfs.columns = abbr
plot_grouped_bar(normalize_bfs, ax_bfs, title='BFS', xlabel='Work', ylabel='Normalized Time')
ax_bfs.add_line(plt.axhline(y=1, color='red', linestyle='-'))
fig.savefig(meta.PROJECT_DIR + '/figs/bfs.pdf')
fig.savefig(meta.PROJECT_DIR + '/figs/bfs.png')
print(normalize_bfs)

fig, ax_pr = plt.subplots(figsize=(10, 6))
normalize_pr = pr.div(pr.loc['bubble'], axis=1)
normalize_pr = normalize_pr[order]
normalize_pr.columns = abbr
plot_grouped_bar(normalize_pr, ax_pr, title='PageRank', xlabel='Work', ylabel='Normalized Time')
ax_pr.add_line(plt.axhline(y=1, color='red', linestyle='-'))
fig.savefig(meta.PROJECT_DIR + '/figs/pr.pdf')
fig.savefig(meta.PROJECT_DIR + '/figs/pr.png')
print(normalize_pr)

fig, ax_cc = plt.subplots(figsize=(10, 6))
normalize_cc = cc.div(cc.loc['bubble'], axis=1)
normalize_cc = normalize_cc[order]
normalize_cc.columns = abbr
plot_grouped_bar(normalize_cc, ax_cc, title='Connected Components', xlabel='Work', ylabel='Normalized Time')
ax_cc.add_line(plt.axhline(y=1, color='red', linestyle='-'))
fig.savefig(meta.PROJECT_DIR + '/figs/cc.pdf')
fig.savefig(meta.PROJECT_DIR + '/figs/cc.png')
print(normalize_cc)


# In[4]:


threads = [2, 4, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80] 
# bubble = [190.26, 145.466, 70.468, 53.968, 39.857, 32.605, 32.91, 29.475, 24.077, 22.127, 19.647, 16.224]
# graphone = [410.319, 186.171, 109.662, 60.652, 48.511, 42.6684, 39.4966, 79.644, 56.7483, 50.1229, 47.5645, 44.9614]
# lsgraph = [1656.5973, 842.7739, 437.3952, 233.9342, 173.7564, 147.8516, 132.2771, 130.0893, 118.0999, 121.1176, 122.8401, 120.5132]
# xpgraph = [143.994, 89.2436, 56.6133, 40.1948, 32.9233, 30.6649, 32.8192, 53.9582, 48.5061, 48.7862, 44.963, 45.8514]
# bubble = [144.335, 148.986, 60.875, 40.041, 32.336, 26.323, 21.585, 19.345, 16.946, 16.506, 17.298, 16.104]
bubble = [201.732, 200.205, 85.447, 64.127, 50.118, 41.63, 38.865, 31.998, 27.93, 23.325, 23.613, 22.188]
graphone = [400.994, 186.766, 110.68, 57.6406, 41.902, 35.7676, 31.7209, 68.5459, 55.4499, 50.847, 48.7536, 43.0428]
lsgraph = [1626.7609, 833.1905, 502.1721, 277.6587, 203.4625, 163.5335, 140.92, 131.9675, 128.4964, 125.7289, 126.8213, 125.9545]
xpgraph = [141.121, 89.1162, 64.499, 40.0368, 35.3869, 38.5175, 38.5318, 51.6195, 44.5848, 43.0663, 42.9448, 44.4431]

'''
     bubble
2   201.732
4   200.205
8    85.447
16   64.127
24   50.118
32    41.63
40   38.865
48   31.998
56    27.93
64   23.325
72   23.613
80   22.188
'''

'''
     bubble graphone    lsgraph  xpgraph
2   144.335  400.994  1626.7609  141.121
4   148.986  186.766   833.1905  89.1162
8    60.875   110.68   502.1721   64.499
16   40.041  57.6406   277.6587  40.0368
24   32.336   41.902   203.4625  35.3869
32   26.323  35.7676   163.5335  38.5175
40   21.585  31.7209     140.92  38.5318
48   19.345  68.5459   131.9675  51.6195
56   16.946  55.4499   128.4964  44.5848
64   16.506   50.847   125.7289  43.0663
72   17.298  48.7536   126.8213  42.9448
80   16.104  43.0428   125.9545  44.4431'''

ecount = datasets.twitter.ecount
threads = np.asarray(threads)
bubble = np.asarray(bubble)
graphone = np.asarray(graphone)
lsgraph = np.asarray(lsgraph)
xpgraph = np.asarray(xpgraph)

bubble_mops = ecount / bubble * 1e-6
graphone_mops = ecount / graphone * 1e-6
lsgraph_mops = ecount / lsgraph * 1e-6
xpgraph_mops = ecount / xpgraph * 1e-6

fig, ax_scaling = plt.subplots(figsize=(10, 6))
ax_scaling.plot(threads, bubble_mops, label='Bubble', marker='o')
ax_scaling.plot(threads, graphone_mops, label='GraphOne', marker='o')
ax_scaling.plot(threads, lsgraph_mops, label='LSGraph', marker='o')
ax_scaling.plot(threads, xpgraph_mops, label='XPGraph', marker='o')

ax_scaling.set_xlabel('Threads', fontsize=22)
ax_scaling.set_ylabel('Throughput (MOPS)', fontsize=22)
ax_scaling.legend(fontsize=16)

ax_scaling.set_xticks(threads)
ax_scaling.set_xticklabels(threads, fontsize=16)
# ax_scaling.set_yticklabels(ax_scaling.get_yticks().copy(), fontsize=16)
ax_scaling.tick_params(axis='y', which='major', labelsize=16)

fig.savefig(meta.PROJECT_DIR + '/figs/scaling.pdf')
fig.savefig(meta.PROJECT_DIR + '/figs/scaling.png')


# In[5]:


threads = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 68, 72, 76, 80]
random_insert = [43.80, 80.58, 150.00, 226.89, 350.88, 191.92, 385.40, 371.89, 262.03, 276.23, 264.99, 146.90, 258.11, 268.19, 342.19, 380.63, 318.50, 266.82, 270.35, 250.84, 199.74, 413.62]
mini_batch_sort = [46.82, 98.08, 188.13, 326.56, 404.96, 604.29, 758.71, 834.76, 888.93, 1016.70, 966.34, 849.54, 1100.13, 959.55, 1137.18, 1801.38, 1514.38, 1671.84, 1677.62, 1370.80, 1717.38, 1724.81]
big_batch_sort = [528.91, 1000.45, 2067.94, 3977.07, 4618.43, 4897.45, 4882.70, 5012.56, 4082.54, 4677.01]

random_insert = np.asarray(random_insert) * 16
mini_batch_sort = np.asarray(mini_batch_sort) * 16
big_batch_sort = np.asarray(big_batch_sort)

fig, ax_mini_batch = plt.subplots(figsize=(10, 6))
ax_mini_batch.plot(threads, random_insert, label='Random Insert', marker='o')
ax_mini_batch.plot(threads, mini_batch_sort, label='512K Batch Sort', marker='o')
ax_mini_batch.plot(threads[:len(big_batch_sort)], big_batch_sort, label='16MB Batch Sort', marker='o')

ax_mini_batch.set_xlabel('Threads', fontsize=18)
ax_mini_batch.set_ylabel('MB/s (ms)', fontsize=18)

ax_mini_batch.tick_params(axis='both', which='major', labelsize=16)

ax_mini_batch.legend(fontsize=16)





# In[ ]:


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
    ax.legend(handles_line + handles_bar, labels_line + labels_bar)

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
    latest_dirs = get_latest_files(exp_dir, 10)

    print("Latest experiment:", latest_dirs)

    dfs_dict = {}

    for dir in latest_dirs:
        raw_files = sorted(os.listdir(dir))
        exp_data = []
        for file in raw_files:
            # print("Processing", file)
            path = os.path.join(dir, file)
            work, threads = Path(path).stem.split('-')
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
            df = extract_data(exp_data, row=row, col=col, value=value)
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
    final_df = final_df.loc[[1,2,4,8,16,24,32,40,48,56,64,72,80]]

    # fig, ax = plt.subplots(figsize=(10, 6))
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp_thread'], bar_cols=['l2_miss_rate', 'l2_demand_miss_rate', 'l3_miss_rate'])
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp_thread'], bar_cols=['l2_miss_thread', 'l3_miss_thread'])
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp'], bar_cols=['l2_miss_rate', 'l3_miss_rate'])


    # Two subplots, upper for scalability (shorter), lower for latency (higher)
    fig, axes = plt.subplots(
        2, 1, 
        figsize=(4, 2.4), sharex=True,
        gridspec_kw={'height_ratios': [1, 2], 'hspace': 0.2}
    )

    ax_scale = axes[0]
    final_df['Throughput'] = final_df['tp']
    tp_df = final_df[['Throughput']]
    tp_df.index = tp_df.index.astype(str)
    plot_df_line(ax_scale, tp_df)

    # ax_scale.set_ylabel('Throughput (MOPS)')
    ax_scale.set_ylabel('MOPS')
    # ax_scale.set_xscale('log')
    # ax_scale.set_yscale('log')
    ax_scale.set_ylim(0, 1800)


    ax_latency = axes[1]
    final_df['Insertion'] = final_df['cycles_thread']
    final_df['L3 misses stalls'] = final_df['l3_miss_stalls_thread']
    ax1, ax2 = plot_df_line_bar(ax_latency, final_df, line_cols=['Insertion'], bar_cols=['L3 misses stalls'])

    # adjust y-axis limits
    ax1.set_ylim(0, 100)
    ax2.set_ylim(0, 100)

    ax1.set_xlabel('#Threads')
    ax1.set_ylabel('Latency (cycles)')

    fig.savefig(meta.PROJECT_DIR + '/figs/motivation-a.pdf', bbox_inches='tight')



