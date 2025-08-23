#!/usr/bin/env python
# coding: utf-8

# In[51]:


import os
import meta
import datasets

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from parse_output import get_latest_files, get_data_from_dir_custom, extract_multiple_data, extract_data


# In[52]:


def plot_grouped_bar(
        df, ax, 
        colors=None, 
        bar_width=0.3, gap_width=0.3,
        **kwargs
    ):
    """
    生成分组柱状图，每组为DF的一列，每个颜色为DF的一行，并在指定的 ax 上绘制。
    以常见的Evaluation为例，每一行为一个系统，每一列为一个数据集。（打印DF时方便纵向比较）

    其它元素：
    df.index 用于y轴标签，df.columns 用于x轴标签。
    如果 df.index.name 不为 None，则会在y轴上显示该名称。
    如果 df.columns.name 不为 None，则会在x轴上显示该名称。
    不会自动生成图例，用户需要手动添加。（因为图例设置过于复杂）

    参数:
    df : pd.DataFrame
    ax : matplotlib.axes.Axes
        要绘制图表的 Axes 对象。
    colors : list
        颜色列表，长度与 df.index 相同。（即于行数相同）
    bar_width : float
        柱状图柱子的宽度。
    gap_width : float
        每组柱子之间的间距。
    **kwargs : dict
        其他参数，传递给 ax.bar() 函数。
    """

    if colors is None:
        import matplotlib as mpl
        colors = mpl.colormaps['tab10'].colors

    # 在指定的 ax 上绘制柱状图
    n_groups = len(df.columns)      # 组数
    n_group_bars = len(df.index)    # 每组的柱子数
    group_width = bar_width * n_group_bars  # 每组柱子的总宽度
    group_offsets = np.arange(n_groups) * (group_width + gap_width)     # 每组柱子左端的x坐标，组间要加上间隔的宽度
    group_centers = group_offsets + (group_width) / 2                   # 每组柱子的中心x坐标，用于设置x轴刻度和标签

    for i, row in enumerate(df.index):
        type_offsets = group_offsets + i * bar_width
        ax.bar(type_offsets, df.loc[row], bar_width, label=row, align='edge', color=colors[i], edgecolor='black', linewidth=0.5, **kwargs)
    
    # 设置x轴刻度和标签
    ax.set_xticks(group_centers)
    ax.set_xticklabels(df.columns)
    if df.columns.name is not None:
        ax.set_xlabel(df.columns.name)
    


# ## 以下进行数据预处理和画图

# In[53]:


pd.set_option('display.width', 1000)

n_latest = 1
exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'tc/raw')
latest_dirs = get_latest_files(exp_dir, n_latest, 0)
print("Plot latest experiments:", latest_dirs)

expdata_list = []
for dir in latest_dirs:
    expdata = get_data_from_dir_custom(dir, title_args=['work', 'dataset'])
    expdata_list.append(expdata)

ingest_avg = extract_multiple_data(expdata_list, 'work', 'dataset', 'ingest')
tc_avg = extract_multiple_data(expdata_list, 'work', 'dataset', 'tc')

dataset_by_name = lambda name: datasets.dataset_by_name(name, datasets.U_DATASETS)
edges = np.asarray([dataset_by_name(name).ecount for name in ingest_avg.columns], dtype=np.float32)
ingest_tp = (1 / ingest_avg).mul(edges, axis=1) * 1e-6


cols_order = ['LiveJournal', 'Protein', 'Twitter', 'Friendster', 'UK2007', 'Protein2',]
new_cols = ['LJ', 'PR1', 'TW', 'FR', 'UK', 'PR2']
rows_order = ['lsgraph', 'bubble']
new_index = ['LSGraph', 'Bubble-O']

def rename_df(df):
    df = df[cols_order]
    df.columns = new_cols
    df = df.reindex(rows_order)
    df.index = new_index
    return df

ingest_tp = rename_df(ingest_tp)
tc_avg = rename_df(tc_avg)

print(f"Ingest (Undirected) thoughput average of {n_latest} experiments:", ingest_tp, sep='\n', end='\n\n')
print(f"TC average of {n_latest} experiments:", tc_avg, sep='\n', end='\n\n')


# 以下开始画图：

# In[54]:


from matplotlib.colors import to_hex

cmap_blues = mpl.colormaps['Blues']
cmap_oranges = mpl.colormaps['Oranges']
colds = cmap_blues([0.7])
hots = cmap_oranges([0.8])
colors = list(colds) + list(hots)
mpl.colors.ListedColormap(colors, name='custom_cmap', N=len(colors))


# In[55]:


fig, ax_tc = plt.subplots(figsize=(5, 2))

plot_grouped_bar(ingest_tp, ax_tc, colors=colors)
ax_tc.legend(loc='upper center', bbox_to_anchor=(0.5, 1.2), ncol=5, fontsize=8, labelspacing=0.3, columnspacing=1.0, handlelength=1.0, handletextpad=0.5)
ax_tc.set_ylabel('Throughput (MEPS)')

fig.savefig(meta.PROJECT_DIR + '/figs/tc_ingest.pdf', bbox_inches='tight', pad_inches=0)


# In[56]:


def plot_normalized_bar(df, colors, figsize=(5, 2), linewidth=0.3, dpi=300):
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    df_norm = df.div(df.loc['Bubble-O'], axis=1)
    print(df_norm)
    plot_grouped_bar(df_norm, ax, colors=colors, gap_width=1.0)
    # ax.add_line(plt.axhline(y=1, color='grey', linestyle='--', linewidth=linewidth))
    ax.grid(axis='y', linestyle='-', linewidth=linewidth)
    ax.set_axisbelow(True)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=5, fontsize=8, labelspacing=0.3, columnspacing=1.0, handlelength=1.0, handletextpad=0.5)
    ax.set_ylabel('Time (normalized)')
    ax.yaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
    return fig, ax

figsize = (4.5, 1.5)
linewidth = 0.5
dpi = 100

fig, ax_bfs = plot_normalized_bar(tc_avg, colors, figsize, linewidth, dpi)
ax_bfs.set_xlim(-1, 1.6*6)

fig.savefig(meta.PROJECT_DIR + '/figs/tc.pdf', bbox_inches='tight', pad_inches=0)


# In[57]:


latex_cols_order = ['Bubble-O', 'Bubble-U', 'LSGraph', 'XPGraph', 'GraphOne']
latex_cols_name = [f'{{{col}}}' for col in latex_cols_order]
mem_table = memory_bfs_avg.T[latex_cols_order].copy()
mem_table.columns = latex_cols_name

# Find the minimum value index in each row
min_idx = mem_table.idxmin(axis=1)

# Format all values to two decimal places
mem_table = mem_table.map(lambda x: f'{x:.2f}')

# Bold the minimum values in each row
for idx, col in zip(mem_table.index, min_idx):
    mem_table.loc[idx, col] = f'\\textbf{{{mem_table.loc[idx, col]}}}'
# print(mem_table)

# print(latex_cols_name)

mem_latex = mem_table.to_latex(
    column_format='l' + 'r' * (len(memory_bfs_avg.columns) - 1),
    escape=False,
)
print(mem_latex)


# 用于论文叙述的数据

# In[ ]:


ingest_bu_norm = ingest_tp.div(ingest_tp.loc['Bubble-U'], axis=1)
ingest_bo_norm = ingest_tp.div(ingest_tp.loc['Bubble-O'], axis=1)

# print('Ingest throughput normalized (Bubble-U):')
# print(ingest_bu_norm, end='\n\n')
# print('Ingest throughput normalized (Bubble-O):')
# print(ingest_bo_norm, end='\n\n')

ingest_bu_speedup = 1 / ingest_bu_norm
ingest_bo_speedup = 1 / ingest_bo_norm

print('Ingest throughput speedup (Bubble-U):')
print(ingest_bu_speedup, end='\n\n')
print('Ingest throughput speedup (Bubble-O):')
print(ingest_bo_speedup, end='\n\n')

