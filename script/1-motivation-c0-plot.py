#!/usr/bin/env python
# coding: utf-8

# In[36]:


import os
import meta
import datasets

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from parse_output import get_latest_files, get_data_from_dir_custom, extract_multiple_data, extract_data


# In[37]:


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
    


# In[38]:


threads = [4, 8, 16, 32, 64, 80]    # index
single_thread = [250.08, 170.14, 142.63, 99.11, 74.58, 66.33]
parallel_sort = [19.15, 33.04, 52.33, 76.45, 82.89, 78.27]

df2 = pd.DataFrame.from_dict({
    'threads': threads,
    'single_thread': single_thread,
    'parallel_sort': parallel_sort
})

# make threads the index
df2.set_index('threads', inplace=True)

df2 = df2.loc[[8, 16, 32, 64, 80]]

df2 = df2.T



# colors = ["#000080", "#B22222"]
colors2 = ["#ACD6EC", "#F5A889"]
# colors = ["#FF7F50", "#4682B4"]
# colors = ["#6888F5", "#D77071"]

fig2, ax2 = plt.subplots(figsize=(2.5, 1.8))

plot_grouped_bar(df2, ax2, colors=colors2, bar_width=0.3, gap_width=0.3)
ax2.set_ylabel('Throughput (MEPS)', labelpad=0, fontsize=9)
ax2.set_xlabel('Writer Threads', labelpad=0)

ax2.set_yticks([0, 50, 100, 150, 200])
ax2.set_yticklabels([0, 50, 100, 150, 200], fontsize=9)

ax2.legend(["Single Thread", "Parallel Sorting"], fontsize=8, loc='upper center', bbox_to_anchor=(0.5, 1.22), ncol=2, handlelength=1.2, handletextpad=0.2, columnspacing=0.6)

fig2.savefig(meta.PROJECT_DIR + '/figs/motivation-c0.pdf', bbox_inches='tight', pad_inches=0)

