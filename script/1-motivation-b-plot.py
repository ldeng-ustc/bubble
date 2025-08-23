#!/usr/bin/env python
# coding: utf-8

# In[198]:


import os
import meta
import datasets

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from parse_output import get_latest_files, get_data_from_dir_custom, extract_multiple_data, extract_data


# In[199]:


def plot_lines(
        df, ax,
        colors: str|list = None,
        marker: str|list = 'o',
        markersize=5,
        linestyle='-',
        linewidth=2,
        zorder=None,
        **kwargs
    ):
    """在ax上绘制折线图，每条线为df中的一列。
    """
    from itertools import cycle

    def cycle_or_repeat(list_or_elem):
        from collections.abc import Iterable
        if isinstance(list_or_elem, Iterable) and not isinstance(list_or_elem, str):
            yield from cycle(list_or_elem)
        else:
            yield from cycle([list_or_elem])

    if colors is None:
        import matplotlib as mpl
        colors = mpl.colormaps['tab10'].colors
    colors_list = cycle_or_repeat(colors)
    markers_list = cycle_or_repeat(marker)
    markersize_list = cycle_or_repeat(markersize)
    linestyle_list = cycle_or_repeat(linestyle)
    linewidth_list = cycle_or_repeat(linewidth)
    zorder_list = cycle_or_repeat(zorder if zorder is not None else 1)

    for col in df.columns:
        ax.plot(df.index, df[col], 
                label=col,
                color=next(colors_list),
                marker=next(markers_list),
                markersize=next(markersize_list),
                linestyle=next(linestyle_list),
                linewidth=next(linewidth_list),
                zorder=next(zorder_list),
                clip_on=False,      # 允许线条超出坐标轴范围
                **kwargs
        )


# In[205]:


pd.set_option('display.width', 1000)

n_latest = 1
exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'motivation-b/raw')
latest_dir = get_latest_files(exp_dir, n_latest, 0)[0]
print("Plot latest experiments:", latest_dir)


expdata = get_data_from_dir_custom(latest_dir, title_args=['dataset', 'thread', 'batch'])
print(expdata)

expdata = sorted(expdata, key=lambda x: int(x['thread']))

dfs = []
for d in expdata:
    cols = [d['thread']]
    rows = d['partitions']
    df = pd.DataFrame(columns=cols, index=rows)
    for i, u in enumerate(d['utilization']):
        df.iloc[i] = u
    dfs.append(df)
    print(df)


# In[201]:


from matplotlib.colors import to_hex

cmap_blues = mpl.colormaps['Blues']
cmap_oranges = mpl.colormaps['Oranges']
# colds = cmap_blues([1.0, 0.7, 0.2])
hots = cmap_oranges([0.4, 0.55, 0.7, 0.85, 1.0])
colors = list(hots)
mpl.colors.ListedColormap(colors, name='custom_cmap', N=len(colors))


# In[202]:


# colors = mpl.colormaps['tab10'].colors
markers = ['v', '^', 'o', 'D', 's']
fig, ax = plt.subplots(figsize=(2.5, 1.8))
for i, df in enumerate(dfs):
    plot_lines(df, ax, colors=to_hex(colors[i]), marker=markers[i], markersize=4, linestyle='-', linewidth=2, zorder=100)

ax.set_xscale('log')
# ax.set_xticks([16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192])
# ax.set_xticklabels([16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192])
ax.set_xticks([16, 64, 256, 1024, 4096])
ax.set_xticklabels([16, 64, 256, 1024, 4096])
ax.set_xticks([16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192], minor=True, labels=[])

ax.set_yticks([0, 0.25, 0.5, 0.75, 1])
ax.set_yticklabels([0, 25, 50, 75, 100], fontsize=9)

# ax.minorticks_off()

ax.set_xlim(10, 12000)
ax.set_ylim(0, 1.1)

ax.set_xlabel('#Partitions', labelpad=0)
ax.set_ylabel('CPU utilization (%)   ', labelpad=-2, fontsize=9)

ax.grid(axis='y', linestyle='--', linewidth=0.5, color='lightgray', zorder=0)

legend = ax.legend(
    # title='Threads', title_fontsize=8,
    loc='upper center', bbox_to_anchor=(0.54, 1.2),
    ncol=5, fontsize=8, handlelength=1.2, handletextpad=0.2, columnspacing=0.6, frameon=False
    # loc='lower right', ncols=2,
    # fontsize=8, handlelength=1.7, handletextpad=0.2, columnspacing=1
    # loc='center left', bbox_to_anchor=(1.0, 0.5), ncols=1,
    # fontsize=9, handlelength=1.2, handletextpad=0.3, columnspacing=1, labelspacing=0.6,
)
ax.text(
    0.41, 1.06, 'Threads                                                       ',
    ha='center', va='bottom', fontsize=8,
    transform=ax.transAxes,
    bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='gray', lw=0.5, alpha=0.8)
)

fig.savefig(meta.PROJECT_DIR + '/figs/motivation-b.pdf', bbox_inches='tight', pad_inches=0)


# In[203]:


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
    


# In[204]:


# colors = mpl.colormaps['tab10'].colors
markers = ['v', '^', 'o', 'D', 's']
fig, ax = plt.subplots(figsize=(2.5, 1.8))
for i, df in enumerate(dfs):
    plot_lines(df, ax, colors=to_hex(colors[i]), marker=markers[i], markersize=4, linestyle='-', linewidth=2, zorder=100)

ax.set_xscale('log')
# ax.set_xticks([16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192])
# ax.set_xticklabels([16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192])
ax.set_xticks([16, 64, 256, 1024, 4096])
ax.set_xticklabels([16, 64, 256, 1024, 4096])
ax.set_xticks([16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192], minor=True, labels=[])

ax.set_yticks([0, 0.25, 0.5, 0.75, 1])
ax.set_yticklabels([0, 25, 50, 75, 100], fontsize=9)

# ax.minorticks_off()

ax.set_xlim(10, 12000)
ax.set_ylim(0, 1.1)

ax.set_xlabel('#Partitions', labelpad=0)
ax.set_ylabel('CPU utilization (%)   ', labelpad=-2, fontsize=9)

ax.grid(axis='y', linestyle='--', linewidth=0.5, color='lightgray', zorder=0)

legend = ax.legend(
    # title='Threads', title_fontsize=8,
    loc='upper center', bbox_to_anchor=(0.54, 1.2),
    ncol=5, fontsize=8, handlelength=1.2, handletextpad=0.2, columnspacing=0.6, frameon=False
    # loc='lower right', ncols=2,
    # fontsize=8, handlelength=1.7, handletextpad=0.2, columnspacing=1
    # loc='center left', bbox_to_anchor=(1.0, 0.5), ncols=1,
    # fontsize=9, handlelength=1.2, handletextpad=0.3, columnspacing=1, labelspacing=0.6,
)
ax.text(
    0.41, 1.06, 'Threads                                                       ',
    ha='center', va='bottom', fontsize=8,
    transform=ax.transAxes,
    bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='gray', lw=0.5, alpha=0.8)
)

fig.savefig(meta.PROJECT_DIR + '/figs/motivation-b.pdf', bbox_inches='tight', pad_inches=0.01)






