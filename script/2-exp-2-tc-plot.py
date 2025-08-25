#!/usr/bin/env python
# coding: utf-8

import os
import meta
import datasets

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from parse_output import get_latest_files, get_data_from_dir_custom, extract_multiple_data, extract_data

def plot_grouped_bar(
        df, ax, 
        colors=None, 
        bar_width=0.3, gap_width=0.3,
        **kwargs
    ):
    """
    Generate grouped bar chart, where each group is a column of DF, each color is a row of DF, 
    and plot on the specified ax.
    Taking common Evaluation as an example, each row is a system, each column is a dataset. 
    (Convenient for vertical comparison when printing DF)

    Other elements:
    df.index is used for y-axis labels, df.columns is used for x-axis labels.
    If df.index.name is not None, it will be displayed on the y-axis.
    If df.columns.name is not None, it will be displayed on the x-axis.
    Legend is not automatically generated, users need to add it manually. (Legend settings are too complex)

    Parameters:
    df : pd.DataFrame
    ax : matplotlib.axes.Axes
        The Axes object to plot the chart on.
    colors : list
        Color list, same length as df.index. (i.e., same as number of rows)
    bar_width : float
        Width of the bars in the bar chart.
    gap_width : float
        Gap between each group of bars.
    **kwargs : dict
        Other parameters passed to ax.bar() function.
    """

    if colors is None:
        import matplotlib as mpl
        colors = mpl.colormaps['tab10'].colors

    # Plot bar chart on the specified ax
    n_groups = len(df.columns)      # Number of groups
    n_group_bars = len(df.index)    # Number of bars per group
    group_width = bar_width * n_group_bars  # Total width of bars per group
    group_offsets = np.arange(n_groups) * (group_width + gap_width)     # x-coordinate of left end of each group, add gap width between groups
    group_centers = group_offsets + (group_width) / 2                   # x-coordinate of center of each group, used for setting x-axis ticks and labels

    for i, row in enumerate(df.index):
        type_offsets = group_offsets + i * bar_width
        ax.bar(type_offsets, df.loc[row], bar_width, label=row, align='edge', color=colors[i], edgecolor='black', linewidth=0.5, **kwargs)
    
    # Set x-axis ticks and labels
    ax.set_xticks(group_centers)
    ax.set_xticklabels(df.columns)
    if df.columns.name is not None:
        ax.set_xlabel(df.columns.name)


# Data preprocessing and plotting

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

# Start plotting

from matplotlib.colors import to_hex

cmap_blues = mpl.colormaps['Blues']
cmap_oranges = mpl.colormaps['Oranges']
colds = cmap_blues([0.7])
hots = cmap_oranges([0.8])
colors = list(colds) + list(hots)
mpl.colors.ListedColormap(colors, name='custom_cmap', N=len(colors))

fig, ax_tc = plt.subplots(figsize=(5, 2))

plot_grouped_bar(ingest_tp, ax_tc, colors=colors)
ax_tc.legend(loc='upper center', bbox_to_anchor=(0.5, 1.2), ncol=5, fontsize=8, labelspacing=0.3, columnspacing=1.0, handlelength=1.0, handletextpad=0.5)
ax_tc.set_ylabel('Throughput (MEPS)')

fig.savefig(meta.PROJECT_DIR + '/figs/tc_ingest.pdf', bbox_inches='tight', pad_inches=0)

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
