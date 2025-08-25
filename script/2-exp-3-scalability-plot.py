#!/usr/bin/env python
# coding: utf-8

import os
import meta
import datasets
from parse_output import get_latest_files, extract_multiple_data, get_data_from_dir_custom

# Load experimental data
exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'scalability/raw')
latest_n = 1
latest_dirs = get_latest_files(exp_dir, latest_n)

exp_data_list = []
for dir in latest_dirs:
    exp_data = get_data_from_dir_custom(dir, ['work', 'dataset', 'threads'], logging=False)
    exp_data_list.append(exp_data)

def get_scalability_df(exp_data_list, dataset):
    df = extract_multiple_data(exp_data_list, row='threads', col='work', value='ingest', condition=lambda x: x['dataset'] == dataset)
    df.index = df.index.astype(int)
    df.sort_index(inplace=True)
    
    n_edges = datasets.dataset_by_name(dataset).ecount
    df_band = n_edges / df / 1e6

    rename_dict = {'graphone': 'GraphOne', 'lsgraph': 'LSGraph', 'xpgraph': 'XPGraph', 'bubble': 'Bubble-U', 'bubble_ordered': 'Bubble-O'}
    df_band.rename(columns=rename_dict, inplace=True)

    df_band = df_band[['LSGraph', 'GraphOne', 'XPGraph', 'Bubble-O', 'Bubble-U']]
    return df_band

df_band_twitter = get_scalability_df(exp_data_list, 'Twitter')
df_band_friendster = get_scalability_df(exp_data_list, 'Friendster')

df_norm_twitter = df_band_twitter / df_band_twitter.min()
df_norm_friendster = df_band_friendster / df_band_friendster.min()

print('Twitter:', df_norm_twitter, sep='\n')
print('Friendster:', df_norm_friendster, sep='\n')

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
    """Plot line charts on ax, each line represents a column in df."""
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
                clip_on=False,      # Allow lines to extend beyond axis range
                **kwargs
        )

import matplotlib as mpl
from matplotlib.colors import to_hex

cmap_blues = mpl.colormaps['Blues']
cmap_oranges = mpl.colormaps['Oranges']
colds = cmap_blues([1.0, 0.7, 0.2])
hots = cmap_oranges([0.5, 0.8])
colors = list(colds) + list(hots)
mpl.colors.ListedColormap(colors, name='custom_cmap', N=len(colors))

import matplotlib.pyplot as plt

def plot_scalability(df_band, fig_size=(2.5, 2)):
    fig, ax = plt.subplots(figsize=fig_size)
    markers = ['s', 'o', '^', 'D', 'v']
    colors = None
    zorder = [200 - i for i in range(len(df_band.columns))]
    plot_lines(df_band, ax, colors=colors, marker=markers, zorder=zorder, markersize=4, linewidth=2)

    ax.set_xticks(df_band.index, minor=False)
    ax.tick_params(axis='x', which='minor', bottom=False)
    ax.set_xticklabels(df_band.index, minor=False)
    ax.set_xlim(df_band.index[0], df_band.index[-1])

    maxy = df_band.max().max()
    maxy = ((maxy // 10) + 1) * 10
    ax.set_ylim(0, maxy)

    # Set y axis ticks on both sides
    ax.yaxis.set_ticks_position('both')
    ax.tick_params(axis='y', labelleft=True, labelright=True, zorder=10)

    ax.set_xlabel('#Threads')
    ax.set_ylabel('Throughput (MEPS)', labelpad=-0.7)
    return fig, ax

# Generate plots
fig_twitter, ax_twitter = plot_scalability(df_band_twitter)
fig_friendster, ax_friendster = plot_scalability(df_band_friendster)

# Create legend figure
fig_legend, ax_legend = plot_scalability(df_band_twitter, fig_size=(3, 0.5))
ax_legend.axis('off')

legend_bbox = (0, 0, 2.5, 0)
lg = ax_legend.legend(title=None, loc='lower center', mode="expand", ncol=5, handlelength=1.5, columnspacing=1.0, bbox_to_anchor=legend_bbox)

for artist in ax_legend.get_children():
    if artist != lg:
        artist.set_visible(False)

fig_legend.tight_layout()

# Save figures
fig_twitter.savefig(meta.PROJECT_DIR + '/figs/scale_tw.pdf', bbox_inches='tight', pad_inches=0)
fig_friendster.savefig(meta.PROJECT_DIR + '/figs/scale_fr.pdf', bbox_inches='tight', pad_inches=0)
fig_legend.savefig(meta.PROJECT_DIR + '/figs/scale_legend.pdf', bbox_inches='tight', pad_inches=0)


