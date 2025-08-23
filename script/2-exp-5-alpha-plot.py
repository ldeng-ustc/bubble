#!/usr/bin/env python
# coding: utf-8

# In[36]:


import os
import meta
import datasets
import pandas as pd
from parse_output import get_latest_files, extract_multiple_data_to_list, get_data_from_dir_custom

def get_data_list(latest_n=10, ignore_latest=0):
    exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'alpha/raw')
    latest_dirs = get_latest_files(exp_dir, latest_n, ignore_latest=ignore_latest)
    
    exp_data_list = []
    for dir in latest_dirs:
        exp_data = get_data_from_dir_custom(dir, ['work', 'dataset', 'alpha'], logging=False)
        exp_data_list.append(exp_data)
    
    return exp_data_list

exp_data_list = get_data_list(latest_n=32, ignore_latest=0)

print(exp_data_list[:5])


# In[37]:


import numpy as np
from parse_output import agg_frames, mean_box
def get_alpha_df(exp_data_list, dataset, agg_func=mean_box):
    dfs = extract_multiple_data_to_list(exp_data_list, row='alpha', col='dataset', value='ingest', condition=lambda x: x['dataset'] == dataset)
    # df = pd.concat(dfs).groupby(level=0).mean()
    df = agg_frames(dfs, mean_box)
    df.index = df.index.astype(float)
    df.sort_index(inplace=True)
    print(df)
    
    n_edges = datasets.dataset_by_name(dataset).ecount
    # df_res = n_edges / df / 1e6
    df_res = df.copy()
    df_res.columns = ['Ingest']

    dfs = extract_multiple_data_to_list(exp_data_list, row='alpha', col='dataset', value='bfs', condition=lambda x: x['dataset'] == dataset)
    # df = pd.concat(dfs).groupby(level=0).mean()
    # df = agg_frames(dfs, mean_box)
    # df = agg_frames(dfs, np.median)
    df = agg_frames(dfs, agg_func)
    df.index = df.index.astype(float)
    df.sort_index(inplace=True)
    # print(df)
    df_res['BFS'] = df[dataset]

    return df_res

def get_alpha_df_final(exp_data_list, name, base=2.0):
    df = get_alpha_df(exp_data_list, name)
    df = df / df.loc[base,]
    return df

df_twitter = get_alpha_df_final(exp_data_list, 'Twitter')
df_friendster = get_alpha_df_final(exp_data_list, 'Friendster')


print('Twitter:', df_twitter, sep='\n')
print('Friendster:', df_friendster, sep='\n')


# In[38]:


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


# In[39]:


import matplotlib.pyplot as plt
import numpy as np

def plot_scalability(df_band, fig_size=(2.5, 2)):
    fig, ax = plt.subplots(figsize=fig_size)
    markers = ['s', 'o', '^', 'D', 'v']
    colors = None
    zorder = [200 - i for i in range(len(df_band.columns))]
    plot_lines(df_band, ax, colors=colors, marker=markers, zorder=zorder, markersize=4, linewidth=2)

    xmin = np.floor(df_band.index.min())
    xmax = np.max(df_band.index.max())
    idx = list(np.arange(xmin, xmax + 0.1, 0.5))
    idx_main = list(np.arange(xmin, xmax + 0.1, 1.0))
    ax.set_xticks(idx_main, minor=False)
    ax.set_xticks(idx, minor=True)
    # ax.tick_params(axis='x', which='minor', bottom=False)
    ax.set_xticklabels(idx_main, minor=False)
    ax.set_xlim(xmin, xmax)

    ROUND = 0.1
    ymin = np.floor(df_band.min().min() / ROUND) * ROUND
    ymax = np.ceil(df_band.max().max() / ROUND) * ROUND
    ax.set_ylim(ymin, ymax)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.1f}'))

    # 双侧y轴
    # ax.yaxis.set_ticks_position('both')
    # ax.tick_params(axis='y', labelleft=True, labelright=True, zorder=10)


    ax.legend(
        loc='upper left', 
        bbox_to_anchor=(0.08, 1.05), 
        ncol=2,
        fontsize=8, 
        handlelength=1.8,
        handletextpad=0.4,
        columnspacing=1.0,
        # labelspacing=1,
        handleheight=0.2,
        labels=['Ingestion', 'BFS'], # manually add them afterwards
        frameon=True,
    )

    ax.set_xlabel('Alpha', labelpad=-0.7)
    ax.set_ylabel('Running Time    \n(Normalized)    ', labelpad=-0.7,)
    return fig, ax


exp_data_list = get_data_list(latest_n=32, ignore_latest=0)

agg_func = lambda x: mean_box(x, box=(0, 10), ratio=1.0)
df_twitter = get_alpha_df(exp_data_list, 'Twitter', agg_func)
df_friendster = get_alpha_df(exp_data_list, 'Friendster', agg_func)


df_twitter = df_twitter / df_twitter.loc[2.0,]
df_friendster = df_friendster / df_friendster.loc[2.0,]

print('Twitter:', df_twitter, sep='\n')
print('Friendster:', df_friendster, sep='\n')

df_twitter = df_twitter.loc[df_twitter.index <= 8.0]
df_friendster = df_friendster.loc[df_friendster.index <= 8.0]

#ALPHAS = []
#ALPHAS += [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0] 
#ALPHAS += [2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0]
#ALPHAS += [3.5, 4.0, 5.0, 6.0, 7.0, 8.0]
#ALPHAS += [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]

ALPHAS  = [1.1, 1.2, 1.3, 1.4, 1.6, 1.8, 2.0]
ALPHAS += [2.3, 2.5, 2.7, 3.0, 3.5, 4.0, 5.0, 6.0]
df_twitter = df_twitter.loc[ALPHAS]
df_friendster = df_friendster.loc[ALPHAS]

fig_size = (4.6, 1.0)
fig_twitter, ax_twitter = plot_scalability(df_twitter, fig_size)
fig_friendster, ax_friendster = plot_scalability(df_friendster, fig_size)



# fig_legend, ax_legend = plot_scalability(df_twitter, fig_size=(3, 0.5))
# ax_legend.axis('off')

# legend_bbox = (0, 0, 2.5, 0)
# lg = ax_legend.legend(title=None, loc='lower center', mode="expand", ncol=5, handlelength=1.5, columnspacing=1.0, bbox_to_anchor=legend_bbox)

# for artist in ax_legend.get_children():
    # if artist != lg:
        # artist.set_visible(False)

# fig_legend.tight_layout()

fig_twitter.savefig('../figs/alpha_tw.pdf', bbox_inches='tight', pad_inches=0)
fig_friendster.savefig('../figs/alpha_fr.pdf', bbox_inches='tight', pad_inches=0)
# fig_legend.savefig('../figs/alpha_legend.pdf', bbox_inches='tight', pad_inches=0)



# In[40]:


import matplotlib.pyplot as plt
import numpy as np

def plot_one_alpha(ax, df_band):
    markers = ['s', 'o', '^', 'D', 'v']
    colors = None
    zorder = [200 - i for i in range(len(df_band.columns))]
    plot_lines(df_band, ax, colors=colors, marker=markers, zorder=zorder, markersize=4, linewidth=2)

    xmin = np.floor(df_band.index.min())
    xmax = np.max(df_band.index.max())
    idx = list(np.arange(xmin, xmax + 0.1, 0.5))
    idx_main = list(np.arange(xmin, xmax + 0.1, 1.0))
    ax.set_xticks(idx_main, minor=False)
    ax.set_xticks(idx, minor=True)
    # ax.tick_params(axis='x', which='minor', bottom=False)
    ax.set_xticklabels(idx_main, minor=False)
    ax.set_xlim(xmin, xmax)

    ROUND = 0.1
    ymin = np.floor(df_band.min().min() / ROUND) * ROUND
    ymax = np.ceil(df_band.max().max() / ROUND) * ROUND
    ax.set_ylim(ymin, ymax)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.1f}'))

    # 双侧y轴
    ax.yaxis.set_ticks_position('both')
    ax.tick_params(axis='y', labelleft=True, labelright=True, zorder=10)

def plot_alpha_td(df1, df2, fig_size=(4, 2.5)):
    fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, figsize=fig_size)
    plot_one_alpha(ax1, df1)
    plot_one_alpha(ax2, df2)

    ax2.xaxis.set_ticks_position('top')

    fig.subplots_adjust(hspace=0.5)


    return fig, (ax1, ax2)


exp_data_list = get_data_list(latest_n=32, ignore_latest=0)

agg_func = lambda x: mean_box(x, box=(0, 10), ratio=1.0)
df_twitter = get_alpha_df(exp_data_list, 'Twitter', agg_func)
df_friendster = get_alpha_df(exp_data_list, 'Friendster', agg_func)


df_twitter = df_twitter / df_twitter.loc[2.0,]
df_friendster = df_friendster / df_friendster.loc[2.0,]

print('Twitter:', df_twitter, sep='\n')
print('Friendster:', df_friendster, sep='\n')

df_twitter = df_twitter.loc[df_twitter.index <= 8.0]
df_friendster = df_friendster.loc[df_friendster.index <= 8.0]

#ALPHAS = []
#ALPHAS += [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0] 
#ALPHAS += [2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0]
#ALPHAS += [3.5, 4.0, 5.0, 6.0, 7.0, 8.0]
#ALPHAS += [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]

ALPHAS  = [1.1, 1.2, 1.3, 1.4, 1.6, 1.8, 2.0]
ALPHAS += [2.1, 2.3, 2.5, 2.7, 3.0, 3.5, 4.0, 5.0, 6.0]
df_twitter = df_twitter.loc[ALPHAS]
df_friendster = df_friendster.loc[ALPHAS]

fig_size = (4, 2.5)
fig_twitter, (ax1, ax2) = plot_alpha_td(df_twitter, df_friendster, fig_size)
# fig_twitter, ax_twitter = plot_scalability(df_twitter, fig_size)
# fig_friendster, ax_friendster = plot_scalability(df_friendster, fig_size)


# fig_legend, ax_legend = plot_scalability(df_twitter, fig_size=(3, 0.5))
# ax_legend.axis('off')

# legend_bbox = (0, 0, 2.5, 0)
# lg = ax_legend.legend(title=None, loc='lower center', mode="expand", ncol=5, handlelength=1.5, columnspacing=1.0, bbox_to_anchor=legend_bbox)

# for artist in ax_legend.get_children():
    # if artist != lg:
        # artist.set_visible(False)

# fig_legend.tight_layout()

#fig_twitter.savefig('../figs/alpha_tw.pdf', bbox_inches='tight', pad_inches=0)
#fig_friendster.savefig('../figs/alpha_fr.pdf', bbox_inches='tight', pad_inches=0)
# fig_legend.savefig('../figs/alpha_legend.pdf', bbox_inches='tight', pad_inches=0)


