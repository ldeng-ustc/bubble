#!/usr/bin/env python
# coding: utf-8

import os
import meta
import datasets

import numpy as np
import pandas as pd
from parse_output import get_latest_files, get_data_from_dir_custom, extract_multiple_data

# Data preprocessing

pd.set_option('display.width', 1000)

n_latest = 1
exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'basic_benchmarks/raw')
latest_dirs = get_latest_files(exp_dir, n_latest)
print("Plot latest experiments:", latest_dirs)

expdata_list = []
for dir in latest_dirs:
    expdata = get_data_from_dir_custom(dir, title_args=['work', 'dataset'])
    expdata_list.append(expdata)

memory_ingest_avg = extract_multiple_data(expdata_list, 'work', 'dataset', 'rss_ingest')
memory_bfs_avg = extract_multiple_data(expdata_list, 'work', 'dataset', 'rss_bfs')

# Convert memory from bytes to GB
memory_ingest_avg = memory_ingest_avg / 1024**3
memory_bfs_avg = memory_bfs_avg / 1024**3

cols_order = ['LiveJournal', 'Protein', 'Twitter', 'Friendster', 'UK2007', 'Protein2']
new_cols = ['LJ', 'PR1', 'TW', 'FR', 'UK', 'PR2']
rows_order = ['lsgraph', 'xpgraph', 'graphone', 'bubble', 'bubble_ordered']
new_index = ['LSGraph', 'XPGraph', 'GraphOne', 'Bubble-U', 'Bubble-O']

def rename_df(df):
    df = df[cols_order]
    df.columns = new_cols
    df = df.reindex(rows_order)
    df.index = new_index
    return df

memory_bfs_avg = rename_df(memory_bfs_avg)

print(f"Memory BFS (GB) average of {n_latest} experiments:", memory_bfs_avg, sep='\n', end='\n\n')

# Generate LaTeX table for memory usage
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

mem_latex = mem_table.to_latex(
    column_format='l' + 'r' * (len(memory_bfs_avg.columns) - 1),
    escape=False,
)
print(mem_latex)

memory_bfs_avg_norm = memory_bfs_avg.div(memory_bfs_avg.loc['Bubble-U'], axis=1)
print(1 / memory_bfs_avg_norm)
