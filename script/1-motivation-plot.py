import os
import meta
import datasets
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from parse_output import get_latest_files, parse_output, extract_data
from pathlib import Path

def plot_df_as_line_chart(ax, df):
    """
    在指定的 Axes 对象上绘制 DataFrame 的每一列为折线图。

    参数:
    ax (matplotlib.axes.Axes): 用于绘图的 Axes 对象。
    df (pd.DataFrame): 输入的 DataFrame，每一列对应一条线。
    """
    # 遍历 DataFrame 的每一列，并在指定的 Axes 上绘制折线图
    for column in df.columns:
        ax.plot(df.index, df[column], label=column, marker='o')
    
    # 添加图例
    ax.legend()
    
    # 添加标题和标签
    ax.set_xlabel('Threads')
    ax.set_ylabel('Bandwidth (MB/s)')


# def plot_df_line_bar(ax, df, line_cols: list[str], bar_cols: list[str]):
#     """
#     在指定的 Axes 对象上绘制 DataFrame 的列，一部分列为折线图，一部分列为柱状图，使用不同的 y 轴。

#     参数:
#     ax (matplotlib.axes.Axes): 用于绘图的 Axes 对象。
#     df (pd.DataFrame): 输入的 DataFram
#     line_cols (list[str]): 需要绘制为折线图的列。
#     bar_cols (list[str]): 需要绘制为柱状图的列。
#     """
#     # 绘制折线图
#     for column in line_cols:
#         ax.plot(df.index, df[column], label=column, marker='o')
    
#     # 绘制柱状图
#     bar_width = 0.35
#     x = np.arange(len(df.index))
#     for i, column in enumerate(bar_cols):
#         ax.bar(x + i * bar_width, df[column], bar_width, label=column)
    
#     # 添加图例
#     ax.legend()
    
#     # 添加标题和标签
#     ax.set_xlabel('Threads')
#     ax.set_ylabel('Bandwidth (MB/s)')



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
        ax.plot(x, df[col], label=col, marker='o')

    # 创建右侧y轴并绘制柱状图
    if bar_cols:
        ax2 = ax.twinx()
        bar_width = 0.8  # 柱状图总宽度
        n_bars = len(bar_cols)
        width_per_bar = bar_width / n_bars

        for i, col in enumerate(bar_cols):
            offset = i * width_per_bar - bar_width / 2
            ax2.bar(x + offset, df[col], width_per_bar, label=col)

        # 合并图例
        handles_line, labels_line = ax.get_legend_handles_labels()
        handles_bar, labels_bar = ax2.get_legend_handles_labels()
        ax.legend(handles_line + handles_bar, labels_line + labels_bar, loc='upper right')
        
        # 设置右侧y轴标签（根据需求自定义）
        ax2.set_ylabel('Bar Values')
    else:
        ax.legend(loc='upper left')

    # 设置公共标签
    ax.set_xlabel('Threads')
    ax.set_ylabel('Bandwidth (MB/s)')
    return ax, ax2

if __name__ == '__main__':
    exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'motivation/raw')
    latest_dirs = get_latest_files(exp_dir, 10)

    print("Latest experiment:", latest_dirs)

    dfs_tp = []
    dfs_l2_total = []
    dfs_l2_miss = []
    dfs_l2_demand_total = []
    dfs_l2_demand_miss = []
    dfs_l3_total = []
    dfs_l3_miss = []
    dfs_l3_miss_cycles = []
    dfs_l3_miss_stalls = []

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

        # df = extract_data(exp_data, row='threads', col='work', value='bandwidth')
        df = extract_data(exp_data, row='threads', col='work', value='throughput')
        dfs_tp.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='l2_rqsts.references')
        dfs_l2_total.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='l2_rqsts.miss')
        dfs_l2_miss.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='l2_rqsts.all_demand_references')
        dfs_l2_demand_total.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='l2_rqsts.all_demand_miss')
        dfs_l2_demand_miss.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='cache-references')
        dfs_l3_total.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='cache-misses')
        dfs_l3_miss.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='cycle_activity.cycles_l3_miss')
        dfs_l3_miss_cycles.append(df)
        df = extract_data(exp_data, row='threads', col='work', value='cycle_activity.stalls_l3_miss')
        dfs_l3_miss_stalls.append(df)

    # avg_df_tp = sum(dfs_tp) / len(dfs_tp)
    # avg_df_l2_total = sum(dfs_l2_total) / len(dfs_l2_total)
    # avg_df_l2_miss = sum(dfs_l2_miss) / len(dfs_l2_miss)
    # avg_df_l2_demand_total = sum(dfs_l2_demand_total) / len(dfs_l2_demand_total)
    # avg_df_l2_demand_miss = sum(dfs_l2_demand_miss) / len(dfs_l2_demand_miss)
    # avg_df_l3_total = sum(dfs_l3_total) / len(dfs_l3_total)
    # avg_df_l3_miss = sum(dfs_l3_miss) / len(dfs_l3_miss)

    dfs_dict = {
        'tp': dfs_tp,
        'l2_total': dfs_l2_total,
        'l2_miss': dfs_l2_miss,
        'l2_demand_total': dfs_l2_demand_total,
        'l2_demand_miss': dfs_l2_demand_miss,
        'l3_total': dfs_l3_total,
        'l3_miss': dfs_l3_miss,
        'l3_miss_cycles': dfs_l3_miss_cycles,
        'l3_miss_stalls': dfs_l3_miss_stalls
    }

    for name, df in dfs_dict.items():
        tmp = sum(df) / len(df)
        if name != 'tp':
            tmp /= 1e6
        dfs_dict[name] = tmp
    dfs_dict['l3_miss_rate'] = dfs_dict['l3_miss'] / dfs_dict['l3_total']
    dfs_dict['l2_miss_rate'] = dfs_dict['l2_miss'] / dfs_dict['l2_total']
    dfs_dict['l2_demand_miss_rate'] = dfs_dict['l2_demand_miss'] / dfs_dict['l2_demand_total']

    dfs_dict['tp_thread'] = dfs_dict['tp'].div(dfs_dict['tp'].index, axis=0)
    dfs_dict['l3_miss_thread'] = dfs_dict['l3_miss'].div(dfs_dict['l3_miss'].index, axis=0)
    dfs_dict['l2_miss_thread'] = dfs_dict['l2_miss'].div(dfs_dict['l2_miss'].index, axis=0)
    dfs_dict['l2_demand_miss_thread'] = dfs_dict['l2_demand_miss'].div(dfs_dict['l2_demand_miss'].index, axis=0)

    # Combine all dataframes, each df as a column
    pd.set_option('display.width', 1000)
    final_df = pd.DataFrame(columns=dfs_dict.keys())
    for name, df in dfs_dict.items():
        final_df[name] = df
    print(final_df)

    fig, ax = plt.subplots(figsize=(10, 6))
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp_thread'], bar_cols=['l2_miss_rate', 'l2_demand_miss_rate', 'l3_miss_rate'])
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp_thread'], bar_cols=['l2_miss_thread', 'l3_miss_thread'])
    # ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp'], bar_cols=['l2_miss_rate', 'l3_miss_rate'])
    ax1, ax2 = plot_df_line_bar(ax, final_df, line_cols=['tp'], bar_cols=['l3_miss_cycles', 'l3_miss_stalls'])

    # adjust y-axis limits
    # ax1.set_ylim(-40, 70)
    # ax2.set_ylim(0, 2)

    # set y-axis ticks
    ax1.set_yticks([0, 20, 40, 60])
