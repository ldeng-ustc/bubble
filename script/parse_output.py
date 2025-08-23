import os
import re
import math
import numpy as np
import pandas as pd
from collections.abc import Callable
from pathlib import Path

def get_latest_files(dir: str, n: int = 1, ignore_latest: int = 0) -> list[str]:
    files = os.listdir(dir)
    files = [os.path.join(dir, f) for f in files]
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-n:-ignore_latest] if ignore_latest > 0 else files[-n:]

def get_latest_file(dir: str):
    return get_latest_files(dir, 1)[0]

def parse_output(file: str) -> dict[str, float]:
    """将实验原始输出文件中带标签的行数据提取为字典"""
    EXPOUT_TAG = "[EXPOUT]"
    with open(file, "r") as f:
        lines = f.readlines()
    expout_lines = [line for line in lines if EXPOUT_TAG in line]

    res = {}
    for line in expout_lines:
        valid_part = line.split(EXPOUT_TAG)[1]
        key, value = valid_part.split(":")
        key = key.lower().strip().replace(" ", "_")
        # print(key, ':', value)
        # get number part of the value

        value = value.strip()
        # 如果首位是数值或正负号、小数点，认为是数值
        if value[0] in ['-', '+'] or value[0].isdigit() or value[0] == '.':
            value = re.search(r"[-+]?\d*\.\d+|\d+", value).group()
            value = float(value)
        elif value[0] == '[': # list
            value = value[1:-1].split(',')
            value = [float(v.strip()) for v in value]
        res[key] = value
    
    return res

def get_data_from_dir(dir):
    raw_files = sorted(os.listdir(dir))
    exp_data = []
    for file in raw_files:
        print("Processing", file)
        path = os.path.join(dir, file)
        work, dataset_name = os.path.basename(path).split('.')[0].split('-')
        res = parse_output(path)
        res['dataset'] = dataset_name
        res['work'] = work
        exp_data.append(res)
        print("Data:", res)
    return exp_data

def extract_data(exp_data: list[dict], row: str, col: str, value: str, condition: callable=None) -> pd.DataFrame:
    """将格式为字典的多个实验数据整理为Dataframe，每个字典对应DF中一个值
    Args:
        exp_data (list[dict]): 要整理的字典列表
        row (str): 行索引，每个字典中d[row]将对应到DF的行索引
        col (str): 列索引，每个字典中d[col]将对应到DF的列索引
        value (str): 值索引，每个字典中d[value]将对应到DF的值，即d[value]将被写入到df.at[d[row], d[col]]
        condition (callable, optional): 条件函数，返回True的字典将被写入到df中. 否则将被忽略. 默认为None，所有字典都将被写入到df中.

    Returns:
        pd.DataFrame: 整理后的DataFrame
    """
    if condition is None:
        condition = lambda x: True
    
    rows = set()
    cols = set()
    for d in exp_data:
        if not condition(d):
            continue
        rows.add(d[row])
        cols.add(d[col])
    rows = sorted(list(rows))
    cols = sorted(list(cols))

    df = pd.DataFrame(index=rows, columns=cols)
    for d in exp_data:
        if not condition(d):
            continue
        df.at[d[row], d[col]] = d.get(value, math.nan)
    df.index.name = row
    df.columns.name = col
    return df

def get_data_from_dir_custom(
    dir: str,
    title_args: list[str]=['work'], 
    title_sep:str='-',
    logging: bool=False,
) -> list[dict]:
    """将目录下所有文件整理为数据（字典），每个文件对应一个字典，并能自定义文件名的解析方式
    如，文件名为 `work1-dataset1.txt`，title_args=['work', 'dataset']，title_sep='-'
    则解析解析结果会添加以下内容：
    {work: 'work1', dataset: 'dataset1', ...}
    
    Args:
        dir (str): 要整理的目录
        title_args (list[str], optional): 文件名要解析为的参数列表 Defaults to ['work'].
        title_sep (str, optional): 文件名分割符 Defaults to '-'.
        logging (bool, optional): 是否开启日志 Defaults to False.

    Returns:
        list[dict]: 解析后的数据
    """
    
    def log(*args, **kwargs):
        if logging:
            print(*args, **kwargs)

    raw_files = sorted(os.listdir(dir))
    exp_data = []
    for file in raw_files:
        log("Processing:", file)
        path = os.path.join(dir, file)
        title_parts = Path(path).stem.split(title_sep)
        if len(title_parts) != len(title_args):
            error_str = f"Cannot parse {title_parts} to {title_args}, length mismatch"
            raise ValueError(error_str)
        
        res = parse_output(path)
        for arg_name, arg_value in zip(title_args, title_parts):
            res[arg_name] = arg_value
        log("Data:", res)
        exp_data.append(res)
    return exp_data

def extract_multiple_data(
    exp_data_list: list[list[dict]],
    row: str, col: str, value: str,
    condition: callable=None,
) -> pd.DataFrame:
    """与extract_data类似，但聚合多个实验数据（当前只能取平均）"""
    if condition is None:
        condition = lambda x: True

    # Get all valid rows and columns
    rows = set()
    cols = set()
    for d in exp_data_list[0]:
        if not condition(d):
            continue
        rows.add(d[row])
        cols.add(d[col])
    rows = sorted(list(rows))
    cols = sorted(list(cols))

    # Create a DataFrame for each exp_data
    dfs = []
    for exp_data in exp_data_list:
        df = pd.DataFrame(index=rows, columns=cols)
        for d in exp_data:
            if not condition(d):
                continue
            df.at[d[row], d[col]] = d.get(value, math.nan)
        dfs.append(df)
    
    # Aggregate the DataFrames
    aggregated_df = pd.concat(dfs)
    aggregated_df = aggregated_df.groupby(level=0).mean()
    return aggregated_df

def extract_multiple_data_to_list(
    exp_data_list: list[list[dict]],
    row: str, col: str, value: str,
    condition: callable=None,
) -> list[pd.DataFrame]:
    """与extract_multiple_data类似，但不进行聚合，所有数据放到ndarray"""
    if condition is None:
        condition = lambda x: True

    # Get all valid rows and columns
    rows = set()
    cols = set()
    for d in exp_data_list[0]:
        if not condition(d):
            continue
        rows.add(d[row])
        cols.add(d[col])
    rows = sorted(list(rows))
    cols = sorted(list(cols))

    # Create a DataFrame for each exp_data
    dfs = []
    for exp_data in exp_data_list:
        df = pd.DataFrame(index=rows, columns=cols)
        for d in exp_data:
            if not condition(d):
                continue
            df.at[d[row], d[col]] = d.get(value, math.nan)
        dfs.append(df)
    
    return dfs


def agg_frames(df_list: list[pd.DataFrame], fn: Callable[[list], any]) -> pd.DataFrame:
    """
    对一个 DataFrame 列表进行位置聚合。

    参数：
    df_list (List[pd.DataFrame]): 包含相同形状的 DataFrame 的列表。
    fn (Callable[[List[Any]], Any]): 一个聚合函数，接收一个包含对应位置元素的列表，并返回一个聚合值。

    返回：
    pd.DataFrame: 一个新的 DataFrame，其中每个单元格都是聚合函数 fn 的结果。
    """
    if not df_list:
        return pd.DataFrame()
    
    all_cnames = set()
    all_rnames = set()
    for df in df_list:
        all_cnames.update(df.columns)
        all_rnames.update(df.index)

    result_df = pd.DataFrame(index=sorted(all_rnames), columns=sorted(all_cnames))

    for rname in result_df.index:
        for cname in result_df.columns:
            # 收集所有 DataFrame 在 (r, c) 位置的元素
            values_at_pos = [df.at[rname, cname] for df in df_list if rname in df.index and cname in df.columns]
            # 调用聚合函数 fn 进行处理
            result_df.at[rname, cname] = fn(values_at_pos) if values_at_pos else np.nan
            
    return result_df

def mean_box(values: list, box=(25, 75), ratio=1.5) -> float:
    """
    聚合函数：使用NumPy，去除NaN值、去除极端值后取平均。

    参数：
    values (List[Any]): 包含同一位置上所有元素的列表。

    返回：
    Any: 聚合后的平均值，如果处理后没有值则返回NaN。
    """
    # 1. 将列表转换为NumPy数组，并去除NaN值
    # nan_to_num 可以将NaN转换为0，这里用起来不合适，还是直接用 isnan 比较好
    arr = np.array(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    # print(arr)

    # 如果去除NaN后没有数据，直接返回NaN
    if arr.size == 0:
        return np.nan

    # 2. 使用四分位距（IQR）方法去除极端值
    # 如果数据点少于4个，无法有效计算四分位数，直接返回平均值
    if arr.size < 4:
        return np.mean(arr)

    q1, q3 = np.percentile(arr, [*box])
    iqr = q3 - q1

    # 定义极端值的上下边界
    lower_bound = q1 - ratio * iqr
    upper_bound = q3 + ratio * iqr

    # 筛选出非极端值
    filtered_arr = arr[(arr >= lower_bound) & (arr <= upper_bound)]

    # 3. 对处理后的值取平均
    if filtered_arr.size == 0:
        return np.nan
    else:
        return np.mean(filtered_arr)
