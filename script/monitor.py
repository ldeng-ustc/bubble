#!/usr/bin/env python3
import os
import time
import atexit
import logging
import shutil
from typing import Any
from traceback import format_exc
from threading import Thread
import psutil
import pandas as pd

def build_columns(metrics: dict, full_name=False) -> list[str]:
    columns = []
    for group, gmetrics in metrics.items():
        if group == 'once':
            continue
        for key, value in gmetrics.items():
            if value is None:
                columns.append(f"{group}.{key}" if full_name else key)
            else:
                columns.extend([(f"{group}.{key}.{v}" if full_name else v) for v in value])
    return columns

def build_once_columns(once_metrics: dict[str, list|None], full_name=False):
    columns = []
    for key, val in once_metrics.items():
        if val is None:
            columns.append(key)
        else:
            columns.extend([(f"{key}.{v}" if full_name else v) for v in val])
    return columns

def get_metrics(p: psutil.Process, metrics: dict[str, dict]):
    row = []
    for group, gmetrics in metrics.items():
        if group == "sys":
            for key, value in gmetrics.items():
                if value is None:
                    row.append(getattr(psutil, key)())
                else:
                    row.extend(getattr(psutil, key)()._asdict()[v] for v in value)
        elif group == "proc":
            pdict = p.as_dict(list(gmetrics.keys()))
            for key, value in gmetrics.items():
                if value is None:
                    row.append(pdict[key])
                else:
                    row.extend(pdict[key]._asdict()[v] for v in value)
        elif group == "once":
            pass
        else:
            logging.warning(f"Unkonw metrics group: '{group}'")
    return row

def get_once_metrics(once_metrics: dict, wait_res: tuple[int, int, Any], duration_ns):
    row = []
    for key, val in once_metrics.items():
        if key == 'walltime':
            row.append(duration_ns / 1e9)
        elif key == 'pid':
            row.append(wait_res[0])
        elif key == 'exitcode':
            row.append(os.waitstatus_to_exitcode(wait_res[1]))
        elif key == 'rusage':
            ru = wait_res[2]
            row.extend(getattr(ru, v) for v in val)
    return row



def monitor_loop(p: psutil.Process, metrics: dict[str, dict], data_list: list[list], visible=True, interval: float=1):
    if visible:
        print(''.join(f"{c:>16}" for c in build_columns(metrics)))
    while p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
        time_start = time.time_ns()
        try:
            row = get_metrics(p, metrics)
            data_list.append(row)
            if visible:
                print(''.join(f"{v:16}" for v in row))
        except (AttributeError, psutil.NoSuchProcess) as e:
            logging.warning(f"Catch {type(e)}, message: '{e}'. Maybe the monitored process was exited.")
            logging.debug(format_exc())
        duration = (time.time_ns() - time_start) / (10**9)
        sleep_time = max(interval - duration, 0)
        time.sleep(sleep_time)

def monitor(argv, metrics, file=None, visible=True, *args, **kwargs):
    data = []
    st = time.time_ns()
    p = psutil.Popen(argv, *args, **kwargs)
    atexit.register(lambda: (p.terminate() if p.is_running() else None))
    if file is not None:
        atexit.register(lambda: pd.DataFrame(data, columns=build_columns(metrics)).to_csv(file))
    Thread(target=monitor_loop, args=(p, metrics, data, visible), daemon=True).start()
    wait_res  = os.wait3(0)
    ed = time.time_ns()

    data_df = pd.DataFrame(data, columns=build_columns(metrics))

    once_metrics = metrics.get('once', {})
    once_data = get_once_metrics(once_metrics, wait_res, ed-st)
    once_data_df = pd.DataFrame([once_data], columns=build_once_columns(once_metrics))
    if visible:
        print(data_df)
    return data_df, once_data_df

process_metrics = {
    'cpu_percent': None,
    'cpu_times': ['user', 'system'],
    'memory_info': ['rss', 'vms'],
    'io_counters': ['read_count', 'write_count', 'read_bytes', 'write_bytes', 'read_chars', 'write_chars'],
}

system_metrics = {
    'swap_memory': ['sin', 'sout'],
}

once_metrics = {
    'exitcode': None,
    'walltime': None,
    'rusage': ['ru_utime', 'ru_stime', 'ru_maxrss', 'ru_inblock', 'ru_oublock']
}

metrics = {
    "sys": system_metrics,
    "proc": process_metrics,
    "once": once_metrics
}


loop_metrics = [
    'process.cpu_percent',
    'proc.cpu_times.user',
    'p.cpu_times.system',
]
