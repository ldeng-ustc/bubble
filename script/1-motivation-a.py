#!/usr/bin/env python3
import os
import datetime
import subprocess
import argparse

import meta
import datasets
from datasets import Dataset

def datetime_code():
    # YYYY-MM-DD-HHmm
    return datetime.datetime.now().strftime("%Y-%m-%d-%H%M")

def raw_output_file(dir: str, work: str, threads: int, batch: int | None) -> str:
    if batch is None:
        batch = ""
    return os.path.join(dir, f"{work}-{threads}-{batch}.txt")


def run_work_on_threads(work: str, output_file: str, threads: int, batch=None):
    # output_file = './tmp/log.txt'
    fifo_path = '/tmp/bubble_perf_fifo'
    
    if os.path.exists(fifo_path):
        os.remove(fifo_path)
    os.mkfifo(fifo_path)

    if batch is None:
        batch = ""

    perf_command = "perf stat " \
                        f"--control=fifo:{fifo_path} " \
                        "-x , --no-big-num -D -1 " \
                        "-e cycles,l2_rqsts.references,l2_rqsts.miss,l2_rqsts.all_demand_references,l2_rqsts.all_demand_miss,cache-misses,cache-references," \
                        "cycle_activity.cycles_l3_miss,cycle_activity.stalls_l3_miss -- \\\n"
    command = f"./build/app/motivation/{work} {threads} {batch} enable_perf | tee {output_file}"
    print(f"$ {perf_command} {command}")

    # use subprocess to run the command
    proc = subprocess.Popen(perf_command + command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    pout = proc.stdout.read().decode('utf-8')
    perr = proc.stderr.read().decode('utf-8')

    # print(f"stdout: {pout}")
    # print(f"stderr: {perr}")

    keys = [
        'cycles',
        'l2_rqsts.references', 
        'l2_rqsts.miss', 
        'l2_rqsts.all_demand_references', 
        'l2_rqsts.all_demand_miss', 
        'cache-misses', 
        'cache-references',
        'cycle_activity.cycles_l3_miss',
        'cycle_activity.stalls_l3_miss'
    ]
    perf_data = perr.strip().split('\n')
    perf_dict = {}
    for line in perf_data:
        for key in keys:
            if key not in line:
                continue
            words = line.split(',')
            event_name = words[2]
            event_count = words[0]
            perf_dict[event_name] = event_count
            with open(output_file, 'a') as f:
                f.write(f"[EXPOUT]{event_name}: {event_count}\n")
    l2_miss_rate = float(perf_dict['l2_rqsts.miss']) / float(perf_dict['l2_rqsts.references'])
    l2_demand_miss_rate = float(perf_dict['l2_rqsts.all_demand_miss']) / float(perf_dict['l2_rqsts.all_demand_references'])
    llc_miss_rate = float(perf_dict['cache-misses']) / float(perf_dict['cache-references'])
    with open(output_file, 'a') as f:
        f.write(f"[EXPOUT]l2_miss_rate: {l2_miss_rate}\n")
        f.write(f"[EXPOUT]l2_miss_rate_demand: {l2_demand_miss_rate}\n")
        f.write(f"[EXPOUT]llc_miss_rate: {llc_miss_rate}\n")
    os.remove(fifo_path)



if __name__ == '__main__':
    WORK_DIR = meta.PROJECT_DIR
    SCRIPT_DIR = meta.SCRIPT_DIR
    OUTPUT_DIR = os.path.join(meta.EXPERIMENTS_DIR, "motivation")

    # THREADS_TO_TEST = [4, 8, 16, 32, 64, 80]
    # THREADS_TO_TEST = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 68, 72, 76, 80]
    THREADS_TO_TEST = [1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80]

    os.chdir(WORK_DIR)
    dtcode = datetime_code()
    raw_output_dir = os.path.join(OUTPUT_DIR, "raw", f"{dtcode}")
    print(f"raw_output_dir: {raw_output_dir}")
    os.makedirs(raw_output_dir, exist_ok=True)

    works = ['mini_process_sort', 'mini_process_shuffle', 'random_insertion_uniform']
    batch_list = [1024* 2**i for i in range(5, 15)]
    batchs_for_works = [batch_list[:], batch_list[:], [None]]

    for work, batchs in zip(works, batchs_for_works):
        for batch in batchs:
            script = os.path.join(SCRIPT_DIR, work, "basic_benchmarks.sh")
            print(f"Running {work} (batch={batch}):")
            for t in THREADS_TO_TEST:
                print(f"\t{work} on {t} threads")
                if t > os.cpu_count():
                    print(f"Skipping {t} threads, only {os.cpu_count()} available")
                    continue
                work_raw_output_file = raw_output_file(raw_output_dir, work, t, batch)
                run_work_on_threads(work, work_raw_output_file, t, batch)
