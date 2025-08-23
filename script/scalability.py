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

def raw_output_file(dir: str, work: str, dataset: Dataset, threads: int):
    return os.path.join(dir, f"{work}-{dataset.name}-{threads}.txt")


def run_work_on_dataset_with_threads(work: str, dataset: Dataset, output_file: str, threads: int):
    total_threads = os.cpu_count()
    # core_list = list(range(0, total_threads, 2)) + list(range(1, total_threads, 2))
    numa_threads = total_threads // 2
    physical_list = [i // 2 for i in range(total_threads)]
    numa_list = [i % 2 for i in range(total_threads)]
    core_list = [p + n * numa_threads for p, n in zip(physical_list, numa_list)]
    
    used_cores = core_list[:threads]
    used_cores_str = ",".join(map(str, used_cores))
    command = f"taskset --cpu-list {used_cores_str} \\\n   {work} {dataset.path} {dataset.vcount} {output_file} {threads}"
    # command = f"{work} {dataset.path} {dataset.vcount} {output_file} {threads}"
    print(f"$ {command}")
    # os.system(command)
    # subprocess.Popen()
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # wait process to finish, or timeout and kill it
    try:
        wait_time = max(3600//(threads//4), 600)
        proc.wait(timeout=wait_time)
    except subprocess.TimeoutExpired:
        proc.terminate()
        if proc.poll() is None:
            proc.kill()


if __name__ == '__main__':
    WORK_DIR = meta.PROJECT_DIR
    SCRIPT_DIR = meta.SCRIPT_DIR
    OUTPUT_DIR = os.path.join(meta.EXPERIMENTS_DIR, "scalability")

    works = [
    "lsgraph", 
    "graphone", 
    "xpgraph", 
    "bubble",
    "bubble_ordered",
    ]
    # WORKS = ["bubble"]
    # DATASETS_TO_TEST = [datasets.friendster, datasets.twitter, datasets.protein3]
    DATASETS_TO_TEST = [datasets.friendster, datasets.twitter]
    # DATASETS_TO_TEST = [datasets.livejournal]
    # THREADS_TO_TEST = [2, 4, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80]
    # THREADS_TO_TEST = [4, 8, 16, 32, 48, 64, 80]
    THREADS_TO_TEST = [4, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80]
    # THREADS_TO_TEST = [80]
    

    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--works", type=str, help="Works to run")
    args = args_parser.parse_args()
    if args.works:
        works = args.works.split(",")

    os.chdir(WORK_DIR)
    dtcode = datetime_code()
    raw_output_dir = os.path.join(OUTPUT_DIR, "raw", f"{dtcode}")
    print(f"raw_output_dir: {raw_output_dir}")
    os.makedirs(raw_output_dir, exist_ok=True)

    for work in works:
        script = os.path.join(SCRIPT_DIR, work, "scale.sh")
        print(f"Running {work}:")
        for dataset in DATASETS_TO_TEST:
            for t in THREADS_TO_TEST:
                print(f"\t{work}.{dataset.name} on {t} threads")
                if t > os.cpu_count():
                    print(f"Skipping {t} threads, only {os.cpu_count()} available")
                    continue
                work_raw_output_file = raw_output_file(raw_output_dir, work, dataset, t)
                run_work_on_dataset_with_threads(script, dataset, work_raw_output_file, t)
