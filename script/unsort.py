#!/usr/bin/env python3
import os
import datetime

import meta
import datasets
import subprocess
from datasets import Dataset

def datetime_code():
    # YYYY-MM-DD-HHmm
    return datetime.datetime.now().strftime("%Y-%m-%d-%H%M")

def raw_output_file(dir: str, work: str, dataset: Dataset, sort_batch_size: int = None) -> str:
    return os.path.join(dir, f"{work}-{dataset.name}-{sort_batch_size}.txt")


def run_work_on_dataset(work: str, dataset: Dataset, output_file: str, sort_batch_size: int = None):
    total_threads = os.cpu_count()
    command = f"  {work} {dataset.path} {dataset.vcount} {output_file} {total_threads} {sort_batch_size}"
    print(f"$ {command}")
    # os.system(command)
    # subprocess.Popen()
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # wait process to finish, or timeout and kill it
    try:
        wait_time = 1800
        proc.wait(timeout=wait_time)
    except subprocess.TimeoutExpired:
        proc.terminate()
        if proc.poll() is None:
            proc.kill()

if __name__ == '__main__':
    WORK_DIR = meta.PROJECT_DIR
    SCRIPT_DIR = meta.SCRIPT_DIR
    OUTPUT_DIR = os.path.join(meta.EXPERIMENTS_DIR, "sort_batch_size")
    WORKS = ['bubble', 'bubble_ordered']
    DATASETS = [datasets.friendster, datasets.twitter]

    os.chdir(WORK_DIR)
    dtcode = datetime_code()
    raw_output_dir = os.path.join(OUTPUT_DIR, "raw", f"{dtcode}")
    print(f"raw_output_dir: {raw_output_dir}")
    os.makedirs(raw_output_dir, exist_ok=True)

    for work in WORKS:
        # work_raw_output_file = os.path.join(raw_output_dir, f"{work}.txt")
        script = os.path.join(SCRIPT_DIR, work, "unsort.sh")

        for dataset in DATASETS:
            for sort_batch_size in [64*2**i for i in range(0, 9)]:
                work_raw_output_file = raw_output_file(raw_output_dir, work, dataset, sort_batch_size)
                run_work_on_dataset(script, dataset, work_raw_output_file, sort_batch_size)

        # run_work_on_dataset(script, datasets.livejournal, work_raw_output_file)
        # run_work_on_dataset(script, datasets.friendster, work_raw_output_file)
        # print(f"Running {work} into {work_raw_output_file}")
        # print(f"\tscript: {script}")
        # cmd = f"{script} "