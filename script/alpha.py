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

def raw_output_file(dir: str, work: str, dataset: Dataset, alpha: float):
    return os.path.join(dir, f"{work}-{dataset.name}-{alpha:.2f}.txt")


def run_work_on_dataset_with_threads(work: str, dataset: Dataset, output_file: str, alpha: float):
    total_threads = os.cpu_count()

    command = f"{work} {dataset.path} {dataset.vcount} {output_file} {total_threads} {alpha}"
    print(f"$ {command}")
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    proc.wait()

if __name__ == '__main__':
    WORK_DIR = meta.PROJECT_DIR
    SCRIPT_DIR = meta.SCRIPT_DIR
    OUTPUT_DIR = os.path.join(meta.EXPERIMENTS_DIR, "alpha")

    WORK = "bubble"
    THREADS = 80
    # DATASETS_TO_TEST = [datasets.friendster, datasets.twitter]
    DATASETS_TO_TEST = [datasets.protein2]
    ALPHAS = []
    ALPHAS += [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0] 
    ALPHAS += [2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0]
    ALPHAS += [3.5, 4.0, 5.0, 6.0, 7.0, 8.0]
    # ALPHAS += [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]

    os.chdir(WORK_DIR)
    dtcode = datetime_code()
    raw_output_dir = os.path.join(OUTPUT_DIR, "raw", f"{dtcode}")
    print(f"raw_output_dir: {raw_output_dir}")
    os.makedirs(raw_output_dir, exist_ok=True)

    script = os.path.join(SCRIPT_DIR, WORK, "alpha.sh")
    print(f"Running {WORK}:")
    for dataset in DATASETS_TO_TEST:
        for a in ALPHAS:
            alpha = a
            work_raw_output_file = raw_output_file(raw_output_dir, WORK, dataset, alpha)
            print(f"\t{WORK}.{dataset.name} with alpha={alpha}")
            run_work_on_dataset_with_threads(script, dataset, work_raw_output_file, alpha)
