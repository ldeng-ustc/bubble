#!/usr/bin/env python3
import os
import datetime

import meta
import datasets
from datasets import Dataset

def datetime_code():
    # YYYY-MM-DD-HHmm
    return datetime.datetime.now().strftime("%Y-%m-%d-%H%M")

def raw_output_file(dir: str, work: str, dataset: Dataset):
    return os.path.join(dir, f"{work}-{dataset.name}.txt")


def run_work_on_dataset(work: str, dataset: Dataset, output_file: str):
    os.system(f"{work} {dataset.path} {dataset.vcount} {output_file} {os.cpu_count()}")

if __name__ == '__main__':
    WORK_DIR = meta.PROJECT_DIR
    SCRIPT_DIR = meta.SCRIPT_DIR
    OUTPUT_DIR = os.path.join(meta.EXPERIMENTS_DIR, "basic_benchmarks")
    WORKS = meta.WORKS

    os.chdir(WORK_DIR)
    dtcode = datetime_code()
    raw_output_dir = os.path.join(OUTPUT_DIR, "raw", f"{dtcode}")
    print(f"raw_output_dir: {raw_output_dir}")
    os.makedirs(raw_output_dir, exist_ok=True)

    for work in WORKS:
        # work_raw_output_file = os.path.join(raw_output_dir, f"{work}.txt")
        script = os.path.join(SCRIPT_DIR, work, "basic_benchmarks.sh")

        for dataset in datasets.DATASETS:
            work_raw_output_file = raw_output_file(raw_output_dir, work, dataset)
            run_work_on_dataset(script, dataset, work_raw_output_file)

        # run_work_on_dataset(script, datasets.livejournal, work_raw_output_file)
        # run_work_on_dataset(script, datasets.friendster, work_raw_output_file)
        # print(f"Running {work} into {work_raw_output_file}")
        # print(f"\tscript: {script}")
        # cmd = f"{script} "