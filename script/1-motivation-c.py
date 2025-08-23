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

def raw_output_file(dir: str, dataset:str, threads: int, batch: int) -> str:
    return os.path.join(dir, f"{dataset}-{threads}-{batch}.txt")


def run_on_threads(output_file: str, dataset: Dataset, threads: int, batch: int, parts: list[int]):

    parts_str = ",".join([str(p) for p in parts])
    command = f"./build/app/motivation/work_imbalance -i {dataset.path} -v {dataset.vcount} -t {threads} -b {batch} -p {parts_str} > {output_file} 2>&1"
    print(f"$ {command}")

    # use subprocess to run the command
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    pout = proc.stdout.read().decode('utf-8')
    perr = proc.stderr.read().decode('utf-8')


if __name__ == '__main__':
    WORK_DIR = meta.PROJECT_DIR
    SCRIPT_DIR = meta.SCRIPT_DIR
    OUTPUT_DIR = os.path.join(meta.EXPERIMENTS_DIR, "motivation-b")

    THREADS_TO_TEST = [16,32,64,128,256]

    os.chdir(WORK_DIR)
    dtcode = datetime_code()
    raw_output_dir = os.path.join(OUTPUT_DIR, "raw", f"{dtcode}")
    print(f"raw_output_dir: {raw_output_dir}")
    os.makedirs(raw_output_dir, exist_ok=True)
    batch_size = 65536

    for t in THREADS_TO_TEST:
        print(f"{t} threads")
        ds = datasets.dataset_by_name("LiveJournal")

        work_raw_output_file = raw_output_file(raw_output_dir, ds.name, t, batch_size)
        print(f"work_raw_output_file: {work_raw_output_file}")
        parts = [t*2**i for i in range(0, 6)]
        run_on_threads(work_raw_output_file, ds, t, batch_size, parts)
