#!/usr/bin/env python3
import os
import datetime
import subprocess

import meta
import datasets
from datasets import Dataset

def datetime_code():
    # YYYY-MM-DD-HHmm
    return datetime.datetime.now().strftime("%Y-%m-%d-%H%M")

def raw_output_file(dir: str, work: str, dataset: Dataset):
    return os.path.join(dir, f"{work}-{dataset.name}.txt")


def run_work_on_dataset(work: str, dataset: Dataset, output_file: str):
    # os.system(f"{work} {dataset.path} {dataset.vcount} {output_file} {os.cpu_count()}")
    SCRIPT_DIR = meta.SCRIPT_DIR
    script = os.path.join(SCRIPT_DIR, work, "run_tc.sh")
    command = f"{script} {dataset.path} {dataset.vcount} {output_file} {os.cpu_count()}"
    print(f"$ {command}")
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    # pout = proc.stdout.read().decode('utf-8')
    # perr = proc.stderr.read().decode('utf-8')

if __name__ == '__main__':
    WORK_DIR = meta.PROJECT_DIR
    OUTPUT_DIR = os.path.join(meta.EXPERIMENTS_DIR, "tc")
    WORKS = ["lsgraph", "bubble",]
    DATASETS = datasets.U_DATASETS
    # DATASETS = [datasets.u_uk2007, datasets.u_protein2]

    os.chdir(WORK_DIR)
    dtcode = datetime_code()
    raw_output_dir = os.path.join(OUTPUT_DIR, "raw", f"{dtcode}")
    print(f"raw_output_dir: {raw_output_dir}")
    os.makedirs(raw_output_dir, exist_ok=True)

    for work in WORKS:
        # work_raw_output_file = os.path.join(raw_output_dir, f"{work}.txt")
        

        for dataset in DATASETS:
            work_raw_output_file = raw_output_file(raw_output_dir, work, dataset)
            run_work_on_dataset(work, dataset, work_raw_output_file)

        # run_work_on_dataset(script, datasets.livejournal, work_raw_output_file)
        # run_work_on_dataset(script, datasets.friendster, work_raw_output_file)
        # print(f"Running {work} into {work_raw_output_file}")
        # print(f"\tscript: {script}")
        # cmd = f"{script} "