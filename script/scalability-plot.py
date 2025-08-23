import os
import meta
import datasets
from parse_output import get_latest_file, parse_output, extract_data

if __name__ == '__main__':
    exp_dir = os.path.join(meta.EXPERIMENTS_DIR, 'scalability/raw')
    latest_dir = get_latest_file(exp_dir)

    print("Latest experiment:", latest_dir)

    raw_files = sorted(os.listdir(latest_dir))
    exp_data = []
    for file in raw_files:
        print("Processing", file)
        path = os.path.join(latest_dir, file)
        work, dataset_name, threads = os.path.basename(path).split('.')[0].split('-')
        threads = int(threads)
        res = parse_output(path)
        res['dataset'] = dataset_name
        res['work'] = work
        res['threads'] = threads
        exp_data.append(res)
        print("Data:", res)
    

    dataset_list = list(set([d['dataset'] for d in exp_data]))

    for d in dataset_list:
        df = extract_data(exp_data, row='threads', col='work', value='ingest', condition=lambda x: x['dataset'] == d)
        print(f"{d}:")
        print(df)
