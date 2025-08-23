#python3
import os, sys
import shutil
import argparse
from datasets import Dataset, DATASETS, U_DATASETS, dataset_by_name

webgraph_name = {
    "LiveJournal": "livejournal",
    "Protein": "protein3",
    "Twitter": "twitter",
    "Friendster": "friendster",
    "UK2007": "uk-2007-02",
    "Protein2": "protein2",
}

if __name__ == "__main__":
    # cd to current directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    # cd to root directory of the project
    os.chdir('..')
    assert os.path.exists("script/datasets.py"), "Workspace seems not correct."

    # Parse arguments
    parser = argparse.ArgumentParser(description="Generate Datasets of Bubble")
    parser.add_argument("-i", type=str, default="../bubble-datasets.tar.gz",
                help="Compressed datasets directory.")
    parser.add_argument("-w", type=str, default="../webgraph-rs/",
                help="Directory of webgraph-rs code.")
    parser.add_argument("-o", type=str, default="../bubble-datasets/",
                help="Directory of output datasets.")

    args = vars(parser.parse_args())
    input_path = args["i"]
    webgraph_path = args["w"]
    output_path = args["o"]

    assert os.path.exists(input_path), f"Input path {input_path} does not exist."
    assert os.path.exists(webgraph_path), f"Webgraph path {webgraph_path} does not exist."

    # Try to compile webgraph-rs, by makefile
    ret = os.system(f"cd {webgraph_path} && cargo build --release")
    assert ret == 0, "Failed to compile webgraph."
    assert os.path.exists(os.path.join(webgraph_path, "target/release/webgraph")), \
        "Webgraph-rs binary conversion tool not found. Please check the compilation."
    print("Webgraph-rs compiled successfully.")

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Decompress the input file
    print(f"Decompressing {input_path} to {output_path}...")
    ret = os.system(f"tar -xvf {input_path} --directory {output_path}")
    assert ret == 0, f"Failed to decompress {input_path}."
    print("Renamed subdirectory to clarify format.")
    if os.path.exists(os.path.join(output_path, "webgraph")):
        # If webgraph directory already exists, remove it
        shutil.rmtree(os.path.join(output_path, "webgraph"))
    os.rename(os.path.join(output_path, "bubble-datasets"), os.path.join(output_path, "webgraph"))
    
    print("Renamed wrong filename.")
    assert os.path.exists(os.path.join(output_path, "webgraph", "uk2007-02")), \
        "UK2007 dataset directory not found. Please check the decompression."
    os.rename(os.path.join(output_path, "webgraph", "uk2007-02"), 
              os.path.join(output_path, "webgraph", "uk-2007-02"))
    
    webgraph_data_dir = os.path.join(output_path, "webgraph")
    for wname in webgraph_name.values():
        wdir = os.path.join(webgraph_data_dir, wname)
        assert os.path.exists(wdir), f"Webgraph dataset {wname} not found in {webgraph_data_dir}."
        # Check .properties and .graph files
        prop_file = os.path.join(wdir, f"{wname}-hc.properties")
        graph_file = os.path.join(wdir, f"{wname}-hc.graph")
        assert os.path.exists(prop_file), f"Properties file for {wname} not found: {prop_file}"
        assert os.path.exists(graph_file), f"Graph file for {wname} not found: {graph_file}"

    print("Webgraph datasets decompressed successfully.")

    # Convert datasets to binary format (32-bit)
    for name, wname in webgraph_name.items():
        wdir = os.path.join(webgraph_data_dir, wname)
        wpath = os.path.join(wdir, f"{wname}-hc")
        wlabelpath = os.path.join(wdir, f"{wname}-hc.nodes")
        has_labels = os.path.exists(wlabelpath)

        ds = dataset_by_name(name, DATASETS)
        bin_path = os.path.join(output_path, ds.path)

        print(f"Converting {name}")
        print(f"  from: {wpath}.*")
        print(f"  to: {bin_path}")
        print(f"  has labels: {has_labels}")

        os.makedirs(os.path.dirname(bin_path), exist_ok=True)
        converter = os.path.join(webgraph_path, "target/release/webgraph")
        if has_labels:
            ret = os.system(f"{converter} to binary-arcs --labels {wlabelpath} {wpath} > {bin_path}")
        else:
            ret = os.system(f"{converter} to binary-arcs {wpath} > {bin_path}")
        assert ret == 0, f"Failed to convert {name} to binary format."
    print("All datasets converted to binary format successfully.")

    # Generate undirected datasets
    print("Generating undirected datasets...")
    data_pp_dir = "./build/app/data"
    converter_undirected = os.path.join(data_pp_dir, "convert_to_undirect")
    assert os.path.exists(converter_undirected), f"Converter for undirected datasets not found: {converter_undirected}"
    for ds in DATASETS:
        uds = dataset_by_name(ds.name, U_DATASETS)
        assert uds is not None, f"Undirected dataset {ds.name} not found in U_DATASETS."
        
        bin_path = os.path.join(output_path, ds.path)
        ubin_path = os.path.join(output_path, uds.path)
        assert os.path.exists(bin_path), f"Binary file for {ds.name} not found: {bin_path}"
        print(f"Converting {ds.name} to undirected format...")
        print(f"  from: {bin_path}")
        print(f"  to: {ubin_path}")
        ret = os.system(f"{converter_undirected} --short -f {bin_path} -o {ubin_path}")
        assert ret == 0, f"Failed to convert {ds.name} to undirected format."
        print(f"{ds.name} converted to undirected format successfully.")


    # Shuffle the binary files
    print("Shuffling binary files...")
    # Check required executables has been compiled
    
    shuffler_path = os.path.join(data_pp_dir, "shuffle_binary")
    assert os.path.exists(shuffler_path), f"Shuffle excutor not found: {shuffler_path}"

    for ds in DATASETS:
        bin_path = os.path.join(output_path, ds.path)
        assert os.path.exists(bin_path), f"Binary file for {ds.name} not found: {bin_path}"

        # Move unshuffled binary file to another path
        sbin_path = bin_path + ".unshuffled"
        if os.path.exists(sbin_path):
            os.remove(sbin_path)
        os.rename(bin_path, sbin_path)

        print(f"Shuffling {ds.name} binary file: {bin_path}")
        ret = os.system(f"{shuffler_path} --short -f {sbin_path} -o {bin_path}")
        assert ret == 0, f"Failed to shuffle {ds.name} binary file."
        print(f"{ds.name} binary file shuffled successfully.")

    print("All binary files shuffled successfully.")

    # Create symbolic links to datasets directory
    os.symlink(os.path.join(output_path, "data"), "data")
    print("Symbolic link to datasets directory created successfully.")

    # Check datasets files
    print("Checking datasets files...")
    for ds in DATASETS:
        assert os.path.exists(ds.path), f"Dataset file {ds.name} not found: {ds.path}"
    for uds in U_DATASETS:
        assert os.path.exists(uds.path), f"Undirected dataset file {uds.name} not found: {uds.path}"
    print("All dataset files checked successfully.")
    print("You can evaluate Bubble with the generated datasets now.")
