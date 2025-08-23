#!/bin/bash
# $1: Dataset file name (binary 64), $2: Vertex count, $3: STDOUT file

RED='\033[0;31m'
NC='\033[0m' # No Color

# Check the number of arguments
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <dataset> <vertex_count> <stdout_file> <ingest_thread>"
    echo "Example: $0 data/friendster.bin 69000000 ./tmp"
    echo -e $RED"File <stdout_file> will be overwritten, please choose carefully."$NC
    exit 1
fi

# Print the command to be executed
set -x

script_file="$0"
dataset="$1"
vertex_count="$2"
stdout_file="$3"
ingest_thread="$4"

# Set pwd to the project root
pushd "$(dirname "$script_file")"/../..

    TMP_OUTPUT_FILE=`realpath $stdout_file`

    DATASET_FILE=`realpath $dataset`
    DATASET_DIR=`dirname $DATASET_FILE`

    # Run the benchmark
    pushd .
        stdbuf -oL -eL ./build/app/benchmarks --b32 -t $ingest_thread -f $DATASET_FILE 2>&1 | tee $TMP_OUTPUT_FILE
    popd # Go back to the project root

popd # Go back to original directory
