#!/bin/sh -ex

# $ hailctl dataproc submit --help
# usage: hailctl dataproc submit [-h] [--files FILES] [--pyfiles PYFILES]
#                                [--properties PROPERTIES] [--dry-run]
#                                name script

# Submit a Python script to a running Dataproc cluster. To pass arguments to the
# script being submitted, just list them after the name of the script.

# positional arguments:
#   name                  Cluster name.
#   script                Path to script.

# optional arguments:
#   -h, --help            show this help message and exit
#   --files FILES         Comma-separated list of files to add to the working
#                         directory of the Hail application.
#   --pyfiles PYFILES     Comma-separated list of files (or directories with
#                         python files) to add to the PYTHONPATH.
#   --properties PROPERTIES, -p PROPERTIES
#                         Extra Spark properties to set.
#   --dry-run             Print gcloud dataproc command, but don't run it.


# Prerequisites:
# - Homebrew (https://brew.sh/)

# Install coreutils (because we need shuf)
command -v shuf >/dev/null 2>&1 || {
    echo "shuf not found, installing coreutils";
    brew install coreutils;
}


DATETIME=$(date "+%Y%m%d%H%M%S")
#MAP=450k_sample_map.tsv
MAP=gs://broad-pharma5-ukbb-outputs/450k_sample_map.tsv

# TEST_MAP=${DATETIME}_test_map.tsv
# shuf -n 200 ${MAP} > ${TEST_MAP}
# MAP=${TEST_MAP}


CLOUDSDK_CORE_PROJECT=broad-pharma5 hailctl dataproc submit \
  --files=${MAP} \
  ukbbcallset \
  test_combiner.py --sample_map=${MAP} --output_cloud_path=gs://broad-pharma5-ukbb-outputs/hail_450k_dataproc_${DATETIME}.mt \
  --overwrite_existing --tmp_bucket=gs://broad-pharma5-ukbb-processing/tmp_${DATETIME}
