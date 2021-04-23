#!/bin/sh -ex

command -v shuf >/dev/null 2>&1 || {
    echo "shuf not found, installing coreutils";
    brew install coreutils;
}

SAMPLE_MAP=$1
CLUSTER_NAME=$2
OUTPUT_DIR=$3
CALL_SET_NAME=$4
GOOGLE_PROJECT=$5


CLOUD_MAP=${OUTPUT_DIR}/sample_map
OUTPUT_MT=${OUTPUT_DIR}/${CALL_SET_NAME}.mt

DATETIME=$(date "+%Y%m%d%H%M%S")
TMP_DIR=${OUTPUT_DIR}/tmp_${DATETIME}

gsutil cp ${SAMPLE_MAP} ${CLOUD_MAP}

CLOUDSDK_CORE_PROJECT=${GOOGLE_PROJECT} hailctl dataproc submit \
    --files=${MAP} \
    ${CLUSTER_NAME} \
    ./test_combiner.py \
    ${CLOUD_MAP} ${OUTPUT_MT} \
    ${TMP_DIR}
