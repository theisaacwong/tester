#!/bin/sh -ex

GOOGLE_PROJECT=$1
CLUSTER_NAME=$2
INPUT_MATRIX_TABLE=$3
OUTPUT_VCF=$4

CLOUDSDK_CORE_PROJECT=${GOOGLE_PROJECT} hailctl dataproc submit \
    ${CLUSTER_NAME} \
    /extract_fingerprint_sites.py \
    -m ${INPUT_MATRIX_TABLE} -o ${OUTPUT_VCF}
