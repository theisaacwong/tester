#!/bin/sh -ex

SAMPLE_MAP=$1
CLUSTER_NAME=$2
OUTPUT_DIR=$3
CALL_SET_NAME=$4
GOOGLE_PROJECT=$5
GVCF_HEADER_PATH=$6
OVERWRITE=$7

if [ "$OVERWRITE" = true ];
then
  OVERWRITE_CMD="-o"
else
   OVERWRITE_CMD=""
fi

DATETIME=$(date "+%Y%m%d%H%M%S")

CLOUDSDK_CORE_PROJECT=${GOOGLE_PROJECT} hailctl dataproc submit \
  --files=${SAMPLE_MAP} \
  ${CLUSTER_NAME} \
  /test_combiner.py \
  -g ${GVCF_HEADER_PATH} \
  -s ${SAMPLE_MAP} \
  -c ${OUTPUT_DIR}${CALL_SET_NAME}.mt \
  -t ${OUTPUT_DIR}/tmp_${DATETIME} \
  "$OVERWRITE_CMD"
