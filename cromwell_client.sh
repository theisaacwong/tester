#!/usr/bin/env bash

GOOGLE_TOKEN=$(gcloud auth print-access-token)
CROMWELL_URL=https://cromwell-pharma5.gotc-prod.broadinstitute.org/api/workflows/v1


KEYS_TO_EXCLUDE="?excludeKey=executionEvents&excludeKey=runtimeAttributes&excludeKey=callCaching&excludeKey=inputs&excludeKey=commandLine&excludeKey=jes"

function do_post() {
  curl --silent -X POST -H "Authorization: Bearer ${GOOGLE_TOKEN}" "$@"
}

function do_get() {
  curl --silent -X GET -H "Authorization: Bearer ${GOOGLE_TOKEN}" "$@"
}

function do_submit() {
  do_post \
    -F workflowSource=@GenotypeAndFilter.AS.wdl \
    -F workflowInputs=@GenotypeAndFilter.AS.json  \
    -F workflowOptions=@pharma5.options.json  \
    ${CROMWELL_URL} 2>/dev/null | jq .
}

function do_metadata() {
  do_get ${CROMWELL_URL}/${1}/metadata${KEYS_TO_EXCLUDE}
}

function do_full_metadata() {
  do_get ${CROMWELL_URL}/${1}/metadata?expandSubWorkflows=true | jq .
}

function do_status() {
  do_get ${CROMWELL_URL}/${1}/status | jq .status
}

function do_abort() {
  do_post ${CROMWELL_URL}/${1}/abort | jq .
}

if [[ $1 == "submit" ]]; then
  do_submit $2
elif [[ $1 == "metadata" ]]; then
  do_metadata $2
elif [[ $1 == "full-metadata" ]]; then
  do_full_metadata $2
elif [[ $1 == "status" ]]; then
  do_status $2
elif [[ $1 == "abort" ]]; then
  do_abort $2
fi