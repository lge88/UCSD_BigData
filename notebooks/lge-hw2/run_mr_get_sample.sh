#! /bin/sh
JOB_FLOW_ID='j-XF54NKBXHHP0'

MODE=$1
if [[ $MODE == '' ]]; then
  MODE=local
fi

if [[ $MODE == 'emr' ]]; then
  python mr_get_sample.py -r emr \
    --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.conf \
    --probability=$2 \
    --output-dir=$4 --no-output $3
else
  python mr_get_sample.py --probability=$2 $3
fi
