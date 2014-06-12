#! /bin/sh
# JOB_FLOW_ID='j-XF54NKBXHHP0'
# JOB_FLOW_ID='j-34Q4HR41T9BWU'
# JOB_FLOW_ID='j-NIGXU2E6YEU3'
JOB_FLOW_ID='j-3SOQM885E1GLH'

MODE=$1
if [[ $MODE == '' ]]; then
  MODE=local
fi

if [[ $MODE == 'emr' ]]; then
  python mr_kstest_merge.py -r emr \
    --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.iam.conf \
    --output-dir=s3://lge.bucket/weather/node-daily-avg-tminmax.csv/ --no-output \
    s3://lge.bucket/weather/node-daily-avg-tminmax/
else
  python mr_node_daily_avg_tminmax.py \
    --station-to-node data/station-to-node-table-yoav.csv \
    data/weather-tminmax-head-100.txt
fi
