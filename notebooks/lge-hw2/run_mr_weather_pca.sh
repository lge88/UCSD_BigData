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
  python mr_weather_pca.py -r emr \
    --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.conf \
    --explained-variance-ratio-threshold 0.95 \
    --station-to-node data/station-to-node-table-yoav.csv \
    --setup 'export PYTHONPATH=$PYTHONPATH:geo_partition.tar.gz#/' \
    --output-dir=s3://lge.bucket/weather/node-descriptor-k-n-dl/ --no-output \
    s3://lge.bucket/weather/weather-tminmax/
elif [[ $MODE == 'emr-test' ]]; then
  python mr_weather_pca.py -r emr \
    --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.conf \
    --file s3://weather-analysis/station-to-node-table-yoav.csv \
    --setup 'export PYTHONPATH=$PYTHONPATH:geo_partition.tar.gz#/' \
    --output-dir=s3://weather-analysis/node-descriptor-k-n-dl --no-output \
    data/weather-tminmax-head-100.txt
else
  python mr_weather_pca.py \
    --reduced-dimension 5 \
    --explained-variance-ratio-threshold 0.99 \
    --store-mu \
    --store-eigen-vectors \
    --station-to-node data/station-to-node-table-yoav.csv \
    data/weather-tminmax-head-100.txt
fi
