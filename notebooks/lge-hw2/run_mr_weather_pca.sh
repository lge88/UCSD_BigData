#! /bin/sh
JOB_FLOW_ID='j-XF54NKBXHHP0'

MODE=$1
if [[ $MODE == '' ]]; then
  MODE=local
fi

if [[ $MODE == 'emr' ]]; then
  python mr_pca_desc_len_only.py -r emr \
    --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.conf \
    --explained-variance-ratio-threshold 0.99 \
    --station-to-node s3://weather-analysis/station-to-node-table-yoav.csv \
    --setup 'export PYTHONPATH=$PYTHONPATH:geo_partition.tar.gz#/' \
    --output-dir=s3://weather-analysis/node-descriptor-k-n-dl-1-of-2 --no-output \
    s3://weather-analysis/weather-tminmax-1-of-2/
elif [[ $MODE == 'emr-test' ]]; then
  python mr_pca_desc_len_only.py -r emr \
    --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.conf \
    --file s3://weather-analysis/station-to-node-table-yoav.csv \
    --setup 'export PYTHONPATH=$PYTHONPATH:geo_partition.tar.gz#/' \
    --output-dir=s3://weather-analysis/node-descriptor-k-n-dl --no-output \
    weather-tminmax-head-100.txt
else
  python mr_pca_desc_len_only.py \
    --reduced-dimension 5 \
    --explained-variance-ratio-threshold 0.99 \
    --store-mu \
    --store-eigen-vectors \
    --station-to-node data/station-to-node-table-yoav.csv \
    weather-tminmax-head-100.txt
fi
