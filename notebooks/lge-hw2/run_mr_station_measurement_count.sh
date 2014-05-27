#! /bin/sh
JOB_FLOW_ID='j-XF54NKBXHHP0'

MODE=$1
if [[ $MODE == '' ]]; then
  MODE=local
fi

if [[ $MODE == 'emr' ]]; then
  python mr_station_measurement_count.py -r emr \
    --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.conf \
    s3://lge-bucket/weather-data/weather.csv > data/station-measurement-counts.txt
else
  python mr_station_measurement_count.py \
    data/weather-923-of-9358395-shuffled.csv
fi
