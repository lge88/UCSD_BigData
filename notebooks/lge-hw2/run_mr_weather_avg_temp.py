#! /bin/sh

# created by yoavfreund
# JOB_FLOW_ID='j-34Q4HR41T9BWU'
JOB_FLOW_ID='j-NIGXU2E6YEU3'
# JOB_FLOW_ID='j-3SOQM885E1GLH'

# created by me
# JOB_FLOW_ID='j-2TQIUUB12JN2E'

MODE=$1
if [[ $MODE == '' ]]; then
  MODE=local
fi

if [[ $MODE == 'emr' ]]; then
  python mr_weather_avg_temp.py \
    -r emr --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.iam.conf \
    --output-dir s3://lge.bucket/weather/weather-avg-temp-A-b-r-p-e/ \
    --no-output \
    s3://lge.bucket/weather/weather-tminmax/
elif [[ $MODE == 'emr-test-1' ]]; then
  python mr_weather_avg_temp.py \
    -r emr --emr-job-flow-id ${JOB_FLOW_ID} \
    -c ~/.mrjob.iam.conf \
    --output-dir s3://lge.bucket/weather/weather-avg-temp-A-b-r-p-e-1-of-100/ \
    --no-output \
    s3://lge.bucket/weather/weather-tminmax-1-of-100/
else
  python mr_weather_avg_temp.py data/weather-tminmax-head-100.txt
fi
