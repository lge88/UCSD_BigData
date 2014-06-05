#! /bin/sh
JOB_FLOW_ID='j-2MX6SRUZWX3O'

python mr_get_sample.py -r emr --emr-job-flow-id ${JOB_FLOW_ID} \
  -c ~/.mrjob.iam.conf \
  --probability=0.0001 \
  --output-dir=s3://lge-bucket/weather-data/weather-1-of-10000.csv/ \
  --no-output \
  s3://lge-bucket/weather-data/weather.csv
