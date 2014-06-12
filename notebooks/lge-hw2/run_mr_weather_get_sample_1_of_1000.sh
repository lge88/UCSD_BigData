#! /bin/sh
JOB_FLOW_ID='j-31PKW9W4C2YQK'

python mr_get_sample.py -r emr --emr-job-flow-id ${JOB_FLOW_ID} \
  -c ~/.mrjob.iam.conf \
  --probability=0.001 \
  --output-dir=s3://lge.bucket/weather/weather-tminmax-1-of-1000/ \
  --no-output \
  s3://lge.bucket/weather/weather-tminmax/
