#! /bin/sh
# JOB_FLOW_ID='j-34Q4HR41T9BWU'

# python mr_get_sample.py -r emr --emr-job-flow-id ${JOB_FLOW_ID} \
#   -c ~/.mrjob.iam.conf \
#   --probability=0.01 \
#   --output-dir=s3://lge.bucket/weather/weather-1-of-100.csv/ \
#   --no-output \
#   hdfs:/weather.raw_data/ALL.csv

#! /bin/sh
JOB_FLOW_ID='j-31PKW9W4C2YQK'

python mr_get_sample.py -r emr --emr-job-flow-id ${JOB_FLOW_ID} \
  -c ~/.mrjob.iam.conf \
  --probability=0.01 \
  --output-dir=s3://lge.bucket/weather/weather-tminmax-1-of-100/ \
  --no-output \
  s3://lge.bucket/weather/weather-tminmax/
