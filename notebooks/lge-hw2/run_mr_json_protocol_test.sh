#! /bin/sh
python mr_json_protocol_test.py -r emr --emr-job-flow-id j-32917857IT6TB \
  --output-dir s3://weather-analysis/weather-json-protocol-test-2 --no-output \
  s3://weather-analysis/weather-tminmax/part-00000

# python  mr_json_protocol_test.py weather-tminmax-head-100.txt
# python mr_concat_tmin_tmax.py s3://lge-bucket/weather-data/weather-9424-of-9358395.csv
