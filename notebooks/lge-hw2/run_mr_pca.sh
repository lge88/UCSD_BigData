#! /bin/sh
python mr_pca.py -r emr \
  --emr-job-flow-id j-1DTZ2SXHILM9C \
  -c ~/.mrjob.conf \
  --file s3://weather-analysis/station-to-node-table-yoav.csv \
  --output-dir=s3://weather-analysis/node-descriptor-level-0 --no-output \
  --setup 'export PYTHONPATH=$PYTHONPATH:geo_partition.tar.gz#/' \
  s3://weather-analysis/weather-tminmax/

# python mr_json_protocol_test.py -r emr --emr-job-flow-id j-1DTZ2SXHILM9C \
#   --output-dir s3://weather-analysis/weather-json-protocol-test-2 --no-output \
#   s3://weather-analysis/weather-tminmax/part-00000

  # s3://weather-analysis/weather-tminmax/

# python mr_pca.py weather-tminmax-head-100.txt
