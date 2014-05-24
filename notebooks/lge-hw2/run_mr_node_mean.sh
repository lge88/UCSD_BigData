#! /bin/sh
# python mr_node_mean.py -r emr --emr-job-flow-id j-1LC55Y5RT1C0G \
#   --file s3://weather-analysis/station-to-node-table-yoav.csv
#   --output-dir s3://weather-analysis/weather-node-mean --no-output \
#   s3://weather-analysis/weather-node-mean

python mr_node_mean.py weather-tminmax-head-100.txt
# python mr_concat_tmin_tmax.py s3://lge-bucket/weather-data/weather-9424-of-9358395.csv
