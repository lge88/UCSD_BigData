#! /bin/sh

JID='j-31PKW9W4C2YQK'

python mr_weather_concat_tminmax.py -r emr --emr-job-flow-id $JID \
  --setup 'export PYTHONPATH=$PYTHONPATH:weather_data_parser.tar.gz#/' \
  --output-dir s3://lge.bucket/weather/weather-tminmax/ --no-output \
  hdfs:/weather.raw_data/ALL.csv

# python mr_concat_tmin_tmax.py weather-tmin-tmax-tweak-10.csv
# python mr_concat_tmin_tmax.py s3://lge-bucket/weather-data/weather-9424-of-9358395.csv
