#! /bin/sh
python mr_concat_tmin_tmax.py -r emr --emr-job-flow-id j-32917857IT6TB \
  --setup 'export PYTHONPATH=$PYTHONPATH:weather_data_parser.tar.gz#/' \
  --output-dir s3://weather-analysis/weather-tminmax --no-output \
  s3://lge-bucket/weather-data/weather.csv

# python mr_concat_tmin_tmax.py weather-tmin-tmax-tweak-10.csv
# python mr_concat_tmin_tmax.py s3://lge-bucket/weather-data/weather-9424-of-9358395.csv
