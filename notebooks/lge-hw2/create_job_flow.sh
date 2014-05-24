#! /bin/sh
# elastic-mapreduce -c ~/.ssh/aws-root-credentials.json \
#   --create --alive --name mr-weather \
#   --num-instances 6 --slave-instance-type c1.xlarge

mrjob create-job-flow -c ./mr_bootstrap.conf
