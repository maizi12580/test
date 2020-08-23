#!/bin/bash

# nohup ./spark_run.sh &

BasePath=$(dirname $(readlink -f $0))
echo ${BasePaTH}
run_file=${BasePath}/spark_run.py
python ${run_file}
