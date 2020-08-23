#!/bin/bash

export JAVA_HOME=/usr/local/src/Java/jdk1.8.0_92

LocalPath=$(dirname $(readlink -f $0))
echo "${LocalPath}"
# Run Synchonouse Program
echo ">>>>>`date` -- Begin to Run Sync Prefetch ETL file"
echo "${LocalPath}"
python ${LocalPath}/prefetch_file/prefetch_etl_file.py
RET1=${?}
echo "<<<<<`date` -- Complete to Run Sync Prefetch ETL file"

# echo 0
if [[ ${RET1} -eq 0 ]] ; then
   echo 0
   echo 0
   echo 0
   echo 0
   echo 0
else
   echo 1
   echo 1
   echo 1
   echo 1
   echo 1
fi

