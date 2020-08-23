#!/bin/bash

export JAVA_HOME=/usr/local/src/Java/jdk1.8.0_92

if [ -f /home/db2inst1/sqllib/db2profile ]; then
    . /home/db2inst1/sqllib/db2profile
fi

LocalPath=$(dirname $(readlink -f $0))
# Run Synchonouse Program
echo ">>>>>`date` -- Begin to Run Sync Meta Data"
echo "${LocalPath}"
python ${LocalPath}/sync_meta_data.py
RET1=${?}
echo "<<<<<`date` -- Complete to Run Sync Meta Data"

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

