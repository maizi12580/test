#!/bin/bash

#readlink是linux用来找出符号链接所指向的位置,整个语句为读取当前目录
HomePath=$(dirname $(readlink -f $0))

SDB=/opt/sequoiadb/bin/sdb

echo "/************************BEGIN TO SYNC DATA***************************/"

# Run Synchonouse Program date
echo ">>>>>Begin to Run Sync"`date`
echo $HomePath
python ${HomePath}/sync_manage.py
RET1=${?}
echo "<<<<<Complete to Run Sync"`date`

# clean data
echo ">>>>>Begin to Clear Files"`date`
#python ${HomePath}/sync_clear_file.py
echo "<<<<<Complete to Clear Files"`date`


# copy camaon log file to ./tmp folder
mkdir -p ${HomePath}/tmp/sdbshell_log
rm -rf ${HomePath}/tmp/sdbshell_log
mkdir -p ${HomePath}/tmp/sdbshell_log
cp -r /tmp/shell* ${HomePath}/tmp/sdbshell_log
echo "/************************Complete TO SYNC DATA***************************/"

#if [[ ${RET1} -eq 0 ]] && [[ ${RET2} -eq 0 ]]; then
if [[ ${RET1} -eq 0 ]] ; then
   echo 0
   echo 0
   echo 0
   echo 0
   echo 0
else
   echo ${RET2}
   echo ${RET2}
   echo ${RET2}
   echo ${RET2}
   echo ${RET2}
fi

