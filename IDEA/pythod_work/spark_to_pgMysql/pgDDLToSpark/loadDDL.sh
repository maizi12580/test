#!/bin/bash

if [ $# != 1 ]
then
	echo "参数错误"
	exit -1
fi

if [ ! -d $1 ]
then 
	echo "文件夹不存在"
	exit -1;
fi
folder=`echo $1 | cut -d "/" -f 1`
for file in `ls ${folder}`
do
	echo ${file}
#	/sdbdata/data01/spark-2.3.2-bin-hadoop2.6/bin/beeline -u "jdbc:hive2://10.133.204.46:10000" -n 'sdbadmin' -p '!q@w#e$r2019' -f $1/${file}

done
