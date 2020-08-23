#!/bin/bash

if [ $# -ne 2 ]; then
    echo "error .. need to username and js"
    exit
fi
USERNAME = $1
JS = $2
if [ ! -f "${JS}" ]; then
    echo "JS 不存在"
    exit 1;
fi
BASE=`cat /etc/default/sequoiadb |grep INSTALL_DIR|awk -F '=' '{print $2}'`/bin
echo -n "please input password:"
read -s password
db="var db = new Sdb(\"localhost\",11810,\"${USERNAME}\",\"${password}\")"
$BASE/sdb "${db}"

