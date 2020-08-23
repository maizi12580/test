#!/usr/bin/python
#coding=utf-8

import datetime
from config.global_config import *
import pexpect
from pexpect import pxssh
import base64


"""
restart_restart_user_name_name = 'sdbadmin'
restart_client_ip = '21.2.2.58'
restart_restart_password = ''
restart_time_out = 3600
"""

def spark_restart():
    """
    stop_thrift = '/data01/spark/sbin/stop-thriftserver.sh'
    stop_all = '/data01/spark/sbin/stop-all.sh'
    start_all = '/data01/spark/sbin/start-all.sh'
    start_thrift = '/data01/spark/sbin/start-thriftserver.sh --master spark://21.2.2.58:7077 --total-executor-cores 32 --executor-memory 4g'
    """
    try:
        stop_thrift = spark_home + 'sbin/stop-thriftserver.sh'
        stop_all = spark_home + 'sbin/stop-all.sh'
        start_all = spark_home + 'sbin/start-all.sh'
        start_thrift = spark_home + 'sbin/start-thriftserver.sh --master spark://' + \
                       hive2_host + ':' + master_port + ' --total-executor-cores ' + \
                       total_exe_cores + ' --executor-memory ' + exe_memory

        s = pxssh.pxssh(timeout=restart_time_out)
        password = base64.decodestring(restart_password)
        s.login(restart_client_ip, restart_user_name, password, original_prompt='[$#>]')
        # command1
        command1 = stop_thrift
        s.sendline(command1)
        s.prompt()
        print s.before
        # command2
        command2 = stop_all
        s.sendline(command2)
        s.prompt()
        print s.before
        # command3
        command3 = start_all
        s.sendline(command3)
        s.prompt()
        print s.before
        # command4
        command4 = start_thrift
        s.sendline(command4)
        s.prompt()
        print s.before
        s.logout()
        #
    except pxssh.ExceptionPxssh, e:
        print 'pxssh failed on login.'
        print str(e)
        #

def main():
    
    print datetime.datetime.now()
    print 'Begin to restart Spark ...'
    spark_restart()
    print 'Success to restart Spark'
    print datetime.datetime.now()


if __name__ == '__main__':
   main()
