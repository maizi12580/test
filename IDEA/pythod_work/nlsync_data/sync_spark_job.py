#!/usr/bin/python
#coding=utf-8


import datetime
from config.global_config import *
import pexpect
from pexpect import pxssh
import base64
from sync_sdb import *
import time
import setproctitle
import socket

class SyncSpark:
    def __init__(self):
        # the next variable get from global table
        self.spark_home = spark_home
        self.hive2_host = hive2_host
        self.hive2_port = hive2_port
        self.master_port = master_port
        self.total_exe_cores = total_exe_cores
        self.exe_memory = exe_memory
        self.restart_client_ip = restart_client_ip
        self.restart_user_name = restart_user_name
        self.restart_password = restart_password
        self.restart_time_out = restart_time_out
        self.config_cs = sync_config_cs
        self.config_cl = sync_config_cl
        self.local_cs = sync_local_cs
        self.local_cl = sync_local_cl
        self.sparkloc_cs = sync_sparkloc_cs
        self.sparkloc_cl = sync_sparkloc_cl
        self.host_name = host_name
        self.server_port = server_port
        self.user_name = user_name
        self.password = password
        self.runsql_num_restart = runsql_num_restart
        self.sparksql_wait = sparksql_wait
        self.spark_restart_wait_time = spark_restart_wait_time
        self.spark_restart_timeout = 1800
        self.spark_err_file = spark_err_file
        # SparkSQL(beeline) command line
        self.spark_cmd = self.spark_home + '/bin/beeline -u jdbc:hive2://' +\
                         self.hive2_host + ':' + self.hive2_port + ' -e "%s"'
        # SequoiaDB connection
        self.scdb = SCSDB(self.host_name, self.server_port, self.user_name, self.password)

    def spark_restart_shell(self):
        """
        stop_thrift = '/data01/spark/sbin/stop-thriftserver.sh'
        stop_all = '/data01/spark/sbin/stop-all.sh'
        start_all = '/data01/spark/sbin/start-all.sh'
        start_thrift = '/data01/spark/sbin/start-thriftserver.sh --master spark://21.2.2.58:7077 --total-executor-cores 32 --executor-memory 4g'
        """
        try:
            stop_thrift = self.spark_home + '/sbin/stop-thriftserver.sh'
            stop_all = self.spark_home + '/sbin/stop-all.sh'
            start_all = self.spark_home + '/sbin/start-all.sh'
            start_thrift = self.spark_home + '/sbin/start-thriftserver.sh --master spark://' + \
                           self.hive2_host + ':' + self.master_port + ' --total-executor-cores ' + \
                           self.total_exe_cores + ' --executor-memory ' + self.exe_memory

            s = pxssh.pxssh(timeout=self.restart_time_out)
            password = base64.decodestring(self.restart_password)
            s.login(self.restart_client_ip, self.restart_user_name, self.password, original_prompt='[$#>]')
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

    def spark_restart(self):
        restart_timeout = self.spark_restart_timeout
        restart_sleep = self.spark_restart_wait_time
        # using SQL: 'show databases' to check spark is restart ok or not
        inspect_sql = 'show databases;'
        total_sleep_time = 0

        print '>>>Begin to restart Spark! Time: %s' % datetime.datetime.now()
        # the first time to restart spark
        self.spark_restart_shell()
        while True:
            (status, output) = commands.getstatusoutput(self.spark_cmd % inspect_sql)
            if 0 == status:
                break
            elif total_sleep_time >= restart_timeout:
                self.spark_restart_shell()
            else:
                total_sleep_time += restart_sleep
                time.sleep(restart_sleep)
                #logger.error('Spark SQL: %s, Failed: return code: %s, error file: %s' % (run_sql, status, error_file))
                print 'Spark Restart is Running! status: %s, output: %s, time: %s' % (status,
                                                                                      output,
                                                                                      datetime.datetime.now())
        print '<<<Success to restart Spark! Time: %s' % datetime.datetime.now()


    def run_spark(self):
        """
        DESC: Spark任务进程
        OPTS:
        """
        condition = {"sprk_st": "wait", "sync_sys": {"$ne": ""}}
        run_condition = {"sprk_st": "running"}
        selector = {}
        orderby = {"init_tm": 1}
        hint = {}
        skip_num = 0L
        return_num = 1L
        # check spark run status
        config_cond = {"is_sync": {"$ne": "false"}}   # here field 'is_sync' make sure table run or not
        success_cond = {"sync_st": "success"}
        failed_cond = {"sync_st": "failed"}
        process_name = 'SYNC|%s|Spark|%s' % (socket.gethostname(),
                                             datetime.datetime.now().strftime("%Y%m%d"))
        setproctitle.setproctitle(process_name)

        sql_run_times = 0
        # before run spark sql, we restart spark first
        if "true" == is_spark_run:
            self.spark_restart()
        while True:
            config_cnt = self.scdb.sync_get_count(self.config_cs, self.config_cl, config_cond)
            success_cnt = self.scdb.sync_get_count(self.local_cs, self.local_cl, success_cond)
            failed_cnt = self.scdb.sync_get_count(self.local_cs, self.local_cl, failed_cond)
            sum_success_failed = success_cnt + failed_cnt
            # when the sum of running success and running failed is equal total config data, we run spark over!
            if config_cnt == sum_success_failed:
                print 'Synchronous data over, total: %s, success: %s, failed: %s' % (sum_success_failed, success_cnt, failed_cnt)
                over_condition = {"sprk_st": {"$ne": "success"}, "sync_sys": {"$ne": ""}}
                over_ruler = {'$set': {'sprk_st': 'failed'}}
                self.scdb.sync_update(self.sparkloc_cs, self.sparkloc_cl, over_ruler, over_condition)
                break

            # here when run some sql, we restart spark(run sql number restart spark)
            if sql_run_times == self.runsql_num_restart:
                self.spark_restart()
                sql_run_times = 0

            # check the spark local table have running sql or not.if have running sql, spark process need to wait
            records = self.scdb.sync_query(self.sparkloc_cs, self.sparkloc_cl, run_condition, selector, orderby, hint, skip_num, return_num)
            if 0 != len(records):
                time.sleep(self.sparksql_wait)
                continue

            # 1. 从队列nlsync.sparkloc中取出需要的SQL
            records = self.scdb.sync_query(self.sparkloc_cs, self.sparkloc_cl, condition, selector, orderby, hint, skip_num, return_num)
            if 0 == len(records):
                time.sleep(self.sparksql_wait)
                continue
            else:
                run_sql = records[0]['run_sql']
                tbl_name = records[0]['tbl_name']
                sync_dt = records[0]['sync_dt']

            # 2. 将此SQL的状态更改为running
            ruler = {'$set': {'sprk_st': 'running'}}
            matcher = {'run_sql': run_sql, 'tbl_name': tbl_name, 'sync_dt': sync_dt}
            self.scdb.sync_update(self.sparkloc_cs, self.sparkloc_cl, ruler, matcher)

            # 3. 使用beeline 跑 SPARK SQL
            begin_tm = str(datetime.datetime.now())
            (status, output) = commands.getstatusoutput(self.spark_cmd % run_sql)
            sql_run_times += 1
            end_tm = str(datetime.datetime.now())

            # 4. Check Spark SQL run success or failed
            if 0 == status:
                ruler = {'$set': {'sprk_st': 'success', 'bg_tm': begin_tm, 'ed_tm': end_tm}}
                matcher = {'run_sql': run_sql, 'tbl_name': tbl_name, 'sync_dt': sync_dt}
                try:
                    self.scdb.sync_update(self.sparkloc_cs, self.sparkloc_cl, ruler, matcher)
                except (SDBBaseError, SDBTypeError), e:
                    raise
            else:
                #logger.error('Spark SQL: %s, Failed: return code: %s, output: %s' % (run_sql, status, output))
                print 'Spark SQL: %s, Failed: return code: %s, output: %s' % (run_sql, status, output)

                ruler = {'$set': {'sprk_st': 'failed', 'bg_tm': begin_tm, 'ed_tm': end_tm}}
                matcher = {'run_sql': run_sql, 'tbl_name': tbl_name, 'sync_dt': sync_dt}
                try:
                    self.scdb.sync_update(self.sparkloc_cs, self.sparkloc_cl, ruler, matcher)
                    #logger.info('Table: %s.%s Spark SQL: %s run failed, Info: %s' % (cs_name, cl_name, run_sql, matcher))
                    print 'Table: %s.%s Spark SQL: %s run failed, Info: %s' % (sync_sparkloc_cs, sync_sparkloc_cl, run_sql, matcher)
                except (SDBBaseError, SDBTypeError), e:
                    #logger.error('table: ' + cs_name + '.' + cl_name +
                    #             'update ' + ruler + ' failed! error info: ' +
                    #             pysequoiadb.error + ', error code: ' + e.code)
                    raise
        return

def main():

    print '[TIME: %s]Begin to Run Spark!' % datetime.datetime.now()
    sync_spark = SyncSpark()
    sync_spark.run_spark()
    print '[TIME: %s]Success to Run Spark!' % datetime.datetime.now()


if __name__ == '__main__':
   main()
