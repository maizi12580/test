#!/usr/bin/python
#coding=utf-8

import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)

import collections
import base64
import commands
import logging
import logging.config
from spark_restart import *
import time

def query_sparksql(cs_name, cl_name, condition={},
                   selector={}, order_by={},
                   hint={}, skip=0L, num_to_return=-1L):
    try:
        db = client(host_name, server_port, user_name, base64.decodestring(password))
        db.set_session_attri({'PreferedInstance': 'M'})
        cs = db.get_collection_space(cs_name)
        cl = cs.get_collection(cl_name)
        cursor = cl.query(condition=condition, selector=selector,
                          order_by=order_by, hint=hint,
                          skip=skip, num_to_return=num_to_return)
        records = []
        while True:
            try:
                record = cursor.next()
                records.append(record)
            except SDBEndOfCursor:
                break
            except SDBBaseError:
                raise
        db.disconnect()
        return records
    except (SDBBaseError, SDBTypeError), e:
        print 'error info: %s, error code: %s' % (pysequoiadb.error, e.code)
        raise

def update_sparksql(cs_name, cl_name, ruler, matcher):
    try:
        db = client(host_name, server_port, user_name, base64.decodestring(password))
        db.set_session_attri({'PreferedInstance': 'M'})
        cs = db.get_collection_space(cs_name)
        cl = cs.get_collection(cl_name)
        cl.update(rule=ruler, condition=matcher)
    except (SDBBaseError, SDBTypeError), e:
        print 'error info: %s, error code: %s' % (pysequoiadb.error, e.code)
        raise

def run_spark():
    """
    DESC: Spark jobs run backstage util
    OPTS:
    """
    # logging config
    log_config = "./config/spark_run_log.conf"
    logging.config.fileConfig(log_config)
    logger = logging.getLogger('spark')

    # initalize variable
    cs_name = nlsync_csname
    cl_name = nlsync_sparkloc_cl
    condition = {'sprk_st': 'wait'}
    selector = {}
    orderby = {'init_tm': 1}
    hint = {}
    skip_num = 0L
    return_num = 1L
    time_wait = sparksql_wait



    sql_run_times = 0
    while True:
        # get run sql from table nlsync.sparkloc
        try:
            records = query_sparksql(cs_name, cl_name, condition=condition, selector=selector,
                                     order_by=orderby, hint=hint, skip=skip_num, num_to_return=return_num)
        except (SDBBaseError, SDBTypeError), e:
            if -23 == e.code:
                logger.warn('table: %s.%s query failed! because of table truncate, error code: %s' %
                            (cs_name, cl_name, e.code))
                records = []
            else:
                logger.error('table: %s.%s query failed! error info: %s, error code: %s' %
                             (cs_name, cl_name, pysequoiadb.error, e.code))
                raise
        # here when run some sql, we restart spark(run sql number restart spark)
        if sql_run_times == runsql_num_restart:
            logger.info('Begin to restart Spark!')
            spark_restart()
            logger.info('Success to restart Spark! Wait 60s')
            # sleep 60
            time.sleep(60)
            sql_run_times = 0

        # run spark sql in nlsync.sparkloc
        if 0 == len(records):
            logger.info('sleep times: %s' % time_wait)
            time.sleep(time_wait)
        else:
            # 1. 从队列中取出需要的SQL
            logger.info('Spark SQL Run: [%s]' % records[0])
            run_sql = records[0]['run_sql']
            tbl_name = records[0]['tbl_name']
            sync_dt = records[0]['sync_dt']


            # 2. 将此SQL的状态更改为running
            ruler = {'$set': {'sprk_st': 'running'}}
            matcher = {'run_sql': run_sql, 'tbl_name': tbl_name, 'sync_dt': sync_dt}
            try:
                update_sparksql(cs_name, cl_name, ruler, matcher)
                logger.info('Table: %s.%s Spark SQL: %s is running, Info: %s' %
                             (cs_name, cl_name, run_sql, matcher))
            except (SDBBaseError, SDBTypeError), e:
                logger.error('table: %s.%s update ruler: %s failed! error info: %s, error code: %s' %
                             (cs_name, cl_name, ruler, pysequoiadb.error, e.code))
                raise

            # 3. 使用beeline 跑 SPARK SQL

            #spark_cmd = spark_home + 'bin/beeline -u jdbc:hive2://' +\
            #            hive2_host + ':' + hive2_port + ' -e "%s"'
            spark_cmd = spark_home + 'bin/spark-sql -e "%s"'
            # before run sql, we need to check the spark is ok or not
            (status, output) = commands.getstatusoutput(spark_cmd % 'show databases;')
            logger.info('Run Command: ' + (spark_cmd % 'show databases;'))
            if 0 == status:
                logger.info('Spark Status is OK')
            else:
                logger.info('Spark Status is Not OK, status: %s! Here We restart it!' % status)
                logger.info('Begin to restart Spark!')
                spark_restart()
                logger.info('Success to restart Spark! Wait 60s')
                # sleep 60
                time.sleep(60)

            logger.info('Run Command: ' + (spark_cmd % run_sql))
            begin_tm = str(datetime.datetime.now())
            (status, output) = commands.getstatusoutput(spark_cmd % run_sql)
            end_tm = str(datetime.datetime.now())

            # 4. Check Spark SQL run success or failed
            if 0 == status:
                ruler = {'$set': {'sprk_st': 'success', 'bg_tm': begin_tm, 'ed_tm': end_tm}}
                matcher = {'run_sql': run_sql, 'tbl_name': tbl_name, 'sync_dt': sync_dt}
                try:
                    update_sparksql(cs_name, cl_name, ruler, matcher)
                    logger.info('Table: %s.%s Spark SQL: %s run success, Info: %s' % (cs_name, cl_name, run_sql, matcher))
                except (SDBBaseError, SDBTypeError), e:
                    logger.error('table: %s.%s update ruler: %s failed! error info: %s, error code: %s' % (cs_name, cl_name, ruler, pysequoiadb.error, e.code))
                    raise

                sql_run_times += 1
            else:
                if 1024 <= len(output):
                    error_file = spark_err_file
                    fp = open(error_file, 'a');
                    logger.error('Spark SQL: %s, Failed: return code: %s, error file: %s' % (run_sql, status, error_file))
                    fp.write("============================================================================================\r\n")
                    fp.write(">>>: %s\r\n" % datetime.datetime.now())
                    fp.write("--------------------------------------------------------------------------------------------\r\n")
                    fp.write(output)
                    fp.write("============================================================================================\r\n")
                    fp.close()
                else:
                    logger.error('Spark SQL: %s, Failed: return code: %s, output: %s' % (run_sql, status, output))

                ruler = {'$set': {'sprk_st': 'failed', 'bg_tm': begin_tm, 'ed_tm': end_tm}}
                matcher = {'run_sql': run_sql, 'tbl_name': tbl_name, 'sync_dt': sync_dt}
                try:
                    update_sparksql(cs_name, cl_name, ruler, matcher)
                    logger.info('Table: %s.%s Spark SQL: %s run failed, Info: %s' % (cs_name, cl_name, run_sql, matcher))
                except (SDBBaseError, SDBTypeError), e:
                    logger.error('table: ' + cs_name + '.' + cl_name +
                                 'update ' + ruler + ' failed! error info: ' +
                                 pysequoiadb.error + ', error code: ' + e.code)
                    raise
                sql_run_times += 1

if __name__ == '__main__':
    run_spark()
