#!/usr/bin/python
#coding=utf-8
"""
This script config is used to run by global config
This config have next items:
    1. SQL Command Line
    2. Multiprocess Information
    3. Sync Record Tables
    4. SequoiaDB Connection
"""

"""
Name: SQL Command Line
Desc: sparksql_cmd: specify spark command line for running spark sql
      postgresql_cmd: specify postgres command line for running postgres sql
"""
is_spark_run = "false"
sparksql_cmd = '/data01/spark/bin/beeline -u jdbc:hive2://21.5.18.86:10000 -e "%s"'
postgresql_cmd = '/opt/SequoiaSQL/bin/psql -h 21.5.18.86 -p 5432 -w  foo -c "%s"'



"""
Name: Multiprocess Information
Desc: multi_spark: the host run spark process in multiprocess
      multi_hosts: the host run import data process(normal process) in multiprocess
      multi_thread_num: the thread number run import data process(normal process) in multiprocess
"""
sync_master = {'HostName': 'T-DSJ-HISDB01', 'RunShell': 'sh /home/sequoiadb/synchronous_data/nlsync_data/nlsync.sh'}
#sync_hosts = 'T-DSJ-HISDB01:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/,T-DSJ-HISDB02:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/,T-DSJ-HISDB03:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/'
#sync_hosts = 'T-DSJ-HISDB01:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/,T-DSJ-HISDB03:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/'
sync_hosts = 'T-DSJ-HISDB01:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/'
#sync_hosts = 'T-DSJ-HISDB01:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/,T-DSJ-HISDB04:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/,T-DSJ-HISDB07:sdbadmin::/home/sequoiadb/synchronous_data/nlsync_data/'
sync_thread_num = 10
#sync_thread_num = 20
#sync_thread_num = 3
check_rcwait_time = 60
sync_retry_times = 3
transcode_paths = ['/data01/etldata/odsdata/transcode',
                   '/data02/etldata/odsdata/transcode',
                   '/data03/etldata/odsdata/transcode']
sync_bakfail_file = '/data03/etldata/odsdata/transcode/sync_failbak/backup.fail'
remote_start_sleep = 60
threshold = 2000000000L


"""
Name: Spark Information
Desc: Spark for Run SQL
      1
"""
runsql_num_restart = 10
spark_home = '/data01/spark'
hive2_host = '21.5.18.86'
hive2_port = '10000'
sparksql_wait = 5
spark_restart_wait_time = 60
spark_err_file = '/sdbdata/data03/etldata/odsdata/transcode/spark_backjobs.err'

master_port = '7077'
#total_exe_cores = '160'
total_exe_cores = '32'
#exe_memory = '64g'
exe_memory = '4g'
#total_exe_cores = '64'
#exe_memory = '16g'
restart_user_name = 'sdbadmin'
restart_client_ip = '21.5.18.86'
restart_password = 'a2ZwdE5MU0RCMjAxNyE='
restart_time_out = 3600


"""
Name: Sync Record Tables
Desc: the tables record the result of the synchronous data
      1. table <mdm.metatbl> record meta data infomation about source data structure in SDB
      2. table <sync.config> record configure infomation when data synchronize to SDB
      3. table <sync.local> record running infomation when data synchronize to SDB
      4. table <sync.local> record running infomation when data synchronize to SDB
      5. table <sync.local> record running infomation when data synchronize to SDB
      6. table <sync.local> record running infomation when data synchronize to SDB
      7. table <sync.log> record running infomation when data synchronize to SDB
"""
# mdm.metatbl
mdm_metatbl_cs = 'mdm'
mdm_metatbl_cl = 'metatbl'
# mdm.metahis
mdm_metahis_cs = 'mdm'
mdm_metahis_cl = 'metahis'
# sync.config
sync_config_cs = 'nlsync'
sync_config_cl = 'config'
# sync.local
sync_local_cs = 'nlsync'
sync_local_cl = 'local'
# sync.history
sync_history_cs = 'nlsync'
sync_history_cl = 'history'
# sync.sparkloc
sync_sparkloc_cs = 'nlsync'
sync_sparkloc_cl = 'sparkloc'
# sync.sparkhis
sync_sparkhis_cs = 'nlsync'
sync_sparkhis_cl = 'sparkhis'
# sync.log
sync_log_cs = 'nlsync'
sync_log_cl = 'log'



"""
Name: SequoiaDB Connection
Desc: host_name: the host name connect to SDB
      server_port: the server port connect to SDB
      user_name: the user name connect to SDB
      password: the password connect to SDB
"""
host_name = 'localhost'
hosts = 'T-DSJ-HISDB01:31810,T-DSJ-HISDB02:31810'
"""
server_port = 11810
user_name = 'nlsdbapp'
password = 'a2ZwdE5MU0RCMjAxNw=='
sys_user = 'sdbadmin'
sys_passwd = 'a2ZwdE5MU0RCMjAxNyE='
"""
server_port = 31810
user_name = 'sdbapp'
password = 'a2ZwdFNEQjIwMTY=\n'
sys_user = 'sdbadmin'
sys_passwd = 'a2ZwdFNEQjIwMTYh\n'

"""
Name: SequoiaDB Import Upsert
Desc: host_name: the host name connect to SDB
      server_port: the server port connect to SDB
"""
upsert_home = '/home/sequoiadb/synchronous_data/nlsync_data/sdbupsert/'
upsert_jar = 'sdbupsert.jar'
upsert_property = 'sdb.properties'
upsert_logdir = '/sdbdata/data12'
#logfile_dir= '/sdbdata/data01/sequoiadb/sync_log.log'
logfile_dir= '/data01/sequoiadb/sync_log.log'

"""
Name: Clear File
Desc:
"""
#backup_dir = '/sdbdata/data11/odsdata'
#backup_num = '0'
sync_data_num = '2'
store_data_dirs = ['/data01/etldata/odsdata/']
delta_day_clear = '60'

"""
Name: Synchronous Meta Data From ODS
"""
db2_database = "ODS_DB"
db2_username = "hqpuser"
db2_password = "Z2ZnZGIxMjMh"
db2_bin_path = "/opt/ibm/db2/V11.1/bin/"
ods_schema = ["OTL"]
#ods_schema = ["ECF"]

# collection_space name                                                       
collection_spaces = ["CBE","SMC", "WWCS_HIST" ,"IBS", "EBS", "ZFS", "SWT", "ECF"]
# retry times for table structure return lines is not equal to return numbers
retry_times = 3
# save meta history time(month). -1: don't remove data
# if greater than this time, we remove meta history data.
metahis_save_time = -1
# preftech ETL file process num
#preftech_process_num = 3 
prefetch_process_num = 1

prefetch_one_day = "true"

# synchonous meta data thread number
sync_meta_thread_num = 10


# 备份功能的配置

# 备份表的列表
#bak_tbl_list = ["test.config","test.metatbl"] 
# 备份文件存放路径
#bakfile_path = "/home/sequoiadb/qzq/backup/"
# 最多保存的备份文件数量（对同一张表）
#max_bakfiles = 3
# 每个备份文件的最大大小（单位M）
#max_bakfile_size = 100



db2_sdb_mapping = { 
    "CHARACTER":"string",
    "CHAR":"string",
    "VARCHAR":"string",
    "LONG VARCHAR":"string",
    "BIGINT":"int",
    "INT":"int",
    "SMALLINT":"int",
    "REAL":"double",
    "DOUBLE":"double",
    "TIMESTAMP":"string",
    "DATE":"string",
    "TIME":"string",
    "CLOB":"string",
    "DECIMAL":"decimal|int|long|double"
}



# 数据同步运行最大时长，单位:小时
#sync_total_hours = 0.5
# 如果存在失败的，重拉等待时间，单位:分钟
#restart_wait_minutes = 2
bak_tbl_list = ["nlsync.config","mdm.metatbl"]
bakfile_path = "/home/sequoiadb/synchronous_data/nlsync_data/config/tbl_backup/"
max_bakfiles = 3
max_bakfile_size = 256
sync_total_hours = 2
restart_wait_minutes = 5
