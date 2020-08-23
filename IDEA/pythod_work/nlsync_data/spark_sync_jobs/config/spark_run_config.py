#!/usr/bin/python
#coding=utf-8
"""
DESC: This python file as a config file for Spark Run SQL
DATE: 2017-04-28
Author: Vector
"""


"""
Name: SequoiaDB Connection
Desc: host_name: the host name connect to SDB
      server_port: the server port connect to SDB
      user_name: the user name connect to SDB
      password: the password connect to SDB
"""
host_name = 'A-DSJ-HISDB04'
server_port = 11810
user_name = 'sdbapp'
password = 'a2ZwdFNEQjIwMTY='


"""
Name: Spark base config
Desc: spark_home: spark home where spark install.such as '/opt/spark/'
      hive2_host: hive2 client ip
      hive2_port: hive2 serivce port
      master_port: the master node service port
      total_exe_cores: total executor cores for spark running
      exe_memory: executor memory for spark running
"""
spark_home = '/data01/spark/'
hive2_host = '21.2.2.58'
hive2_port = '10000'
master_port = '7077'
total_exe_cores = '32'
exe_memory = '4g'


"""
Name: The Config for SparkSQL running
Desc: runsql_num_restart: the number of running sql over and then restart spark cluster
      sparksql_wait: the seconds SparkSQL client wait for SQL from SQL Queue Table
"""
runsql_num_restart = 30
sparksql_wait = 5
spark_err_file = '/data03/etldata/spark_backjobs.err'



"""
Name: Spark Restart INFO
Desc: restart_user_name: the user name when restart spark cluster
      restart_client_ip: the ip/host name when restart spark cluster
      restart_password: the password when restart spark cluster
      restart_time_out: the seconds of time out when restart spark cluster
"""
restart_user_name = 'sdbadmin'
restart_client_ip = '21.2.2.58'
restart_password = 'a2ZwdFNEQjIwMTYh'
restart_time_out = 3600


"""
Name: nlsync Record Tables
Desc: the tables record the result of the nlsynchronous data
      1. table <mdm.metatbl> record meta data infomation about source data structure in SDB
      2. table <nlsync.config> record configure infomation when data nlsynchronize to SDB
      3. table <nlsync.local> record running infomation when data nlsynchronize to SDB
      4. table <nlsync.local> record running infomation when data nlsynchronize to SDB
      5. table <nlsync.local> record running infomation when data nlsynchronize to SDB
      6. table <nlsync.local> record running infomation when data nlsynchronize to SDB
      7. table <nlsync.log> record running infomation when data nlsynchronize to SDB
"""
# meta data manage collection space and collection
mdm_metatbl_cs = 'mdm'
mdm_metatbl_cl = 'metatbl'

# synchoronous collection space name
nlsync_csname = 'nlsync'

# synchoronous collection name
nlsync_config_cl = 'config'        # nlsync.config
nlsync_local_cl = 'local'          # nlsync.local
nlsync_history_cl = 'history'      # nlsync.history
nlsync_sparkloc_cl = 'sparkloc'    # nlsync.sparkloc
nlsync_sparkhis_cl = 'sparkhis'    # nlsync.sparkhis
nlsync_log_cl = 'log'              # nlsync.log





