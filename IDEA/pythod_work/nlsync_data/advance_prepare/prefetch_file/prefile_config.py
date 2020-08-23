#!/usr/bin/python
#coding=utf-8


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
# mdm.metahis
mdm_metahis_cs = 'mdm'
mdm_metahis_cl = 'metahis'
# sync.config
sync_config_cs = 'nlsync'
sync_config_cl = 'config'
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
hosts = 'a-hqp-nlsdb01:11810,a-hqp-nlsdb02:11810,a-hqp-nlsdb03:11810,a-hqp-nlsdb04:11810,a-hqp-nlsdb05:11810,a-hqp-nlsdb06:11810,a-hqp-nlsdb07:11810,a-hqp-nlsdb08:11810,a-hqp-nlsdb09:11810,a-hqp-nlsdb10:11810,a-hqp-nlsdb11:11810,a-hqp-nlsdb12:11810,a-hqp-nlsdb13:11810,a-hqp-nlsdb14:11810,a-hqp-nlsdb15:11810,a-hqp-nlsdb16:11810,a-hqp-nlsdb17:11810,a-hqp-nlsdb18:11810,a-hqp-nlsdb19:11810,a-hqp-nlsdb20:11810,a-hqp-nlsdb21:11810,a-hqp-nlsdb22:11810,a-hqp-nlsdb23:11810,a-hqp-nlsdb24:11810,a-hqp-nlsdb25:11810,a-hqp-nlsdb26:11810,a-hqp-nlsdb27:11810,a-hqp-nlsdb28:11810,a-hqp-nlsdb29:11810,a-hqp-nlsdb30:11810'
server_port = 11810
user_name = 'nlsdbapp'
password = 'a2ZwdE5MU0RCMjAxNw=='
sys_user = 'sdbadmin'
sys_passwd = 'a2ZwdE5MU0RCMjAxNyE='

"""
Name: SequoiaDB Import Upsert
Desc: host_name: the host name connect to SDB
      server_port: the server port connect to SDB
"""
#logfile_dir= '/sdbdata/data01/sequoiadb/sync_log.log'
#logfile_dir= '/data01/wst/data/sync_log.log'
logfile_dir= '/sdbdata/data01/sequoiadb/sync_log.log'

# retry times for table structure return lines is not equal to return numbers
retry_times = 3
# preftech ETL file process num
#prefetch_process_num = 3 
prefetch_process_num = 1
prefetch_one_day = "true"


