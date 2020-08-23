#!/usr/bin/python
#coding=utf-8

import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)
from bson.objectid import ObjectId
from bson.min_key import MinKey
from bson.max_key import MaxKey
import logging
import sys
import os
import logging.handlers
import datetime
#from time import time, ctime
import time
from datetime import timedelta
import datetime
import collections
from sync_sdb import *
import socket
from pexpect import pxssh
import pexpect
import commands
from sync_logger import *
from config.global_config import *
import re
from sync_upsert import *
from sync_orm import *
import base64
from sync_migrate import *
from sync_upsert import *
from sync_os import *
from transcode import *
from sync_create_table import *
from sync_detach_attach import *
from sync_update_meta import *
from sync_get_file import *
from sync_spark import *
from sync_clear import *

class SYNC:
    def __init__(self, tbl_name, hostname='localhost', svcport='11810',
                 username='', password='', connect_hosts='', log_handler=''):
        # connect to SDB
        self.hostname = hostname
        self.svcport = svcport
        self.username = username
        self.password = password
        self.connect_hosts = connect_hosts
        self.scdb = SCSDB(self.hostname, self.svcport, self.username, self.password, self.connect_hosts)
        self.tbl_name = tbl_name
        self.cs_name = self.tbl_name.split('.')[0]
        self.cl_name = self.tbl_name.split('.')[1]
        self.tbl_cond = {'tbl_name': self.tbl_name}
        self.log = log_handler
        self.sync_os = SyncOS(log_handler)
        self.full_pref_cs = "nl999"
        # constanc value
        self.full_type = "full"
        self.append_cus_type = "append_cus"
        self.append_his_type = "append_his"
        self.append_his_update_type = "append_his_update"
        self.append_his_insert_type = "append_his_insert"
        self.append_his_dp_type = "append_his_dp"
        self.append_his_dprepo_type = "append_his_dprepo"
        self.append_his_main_type = "append_his_main"
        self.append_his_sub_type = "append_his_sub"
        # Spark
        self.spark_home = spark_home
        self.hive2_host = hive2_host
        self.hive2_port = hive2_port
        self.spark_cmd = self.spark_home + '/bin/beeline -u jdbc:hive2://' +\
                         self.hive2_host + ':' + self.hive2_port + ' -e "%s"'
        self.hosts = hosts
        # Record table
        self.local_cs = sync_local_cs
        self.local_cl = sync_local_cl
        self.config_cs = sync_config_cs
        self.config_cl = sync_config_cl

    def get_sync_date(self):
        cnf_records = self.scdb.sync_query(sync_config_cs, sync_config_cl, self.tbl_cond)
        if 0 == len(cnf_records):
            print 'the table: %s have not in sync.config when get sync data' % self.tbl_name
            self.log.error('the table: %s have not in sync.config when get sync data' % self.tbl_name)
            raise
        sync_file_arr = list(eval(cnf_records[0]['sync_file']))
        #print 'sync file: %s' % sync_file_arr
        # the delta time how long data synchronize to SDB
        dt_delta = cnf_records[0]['dt_delta']
        # sync.history
        his_selector = {'sync_dt': 1}
        his_orderby = {'sync_dt': -1}
        num_to_return = 1L
        his_records = self.scdb.sync_query(sync_history_cs,
                                           sync_history_cl,
                                           condition=self.tbl_cond,
                                           selector=his_selector,
                                           order_by=his_orderby,
                                           hint={}, skip=0L,
                                           num_to_return=num_to_return)

        delta_dt = timedelta(days=int(dt_delta))
        add_dt = timedelta(days=int(1))          # every day add one day data
        if 0 == len(his_records):
            # auto generate date time
            local_time = datetime.datetime.now() - abs(delta_dt)
            local_date = local_time.strftime("%Y%m%d")
            sync_last_dt = datetime.datetime.strptime(str(local_date), '%Y%m%d')
            sync_local_dt = sync_last_dt
            sync_tomor_dt = sync_local_dt + abs(add_dt)
        else:
            # get sync date time from table sync.history
            sync_last_dt = datetime.datetime.strptime(his_records[0]['sync_dt'], '%Y%m%d')
            sync_local_dt = sync_last_dt + abs(add_dt)
            sync_tomor_dt = sync_local_dt + abs(add_dt)
        date_arr = []
        date_arr.append(sync_local_dt.strftime("%Y%m%d"))
        date_arr.append(sync_tomor_dt.strftime("%Y%m%d"))
        date_arr.append(sync_last_dt.strftime("%Y%m%d"))
        return date_arr

    # get new meta table structure
    def sync_initialize_metatbl(self):
        is_metatbl_ready = False
        flag = 'init_metatbl'
        sync_update_meta = SyncUpdateMeta(self.tbl_name, self.hostname, self.svcport,
                                          self.username, self.password, self.connect_hosts,
                                          self.log)
        # get synchronous config table
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)
        sync_dt = sync_local.get_sync_date()
        sync_update_meta.update_meta_table(sync_dt)
        is_metatbl_ready = True

        # [sync.local] fields: sync_sys, tbl_name, sync_dt, sync_file, getfl_tm
        op_flag = []
        op_flag.append(flag)
        sync_flag = str(op_flag)
        syncloc = collections.OrderedDict()
        syncloc['sync_dt'] = sync_dt
        syncloc['sync_flag'] = sync_flag
        # put synchronize result in table: sync.local
        ruler = collections.OrderedDict()
        ruler['$set'] = syncloc
        matcher = self.tbl_cond
        self.log.info('[TABLE: %s][UPDATE META INFO][BEFORE INFO: %s]' % (self.tbl_name, syncloc))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][UPDATE META INFO][AFTER INFO: %s]' % (self.tbl_name, syncloc))
        # need to get meta update success flag----------------------
        return is_metatbl_ready

    # 获取文件
    def file_get_check(self):
        """
        Function Name: 数据文件获取和检查
        Description:   根据配置表(sync.config)中的'sync_file', 进行文件的判断和操作
        Parameters:
        Notes:         如果文件一直获取失败如何操作？
        """
        is_file_ok = False
        flag = 'get_data'
        getf_wait = 3600
        # sync.config
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name)

        # sync.local
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, self.cs_name, self.cl_name)

        sync_dt = sync_local.get_sync_date()
        self.log.info('table: %s, synchronous date: %s' % (self.tbl_name, sync_dt))
        # the delta time how long data synchronize to SDB
        file_host = sync_config.get_table_sync_host()
        sync_file = sync_config.get_table_sync_file() % sync_dt
        get_mode = sync_config.get_table_sync_file_remote_mode()
        frm_ip = sync_config.get_table_sync_file_remote_host()
        frm_file = sync_config.get_table_sync_file_remote_file() % sync_dt
        frm_user = sync_config.get_table_sync_file_remote_user()
        frm_passwd = sync_config.get_table_sync_file_remote_password()
        rely_tables = sync_config.get_rely_table_list()

        # when table have rely table, we from rely table host get sync file
        if rely_tables is not None:
            for rely_table in rely_tables:
                rely_tbl_cond = {"tbl_name": rely_table}
                rely_locrd = self.scdb.sync_query(self.local_cs, self.local_cl, rely_tbl_cond)
                if rely_locrd[0].get("expt_cmd") is None or '' == rely_locrd[0].get("expt_cmd"):
                    continue

                # sync file host and export file
                sync_expt_host = rely_locrd[0].get("proc_info").split(",")[0]
                sync_expt_file = rely_locrd[0].get("expt_file")
                frm_ip = socket.gethostbyname(sync_expt_host)
                frm_file = sync_expt_file

        # create dir for input file
        self.log.info("get sync file: %s" % sync_file)
        dir_name = self.sync_os.get_dirname(sync_file)
        self.sync_os.make_directory(dir_name)
        self.sync_os.give_highest_authority(sync_file)

        # get file here
        get_file = SyncGetFile(self.log)
        get_file_bftm = datetime.datetime.now()
        # getf_status = get_file.sync_get_file(frm_ip, frm_file, frm_user, sync_file)
        getf_status = get_file.get_file(frm_ip, frm_file, frm_user, sync_file,
                                        frm_passwd)
        get_file_aftm = datetime.datetime.now()
        gfile_delta_tm = get_file_aftm - get_file_bftm
        if getf_status:
            is_file_ok = True

        op_flag = sync_local.get_table_sync_flag_list()
        op_flag.append(flag)
        sync_flag = str(op_flag)
        syncloc = collections.OrderedDict()
        syncloc['sync_dt'] = sync_dt
        syncloc['sync_file'] = sync_file
        syncloc['sync_type'] = sync_local.get_table_sync_type()
        syncloc['sync_flag'] = sync_flag
        syncloc['getfl_tm'] = str(gfile_delta_tm)
        # put synchronize result in table: sync.local
        ruler = collections.OrderedDict()
        ruler['$set'] = syncloc
        matcher = self.tbl_cond
        self.log.info('[TABLE: %s][GET FILE][BEFORE INFO: %s]' % (self.tbl_name, syncloc))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][GET FILE][AFTER INFO: %s]' % (self.tbl_name, syncloc))

        return is_file_ok


    def loop_subtbl(self, modify_name, loop_cnt, delta=1):
        suffix = modify_name[-1]
        cscl_name = modify_name[0:(len(modify_name)-1)]
        range_len = loop_cnt
        suffix_sum = int(suffix) + delta
        if int(suffix_sum) > range_len:
            suffix = suffix_sum%range_len
        else:
            suffix = int(suffix_sum)

        cscl_name = cscl_name + str(suffix)
        return cscl_name


    def create_synctbl(self):
        """
        Function Name: 创建同步的SDB表
        Description: 结合同步数据表的类型(全量表/增量流水/增量非流水/挂载/打平中间表)创建SDB表， 用于数据同步
        Parameters:
        """
        is_ctbl_ok = False
        flag = 'create_table'
        repo_ret = None
        dst_sub_tbl = None

        # get synchronous config table
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)

        # get synchronize table's config options
        sync_type = sync_config.get_table_sync_type()
        sync_domain = sync_config.get_table_domain()
        sync_sys = sync_config.get_sync_sys()
        # config: sync_new mean running new mode for synchonous data
        sync_new = sync_config.get_table_sync_new()
        # get prod_tbl_vkey and prod_tbl_hkey
        prod_tbl_vkey = sync_config.get_product_vertical_key_map()
        prod_tbl_hkey = sync_config.get_product_horizontal_key_map()
        # destination table and bound
        dest_tbl = sync_config.get_destination_table()
        dest_csname = sync_config.get_destination_csname()
        dest_clname = sync_config.get_destination_clname()
        sub_tbl_bound = sync_config.get_sub_table_bound()

        create_table = SyncCrtTbl(self.hostname, self.svcport, self.username,
                                  self.password, self.connect_hosts, self.log)

        # [1st] create product main table, just for full type
        if self.full_type == sync_type:
            print "create table, sub table bound: %s" % sub_tbl_bound
            if dest_tbl is not None and sub_tbl_bound is not None:
                dst_sub_tbl = create_table.create_full_subtbl(dest_csname, dest_clname,
                                                              sync_new, sync_domain,
                                                              prod_tbl_vkey, prod_tbl_hkey,
                                                              sub_tbl_bound)
            else:
                """here we need to new format for full table
                dst_sub_tbl = create_table.create_full_subtbl(self.cs_name, self.cl_name,
                                                              sync_new, sync_domain,
                                                              prod_tbl_vkey, prod_tbl_hkey)
                """
                dst_sub_tbl = create_table.initialize_full_table(self.cs_name, self.cl_name,
                                                                 sync_domain, prod_tbl_vkey,
                                                                 prod_tbl_hkey,
                                                                 self.full_pref_cs)

        # get ETL key
        etl_date_key = sync_config.get_table_etl_date_key()
        # get synchronize data repository vertical split key (shard_key.vertical_key) and
        # horizontal split key (shard_key.horizontal_key)
        repo_shard_key = sync_config.get_repository_shard_key()
        # get ETL data date time, here [DEAD CODE: ]
        if repo_shard_key is not None and '' != repo_shard_key:
            etl_key = repo_shard_key
        elif etl_date_key is not None and '' != etl_date_key:
            etl_key = etl_date_key
        else:
            if 'WWCS_HIST' == sync_sys:
                etl_key = 'etl_date'
            else:
                etl_key = 'tx_date'
        if repo_shard_key is not None and '' != repo_shard_key:
            print "--------------------> repo sharding key: %s" % repo_shard_key
            repo_tbl_shard = repo_shard_key
            repo_tbl_vkey = repo_tbl_shard[0]['vertical_key']
            repo_tbl_hkey = repo_tbl_shard[1]['horizontal_key']
        else:
            repo_tbl_vkey = collections.OrderedDict()
            repo_tbl_hkey = collections.OrderedDict()
            repo_tbl_vkey[etl_key] = 1
            repo_tbl_hkey['_id'] = 1

        # synchronize date time split type(such as: '%Y%m%d' or '%Y-%m-%d' or '%Y/%m/%d')
        # 0st: product main table
        # 1st: repository main table
        prod_dt_type = sync_config.get_product_vertical_date_format()
        repo_dt_type = sync_config.get_repository_vertical_date_format()
        # the delta time how long data synchronize to SDB
        dt_delta = sync_config.get_delta_date()

        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, self.cs_name, self.cl_name, self.log)
        sync_date = sync_local.get_sync_date()
        if (self.append_cus_type == sync_type or self.append_his_type == sync_type or
            self.append_his_update_type == sync_type or
            self.append_his_insert_type == sync_type or
            self.append_his_main_type == sync_type or
            self.append_his_sub_type == sync_type) and sync_new != "true":
            repo_ret = create_table.create_repository_table(self.cs_name, self.cl_name,
                                                            sync_domain, repo_tbl_vkey,
                                                            repo_tbl_hkey, repo_dt_type,
                                                            dt_delta, sync_date)

        # [sync.local]
        op_flag = sync_local.get_table_sync_flag_list()
        op_flag.append(flag)
        sync_flag = str(op_flag)
        syncloc = collections.OrderedDict()
        syncloc['sync_sys'] = sync_sys
        syncloc['tbl_name'] = self.tbl_name
        syncloc['sync_type'] = sync_type
        if dst_sub_tbl is not None:
            syncloc['sub_tbl'] = dst_sub_tbl[0]
            syncloc['dst_tbl'] = dst_sub_tbl[1]
        if repo_ret is not None:
            syncloc['repo_mtbl'] = repo_ret[0]
            syncloc['repo_stbl'] = repo_ret[1]
        syncloc['sync_flag'] = sync_flag
        syncloc['ctbl_tm'] = str(datetime.datetime.now())
        # put synchronize result in table: sync.local
        ruler = collections.OrderedDict()
        ruler['$set'] = syncloc
        matcher = self.tbl_cond
        self.log.info('[TABLE: %s][CREATE TABLE][BEFORE INFO: %s]' % (self.tbl_name, syncloc))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][CREATE TABLE][AFTER INFO: %s]' % (self.tbl_name, syncloc))

        is_ctbl_ok = True

        return is_ctbl_ok

    def file_parse_transcd(self, transcode_path):
        """
        Function Name: 将文件进行转码
        Description:   将非UTF-8文件转换为UTF-8文件
        Parameters:
        """
        is_transcode_ok = False
        flag = 'transcode_utf8'
        # sync.config
        cnf_records = self.scdb.sync_query(sync_config_cs, sync_config_cl, self.tbl_cond)
        if 0 == len(cnf_records):
            print 'the table: %s have not in sync.config when file parse and transcode' % self.tbl_name
            self.log.error('the table: %s have not in sync.config when file parse and transcode' % self.tbl_name)
            raise
        sync_file_arr = list(eval(cnf_records[0]['sync_file']))
        sync_type = cnf_records[0]['sync_type']
        # sync.local
        loc_records = self.scdb.sync_query(sync_local_cs, sync_local_cl, self.tbl_cond)
        if 0 == len(loc_records):
            print 'the table: %s have not in sync.local when file parse and transcode' % self.tbl_name
            self.log.error('the table: %s have not in sync.local when file parse and transcode' % self.tbl_name)
            raise
        sync_dt = loc_records[0]['sync_dt']
        sync_flag = list(eval(loc_records[0]['sync_flag']))

        # 文件获取所用参数(机器名[IP]/同步文件存放路径/)
        sync_file = sync_file_arr[0]['sync_file'] % sync_dt
        #print "TRANSCODE FILE: %s" % sync_file
        self.log.warn("TRANSCODE FILE: %s" % sync_file)

        transcode_bftm = datetime.datetime.now()
        # replace non-printable character with null character
        nonp_char_replace = cnf_records[0]["nopr_rplc"]
        sync_sys = cnf_records[0]["sync_sys"]
        # wwcs system don't need to transcode file
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)
        encode_formate = sync_config.get_table_sync_file_endcode_format()
        if 'WWCS_HIST' != sync_sys and 'utf-8' != encode_formate:
            # check transcode path. if don't have, then create
            if os.path.isdir(transcode_path) is False \
               and os.path.isfile(transcode_path) is False \
               and '' != transcode_path \
               and os.path.isabs(transcode_path):
                self.sync_os.make_directory(transcode_path)
                #os.path.exists()
            elif os.path.isfile(transcode_path) is True:
                print '==================ERROR=========================='
                print ' directory: <%s> is file!Please Check!' % transcode_path
                self.log.warn(' directory: <%s> is file!Please Check!' % transcode_path)
                print '==================ERROR=========================='
            else:
                print 'Don\'t need to create path. Path: %s' % transcode_path
                self.log.info('Don\'t need to create path. Path: %s' % transcode_path)
            print 'non printable: %s' % nonp_char_replace
            self.log.info('non printable: %s' % nonp_char_replace)
            """ old
            if '' == nonp_char_replace:
                transcode_ret = self.transcode(sync_file, transcode_path=transcode_path, field_check=False)
            else:
                transcode_ret = self.transcode(sync_file, transcode_path=transcode_path, field_check=False, nonp_char_replace=eval(nonp_char_replace))
            """
            ##########################################
            # new type
            meta_info = SyncMeta(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)
            add_fields = sync_config.get_add_fields_list()
            fields_count = meta_info.get_fields_count()
            if add_fields is not None:
                fields_count = meta_info.get_fields_count() - len(add_fields)

            transcode = Transcode(self.log)
            # here need to give string delimiter and field delimiter, then can match more mode
            transcode_ret = transcode.transcode(sync_file, transcode_path,
                                                fields_count)
            ##########################################
            transcode_aftm = datetime.datetime.now()
            trans_file = transcode_ret[0]
            trans_total = transcode_ret[1]
            trans_success = transcode_ret[2]
            if int(trans_success) >= int(trans_total)*2/3:
                is_transcode_ok = True
        else:
            command = 'wc -l %s' % sync_file
            (status, output) = commands.getstatusoutput(command)
            if 0 == status:
                get_line = int(output.split(' ')[0])
                trans_file = sync_file
                trans_total = get_line
                trans_success = get_line
                trans_failed = 0
                is_transcode_ok = True
                transcode_aftm = datetime.datetime.now()
            else:
                # here we do 'wc -l file' failed, but can go to import
                trans_file = sync_file
                trans_total = 0
                trans_success = 0
                trans_failed = 0
                is_transcode_ok = True
                transcode_aftm = datetime.datetime.now()
                print 'Table: %s transcode failed!error code: %s, output: %s' % (self.tbl_name, status, output)
                self.log.info('Table: %s transcode failed!error code: %s, output: %s' % (self.tbl_name, status, output))

        # 更新数据
        sync_flag.append(flag)
        delta_tm = str(transcode_aftm - transcode_bftm)
        loc_rd = collections.OrderedDict()
        loc_rd['tran_file'] = trans_file
        loc_rd['file_num'] = trans_total
        loc_rd['trok_num'] = trans_success
        loc_rd['tran_tm'] = delta_tm
        loc_rd['sync_flag'] = str(sync_flag)
        # when full table data is null, we sync failed
        """
        if 0 == trans_total and 'full' == sync_type:
            print '<The Full Table Cannot be null!>'
            self.log.info('The Full Table: %s data file cannot be 0 line' % self.tbl_name)
            loc_rd['sync_st'] = 'failed'
            is_transcode_ok = False
        """
        ruler = collections.OrderedDict()
        ruler['$set'] = loc_rd
        matcher = self.tbl_cond
        #print "ruler: %s" % loc_rd
        self.log.info('[TABLE: %s][TRANSCODE][BEFORE INFO: %s]' % (self.tbl_name, loc_rd))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][TRANSCODE][AFTER INFO: %s]' % (self.tbl_name, loc_rd))

        return is_transcode_ok

    def data_import(self):
        """
        Function Name: 创建同步的SDB表
        Description:   结合同步数据表的类型(全量表/增量流水/增量非流水/挂载/打平中间表)创建SDB表， 用于数据同步
        Parameters:
        """
        import_ret = []
        is_import_ok = False
        flag = 'import_data'

        dropidx_num = 2         # need to put in config########################
        # get synchronous meta table fields
        sync_meta = SyncMeta(self.hostname, self.svcport, self.username, self.password,
                             self.connect_hosts, self.cs_name, self.cl_name, self.log)

        # get synchronous config table
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)

        # ETL date key [new config argument]
        etl_date_key = sync_config.get_table_etl_date_key()
        # sync_new
        sync_new = sync_config.get_table_sync_new()

        # get sync local table
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, self.cs_name, self.cl_name, self.log)
        sync_type = sync_local.get_table_sync_type()
        sync_dt = sync_local.get_sync_date()
        sync_flag = sync_local.get_table_sync_flag_list()
        import_file = sync_local.get_transcode_file()
        sync_sys = sync_local.get_table_system()
        file_num = sync_local.get_transcode_ok_num()

        # get ETL data date time, here [DEAD CODE: ]
        repo_shard_key = sync_config.get_product_shard_key()
        if repo_shard_key is not None and '' != repo_shard_key:
            repo_shard_key = sync_config.get_repository_vertical_key_map()
            etl_key = repo_shard_key.keys()[0]
        elif etl_date_key is not None and '' != etl_date_key:
            etl_key = etl_date_key
        elif 'WWCS_HIST' == sync_sys:
            etl_key = 'etl_date'
        else:
            etl_key = 'tx_date'

        date_fm = sync_config.get_repository_vertical_date_format()
        sync_dtfm = datetime.datetime.strptime(sync_dt, '%Y%m%d')
        condition = dict()
        condition[etl_key] = sync_dtfm.strftime(date_fm)

        if self.full_type == sync_type:
            import_csname = sync_local.get_destination_cs()
            import_clname = sync_local.get_destination_cl()
            # before import data, we truncate full table
            self.scdb.sync_truncate(import_csname, import_clname)
        elif (self.append_cus_type == sync_type or self.append_his_update_type == sync_type) and 'true' == sync_new:
            # 增量客户表与增量流水表不再使用repo表
            import_csname = sync_local.get_cs_name()
            import_clname = sync_local.get_cl_name()
        elif (self.append_his_type == sync_type or
              self.append_his_insert_type == sync_type) and 'true' == sync_new:
            # 增量客户表与增量流水表不再使用repo表
            import_csname = sync_local.get_cs_name()
            import_clname = sync_local.get_cl_name()

            count = self.scdb.sync_get_count(import_csname, import_clname, condition)
            # if table have today sync data, we need remove first
            if 0 != count and count is not None and count != file_num:
                print 'table: %s, REMOVING condition: %s; REMOVE Number: %s' % (self.tbl_name, condition, count)
                self.log.info('table: %s, REMOVING condition: %s; REMOVE Number: %s' % (self.tbl_name, condition, count))
                self.scdb.sync_remove(import_csname, import_clname, condition)

            #------------------------------Drop Index------------------------------------
            # before import, when data count great than threshold
            all_count = self.scdb.sync_get_count(import_csname, import_clname)
            indexes_objstr = sync_config.get_table_index_field()
            data_dt = sync_local.get_sync_date()
            date_formate = sync_config.get_product_vertical_date_format()
            if all_count >= threshold:
                # the number of sub collection to dropping indexes
                print 'INDEX TABLE NUMBER: %s' % dropidx_num
                self.log.info('Table: %s, threshold: %s, '
                              'create sub index: %s' % (self.tbl_name, threshold,
                                                        indexes_objstr))
                # drop index
                self.scdb.sync_hisub_drpidx(import_csname, import_clname, indexes_objstr,
                                            date_formate, data_dt, dropidx_num)
            #------------------------------Drop Index------------------------------------
        else:
            import_csname = sync_local.get_repository_sub_cs()
            import_clname = sync_local.get_repository_sub_cl()
            count = self.scdb.sync_get_count(import_csname, import_clname, condition)
            # if table have today sync data, we need remove first
            if 0 != count and None != count and count != file_num:
                print 'REMOVING condition: %s; REMOVE Number: %s' % (condition, count)
                self.log.info('Table: %s remove condition: %s '
                              'remove number: %s' % (self.tbl_name, condition, count))
                self.scdb.sync_remove(import_csname, import_clname, condition)

        # get sync_data
        import_hosts = self.scdb.get_coord_address()
        sync_import = SyncMigrate(self.hostname, self.svcport, self.username, self.password,
                                  import_hosts, self.log)

        # 数据导入前的记录数
        sync_type = sync_config.get_table_sync_type()
        dest_table = sync_config.get_destination_table()
        cs_name = sync_config.get_destination_csname()
        cl_name = sync_config.get_destination_clname()
        sub_bound = sync_config.get_sub_table_bound()
        if self.full_type == sync_type and dest_table is not None and \
           sub_bound is not None:
            bef_count = self.scdb.sync_get_count(cs_name, cl_name, condition={})
        else:
            bef_count = self.scdb.sync_get_count(self.cs_name, self.cl_name, condition={})
        import_bftm = datetime.datetime.now()
        # format meta info
        meta_info = sync_meta.get_meta_ret()
        meta_fields = sync_import.formate_fields(meta_info, etl_key, date_fm, sync_dt)
        #self.log.info('fields: %s' % meta_fields)
        #print 'fields: %s' % meta_fields

        # get import command options
        import_cmd_options = sync_config.get_table_import_options_map()
        #print '========>import_cmd_options: %s' % import_cmd_options

        # import record
        if import_cmd_options is None:
            rec_file = sync_import.mig_import(import_csname, import_clname,
                                              import_file, meta_fields)
        else:
            rec_file = sync_import.mig_import(import_csname, import_clname,
                                              import_file, meta_fields, **import_cmd_options)


        #print 'rec_file: %s' % rec_file

        import_aftm = datetime.datetime.now()
        if self.full_type == sync_type and dest_table is not None and \
           sub_bound is not None:
            aft_count = self.scdb.sync_get_count(cs_name, cl_name, condition={})
        else:
            aft_count = self.scdb.sync_get_count(self.cs_name, self.cl_name, condition={})

        ######################################################################################
        #------------------------------Create Index-------------------------------------------
        all_count = aft_count
        data_dt = sync_local.get_sync_date()
        if (self.append_his_type == sync_type or
            self.append_his_insert_type == sync_type) and 'true' == sync_new and \
           all_count >= threshold:
            #indexes_objstr = indexes_objstr
            #date_formate = date_formate
            # the number of sub collection to dropping indexes
            crtidx_num = dropidx_num
            print 'INDEX TABLE NUMBER: %s' % crtidx_num
            self.log.info('Table: %s, threshold: %s, '
                          'create sub index: %s' % (self.tbl_name, threshold,
                                                    indexes_objstr))
            # create index
            self.scdb.sync_hisub_crtidx(import_csname, import_clname, indexes_objstr, date_formate, data_dt, crtidx_num)
        #------------------------------Create Index-------------------------------------------

        delta_tm = str(import_aftm - import_bftm)
        if 'WWCS_HIST' == sync_sys and sync_type == 'full':
            import_num = self.scdb.sync_get_count(import_csname, import_clname)
        else:
            import_num = self.scdb.sync_get_count(import_csname, import_clname, condition=condition)
        self.log.info('table: %s, import cs_name: %s.%s, get import number: %s,'
                      'import condition: %s' % (self.tbl_name, import_csname, import_clname, import_num, condition))

        # udpate local table
        if flag != sync_flag[-1]:
            sync_flag.append(flag)
        else:
            self.log.info('table: %s retry import data' % self.tbl_name)

        ruler_val = {'begin_num': bef_count, 'end_num': aft_count,
                     'iprt_num': import_num, 'iprt_tm': delta_tm, 'sync_flag': str(sync_flag)}
        #print 'ruler_val: %s' % ruler_val
        self.log.info('[TABLE: %s][IMPORT][BEFORE INFO: %s]' % (self.tbl_name, ruler_val))
        sync_local.update_local(ruler_val)
        self.log.info('[TABLE: %s][IMPORT][AFTER INFO: %s]' % (self.tbl_name, ruler_val))

        iprfile_type = import_file.split(".")[1]
        # if file_num greate than  import num + 10(allow to fail 10)
        if rec_file is not None:
            print 'local type: %s, get type: %s' % (self.append_cus_type, sync_type)
            if self.append_cus_type == sync_type or self.append_his_update_type == sync_type:
                self.log.info("import rec file: %s, table type: %s" % (rec_file, sync_type))
                is_import_ok = True
        else:
            print 'WARNNING,Table: %s, file number: %s, import number: %s' % (self.tbl_name, file_num, import_num)
            if int(file_num) == import_num:
                is_import_ok = True
                if int(file_num) != import_num:
                    self.log.error('WARNNING,Table: %s, file number: %s, import number: %s' % (self.tbl_name, file_num, import_num))

                if 'datutf8' == iprfile_type:
                    self.sync_os.remove_file(import_file)
                    self.log.info('remove import file is : %s' % import_file)

        # if import success, we remove import file
        if 'utf8' == import_file.split('.')[1] and is_import_ok is True:
            self.log.info("remove utf8 file: %s" % import_file)
            self.sync_os.remove_file(import_file)

        import_ret.append(is_import_ok)
        import_ret.append(rec_file)

        return import_ret

    def data_upsert(self, upsert_file):
        is_upsert_ok = False
        flag = 'upsert_data'

        # get sync config table
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)
        etl_date_key = sync_config.get_table_etl_date_key()
        sync_sys = sync_config.get_sync_sys()
        rely_tbl = sync_config.get_rely_table_list()
        # get sync local table
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, self.cs_name, self.cl_name, self.log)

        # get ETL data date time
        repo_shard_key = sync_config.get_product_shard_key()
        if repo_shard_key is not None and '' != repo_shard_key:
            repo_shard_key = sync_config.get_repository_vertical_key_map()
            etl_key = repo_shard_key.keys()[0]
        elif etl_date_key is not None and '' != etl_date_key:
            etl_key = etl_date_key
        elif 'WWCS_HIST' == sync_sys:
            etl_key = 'etl_date'
        else:
            etl_key = 'tx_date'

        sync_dt = sync_local.get_sync_date()
        date_fm = sync_config.get_repository_vertical_date_format()
        sync_dtfm = datetime.datetime.strptime(sync_dt, '%Y%m%d')
        condition = dict()
        condition[etl_key] = sync_dtfm.strftime(date_fm)

        dst_cs_name = sync_local.get_cs_name()
        dst_cl_name = sync_local.get_cl_name()
        if sync_config.get_rely_table_list() is not None:
            if sync_config.get_table_import_options_csname() is not None and sync_config.get_table_import_options_clname() is not None:
                dst_cs_name = sync_config.get_table_import_options_csname()
                dst_cl_name = sync_config.get_table_import_options_clname()
        sync_flag = sync_local.get_table_sync_flag_list()
        file_num = sync_local.get_file_num()
        loc_cnt = self.scdb.sync_get_count(dst_cs_name, dst_cl_name, condition=condition)
        # update before time
        upsert_bftm = datetime.datetime.now()
        upsert_rec_file = None
        if loc_cnt != file_num or ( rely_tbl is not None and "" != rely_tbl ):
            sync_upsert = SyncUpsert(dst_cs_name, dst_cl_name, upsert_home,
                                     upsert_jar, upsert_file, self.log)
            # 1.auto generate config file
            upsert_config = sync_upsert.autogen_upsert_config()
            # 2.move rec file
            upsert_file = sync_upsert.move_rec_file()
            # 3.upsert
            upsert_retry_times = 5
            upsert_ret_info = sync_upsert.sync_upsert(upsert_config, upsert_file,
                                                      upsert_retry_times)
            upsert_rec_file = upsert_ret_info[1].get("RunFile")
        aft_count = self.scdb.sync_get_count(dst_cs_name, dst_cl_name, condition={})
        upsert_edtm = datetime.datetime.now()
        import_num = self.scdb.sync_get_count(dst_cs_name, dst_cl_name, condition=condition)
        upsert_count = import_num - sync_local.get_import_num()
        upsert_time = str(upsert_edtm - upsert_bftm)
        print 'upsert contion: %s, imported number: %s' % (upsert_count, sync_local.get_import_num())
        # udpate local table
        if flag != sync_flag[-1]:
            sync_flag.append(flag)
        else:
            self.log.info('table: %s retry import data' % self.tbl_name)

        ruler_val = {'end_num': aft_count, 'iprt_num': import_num, 'upst_num': upsert_count,
                     'upst_tm': upsert_time, 'sync_flag': str(sync_flag)}
        self.log.info('[TABLE: %s][IMPORT][BEFORE INFO: %s]' % (self.tbl_name, ruler_val))
        sync_local.update_local(ruler_val)
        self.log.info('[TABLE: %s][IMPORT][AFTER INFO: %s]' % (self.tbl_name, ruler_val))

        # 4. if import_count == file_num, we remove
        if int(file_num) <= import_num + 10:
            is_upsert_ok = True
            if int(file_num) != import_num:
                print 'WARNNING,Table: %s, file number: %s, import number: %s' % (self.tbl_name, file_num, import_num)
                self.log.warn('WARNNING,Table: %s, file number: %s, import number: %s' % (self.tbl_name, file_num, import_num))
            else:
                self.log.info('Upsert finish, remove rec file: %s.Table: %s, file number: %s, import number: %s' % (upsert_rec_file, self.tbl_name, file_num, import_num))
                #self.sync_os.remove_file(upsert_rec_file)

        return is_upsert_ok

    def data_export(self):
        is_export_ok = False
        flag = 'export_data'

        # get synchronous config table
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)
        export_file = sync_config.get_table_export_options_file()
        export_fields = sync_config.get_table_export_options_fields()
        etl_date_key = sync_config.get_table_etl_date_key()
        etl_date_fmt = sync_config.get_repository_vertical_date_format()
        sync_sys = sync_config.get_sync_sys()

        # get sync local table
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, self.cs_name, self.cl_name, self.log)
        sync_date = sync_local.get_sync_date()
        import_num = sync_local.get_import_num()
        sync_flag = sync_local.get_table_sync_flag_list()

        export_hosts = self.scdb.get_coord_address()
        sync_export = SyncMigrate(self.hostname, self.svcport, self.username,
                                  self.password, export_hosts, self.log)

        # export
        # get ETL data date time
        repo_shard_key = sync_config.get_product_shard_key()
        if repo_shard_key is not None and '' != repo_shard_key:
            repo_shard_key = sync_config.get_repository_vertical_key_map()
            etl_key = repo_shard_key.keys()[0]
        elif etl_date_key is not None and '' != etl_date_key:
            etl_key = etl_date_key
        elif 'WWCS_HIST' == sync_sys:
            etl_key = 'etl_date'
        else:
            etl_key = 'tx_date'
        sync_datetime = datetime.datetime.strptime(sync_date, '%Y%m%d')
        matcher = '{"%s": "%s"}' % (etl_key, sync_datetime.strftime(etl_date_fmt))
        export_bftm = datetime.datetime.now()
        export_abs_file = export_file % sync_date
        print 'export files: %s' % export_file
        # export
        export_cmd_options = sync_config.get_table_export_options_map()
        if export_cmd_options is None:
            sync_export.mig_export(self.cs_name, self.cl_name,
                                   export_abs_file, matcher)
        else:
            sync_export.mig_export(self.cs_name, self.cl_name,
                                   export_abs_file, matcher, **export_cmd_options)

        export_aftm = datetime.datetime.now()
        export_tm = str(export_aftm - export_bftm)

        # get export file number
        export_num = self.sync_os.get_lineno(export_abs_file)
        if export_num == import_num:
            is_export_ok = True
            # touch ok file
            self.sync_os.touch_ok_file(export_abs_file)

        # udpate local table
        if flag != sync_flag[-1]:
            sync_flag.append(flag)
        else:
            self.log.info('table: %s retry import data' % self.tbl_name)

        # give export file highest autority
        ret = self.sync_os.give_highest_authority(export_abs_file)
        self.log.info('[TABLE: %s][EXPORT][CHMOD AUTORITY INFO: %s]' % (self.tbl_name, ret))

        ruler_val = {'expt_num': export_num, 'expt_file': export_abs_file,
                     'expt_tm': export_tm, 'sync_flag': str(sync_flag)}
        self.log.info('[TABLE: %s][IMPORT][BEFORE INFO: %s]' % (self.tbl_name, ruler_val))
        print 'ruler_val: %s' % ruler_val
        sync_local.update_local(ruler_val)
        self.log.info('[TABLE: %s][IMPORT][AFTER INFO: %s]' % (self.tbl_name, ruler_val))

        return is_export_ok

    def get_truncate_tbl(self, spark_sql):
        """
        find truncate table from spark sql. this table always have name xxxxx_repo_di
        """
        truncate_tbl_sql = []
        truncate_flag = '[TRUNCATE]'
        replace_sql = ''
        if -1 != spark_sql.find(truncate_flag):
            replace_sql = spark_sql.replace(truncate_flag, '')
        else:
            return truncate_tbl_sql

        # get begin string index
        print 'REPLACE SQL: %s' % replace_sql
        begin_idx = 0
        begin_flag = ['into', 'table']
        for flag in begin_flag:
            bgidx = replace_sql.find(flag)
            if -1 == bgidx:
                continue
            else:
                begin_idx = bgidx + len(flag)
                break

        # get end string index
        end_idx = 0
        end_flag = 'select'
        edidx = replace_sql.find(end_flag)
        if -1 != edidx:
            end_idx = edidx

        if 0 == begin_idx or 0 == end_idx:
            print 'BEGIN index: %s, END index: %s' % (begin_idx, end_idx)
            print 'Spark SQL string don\'t have string (into/table) or (select)! You\' SQL: %s' % spark_sql
            self.log.info('Spark SQL string don\'t have string (into/table) or (select)! You\' SQL: %s' % spark_sql)
            raise

        # 0: truncate tbl; 1: sql
        print 'BEGIN index: %s, END index: %s' % (begin_idx, end_idx)
        self.log.info('BEGIN index: %s, END index: %s' % (begin_idx, end_idx))
        print 'truncate table: %s' % (replace_sql[begin_idx:end_idx])
        self.log.info('truncate table: %s' % (replace_sql[begin_idx:end_idx]))
        truncate_tbl = replace_sql[begin_idx:end_idx].replace(' ', '')
        print 'truncate table: %s' % (truncate_tbl)
        self.log.info('truncate table: %s' % (truncate_tbl))
        truncate_tbl_sql.append(truncate_tbl)
        truncate_tbl_sql.append(replace_sql)
        
        return truncate_tbl_sql


    def get_flag_tbl(self, spark_sql, flag='[TRUNCATE]'):
        """
        find flag table from spark sql. this table always have name xxxxx_repo_di
        """
        tbl_sql = []
        #flag = '[TRUNCATE]'
        replace_sql = ''
        tmp_flag = flag
        if '[DROPINDEX]' == tmp_flag:
            flag = '[DROPINDEX-'
        if -1 != spark_sql.find(flag):
            if '[DROPINDEX]' == tmp_flag:
                begin_idx = spark_sql.find(flag)
                end_idx = begin_idx + len(flag) + 2
                #print 'begin: %s, end: %s' % (begin_idx, end_idx) 
                flag = spark_sql[begin_idx:end_idx]
                tmp_flag = flag
                print 'get drop index flag: %s' % flag
            replace_sql = spark_sql.replace(flag, '')
        else:
            return tbl_sql

        # get begin string index
        print 'REPLACE SQL: %s' % replace_sql
        begin_idx = 0
        begin_flag = ['into', 'table']
        for flag in begin_flag:
            bgidx = replace_sql.find(flag)
            if -1 == bgidx:
                continue
            else:
                begin_idx = bgidx + len(flag)
                break

        # get end string index
        end_idx = 0
        end_flag = 'select'
        edidx = replace_sql.find(end_flag)
        if -1 != edidx:
            end_idx = edidx

        if 0 == begin_idx or 0 == end_idx:
            print 'BEGIN index: %s, END index: %s' % (begin_idx, end_idx)
            print 'Spark SQL string don\'t have string (into/table) or (select)! You\' SQL: %s' % spark_sql
            raise

        # 0: flag tbl; 1: sql; 2: flag such as [DROPINDEX-2]
        print 'BEGIN index: %s, END index: %s' % (begin_idx, end_idx)
        print 'flag table: %s' % (replace_sql[begin_idx:end_idx])
        flag_tbl = replace_sql[begin_idx:end_idx].replace(' ', '')
        print 'flag table: %s' % (flag_tbl)
        tbl_sql.append(flag_tbl)
        tbl_sql.append(replace_sql)
        tbl_sql.append(tmp_flag)
        
        return tbl_sql




    def data_update(self):
        """
        Function Name: 根据数据表进行数据更新操作
        Description:   (增量流水/增量非流水/打平中间表)在进行数据同步时需要进行数据同步操作
        Parameters:
        """
        pref_sql_key = ''
        is_update_ok = False
        flag = 'update_data'
        time_clock = 0
        wait_time = 10
        warn_time = 7200
        fail_time = 21600
        # sync.config
        cnf_records = self.scdb.sync_query(sync_config_cs, sync_config_cl, self.tbl_cond)
        if 0 == len(cnf_records):
            print 'the table: %s have not in sync.config when update data' % self.tbl_name
            self.log.error('the table: %s have not in sync.config when update data' % self.tbl_name)
            raise
        date_fm = eval(cnf_records[0]['main_dtp'])

        cnf_keys = cnf_records[0].keys()
        sql_keys = []
        for cnf_key in cnf_keys:
            if 'run_sql' == cnf_key[:7]:
                sql_keys.append(cnf_key)

        sql_keys.sort()
        ######################
        # add 20171014:1:36
        if 0 == len(sql_keys):
            return True
        ######################
        sql_begin_tm = datetime.datetime.now()
        #print '[update]sql_keys: %s' % sql_keys
        pref_sql_key = ''
        pref_sql_status = True
        dropidx_sql = None
        sync_type = cnf_records[0]['sync_type']
        sync_flag = []
        for sql_key in sql_keys:
            # get sql
            sql_value = cnf_records[0][sql_key].split('|')
            if 2 != len(sql_value):
                print 'Not Fomatted SQL, like: SparkSQL|insert into x select * from x=x; Real SQL: %s' % sql_value
                self.log.error('Not Fomatted SQL, like: SparkSQL|insert into x select * from x=x; Real SQL: %s' % sql_value)
                raise
            sql_type = sql_value[0]
            sql = sql_value[1]
            print 'sql_type: %s, sql: %s' % (sql_type, sql)

            #append history dp operation
            if 'append_his_dp' == cnf_records[0]['sync_type']:
                dploc_rds = self.scdb.sync_query(sync_local_cs, sync_local_cl, self.tbl_cond)
                sync_date = dploc_rds[0]['sync_dt']
                maindp_fm1 = eval(cnf_records[0]['main_dtp'])[0]
                maindp_fm2 = eval(cnf_records[0]['main_dtp'])[1]
                sync_dtfm = datetime.datetime.strptime(sync_date, '%Y%m%d')
                maindp_1dt = sync_dtfm.strftime(maindp_fm1)
                maindp_2dt = sync_dtfm.strftime(maindp_fm2)
                # add suffix by date, such as repo_cbe.repo_bptfhist2017-03-27
                maindp_flag1 = '[MAINDT-1]'
                maindp_flag2 = '[MAINDT-2]'
                if -1 != sql.find(maindp_flag1):
                    replace_sql1 = sql.replace(maindp_flag1, maindp_1dt)
                    sql = replace_sql1

                if -1 != sql.find(maindp_flag2):
                    replace_sql2 = sql.replace(maindp_flag2, maindp_2dt)
                    sql = replace_sql2

                print 'SQL NO REPLACE: %s' %  sql

                # get check table
                check_re = '\[CHECK-(.*?)-]'
                check_tbls = re.findall(check_re, sql)
                print 'APPEND_HIS_DP: %s check tables: %s' % (self.tbl_name, check_tbls)
                if 0 != len(check_tbls):
                    # check table is run over or not
                    for check_tbl in check_tbls:
                        # replace
                        check_flag = '[CHECK-%s-]' % check_tbl
                        replace_sql = sql.replace(check_flag, '')
                        sql = replace_sql
                      
                        tbl_cond = {'tbl_name': check_tbl}
                        cktime_out = 21600
                        sleep_tm = 30
                        sleep_clk = 0
                        while True:
                            print 'APPEND_HIS_DP Table: %s table cond: %s' % (check_tbl, tbl_cond)
                            check_rd = self.scdb.sync_query(sync_local_cs, sync_local_cl, tbl_cond) 
                            if 0 != len(check_rd):
                                if 'success' == check_rd[0]['sync_st']:
                                    break
                                else:
                                    print 'Here We Wait table: %s 30 seconds, status: %s' % (check_tbl, check_rd[0]['sync_st'])
                                    self.log.info('Here We Wait table: %s 30 seconds, status: %s' % (check_tbl, check_rd[0]['sync_st']))
                                    time.sleep(sleep_tm)
                                    sleep_clk += sleep_tm
                                    if sleep_clk >= cktime_out:
                                        print 'ERROR, We have Wait table: %s 21600 seconds, status: %s' % (check_tbl, check_rd[0]['sync_st'])
                                        self.log.info('ERROR, We have Wait table: %s 21600 seconds, status: %s' % (check_tbl, check_rd[0]['sync_st']))
                                        return is_update_ok
                                       
                            else:
                                time.sleep(sleep_tm)
                                sleep_clk += sleep_tm
                                if sleep_clk >= cktime_out:
                                    print 'ERROR, We have Wait table: %s 21600 seconds' % (check_tbl)
                                    self.log.info('ERROR, We have Wait table: %s 21600 seconds' % (check_tbl))
                                    return is_update_ok

                        print 'Table: %s was checked over' % check_tbl 

                # get truncate table and truncate
                truncate_re = '\[TRUNCATE-(.*?)-]'
                truncate_tbls = re.findall(truncate_re, sql)
                if 0 != len(truncate_tbls):
                    for truncate_tbl in truncate_tbls:
                        # repalce
                        truncate_flag = '[TRUNCATE-%s-]' % truncate_tbl
                        replace_sql = sql.replace(truncate_flag, '')
                        sql = replace_sql
                        # truncate cs and cl
                        trun_cs = truncate_tbl.split('.')[0]
                        trun_cl = truncate_tbl.split('.')[1]
                        # when truncate table equal product table, we interrupt
                        if self.tbl_name != truncate_tbl:
                            self.scdb.sync_truncate(trun_cs, trun_cl)
                        else:
                            print 'CRITICAL, TRUNCATE TABLE: %s is equal PRODUCT TABLE: %s; run sql:%s' % (truncate_tbl[0], self.tbl_name, sql)
                            self.log.error('CRITICAL, TRUNCATE TABLE: %s is equal PRODUCT TABLE: %s; run sql:%s' % (truncate_tbl[0], self.tbl_name, sql))
                            raise

                    print 'Table: %s, dp end sql: %s' % (self.tbl_name, sql)
                    self.log.info('Table: %s, dp end sql: %s' % (self.tbl_name, sql))

            #[DROPINDEX-2] drop index in append_his
            tmp_sql = sql
            dropidx_tbl = self.get_flag_tbl(sql, flag='[DROPINDEX]')
            self.log.info('[DROPINDEX]get drop index table: %s ' % dropidx_tbl)
            if 0 != len(dropidx_tbl):
                dropidx_sql = tmp_sql
                # table
                self.log.info('[DROPINDEX]drop index table: %s ' % dropidx_tbl)
                tbl_split = dropidx_tbl[0].split('.')
                dropidx_cs = tbl_split[0]
                dropidx_cl = tbl_split[1]
                # index argument
                index_tbl = dropidx_tbl[0]
                if index_tbl == self.tbl_name:
                    loc_idxrd = self.scdb.sync_query(sync_local_cs, sync_local_cl, self.tbl_cond)
                    if 0 == len(loc_idxrd):
                        raise
                else:
                    print 'INDEX TABLE: %s, RUN TABLE: %s' % (index_tbl, self.tbl_name)
                    self.log.info('INDEX TABLE: %s, RUN TABLE: %s' % (index_tbl, self.tbl_name))
                    raise
                data_dt = loc_idxrd[0]['sync_dt']
                indexes_objstr = cnf_records[0]['idx_field']
                date_formate = eval(cnf_records[0]['main_dtp'])[0]
                dropidx_str = dropidx_tbl[2]
                dropidx_num = int(dropidx_str[(len(dropidx_str)-2):(len(dropidx_str)-1)])
                print 'INDEX TABLE NUMBER: %s' % dropidx_num
                self.log.info('INDEX TABLE NUMBER: %s' % dropidx_num)
                # drop index
                self.scdb.sync_hisub_drpidx(dropidx_cs, dropidx_cl, indexes_objstr, date_formate, data_dt, dropidx_num)

                # sql
                sql = dropidx_tbl[1]
            
            # when is append_his_main/sub, detach CL and attach CL
            sync_type = cnf_records[0]['sync_type']
            print '1Table: %s have sync type: %s' % (self.tbl_name, sync_type)
            self.log.info('1Table: %s have sync type: %s' % (self.tbl_name, sync_type))
            # before update, get the count of main tbl
            if sql_key == sql_keys[0]:
                updt_bcnt = self.scdb.sync_get_count(self.cs_name, self.cl_name) 
                ruler = {'$set': {'updt_bnum': updt_bcnt}}
                matcher = self.tbl_cond
                self.scdb.sync_update(sync_local_cs, sync_local_cl,
                                      ruler, matcher)
            if 'SparkSQL' == sql_type:
                # [TRUNCATE]spark sql, check the insert table need to truncate or not
                truncate_tbl = self.get_truncate_tbl(sql)
                self.log.info('[TRUNCATE]get truncate table: %s ' % truncate_tbl)
                if 0 != len(truncate_tbl):
                    # table
                    self.log.info('[TRUNCATE]truncate table: %s ' % truncate_tbl)
                    tbl_split = truncate_tbl[0].split('.')
                    trun_cs = tbl_split[0]
                    trun_cl = tbl_split[1]
                    # sql
                    # when truncate table equal product table, we interrupt
                    if self.tbl_name != truncate_tbl[0]:
                        sql = truncate_tbl[1]
                        self.scdb.sync_truncate(trun_cs, trun_cl)
                    else:
                        print 'CRITICAL, TRUNCATE TABLE: %s is equal PRODUCT TABLE: %s; run sql:%s' % (truncate_tbl[0], self.tbl_name, sql)
                        self.log.error('CRITICAL, TRUNCATE TABLE: %s is equal PRODUCT TABLE: %s; run sql:%s' % (truncate_tbl[0], self.tbl_name, sql))
                        raise

                loc_records = self.scdb.sync_query(sync_local_cs, sync_local_cl,
                                                   self.tbl_cond)
                if 0 == len(loc_records):
                    print 'the table: %s have not in sync.local when update data' % self.tbl_name
                    self.log.error('the table: %s have not in sync.local when update data' % self.tbl_name)
                    raise
                #print '[update]pref_sql_key:%s ' % pref_sql_key
                # 将SQL存入数据同步的SPARK SQL队列, sync.sparkloc SPARK SQL 任务队列
                sparkloc_rd = collections.OrderedDict()
                f_id = ObjectId()
                sparkloc_rd['_id'] = f_id
                sparkloc_rd['sync_sys'] = loc_records[0]['sync_sys']
                sparkloc_rd['tbl_name'] = loc_records[0]['tbl_name']
                sparkloc_rd['sync_dt'] = loc_records[0]['sync_dt']
                sparkloc_rd['sprk_st'] = 'wait'
                # construct sql
                sync_dt_fm = datetime.datetime.strptime(loc_records[0]['sync_dt'], '%Y%m%d')
                sync_dt = sync_dt_fm.strftime(date_fm[1])
                #print 'sql datetime: %s' % (sync_dt)
                if 'append_his' == loc_records[0]['sync_type'] or \
                   'append_his_main' == loc_records[0]['sync_type'] or \
                   'append_his_dp' == loc_records[0]['sync_type']:
                    dst_tbl = self.tbl_name.split('.')
                else:
                    dst_tbl = loc_records[0]['dst_tbl'].split('.')
                cs_name = loc_records[0]['tbl_name'].split('.')
                spark_tbl = '%s.%s' % (cs_name[0], dst_tbl[1])
                # combine SQL
                if 'append_his' == loc_records[0]['sync_type'] or \
                   'append_his_main' == loc_records[0]['sync_type']:
                    sparkloc_rd['run_sql'] = sql % (sync_dt)

                if 'append_cus' == loc_records[0]['sync_type'] or \
                   'append_his_sub' == loc_records[0]['sync_type']:
                    sparkloc_rd['run_sql'] = sql % (spark_tbl, sync_dt)

                if 'append_his_dp' == loc_records[0]['sync_type']: 
                    sparkloc_rd['run_sql'] = sql

                sparkloc_rd['init_tm'] = str(datetime.datetime.now())
                # sync flag
                #sync_flag = eval(loc_records[0]['sync_flag'])

                #print '<<<<<<<<<<<pref sql key: %s, local recrd key' % (pref_sql_key)
                # 执行的SQL放入SPARK SQL队列
                if pref_sql_status is True:
                    self.scdb.sync_insert(sync_sparkloc_cs, sync_sparkloc_cl, sparkloc_rd)
                else:
                    #print 'SQL[%s] run failed' % pref_sql_key
                    break
                while True:
                    # 当第一个SQL未执行完成, 则不将新SQL推送到Spark队列
                    sql_cond = {'run_sql': sparkloc_rd['run_sql']}
                    sprkloc_records = self.scdb.sync_query(sync_sparkloc_cs, sync_sparkloc_cl, sql_cond)
                    if 0 == len(sprkloc_records):
                        raise
                    time.sleep(wait_time)
                    time_clock += wait_time
                    print '===sql:%s' % sparkloc_rd['run_sql']
                    self.log.info('update sql running: %s' % sparkloc_rd['run_sql'])
                    # 当Spark SQL任务队列表内SQL已经完成, 则将更新完成的SQL写入sync.local, 继续下一条
                    if 'success' == sprkloc_records[0]['sprk_st']:
                        ok_sql = sparkloc_rd['run_sql']
                        upd_sql = collections.OrderedDict()
                        upd_sql[sql_key] = ok_sql
                        print '===update sql:%s' % upd_sql
                        # after update, get the count of main tbl
                        if sql_key == sql_keys[-1]:
                            updt_ecnt = self.scdb.sync_get_count(self.cs_name, self.cl_name) 
                            upd_sql['updt_enum'] = updt_ecnt

                        ruler = {'$set': upd_sql}
                        matcher = self.tbl_cond
                        self.scdb.sync_update(sync_local_cs, sync_local_cl,
                                              ruler, matcher)
                        pref_sql_status = True
                        break
                    elif 'failed' == sprkloc_records[0]['sprk_st']:
                        print '[update]Spark SQL Failed'
                        self.log.error('[update]Spark SQL Failed!, SQL: %s' % sparkloc_rd['run_sql'])
                        pref_sql_status = False
                        return is_update_ok
                    else:
                        print 'wait for spark run SQL'
                        time.sleep(10)

                    # 当等待达到1800秒(30min), 进行日志告警
                    if wait_time == time_clock:
                        #print 'WARNNING, WAIT FOR LONG TIME'
                        self.log.warn('TABLE: %s, WARNNING, WAIT FOR LONG TIME' % self.tbl_name)
                    if fail_time == time_clock:
                        #print 'FAIELD, NO WAIT'
                        self.log.info('FAIELD, NO WAIT')
                        break

            elif 'PostgreSQL' == sql_type:
                # 另外则是SequoiaSQL
                pg_cmd = postgresql_cmd
                (status, output) = commands.getstatusoutput(pg_cmd % sql)
                if 0 == status:
                    update_rd = collections.OrderedDict()
                    update_rd[sql_key] = sql
                    # after update, get the count of main tbl
                    if sql_key == sql_keys[-1]:
                        updt_ecnt = self.scdb.sync_get_count(self.cs_name, self.cl_name) 
                        update_rd['updt_enum'] = updt_ecnt
                    ruler = {'$set': update_rd}
                    matcher = self.tbl_cond
                    self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
                    self.log.info('SQL: %s, Output: %s' % (sql_value, output))
                else:
                    print '[update]SequoiaSQL:<%s> Failed, error code: %s, output: %s' % (sql, status, output)
                    self.log.error('SequoiaSQL:<%s> Failed, error code: %s, output: %s' % (sql, status, output))
                    return is_update_ok
            else:
                print 'ERROR, NOT THIS SQL'
                self.log.warn('ERROR, NOT THIS SQL: %s' % sql)
                return is_update_ok

            # SQL 滚动执行情况
            pref_sql_key = sql_key

        # the history table need to attach and detach table
        if 'append_his_main' == sync_type:
            tbl_bound = eval(cnf_records[0]['subtbl_bd'])
            range_key = tbl_bound["LowBound"].keys()[0]
            lowbound = tbl_bound["LowBound"][range_key]
            low_bound = []
            low_bound.append(lowbound)
            print 'lowbound: %s' % list(low_bound)
            detach_subtbl = self.scdb.sync_get_subtbl(self.cs_name, self.cl_name, list(low_bound))
            print 'table: %s.%s, Get DetachCL: %s' % (self.cs_name, self.cl_name, detach_subtbl)
            detach_subsp = detach_subtbl.split(".")
            detach_cl = detach_subsp[0]
            loop_cnt = 3
            attach_cl = self.loop_subtbl(detach_cl, loop_cnt, delta=1)
            attach_cs = attach_cl
            attach_tbl = '%s.%s' % (attach_cs, attach_cl)
            spark_attach = '%s.%s' % (self.cs_name, attach_cl)
            print 'table: %s.%s, Get AttachCL: %s' % (self.cs_name, self.cl_name, attach_tbl)
            while True:
                attach_cnt = self.scdb.sync_get_count(attach_cs, attach_cl)

                if 0 != attach_cnt:
                    #sub table view. such as wwcs_hist.debt_detl_nostm[view] for wwcs_hist.debt_detl
                    # debt_detl have sub tbl: nostm1, next attach nostm2; debt_detl_nostm have sub tblnostm3
                    detach_subview = self.loop_subtbl(attach_cl, loop_cnt, delta=1)
                    detach_tblview = '%s.%s' % (detach_subview, detach_subview)
                    print 'table: %s.%s, Get detach view: %s' % (self.cs_name, self.cl_name, detach_tblview)
                    self.log.info('table: %s.%s, Get detach view: %s' % (self.cs_name, self.cl_name, detach_tblview))
                    snap8 = self.scdb.sync_get_catalog_snapshot(detach_tblview)
                    print 'CATA INFO: %s' % snap8
                    view_maintbl = snap8['MainCLName']
                    # find attach sub table view
                    tbl_cond = {'tbl_name': view_maintbl, 'sprk_st': 'success'}
                    spark_subtbl = self.scdb.sync_query(sync_sparkloc_cs, sync_sparkloc_cl, tbl_cond)
                    if 0 == len(spark_subtbl):
                        time.sleep(20)
                        continue
                    sub_run_over = False
                    for spark_sql in spark_subtbl:
                        sql_str = spark_sql['run_sql']
                        print 'SPARK ATTACH: %s' % spark_attach
                        print 'SPARK ATTACH SQL: %s' % sql_str
                        print 'SPARK FIND SQL: %s' % sql_str.find(spark_attach)
                        if -1 != sql_str.find(spark_attach):
                            sub_run_over = True
                            break

                    if sub_run_over is True:
                        view_maincs = view_maintbl.split('.')[0]
                        view_maincl = view_maintbl.split('.')[1]
                        attach_bound = collections.OrderedDict()
                        low_bound = {}
                        up_bound = {}
                        low_bound['_id'] = MinKey()
                        up_bound['_id'] = MaxKey()
                        attach_bound['LowBound'] = low_bound
                        attach_bound['UpBound'] = up_bound
                        
                        # detach cl
                        print 'DETACH CL: %s' % detach_subtbl
                        self.scdb.sync_detach_cl(self.cs_name, self.cl_name, detach_subtbl)
                        self.scdb.sync_detach_cl(view_maincs, view_maincl, detach_tblview)
                        self.scdb.sync_attach_cl(view_maincs, view_maincl, detach_subtbl, attach_bound)

                        # attach cl
                        attach_bound = collections.OrderedDict()
                        low_bound = {}
                        up_bound = {}
                        low_bound[range_key] = tbl_bound["LowBound"][range_key]
                        up_bound[range_key] = tbl_bound["UpBound"][range_key]
                        attach_bound['LowBound'] = low_bound
                        attach_bound['UpBound'] = up_bound
                        print 'ATTACH CL: %s' % attach_tbl
                        self.scdb.sync_attach_cl(self.cs_name, self.cl_name, attach_tbl, tbl_bound)
                        break
                    else:
                        print 'Table: %s SQL is not running over' % view_maintbl
                        continue
                else:
                    time.sleep(10)
                    continue

        # if have drop index operation, we need create index 
            #[DROPINDEX-2] drop index in append_his
        if '' != dropidx_sql and dropidx_sql is not None:
            print '[DROPINDEX]get create index sql: %s ' % dropidx_sql
            dropidx_tbl = self.get_flag_tbl(dropidx_sql, flag='[DROPINDEX]')
            self.log.info('[DROPINDEX]get create index table: %s ' % dropidx_tbl)
            if 0 != len(dropidx_tbl):
                # table
                self.log.info('[DROPINDEX]create index table: %s ' % dropidx_tbl)
                tbl_split = dropidx_tbl[0].split('.')
                dropidx_cs = tbl_split[0]
                dropidx_cl = tbl_split[1]
                # index argument
                # index argument
                index_tbl = dropidx_tbl[0]
                if index_tbl == self.tbl_name:
                    loc_idxrd = self.scdb.sync_query(sync_local_cs, sync_local_cl, self.tbl_cond)
                    if 0 == len(loc_idxrd):
                        raise
                else:
                    print 'INDEX TABLE: %s, RUN TABLE: %s' % (index_tbl, self.tbl_name)
                    raise
                data_dt = loc_idxrd[0]['sync_dt']
                indexes_objstr = cnf_records[0]['idx_field']
                date_formate = eval(cnf_records[0]['main_dtp'])[0]
                dropidx_str = dropidx_tbl[2]
                dropidx_num = int(dropidx_str[(len(dropidx_str)-2):(len(dropidx_str)-1)])
                print 'INDEX TABLE NUMBER: %s' % dropidx_num
                # create index
                self.scdb.sync_hisub_crtidx(dropidx_cs, dropidx_cl, indexes_objstr, date_formate, data_dt, dropidx_num)

        # 更新操作执行完成
        upend_locrd = self.scdb.sync_query(sync_local_cs, sync_local_cl,
                                           self.tbl_cond)
        sync_flag = eval(upend_locrd[0]['sync_flag'])
        sync_flag.append(flag)
        sql_end_tm = datetime.datetime.now()
        sql_delta_tm = sql_end_tm - sql_begin_tm

        # 更新
        ruler = {'$set': {'updt_btm': str(sql_begin_tm),
                          'updt_etm': str(sql_end_tm),
                          'updt_tm': str(sql_delta_tm),
                          'sync_flag': str(sync_flag)}}
        matcher = self.tbl_cond
        self.log.info('[TABLE: %s][UPDATE][BEFORE INFO: %s]' % (self.tbl_name, ruler))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][UPDATE][AFTER INFO: %s]' % (self.tbl_name, ruler))

        # 检查
        is_update_ok = True

        return is_update_ok


    def create_index(self):
        """
        Function Name: 创建索引操作
        Description:   (全量数据/增量流水(最近子表)/增量非流水/挂载子表/打平中间表)在进行数据同步时需要创建索引
        Parameters:
        """
        is_crtidx_ok = False
        flag = 'create_index'
        # sync.config
        cnf_records = self.scdb.sync_query(sync_config_cs, sync_config_cl, self.tbl_cond)
        if 0 == len(cnf_records):
            #print 'table %s have no config in sync.config!' % (self.tbl_name)
            self.log.warn('table %s have no config in sync.config!' % (self.tbl_name))
            raise
        sync_idx_arr = cnf_records[0]['idx_field']
        # sync.local
        locl_records = self.scdb.sync_query(sync_local_cs, sync_local_cl, self.tbl_cond)
        if 0 == len(locl_records):
            raise
        sync_flag = eval(locl_records[0]['sync_flag'])
        if 0 == len(locl_records):
            #print 'table %s have no info in sync.local!' % (self.tbl_name)
            self.log.warn('table %s have no info in sync.local!' % (self.tbl_name))
        crtidx_bftm = datetime.datetime.now()
        if 'append_his' != locl_records[0]['sync_type'] and \
           'append_his_main' != locl_records[0]['sync_type'] and \
           'append_his_dp' != locl_records[0]['sync_type']:
            dst_sub_tbl = locl_records[0]['dst_tbl'].split('.')
            dst_cs_name = dst_sub_tbl[0]
            dst_cl_name = dst_sub_tbl[1]

            # one table have not only one index, much more
            if 0 != len(eval(sync_idx_arr)):
                print '[Create_Index]sdb table:%s.%s have index keys:%s' % (dst_cs_name, dst_cl_name, sync_idx_arr)
                self.log.info('[Create_Index]sdb table:%s.%s have index keys:%s' % (dst_cs_name, dst_cl_name, sync_idx_arr))
                self.scdb.create_index(dst_cs_name, dst_cl_name, sync_idx_arr)
            else:
                print '[NO_Create_Index]sdb table:%s.%s have no index keys:%s' % (dst_cs_name, dst_cl_name, sync_idx_arr)
                self.log.info('[NO_Create_Index]sdb table:%s.%s have no index keys:%s' % (dst_cs_name, dst_cl_name, sync_idx_arr))

        # 索引创建完成
        crtidx_aftm = datetime.datetime.now()

        # 更新
        sync_flag.append(flag)
        delta_time = crtidx_aftm - crtidx_bftm
        # ???因为正常更新操作后面两张子表也需要进行索引删除操作。
        if 'append_his' == locl_records[0]['sync_type'] or \
           'append_his_main' == locl_records[0]['sync_type'] or \
           'append_his_sub' == locl_records[0]['sync_type'] or \
           'append_his_dp' == locl_records[0]['sync_type']:
            ruler = {'$set': {'cidx_tm': str(delta_time), 'sync_flag': str(sync_flag),
                              'sync_st': 'success'}}
        else:
            ruler = {'$set': {'cidx_tm': str(delta_time), 'sync_flag': str(sync_flag)}}
        matcher = self.tbl_cond

        self.log.info('[TABLE: %s][CREATE INDEX][BEFOR INFO: %s]' % (self.tbl_name, ruler))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][CREATE INDEX][AFTER INFO: %s]' % (self.tbl_name, ruler))

        is_crtidx_ok = True

        return is_crtidx_ok

    def detach_attach(self):
        """
        Function Name: 主子表卸载与挂载
        Description:   (全量数据/增量非流水/挂载子表/打平中间表)在进行数据同步时需要进行主子表切换挂载
                       仅当一个系统所有数据表均准备OK再进行数据的挂载操作
        Parameters:
        """
        is_dtat_ok = False
        flag = 'detach_attach'
        # get synchronous config table
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, self.cs_name, self.cl_name, self.log)
        sync_type = sync_config.get_table_sync_type()
        tbl_name = sync_config.get_table_name()
        sync_new = sync_config.get_table_sync_new()
        dest_name = sync_config.get_destination_table()
        sub_tbl_bound = sync_config.get_sub_table_bound()
        if sync_type == self.full_type and dest_name is not None and \
           sub_tbl_bound is not None:
            main_tbl = dest_name
            cs_name = sync_config.get_destination_csname()
            cl_name = sync_config.get_destination_clname()
        else:
            main_tbl = tbl_name
            cs_name = self.cs_name
            cl_name = self.cl_name

        # get synchronous local table
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, self.cs_name, self.cl_name, self.log)

        # check the sync is run to detach_attach
        sync_flag = sync_local.get_table_sync_flag_list()
        last_sync = sync_flag[-1]
        is_ready = True
        if 'create_index' != last_sync:
            is_ready = False

        # if kinds system table not ready, then return and wait for data ok
        if is_ready is False:
            if self.log is None:
                print 'table: %s is not ready to detach and attach' % self.tbl_name
            else:
                self.log.info('table: %s is not ready to detach and attach' % self.tbl_name)
            return is_dtat_ok

        # now data is ok, we do detach old tables and attach new tables
        dtat_bftm = datetime.datetime.now()
        # get sub table
        loc_sub_table = sync_local.get_product_sub_table()
        dst_sub_table = sync_local.get_destination_table()
        # detach and attach model
        detach_attach = SyncDetachAttach(self.hostname, self.svcport, self.username,
                                         self.password, self.connect_hosts, self.log)
        attach_bound = collections.OrderedDict()
        if loc_sub_table is not None and "" != loc_sub_table:
            # according to config sub table bound
            print "sub table bound: %s" % (sub_tbl_bound)
            if sub_tbl_bound is None:
                attach_bound = detach_attach.get_sub_table_bound(main_tbl, loc_sub_table)
            else:
                attach_bound = sub_tbl_bound

            print "attach bound 1: %s" % attach_bound
            # detach collection
            self.scdb.sync_detach_cl(cs_name, cl_name, loc_sub_table)
            if self.log is None:
                print "table: %s, detach sub table: %s " \
                      "from main table: %s" % (self.tbl_name, loc_sub_table, main_tbl)
            else:
                self.log.info("table: %s, detach sub table: %s from main "
                              "table: %s" % (self.tbl_name, loc_sub_table, main_tbl))
        else:
            sharding_key = detach_attach.get_split_shardingkey(main_tbl)
            if sharding_key is not None:
                shard_key = sharding_key.keys()[0]
            else:
                self.log.error("table: %s failed to get sharding key form %s" % (self.tbl_name,
                                                                                 main_tbl))
                raise
            attach_bound['LowBound'] = {shard_key: MinKey()}
            attach_bound['UpBound'] = {shard_key: MaxKey()}
            print "attach bound 2: %s" % attach_bound
        # attach collection
        attach_status = None
        if self.full_type == sync_type:
            attach_status = self.scdb.sync_attach_cl(cs_name, cl_name, dst_sub_table, attach_bound)
            if self.log is None:
                print "table: %s, attach sub table: %s from main table: %s with " \
                      "bound: %s" % (self.tbl_name, dst_sub_table, main_tbl, attach_bound)
            else:
                self.log.info("table: %s, attach sub table: %s from main table: %s with " \
                              "bound: %s" % (self.tbl_name, dst_sub_table,
                                             main_tbl, attach_bound))
        print "table: %s, status: %s" % (self.tbl_name, attach_status)
        if 0 == attach_status:
            is_dtat_ok = True
        # 挂载完成
        dtat_aftm = datetime.datetime.now()

        # 更新
        sync_flag.append(flag)
        delta_time = dtat_aftm - dtat_bftm
        if sync_type == self.full_type and dest_name is not None and \
           sub_tbl_bound is not None:
            end_number = self.scdb.sync_get_count(cs_name, cl_name)
        else:
            end_number = self.scdb.sync_get_count(self.cs_name, self.cl_name)
        # update rule
        ruler = {'$set': {'dtat_tm': str(delta_time), 'sync_flag': str(sync_flag),
                          'end_num': end_number}}
        matcher = {'tbl_name': tbl_name}
        self.log.info('[TABLE: %s][DETACH ATTACH][BEFORE INFO: %s]' % (self.tbl_name, ruler))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][DETACH ATTACH][AFTER INFO: %s]' % (self.tbl_name, ruler))

        return is_dtat_ok


    def update_sync_status(self, status, condition=None):
        end_time = datetime.datetime.now()
        ruler = {"$set": {"sync_st": status, "end_tm": end_time}}
        if condition is None:
            matcher = self.tbl_cond
        else:
            matcher = condition
        #print 'condition: %s, status: %s' % (condition, status)
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)

    def clear_file(self):
        """
        Function Name: 文件清理
        Description:   按表为单位进行文件清理
        Parameters:
        """
        is_clear_ok = False
        flag = 'clear_file'
        # get synchronous local table
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, self.cs_name, self.cl_name, self.log)

        # check the sync is run to detach_attach
        sync_flag = sync_local.get_table_sync_flag_list()
        sync_file = sync_local.get_table_sync_file()
        sync_date = sync_local.get_sync_date()
        # clear file
        sync_clear = SyncClear(sync_file, sync_date)
        sync_clear.clear_data()
        is_clear_ok = True

        # 更新
        sync_flag.append(flag)
        # update rule
        ruler = {'$set': {'sync_flag': str(sync_flag)}}
        matcher = {'tbl_name': self.tbl_name}
        self.log.info('[TABLE: %s][DETACH ATTACH][BEFORE INFO: %s]' % (self.tbl_name, ruler))
        self.scdb.sync_update(sync_local_cs, sync_local_cl, ruler, matcher)
        self.log.info('[TABLE: %s][DETACH ATTACH][AFTER INFO: %s]' % (self.tbl_name, ruler))

        return is_clear_ok


def main():
    hostname = 'localhost'
    svcport = 11810
    username = 'sdbapp'
    password = 'kfptSDB2016'
    tbl_names = ['smc.gcpocrs']

    modify_name = 'debt_detl_nostm3'
    loop_cnt = 3
    delta = 2
    #end = loop_subtbl(modify_name, loop_cnt, delta)
    table = 'wwcs_hist.debt_detl'
    sync = SYNC(table, hostname, svcport, username, password)
    spark_sql = "insert into wwcs_hist.debt_detl[DROPINDEX-2] select agt_modif_num, agt_num, stmt_month, other, interest, delay_fee, overlmt_fee, comm_fee, member_fee, balance_amt , repay_amt, etl_date, flag, concat_agt, case when flag = '3' then '30000000' else stmt_month end from repo_wwcs_hist.repo_debt_detl where etl_date = '%s';"
    flag = sync.get_flag_tbl(spark_sql, flag='[DROPINDEX]')
    print flag
    #创建表
    """
    for table in tbl_names:
        #sync = SYNC(table, hostname, svcport, username, password)
        #sync.file_get_check()
        #sync.create_synctbl()
        print '=' * 150
        print 'trans before: %s' % str(datetime.datetime.now())
        #sync.file_parse_transcd()
        print 'trans after: %s' % str(datetime.datetime.now())
        print '=' * 150
        #sync.date_import()
    """

if __name__ == '__main__':
    main()
