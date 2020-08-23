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


class SyncUpdateMeta:
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

    def check_contain_chinese(self, check_str):
        for ch in check_str.decode("utf-8"):
            if u'\u4e00' <= ch and ch <= u'\u9fff':
                return True

        return False

    def update_meta_table(self, sync_dt):
        update_meta_ok = True
        is_new_tbl = False
        condition = {"tbl_name" : self.tbl_name}
        config_rd = self.scdb.sync_query(sync_config_cs, sync_config_cl, condition)
        if config_rd[0].get("meta_new") == "true":
            is_new_tbl = True 
        sync_metahis = SyncMetaHis(self.hostname, self.svcport, self.username,
                                   self.password, self.connect_hosts, self.cs_name,
                                   self.cl_name, sync_dt, self.log)
        sync_metatbl = SyncMeta(self.hostname, self.svcport, self.username,
                                self.password, self.connect_hosts, self.cs_name,
                                self.cl_name, self.log)
        sync_config = SyncConfig(self.hostname, self.svcport, self.username,
                                self.password, self.connect_hosts, self.cs_name,
                                self.cl_name, self.log)
        metahis_fields = sync_metahis.get_formate_fields()
        metahis_primary_key = sync_metahis.get_primary_key()
        metahis_system_name_ch = sync_metahis.get_system_name_ch()
        metahis_table_name_ch = sync_metahis.get_table_name_ch()
        metahis_system_name_en = sync_metahis.get_system_name_en()
        metahis_table_name_en = sync_metahis.get_table_name_en()
        metahis_sync_dt = sync_metahis.get_meta_sync_date()
        metatbl_fields = sync_metatbl.get_formate_fields()
        metatbl_primary_key = sync_metatbl.get_primary_key()
        metatbl_system_name_ch = sync_metatbl.get_system_name_ch()
        metatbl_table_name_ch = sync_metatbl.get_table_name_ch()
        metatbl_system_name_en = sync_metatbl.get_system_name_en()
        metatbl_table_name_en = sync_metatbl.get_table_name_en()
        metatbl_sync_bfdt = sync_metahis.get_meta_sync_date()
        update_metatbl_fields = collections.OrderedDict()

        # 将映射信息组装成map
        type_mapping = {}
        prop_cond = {"prop_type":"db2_mapping"}
        prop_rd = self.scdb.sync_query(sync_prop_cs, sync_prop_cl , prop_cond)
        prop_info = prop_rd[0]["info"]
        for mapping_item in prop_info:
            type_key = mapping_item["cnf_key"]
            type_value = mapping_item["cnf_value"]
            type_mapping[type_key] = type_value
       
        print type_mapping

        add_fields_list = sync_config.get_add_fields_list()
        add_fields = None
        metatbl_fields_str = ""
        if add_fields_list is not None and len(add_fields_list) > 0:
            # 保存所有add_fields的key
            add_fields_infos = []
            for add_fields_each in add_fields_list:
                add_fields_info = add_fields_each.split(":")[0].strip()
                add_fields_infos.append(add_fields_info)
                
            add_fields_count = len(add_fields_list)
            add_fields = []
            # 将metatbl中的add_fields踢出来(需要保证add_fields全部是在所有其他字段的后面),加入到add_fields数组中
            # 保存的是对象信息{"field_no","field_info","field_type","field_desc"}
            for i in range(add_fields_count):
                add_field_item = metatbl_fields.pop()
                # 如果pop出来的字段名称不等于add_fields中的一个key,直接失败
                if add_field_item["field_info"] in add_fields_infos:
                    add_fields.append(add_field_item)
                else:
                    self.log.error("add fields info not in the bottom of metatbl,[add_fields:%s,metatbl field:%s]" % (str(add_fields_infos),add_field_item["field_info"]))
                    update_meta_ok = False
                    return update_meta_ok

            # 为了保持原有的顺序，将数组顺序反转
            add_fields.reverse()
            self.log.info("add fields:%s " % str(add_fields_list))
             
        # 用于记录metahis中的字段数量
        field_count = 0

        if 0 != len(metahis_fields):
            for metahis_field in metahis_fields:
                metatbl_type = None 
                field_key = "field%s" % metahis_field.get("field_no")
                # 从metahis获取字段名
                field_info = metahis_field["field_info"] 
                # 从metahis获取中文说明
                field_desc = metahis_field["field_desc"]
                for metatbl_field in metatbl_fields:
                    if metahis_field.get("field_info") == metatbl_field.get("field_info"):
                        # 获取metatbl的类型
                        metatbl_type = metatbl_field["field_type"]
                        # 当metatbl表desc为中文，metahis表desc为None或者""时，desc使用metatbl的
                        if metahis_field.get("field_desc") == "None" or \
                           metahis_field.get("field_desc") == "" or \
                           self.check_contain_chinese(metatbl_field.get("field_desc")) is True:
                            # 中文说明沿用metatbl中的
                            field_desc = metatbl_field["field_desc"]
                        break
                # metahis->metatbl的类型转换，如果是新增字段metatbl_type为None,在转换时当成新字段处理
                field_type = self.transform_type(metahis_field["field_type"],metatbl_type,is_new_tbl,type_mapping)
                if field_type is None:
                    update_meta_ok = False
                    # break
                    # 需要直接在这里返回，否则依旧会更新元数据，并且更新的字段非常不完整
                    return update_meta_ok
                # 组装新的字段信息
                field_value = "%s|%s|%s" % (field_info,field_type,field_desc)
                update_metatbl_fields[field_key] = field_value
                field_count += 1
 
            # add_fields不为None,表示有add_fields字段需要加入到尾部
            if add_fields is not None:
                for add_field in add_fields:
                    field_count += 1
                    # 将不足百位的字段名前加0 : field026
                    field_key = "field%03d" % field_count
                    field_info = add_field["field_info"]
                    field_type = add_field["field_type"]
                    field_desc = add_field["field_desc"]
                    field_value = "%s|%s|%s" % (field_info,field_type,field_desc)
                    update_metatbl_fields[field_key] = field_value
                    

            update_metatbl_fields["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            update_metatbl_fields["sync_dt"] = metahis_sync_dt
            update_metatbl_fields["prim_key"] = metatbl_primary_key
            # 当metatbl表主键字段为None或者空值时，将metahis表的主键字段更新为metatbl表的主键
            if metatbl_primary_key is None or "" == metatbl_primary_key:
                update_metatbl_fields["prim_key"] = metahis_primary_key

            update_metatbl_fields["sysnm_ch"] = metatbl_system_name_ch
            # 当metatbl表系统表中文名字段为None或者空值时，
            # 将metahis表的系统表中文名字段更新为metatbl表的系统表中文名
            if metatbl_system_name_ch is None or "" == metatbl_system_name_ch:
                update_metatbl_fields["sysnm_ch"] = metahis_system_name_ch

            update_metatbl_fields["tblnm_ch"] = metatbl_table_name_ch
            # 当metatbl表表中文名字段为None或者空值时，
            # 将metahis表的表中文名字段更新为metatbl表的表中文名
            if metatbl_table_name_ch is None or "" == metatbl_table_name_ch:
                update_metatbl_fields["tblnm_ch"] = metahis_table_name_ch

            update_metatbl_fields["sysnm_en"] = metatbl_system_name_en
            if metatbl_system_name_en is None or "" == metatbl_system_name_en:
                update_metatbl_fields["sysnm_en"] = metahis_system_name_en

            update_metatbl_fields["tblnm_en"] = metatbl_table_name_en
            if metatbl_table_name_en is None or "" == metatbl_table_name_en:
                update_metatbl_fields["tblnm_en"] = metahis_table_name_en

            # remove and insert mdm.metatbl
            sync_metatbl.remove_metatbl_info()
            # print "insert update metatbl info: %s" % update_metatbl_fields
            sync_metatbl.upsert_metatbl_info(update_metatbl_fields)
        else:
            print 'metahis table don\'t have field info'

        return update_meta_ok


    def transform_type(self, metahis_type, metatbl_type, is_new_tbl,type_mapping):
        """
        metahis中存的是db2的数据类型，需要将其转换为在sdb中的存取类型
        metatbl中存的是sdb的数据类型，如果是None说明原来没有是一个新字段，如果是decimal则和metahis比较，判断是否需要更新
        is_new_tbl : 新旧表的表示，true表示新表，false表示旧表
        type_mapping ：查询资源配置表prop, 将所有映射信息组装成的map
        """ 
        sdb_type = None
        # 如果metatbl_type是None那么说明是一个新字段
        if metatbl_type is None:
            # 获取类型不包括长度
            if metahis_type.find("(") != -1 :
                metahis_type_key = metahis_type[0:metahis_type.index("(")]
            else:
                metahis_type_key = metahis_type
            if metahis_type_key != "DECIMAL": 
                sdb_type = type_mapping[metahis_type_key]
            else:
                column_length = int(metahis_type[metahis_type.index("(") + 1:metahis_type.index(",")])
                scale = int(metahis_type[metahis_type.index(",") + 1 : metahis_type.index(")")])
                # 如果scale为0则判断整数位长度进行转换，如小数位不为0则判断新旧模式，新模式用decimal，旧模式用double
                if scale == 0:
                    decimal_mapping = type_mapping["DECIMAL"][0]
                    if column_length <= decimal_mapping["int"]:
                        sdb_type = "int"
                    elif column_length <= decimal_mapping["long"]:
                        sdb_type = "long"
                    else:
                        sdb_type = "decimal"
                else:
                    decimal_mapping = type_mapping["DECIMAL"][1]
                    # 如果decimal的小数位不为0，需要判断它使用的是新旧规则，新规则转decimal，旧规则转double
                    if is_new_tbl is True:
                        sdb_type = decimal_mapping["meta_new"]
                    else:
                        sdb_type = decimal_mapping["meta_old"]
                # 如果类型是decimal则将metahis转小写即可。这样写：如果需要新旧模式都用decimal修改配置表即可不用改代码
                if sdb_type == "decimal":
                    sdb_type = metahis_type.lower()
            self.log.info("add a new field ods_tye: %s,sdb_tyep: %s" % (metahis_type,sdb_type))              
        # 如果不是新增字段，那么是用原来的类型
        else:
            sdb_type = metatbl_type
            # 针对新表的decimal作特殊处理，旧表没有decimal,沿用metatbl的double
            if "DECIMAL" in metahis_type and "decimal" in metatbl_type and is_new_tbl is True:
                # metahis中的 length,scale, decimal的存储格式 DECIMAL(m,n)
                his_column_length = int(metahis_type[metahis_type.index("(") + 1: metahis_type.index(",")])
                his_scale = int(metahis_type[metahis_type.index(",") + 1 : metahis_type.index(")")])
                # metatbl中的 length,scale, decimal的存储格式 decimal(m,n)
                tbl_column_length = int(metatbl_type[metatbl_type.index("(") + 1: metatbl_type.index(",")])
                tbl_scale = int(metatbl_type[metatbl_type.index(",") + 1 : metatbl_type.index(")")])
                # 从metatbl和metahis中选取长度较长的返回
                column_length = his_column_length if his_column_length > tbl_column_length else tbl_column_length
                scale = his_scale if his_scale > tbl_scale else tbl_scale
                sdb_type = "decimal" + "(" + str(column_length) + "," + str(scale) + ")"
                # self.log.info("change field type, old: %s , new : %s" % (metatbl_type,sdb_type))
        #在这里判断sdb_type是否和is_new_type一致，如果不一致，抛异常
        if is_new_tbl is True and ("double" in sdb_type):
            # print "sdb_type %s is not match with meta_new  %s" % (sdb_type,is_new_tbl) 
            self.log.error("sdb_type %s is not match with meta_new  %s" % (sdb_type,is_new_tbl))
            sdb_type = None
            #raise Exception("sdb_type %s is not match with meta_new  %s" % (sdb_type,is_new_tbl))
        elif is_new_tbl is False and ("decimal" in sdb_type):
            #raise Exception("sdb_type %s is not match with meta_new  %s" % (sdb_type,is_new_tbl))
            # print "sdb_type %s is not match with meta_new  %s" % (sdb_type,is_new_tbl)
            self.log.error("sdb_type %s is not match with meta_new  %s" % (sdb_type,is_new_tbl))
            sdb_type = None
        return sdb_type

def main():
    tbl_name = "wwcs_hist.cs_rec"
    hostname = "localhost"
    svcport = "31810"
    username = "sdbapp"
    password = "a2ZwdFNEQjIwMTY="
    db_hosts = hosts
    connect_hosts = []
    for db_host in db_hosts.split(','):
        host_info = db_host.split(':')
        connect_info = {'host': host_info[0], 'service': host_info[1]}
        connect_hosts.append(connect_info)
    db = SCSDB(hostname, svcport, username, password, connect_hosts)
    log = None
    """
    logging file
    tbl_cond = {'tbl_name': tbl_name}
    log_connect = {'HostName': hostname, 'ServerPort': svcport,
                   'UserName': username, 'Password': password, 
                   'CsName': sync_log_cs, 'ClName': sync_log_cl}
    cnf_records = db.sync_query(sync_config_cs, sync_config_cl, tbl_cond)
    if 0 == len(cnf_records):
        print 'the table: %s have not in sync.config when init class' % tbl_name
        raise
    sync_sys = cnf_records[0]['sync_sys']
    sync_date = get_sync_date()
    print '[sync_date]: %s' % sync_date
    sync_date = sync_date[0]
    log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}
    
    log_table = {'tbl_name': tbl_name}
    log = logging.getLogger("sync_data")
    log.setLevel(logging.INFO)
    logfile_name = logfile_dir
    just_write_table = True
    fh = SCFileHandler(logfile_name, log_table, log_connect, just_write_table)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    log.addHandler(fh)
    """ 
    # connect
    sync_update_meta = SyncUpdateMeta(tbl_name, hostname, svcport,
                                      username, password, connect_hosts,
                                      log)
    # get synchronous config table
    cs_name = tbl_name.split(".")[0]
    cl_name = tbl_name.split(".")[1]
    sync_local = SyncLocal(hostname, svcport, username, password,
                           connect_hosts, cs_name, cl_name, log)                                     
    # sync_dt = sync_local.get_sync_date()
    sync_dt = "20190212"
    sync_update_meta.update_meta_table(sync_dt)                           

    # [sync.local] fields: sync_sys, tbl_name, sync_dt, sync_file, getfl_tm                    
    print "update meta table over!"

if __name__ == '__main__':
    main()
