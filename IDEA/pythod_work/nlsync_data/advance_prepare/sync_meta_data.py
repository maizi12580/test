#!/usr/bin/python
#coding=utf-8

import sys
import os
#sys.path.append("/home/sequoiadb/synchronous_data/nlsync_data")
sys.path.append(os.path.abspath(os.path.join(sys.path[0], "..")))
from multiprocessing import Process, Queue, Pipe, Lock, Value, Array, Manager
from sync_sdb import *
import time
from config.global_config import *
from optparse import OptionParser
import commands
import datetime
from datetime import timedelta
from sync_logger import *
import csv

class SyncMetaData:
    def __init__(self):
        self.db2_database = db2_database
        self.db2_username = db2_username
        self.db2_password = base64.decodestring(db2_password)
        self.ods_schema = ods_schema
        self.db2_bin_path = db2_bin_path
        self.db2_connect_db = '%s/db2 CONNECT TO %s USER %s USING \'"%s"\'' % (self.db2_bin_path,
                                                                           self.db2_database,
                                                                           self.db2_username,
                                                                           self.db2_password)
        self.db2_list_tables = '%s/db2 \"SELECT TABNAME as TABLENAME FROM SYSCAT.COLUMNS ' \
                               'WHERE TABSCHEMA=\'%s\' ' \
                               'GROUP BY TABNAME\"'
        self.db2_describe_table = '%s/db2 "SELECT ' \
                                  't.COLNO||\'|\'||' \
                                  't.COLNAME||\'|\'||' \
                                  't.TYPENAME||\'|\'||' \
                                  't.LENGTH||\'|\'||' \
                                  't.SCALE||\'|\'||' \
                                  'CASE WHEN t.REMARKS is null THEN \'None\' ELSE t.REMARKS  END as field FROM SYSCAT.COLUMNS t ' \
                                  'WHERE TABSCHEMA=\'%s\' AND TABNAME=upper(\'%s\') ' \
                                  'ORDER BY t.COLNO ASC"'
        self.db2_list_tables_cmd_split = "\n\n\n"
        self.db2_list_tables_split = "\n"
        self.db2_table_record_split = " "
        self.db2_record_type={}
        self.retry_times = retry_times
        self.metahis_save_time = metahis_save_time
        # SDB
        self.hostname = host_name
        self.svcport = server_port
        self.username = user_name
        self.password = password
        self.db_hosts = hosts
        self.nlsync_config_cs = sync_config_cs
        self.nlsync_config_cl = sync_config_cl
        self.mdm_metahis_cs = mdm_metahis_cs
        self.mdm_metahis_cl = mdm_metahis_cl
        self.connect_hosts = []
        for db_host in self.db_hosts.split(','):
            host_info = db_host.split(':')
            connect_info = {'host': host_info[0], 'service': host_info[1]}
            self.connect_hosts.append(connect_info)
        self.db = SCSDB(self.hostname, self.svcport, self.username,
                        self.password, self.connect_hosts)
        """
        logging file
        """
        log_connect = {'HostName': self.hostname, 'ServerPort': self.svcport,
                       'UserName': self.username, 'Password': self.password,
                       'CsName': sync_log_cs, 'ClName': sync_log_cl}

        sync_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_table = {'sync_sys': "MDM", 'tbl_name': "MDM.META", 'sync_dt': sync_date}

        self.log = logging.getLogger("sync_mdm")
        self.log.setLevel(logging.INFO)
        logfile_name = logfile_dir
        fh = SCFileHandler(logfile_name, log_table, log_connect)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.log.addHandler(fh)

    def get_tables_from_schema(self, schema, rnum):
        # 1.get tables
        db2_get_tables_cmd = "%s;%s" % (self.db2_connect_db,
                                        self.db2_list_tables % (self.db2_bin_path, schema))
        (status, output) = commands.getstatusoutput(db2_get_tables_cmd)
        self.log.info("get_tables_name commands: %s" % db2_get_tables_cmd)
        if 0 != status:
            print "failed to get db2 command;status: %s  output: %s" % (status, output)
            self.log.error("failed to get db2 command;status: %s  output: %s" % (status,
                                                                                 output))
            raise

        db2_tables = output.split(self.db2_list_tables_cmd_split)[1]
        db2_table_rds = db2_tables.split(self.db2_list_tables_split)
        tbl_count = ''
        if -1 == db2_table_rds[0].find("TABLENAME") or \
           -1 == db2_table_rds[1].find("----"):
            print 'failed get'
            self.log.error('failed get')
            raise
        else:
            db2_table_rds.pop(0)
            db2_table_rds.pop(0)
            db2_table_rds.pop()
            tbl_count = db2_table_rds.pop()
            db2_table_rds.pop()
        # 2. get table name
        tables = []
        for db2_table_rd in db2_table_rds:
            table = db2_table_rd.strip()
            tables.append(table)
        self.log.info("table_rd: %s" % tables)
        # 3. get table name count
        tbl_count = tbl_count.strip().split(" ")[0]
        self.log.info("sys_name: %s,table_rds_count: %s,tbl_count: %s" % (schema,
                                                                          len(tables),
                                                                          tbl_count))
        if int(tbl_count) != len(tables):
            print "sys_name: %s,table_rds_count: %s != tbl_count: %s" % (schema,
                                                                         len(tables),
                                                                         tbl_count)
            self.log.error("sys_name: %s,table_rds_count: %s != tbl_count: %s" % (schema,
                                                                                  len(tables),
                                                                                  tbl_count))
            print "fail retry num: %s" % rnum
            self.log.error("fail retry num: %s" % rnum)
            if rnum < self.retry_times:
                return self.get_tables_from_schema(schema, rnum+1)
            else:
                raise
        return tables

    def get_field_info(self, column_name, data_type_name, column_length, scale,remark):
        sdb_type = None
        # 字符或字符串类型，加上长度
        if "CHARACTER" == data_type_name or "CHAR" == data_type_name or \
                "VARCHAR" == data_type_name or "LONG VARCHAR" == data_type_name:
            sdb_type = data_type_name + "(" + column_length + ")"
        # 整型保持原样
        elif "INTEGER" == data_type_name or "INT" == data_type_name or \
                "SMALLINT" == data_type_name or "BIGINT" == data_type_name:
            sdb_type = data_type_name
        # 浮点型保持原样
        elif "REAL" == data_type_name or "DOUBLE" == data_type_name:
            sdb_type = data_type_name
        # 时间类型保持原样
        elif "TIMESTAMP" == data_type_name or "DATE" == data_type_name or "TIME" == data_type_name:
            sdb_type = data_type_name 
        # decimal类型，加上整数位长度和小数位长度
        elif "DECIMAL" == data_type_name:
            sdb_type = data_type_name + "(" + column_length + "," + scale + ")"
        else:
            print "Type conversion error"
            self.log.error("Type conversion error")
            print "------------------>Type conversion error"
            self.log.error("column_name: %s, data_type_name: %s, column_length: %s, scale: %s"%(column_name, data_type_name, column_length, scale))
	    return None 
            #raise
        # sample such as: "tx_date|string|ODS????"
        field_info = "%s|%s|%s" % (column_name, sdb_type, remark.decode("gb18030").encode("utf-8").strip())

        return field_info

    def get_table_records(self, table_name,collectionSpace,rnum,collection=None):
        # 1.get tables
        tables = []
        for schema in self.ods_schema:
            db2_get_tables_records_cmd = "%s;%s" % (self.db2_connect_db,
                                                    self.db2_describe_table % (self.db2_bin_path,schema,
                                                                               table_name))
            (status, output) = commands.getstatusoutput(db2_get_tables_records_cmd)
            #print "get_tables_records command: %s"%(db2_get_tables_records_cmd)
            self.log.info("get_tables_records command: %s"%(db2_get_tables_records_cmd))
            if 0 != status:
                if "IBS" == table_name.split("_")[0]:
                    db2_get_tables_records_cmd_1 = "%s;%s" % (self.db2_connect_db,
                                                              self.db2_describe_table % (self.db2_bin_path,schema,
                                                                                         table_name.replace("IBS_", "EBS_")))
                    (status, output) = commands.getstatusoutput(db2_get_tables_records_cmd_1)
                    if 0 != status:
                        print "failed to get db2 command"
                        self.log.error("failed to get db2 command")
                        self.log.error("status: %s  output: %s"%(status,output))
                        #raise
                        return None
                else:
                    print "failed to get db2 command"
                    self.log.error("failed to get db2 command")
                    self.log.error("status: %s  output: %s"%(status,output))
                    #raise
                    return None
            db2_records = output.split(self.db2_list_tables_cmd_split)[1]
            db2_table_rds = db2_records.split(self.db2_list_tables_split)
            tbl_count = ''
            if -1 == db2_table_rds[0].find("FIELD") or \
               -1 == db2_table_rds[1].find("------"):
                print 'failed get'
                raise
            else:
                db2_table_rds.pop(0)
                db2_table_rds.pop(0)
                db2_table_rds.pop()
                tbl_count = db2_table_rds.pop()
                db2_table_rds.pop()
            # 2. get table name
            field_num = 1
            tbl_format = {}
            for db2_table_rd in db2_table_rds:
                if field_num == 1:
                    #collection_space_name = table_name.split("_")[0]
                    collection_name = table_name.replace(collectionSpace + '_', '', 1)
                    tbl_format["sysnm_en"] = collectionSpace
                    tbl_format["sysnm_ch"] = collectionSpace
                    tbl_format["tblnm_en"] = collection_name
                    tbl_format["tblnm_ch"] = collection_name
                    if collection is None: 
                        tbl_format["tbl_name"] = '%s.%s' % (collectionSpace.lower(),
                                                            collection_name.lower())
                    else:
                        tbl_format["tbl_name"] = '%s.%s' % (collectionSpace.lower(),
                                                            collection)
                    tbl_format["sync_dt"] = datetime.datetime.now().strftime("%Y%m%d")
                    tbl_format["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    tbl_format["prim_key"] = "" 
                db2_field = db2_table_rd.split("|")
                field_info = self.get_field_info(db2_field[1].lower(), db2_field[2],
                                                 db2_field[3], db2_field[4],db2_field[5].strip())
		if field_info is None:
		    return None
		    #break
                tbl_format["field" + '{0:0>3}'.format(str(field_num))] = field_info
                field_num += 1

            tables.append(tbl_format)
            # 2. get table info count
            tbl_count = tbl_count.strip().split(" ")[0]
            self.log.info("table_name: %s.%s,table_rds_count: %s,tbl_count: %s" % (schema,
                                                                                   table_name,
                                                                                   len(db2_table_rds),
                                                                                   tbl_count))
            if int(tbl_count) != len(db2_table_rds):
                print "table_name: %s.%s,table_rds_count: %s != tbl_count: %s" % (schema,
                                                                                  table_name,
                                                                                  len(db2_table_rds),
                                                                                  tbl_count)
                self.log.error("table_name: %s.%s,table_rds_count: %s != tbl_count: %s" % (schema,
                                                                                           table_name,
                                                                                           len(db2_table_rds),
                                                                                           tbl_count))
                print "fail retry num: %s" % rnum
                self.log.error("fail retry num: %s" % rnum)
                if rnum < retry_times:
                    return self.get_table_records(table_name,collectionSpace, rnum+1)
                else:
                    raise
        return tables

    def sync_meta_main(self):
        cond = {}
        selector = {"tbl_name": "","sync_file":""}
        tbl_names = self.db.sync_query(self.nlsync_config_cs, self.nlsync_config_cl, cond, selector)
        #for schema in self.ods_schema:
        #    rnum = 0
        #    #ods_tables = self.get_tables_from_schema(schema,rnum)
             
        for tbl_name in tbl_names:
	    collectionSpace = tbl_name.get("tbl_name").split(".")[0].upper()
	    collection = tbl_name.get("tbl_name").split(".")[1]
            #if "AGL" != collectionSpace and "IBS" != collectionSpace and "EBS" != collectionSpace:
            #    continue
            #ods_table = tbl_name.get("tbl_name").replace('.', '_').upper()
            """
            if "SMC" != collectionSpace:
                continue
            """
	    sync_file_obj = eval(tbl_name.get("sync_file"))
            pref_file = sync_file_obj[0]["sync_file"]
	    #print "--> %s"%pref_file
	    ods_table=None
	    if pref_file is None or ""==pref_file:
		ods_table = tbl_name.get("tbl_name").replace('.', '_').upper()
	    else:
 	        ods_table = "%s_%s" % (collectionSpace, pref_file.split("/")[-1].replace(".dat",""))
            print "collectionSpace: %s, ODS TABLE: %s" % (collectionSpace, ods_table)
		
            #print "collectionSpace: %s, ODS TABLE: %s" % (ods_table,collectionSpace)
            rnum = 0
            table_info = []
            table_info = self.get_table_records(ods_table,collectionSpace,rnum,collection)

            #print "table info: %s" % table_info
            if table_info is None:
                continue
	    sync_dt = datetime.datetime.now().strftime("%Y%m%d")
            for info in table_info:
	        cond = {"$and":[{"sync_dt":info["sync_dt"]},{"tbl_name":info["tbl_name"]}]}
	        rule = {"$set": info}
                #print "info: %s, cond: %s"%(rule,cond)
	        #print "sync_dt: %s, table: %s"%(info["sync_dt"],info["tbl_name"])
                self.db.sync_upsert(self.mdm_metahis_cs, self.mdm_metahis_cl,rule,cond)

    def clean_data(self, save_month):
        date = datetime.datetime.now()
        day = date.day
        # when day is 1, such as 20130301, we remove data
        if 1 == day:
            year = date.year
            month = date.month
            day = date.day
            if month > save_month:
                month = month - save_month
            elif month > save_month % 12:
                y = save_month/12
                year = year-y
                month = 12*y+month-save_month
            else:
                y = save_month/12+1
                year = year - y
                month = 12*y+month - save_month
            condition = {"sync_dt": {"$lt": "%s%s%s" % (year,'{0:0>2}'.format(month),'{0:0>2}'.format(day))}}
            print "condition: %s" % condition
            self.log.info("condition: %s" % condition)
            self.db.sync_remove(self.mdm_metahis_cs, self.mdm_metahis_cl, condition)

    def append_key(self):
	for i in os.listdir("./primary_keys"):
	    print "fileName: %s, file: %s, split: %s"%(i,os.path.isfile(i),os.path.splitext(i)[1])
	    filename = os.path.abspath('./primary_keys/%s' % i)
	    ext_name = os.path.splitext(i)[1]=='.csv'
	    if os.path.isfile(filename) and ext_name:
		print i
		#format csv file
		csv_reader = csv.reader(open(filename))
		for row in csv_reader:
		    #update 
		    #print "tbl_name: %s, key: %s"%(row[0].strip(),row[1].strip())
		    sync_dt = datetime.datetime.now().strftime("%Y%m%d")
		    cond= {"$and":[{"sync_dt":sync_dt},{"tbl_name":row[0].strip()}]}
		    rule = {"$set": {"prim_key":row[1].strip().replace("+",",")}}
		   # print "cond: %s, rule: %s"%(cond,rule)
		    self.db.sync_update(self.mdm_metahis_cs,self.mdm_metahis_cl,rule,cond)

if __name__ == '__main__':
    sync_meta_data = SyncMetaData()
    sync_meta_data.sync_meta_main()
    #sync_meta_data.append_key()
    if -1 < metahis_save_time:
        sync_meta_data.clean_data(metahis_save_time)
