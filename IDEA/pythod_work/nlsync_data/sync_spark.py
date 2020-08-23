#!/usr/bin/python
#coding=utf-8

import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBError,
                               SDBBaseError,
                               SDBTypeError,
                               SDBEndOfCursor
                                )
from bson.min_key import MinKey
from bson.max_key import MaxKey
from config.global_config import *
from sync_orm import *
from sync_sdb import *
from threading import Thread, Lock
from Queue import Queue
import setproctitle
import base64
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class SyncRebuildSpark:
    def __init__(self, hosts, connect_hosts, hostname, svcport, username, password, db_handler, log_handler=None):
        self.meta_tbl_cs = mdm_metatbl_cs
        self.meta_tbl_cl = mdm_metatbl_cl
        self.hostname = hostname
        self.svcport = svcport
        self.username = username
        self.password = password
        self.hosts = hosts
        self.connect_hosts = connect_hosts
        self.spark_cmd = sparksql_cmd
        self.db = db_handler
        self.log = log_handler
        # config table
        self.config_cs = sync_config_cs
        self.config_cl = sync_config_cl


    def formate_fields(self, meta_infos):
        fields = ''
        field_array = []
        num = 0
        for meta_info in meta_infos:
            tbl_meta_rd = meta_info

            # get table's meta keys and sort by field name, such as "field01", "field02"...
            meta_keys = tbl_meta_rd.keys()
            field_keys = []
            for meta_key in meta_keys:
                if 'field' == meta_key[:5]:
                    field_keys.append(meta_key)
            # sort
            field_keys.sort()

            cnt = 0
            for field in field_keys:
                field_value = tbl_meta_rd.get(field)
                field_arr = field_value.split('|')
                # don't need care about the length of array, just add extend length
                field_name = '{:<50}'.format(field_arr[0])
                if 'long' == field_arr[1]:
                    field_type = '{:<20}'.format('bigint')
                else:
                    field_type = '{:<20}'.format(field_arr[1])
                
                # add field description
                field_type += "comment '%s'" % field_arr[2].replace(";", ",").replace("'", "")

                # the last key don't have symbol ','
                if cnt == len(field_keys) - 1 and num == len(meta_infos) - 1:
                    field_type += '\n'
                else:
                    field_type += ',\n'

                #print "field_name: %s  %s" %  (field_name, field_type)
                field_str = '%s%s' % (field_name, field_type)
                if -1 == field_str.find(","):
                    #field_str_find = field_str.replace("\n", "") + "," + "\n"
                    field_str_find = field_str
                else:
                    field_str_find = field_str

                try:
                    field_array.index(field_str_find)
                    is_exists = True
                except ValueError, e:
                    is_exists = False
                    
                # compare the same fields
                if not is_exists:
                    field_array.append(field_str)
                    fields += field_str
                #print "field_name: %s" %  (field_array)
                cnt += 1
            num += 1

        #print "sync table's fields info: %s" % fields
        return fields

    def generate_spark_table_sql(self, cs_name, cl_name, meta_infos, add_options=None):
        spark_table_head = "create table %s.%s(\n" % (cs_name, cl_name)
        spark_table_body = self.formate_fields(meta_infos)
        spark_sql = ""
        if spark_table_body is None:
            print "WARNNING!collection: %s.%s have no info in meta table: %s.%s" % (cs_name, cl_name, self.meta_tbl_cs, self.meta_tbl_cl)
            self.log.info("WARNNING!collection: %s.%s have no info in meta table: %s.%s" % (cs_name, cl_name, self.meta_tbl_cs, self.meta_tbl_cl))
            return spark_sql

        if add_options is None:
            spark_table_tail = ")USING com.sequoiadb.spark OPTIONS(host '%s', username '%s', password '%s'," \
                               "collectionspace '%s', collection '%s')" % (self.hosts, self.username, base64.decodestring(self.password),
                                                                           cs_name, cl_name)
        else:
            spark_table_tail = ")USING com.sequoiadb.spark OPTIONS(host '%s', username '%s', password '%s'," \
                               "collectionspace '%s', collection '%s', %s)" % (self.hosts, self.username, base64.decodestring(self.password),
                                                                               cs_name, cl_name, add_options)

        create_database = "create database if not exists %s;\n" % cs_name
        drop_table = "drop table if exists %s.%s;\n" % (cs_name, cl_name)
        spark_table = "%s%s%s;\n" % (spark_table_head, spark_table_body, spark_table_tail)
        spark_sql = create_database + drop_table + spark_table
        #print spark_sql
        return spark_sql

    def rebuild_sql_table(self, tbl_name_queue, tbl_name_lock, file_name=None):
        fd = open(file_name, "w+")
        while not tbl_name_queue.empty():
            tbl_name_lock.acquire()
            if tbl_name_queue.empty() and 0 == tbl_name_queue.qsize():
                self.log.info("rebuild spark table queue is empty.")
                tbl_name_lock.release()
                break
            tbl_name = tbl_name_queue.get()
            tbl_name_lock.release()
            # the meta data informations for itself
            main_cs_name = tbl_name.get("tbl_name").split(".")[0]
            main_cl_name = tbl_name.get("tbl_name").split(".")[1]
            print '>>>[queue size: %s] - [table: %s.%s]' % (tbl_name_queue.qsize(), main_cs_name, main_cl_name)
            sync_meta = SyncMeta(self.hostname, self.svcport, self.username,
                                 self.password, self.connect_hosts, main_cs_name,
                                 main_cl_name, self.log)
            meta_infos = []
            meta_info1 = sync_meta.get_meta_ret()
            meta_infos.append(meta_info1)
            # the meta data informations for it's rely table. (for tables upsert into one table)
            rely_tables_cond = {"$and": [{"iprt_cmd": {"$ne": ""}}, 
                                         {"rely_tbl": {"$regex": ".*%s.%s,.*" % (main_cs_name, main_cl_name), "$options": "i"}}]}
            rely_tables_selector = {"tbl_name": 1}
            rely_tables = self.db.sync_query(self.config_cs, self.config_cl, rely_tables_cond, rely_tables_selector)
            for rely_table in rely_tables:
                rely_table_cs = rely_table.get("tbl_name").split(".")[0]
                rely_table_cl = rely_table.get("tbl_name").split(".")[1]
                sync_meta = SyncMeta(self.hostname, self.svcport, self.username,
                                     self.password, self.connect_hosts, rely_table_cs,
                                     rely_table_cl, self.log)
                meta_info2 = sync_meta.get_meta_ret()
                meta_infos.append(meta_info2)
            spark_sql = self.generate_spark_table_sql(main_cs_name, main_cl_name, meta_infos)
            sync_os = SyncOS()
            #print "run sql: %s" % (self.spark_cmd % spark_sql)
            if file_name:
                fd.write(spark_sql)
            else:
                run_sparksql_status = sync_os.cmd_run(self.spark_cmd % spark_sql)
                if run_sparksql_status[0] == 0:
                    print "<<<success to create spark table for %s.%s" % (main_cs_name, main_cl_name)
                    self.log.info("success to create spark table for %s.%s" % (main_cs_name, main_cl_name))
                else:
                    print "<<<failed to create spark table for %s.%s, status: %s" % (main_cs_name, main_cl_name, run_sparksql_status)
                    self.log.error("failed to create spark table for %s.%s, status: %s" % (main_cs_name, main_cl_name, run_sparksql_status))
        fd.close()
        

def main():
    # connection infomation
    hostname = host_name
    svcport = server_port
    username = user_name
    dbpassword = password
    log_cs = sync_log_cs
    log_cl = sync_log_cl
    config_cs = sync_config_cs
    config_cl = sync_config_cl
    #thread_num = sync_meta_thread_num
    thread_num = 1
    # table name
    sync_sys = "SPARK"
    tbl_name = "REBUILD_TABLE"
    file_name = "meta_data_spark.sql"

    #------------------------------
    #- Set Process Name
    #------------------------------
    process_name = 'SYNC|%s|SparkRebuildTable|%s' % (socket.gethostname(),
                                                     datetime.datetime.now().strftime("%Y%m%d"))
    setproctitle.setproctitle(process_name)
    # SDB collection connection
    db_hosts = hosts
    connect_hosts = []
    for db_host in db_hosts.split(','):
        host_info = db_host.split(':')
        connect_info = {'host': host_info[0], 'service': host_info[1]}
        connect_hosts.append(connect_info)
    db = SCSDB(hostname, svcport, username, dbpassword, connect_hosts)

    """
    logging file
    """
    tbl_cond = {'tbl_name': tbl_name}
    log_connect = {'HostName': hostname, 'ServerPort': svcport,
                   'UserName': username, 'Password': password,
                   'CsName': log_cs, 'ClName': log_cl}
    # prefetch file's sync date using local date
    sync_date = datetime.datetime.now().strftime("%Y%m%d")
    print '[sync_date]: %s' % sync_date
    #sync_date = sync_date[0]
    log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}

    log = logging.getLogger("rebuild_spark_table")
    log.setLevel(logging.INFO)
    logfile_name = logfile_dir
    just_write_table = True
    fh = SCFileHandler(logfile_name, log_table, log_connect, just_write_table)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    log.addHandler(fh)

    #---------------------------------------------------
    # multi thread
    #---------------------------------------------------
    table_lock = Lock()
    table_queue = Queue()
    cond = {}
    order_by = {"tbl_name": 1}
    selector = {"tbl_name": ""}
    tbl_names = db.sync_query(config_cs, config_cl,
                              cond, selector, order_by)
    for tbl_name in tbl_names:
        table_queue.put(tbl_name)
    t = [0] * int(thread_num)
    log.info("begin to rebuild spark table. table queue: [%s]" % tbl_names)
    sync_spark = SyncRebuildSpark(hosts, connect_hosts, hostname, svcport, username, password, db, log)
    for num in range(thread_num):
        t[num] = Thread(target=sync_spark.rebuild_sql_table, args=(table_queue, table_lock,file_name))
        t[num].start()

    for num in range(thread_num):
        t[num].join()

    log.info("finish to rebuild spark table")

if __name__ == "__main__":
    main()
