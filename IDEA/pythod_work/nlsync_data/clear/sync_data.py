#!/usr/bin/python
#coding=utf-8

from multiprocessing import Process, Queue, Pipe, Lock, Value, Array, Manager
from util import *
from sync_sdb import *
import time
import os
from config.global_config import *
from optparse import OptionParser
import setproctitle
import traceback



class SyncData:
    def __init__(self, tbl_queue, process_num):
        self.hostname = host_name
        self.svcport = server_port
        self.username = user_name
        self.password = password
        self.config_cs = sync_config_cs
        self.config_cl = sync_config_cl
        self.local_cs = sync_local_cs
        self.local_cl = sync_local_cl
        self.sparkloc_cs = sync_sparkloc_cs
        self.sparkloc_cl = sync_sparkloc_cl
        self.log_cs = sync_log_cs
        self.log_cl = sync_log_cl
        self.tbl_queue = tbl_queue
        self.tbl_name = tbl_queue[1]
        self.process_num = process_num
        # argument for is run spark 
        self.is_spark_run = is_spark_run

        # SDB collection connection
        self.db_hosts = hosts
        self.connect_hosts = []
        for db_host in self.db_hosts.split(','):
            host_info = db_host.split(':')
            connect_info = {'host': host_info[0], 'service': host_info[1]}
            self.connect_hosts.append(connect_info)
        self.db = SCSDB(self.hostname, self.svcport, self.username, self.password, self.connect_hosts)
 
        # LOG handler
        self.tbl_cond = {'tbl_name': self.tbl_name}
        log_connect = {'HostName': self.hostname, 'ServerPort': self.svcport,
                       'UserName': self.username, 'Password': self.password, 
                       'CsName': self.log_cs, 'ClName': self.log_cl}
        cnf_records = self.db.sync_query(self.config_cs, self.config_cl, self.tbl_cond)
        if 0 == len(cnf_records):
            print 'the table: %s have not in sync.config when init class' % self.tbl_name
            raise
        sync_sys = cnf_records[0]['sync_sys']
        sync_date = self.get_sync_date()
        print '[sync_date]: %s' % sync_date
        sync_date = sync_date[0]
        log_table = {'sync_sys': sync_sys, 'tbl_name': self.tbl_name, 'sync_dt': sync_date}

        self.log = logging.getLogger("sync_data")
        self.log.setLevel(logging.INFO)
        logfile_name = logfile_dir
        just_write_table = True
        fh = SCFileHandler(logfile_name, log_table, log_connect, just_write_table)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.log.addHandler(fh)

    def get_sync_date(self):
        cnf_records = self.db.sync_query(self.config_cs, self.config_cl, self.tbl_cond)
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
        his_records = self.db.sync_query(sync_history_cs,
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


    def run_sync(self):
        """
        DESC: 数据同步任务进程
        OPTS:
        """
        tbl_queue = self.tbl_queue
        process_num = self.process_num
        db = self.db
        self.log.info('table queue: %s, length: %s' % (tbl_queue, len(tbl_queue)))
        if 0 == len(tbl_queue):
            return

        #for tbl in tbl_queue:
        tbl = tbl_queue
        sync_sys = tbl[0]
        sync_tbl = tbl[1]
        cs_name = sync_tbl.split(".")[0]
        cl_name = sync_tbl.split(".")[1]
        loc_cond = {'tbl_name': sync_tbl}
        self.log.info('sync table condition: %s' % loc_cond)
        loc_filter = {}

        # add can connect multi hosts
        loc_records = db.sync_query(self.local_cs, self.local_cl, loc_cond, loc_filter)

        # set process name
        sync_dt = loc_records[0].get('sync_dt')
        hostname = socket.gethostname()
        process_name = 'SYNC|%s|%s|%s|%s' % (hostname, sync_sys, sync_tbl, sync_dt)
        setproctitle.setproctitle(process_name)
        self.log.info("table: %s sync Process Name: %s" % (sync_tbl, process_name))

        # When start sync process job, we must update process pid at first time
        self.log.info("table: %s sync PID: %s" % (sync_tbl, str(os.getpid())))
        pid_ruler = {"$set": {"proc_info": hostname + "," + str(os.getpid())}}
        db.sync_update(self.local_cs, self.local_cl, pid_ruler, loc_cond)


        # make sure table is running or not
        is_tbl_run = False
        self.log.info("initialize local record: %s" % loc_records)
        for loc_record in loc_records:
            if sync_tbl == loc_record['tbl_name'] and ('running' == loc_record.get('sync_st') or
                                                       'failed' == loc_record.get('sync_st') or
                                                       'success' == loc_record.get('sync_st')):
                is_tbl_run = True
                break

        #print "EXIST TABLE: %s" % loc_records
        if is_tbl_run is True:
            self.log.warn("Table %s is Running, please run next table!" % sync_tbl)
            return

        # update sync data job's status is running
        update_st = {"$set": {"sync_st": "running"}}
        db.sync_update(self.local_cs, self.local_cl, update_st, loc_cond)

        # add here, for import data(append_cus and append;_his)
        tbl_cond = {'tbl_name': sync_tbl}

        tbl_cnf = db.sync_query(self.config_cs, self.config_cl, tbl_cond)
        tbl_import = tbl_cnf[0].get('iprt_cmd')
        tbl_type = tbl_cnf[0].get('sync_type')
        export_options = tbl_cnf[0].get("expt_cmd")

        # config argument: is_sync
        is_sync = tbl_cnf[0].get('is_sync')

        if 'false' == is_sync:
            self.log.warn("Table %s don't need to synchronous!" % sync_tbl)
            return

        # config argument: sync_new
        sync_new = tbl_cnf[0].get('sync_new')

        # when the table is not running, we go
        sync = SYNC(sync_tbl, self.hostname, self.svcport, self.username,
                    self.password, self.connect_hosts, self.log)

        tbl_cond = {"tbl_name": sync_tbl}
        # make sure table info can write into nlsync.local table
        while True:
            loc_check = db.sync_query(self.local_cs, self.local_cl, tbl_cond)
            if 0 == len(loc_check):
                continue
            else:
                self.log.info("SUCCESS TO INSERT PROCCESS TABLE")
                break

        # here is stream for sync data
        sync_config = SyncConfig(self.hostname, self.svcport, self.username, self.password,
                                 self.connect_hosts, cs_name, cl_name, self.log)
        rely_tbls = sync_config.get_rely_table_list()
        print 'rely tables: %s' % rely_tbls
        if rely_tbls is not None:
            while True:
                count = 0

                for rely_tbl in rely_tbls:
                    rely_tbl_cs = rely_tbl.split(".")[0]
                    rely_tbl_cl = rely_tbl.split(".")[1]
                    sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                                           self.connect_hosts, rely_tbl_cs, rely_tbl_cl, self.log)
                    if 'success' == sync_local.get_sync_status():
                        count += 1
                    elif 'failed' == sync_local.get_sync_status():
                        print 'table: %s rely table: %s is failed' % (sync_tbl, rely_tbl)
                        sync.update_sync_status("failed")
                        return
                        
                    # wait 5 seconds
                    time.sleep(5)
                    print 'table: %s rely on table: %s run over' % (sync_tbl, rely_tbl)

                # when rely table run over, we break
                if len(rely_tbls) == count:
                    print '==============>rely table: %s, status: %s' % (rely_tbl, sync_local.get_sync_status())
                    break
        # 0.更新元数据
        is_update_meta = sync.sync_initialize_metatbl()
        if is_update_meta is False:
            sync.update_sync_status("failed")
            self.log.error("Table %s file get and check is failed!" % sync_tbl)
            return
            
        # 1.源数据文件准备及检验
        is_file_ok = sync.file_get_check()
        if is_file_ok is False:
            sync.update_sync_status("failed")
            self.log.error("Table %s file get and check is failed!" % sync_tbl)
            return

        # 2.创建数据表
        if 'true' != sync_new:
            is_crtbl_ok = sync.create_synctbl()
            if is_crtbl_ok is False:
                sync.update_sync_status("failed")
                self.log.error("Table %s create table is failed!" % sync_tbl)
                return
        elif 'true' == sync_new and 'full' == tbl_type:
            is_crtbl_ok = sync.create_synctbl()
            if is_crtbl_ok is False:
                sync.update_sync_status("failed")
                self.log.error("Table %s create table is failed!" % sync_tbl)
                return

        #等待10秒
        time.sleep(60)

        # 3.源数据文件GBK转码为UTF8
        transcode_path = transcode_paths[int(self.process_num)]
        is_transcode_ok = sync.file_parse_transcd(transcode_path)
        if is_transcode_ok is False:
            sync.update_sync_status("failed")
            self.log.error("Table %s's file transcode is failed!" % sync_tbl)
            return


        # 4.数据导入操作，失败重试次数为3次
        retry_interval_times = 3
        for retry_time in range(retry_interval_times):
            import_ret = sync.data_import()
            is_import_ok = import_ret[0]
            rec_file = import_ret[1]
            if is_import_ok is True:
                break

        if is_import_ok is False:
            print 'import return : %s' % import_ret
            sync.update_sync_status("failed")
            self.log.error("Table %s's file import is failed!" % sync_tbl)
            return

        # upsert
        if rec_file is not None:
            for retry_time in range(retry_interval_times):
                if os.path.isfile(rec_file) is True:
                    is_upsert_ok = sync.data_upsert(rec_file)
                    print 'table: %s upsert status: %s' % (sync_tbl, is_upsert_ok)
                    if is_upsert_ok is True:
                        break
                else:
                    print "rec_file: %s don't exitst" % rec_file
                    break

            if is_upsert_ok is False:
                sync.update_sync_status("failed")
                self.log.error("Table %s file upsert failed!" % sync_tbl)
                return

        # export
        if export_options is not None and '' != export_options:
            for retry_time in range(retry_interval_times):
                is_export_ok = sync.data_export()
                if is_export_ok is True:
                    break

            if is_export_ok is False:
                sync.update_sync_status("failed")
                self.log.error("Table %s file export failed!" % sync_tbl)
                return

        """
        # except full sync table, the others table sync success
        if ('append_cus' == tbl_type or 'append_his' == tbl_type or
            'append_his_dp' == tbl_type or 'append_his_update' == tbl_type or 
            'append_his_insert' == tbl_type) and 'true' == sync_new:
            sync.update_sync_status("success")
            self.log.info('Table %s file import and upsert success' % sync_tbl)
            return
        # 5.数据更新操作(等待spark任务进程, 使用Spark-SQL)
        tbl_cond = {'tbl_name': sync_tbl}
        cnf_rd = db.sync_query(sync_config_cs, sync_config_cl, tbl_cond)
        tbl_type = cnf_rd[0]['sync_type']
        if 'full' != tbl_type and 'true' != sync_new:
            is_update_ok = sync.data_update()
            if is_update_ok is False:
                sync.update_sync_status("failed")
                self.log.error("Table %s file update is failed!" % sync_tbl)
                return
        """

        # 6.创建索引
        if 'full' == tbl_type:
            is_crtidx_ok = sync.create_index()
            if is_crtidx_ok is False:
                sync.update_sync_status("failed")
                self.log.error("Table %s file create index is failed!" % sync_tbl)
                return

        # 7.卸载挂载主子表(仅有全量表可以)
        if 'full' == tbl_type:
            is_dtat_ok = sync.detach_attach()
            if is_dtat_ok is False:
                sync.update_sync_status("failed")
                self.log.error("Table %s file detach attach is failed!" % sync_tbl)
                return

        # going to end, we update the status success
        # update sync data job's status is running
        sync.update_sync_status("success")

        # 7.清理数据
        sync.clear_file()

def sync_data_main():
    """
    参数选项说明:
    sync_tables 数据同步表：数据同步的系统名+表名
    """
    parser = OptionParser()
    parser.add_option("", "--sync_tables",
                      dest='sync_tables',
                      default='',
                      help='run synchronous tables, such as "SYS:sys.cat,SYS:sys.data"')
    parser.add_option("", "--run_proc_number",
                      dest='run_proc_number',
                      default='0',
                      help='run synchronous process number, such as "0"')

    (options, args) = parser.parse_args()
    # get input options
    sync_tables = options.sync_tables
    run_proc_number = options.run_proc_number
    # connection
    hostname = host_name
    svcport = server_port
    username = user_name
    passwd = password
    local_cs = sync_local_cs
    local_cl = sync_local_cl

    # SDB collection connection
    db_hosts = hosts
    connect_hosts = []
    for db_host in db_hosts.split(','):
        host_info = db_host.split(':')
        connect_info = {'host': host_info[0], 'service': host_info[1]}
        connect_hosts.append(connect_info)
    db = SCSDB(hostname, svcport, username, passwd, connect_hosts)

    print 'sync tables: %s, run proccess number: %s' % (sync_tables, run_proc_number)
    if '' != sync_tables:
        sync_tbls = sync_tables.split(',')
        for sync_tbl in sync_tbls:
            sync_queue = []
            sync_opts = sync_tbl.split(':')
            print 'run sync data table: %s' % sync_tbl
            sync_queue.append(sync_opts[0])
            sync_queue.append(sync_opts[1])
            sync_sys = sync_opts[0]
            tbl_name = sync_opts[1]
            table_cond = {"tbl_name": tbl_name}
            try:
                sync_data = SyncData(sync_queue, run_proc_number)
                """
                logging file
                """
                tbl_cond = {'tbl_name': tbl_name}
                log_connect = {'HostName': hostname, 'ServerPort': svcport,
                               'UserName': username, 'Password': password, 
                               'CsName': sync_log_cs, 'ClName': sync_log_cl}
                sync_date = sync_data.get_sync_date()
                print '[sync_date]: %s' % sync_date
                sync_date = sync_date[0]
                log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}

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
                sync running main
                """
                begin_time = str(datetime.datetime.now())
                update_st = {"$set": {"begin_tm": begin_time}}
                db.sync_update(local_cs, local_cl, update_st, table_cond)
                log.info("begin to synchronous data for table: %s on date: %s" % (tbl_name, sync_date))
                sync_data.run_sync()
            except Exception, e:
                table_ruler = {"$set": {"sync_st": "failed"}}
                db.sync_update(local_cs, local_cl, table_ruler, table_cond)
                print traceback.format_exc()
                log.info("failed to synchronous data, exception is [%s]" % traceback.format_exc())
            finally:
                end_time = str(datetime.datetime.now())
                update_st = {"$set": {"end_tm": end_time}}
                db.sync_update(local_cs, local_cl, update_st, table_cond)
                log.info("finish to synchronous data for table: %s on date: %s" % (tbl_name, sync_date))
                
    else:
        print 'synchronous data have not specify table!'


if __name__ == '__main__':
    sync_data_main()
