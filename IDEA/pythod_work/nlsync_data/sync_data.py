#!/usr/bin/python
#coding=utf-8

from multiprocessing import Process, Queue, Pipe, Lock, Value, Array, Manager
from util import *
from sync_sdb import *
from sync_clear import *
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
        loc_records = self.db.sync_query(self.local_cs, self.local_cl, self.tbl_cond)
        if 0 == len(loc_records):
            print 'the table: %s have not in sync.config when init class' % self.tbl_name
            raise
        self.sync_date = loc_records[0]['sync_dt']
        print 'util - [sync_date]: %s' % self.sync_date
        log_table = {'sync_sys': sync_sys, 'tbl_name': self.tbl_name, 'sync_dt': self.sync_date}

        self.log = logging.getLogger("sync_data")
        self.log.setLevel(logging.INFO)
        logfile_name = logfile_dir
        just_write_table = True
        fh = SCFileHandler(logfile_name, log_table, log_connect, just_write_table)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.log.addHandler(fh)

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
        # add can connect multi hosts
        sync_local = SyncLocal(self.hostname, self.svcport, self.username, self.password,
                               self.connect_hosts, cs_name, cl_name, self.log)
        # get sync date
        sync_dt = sync_local.get_sync_date()
        # get sync status
        sync_st = sync_local.get_sync_status()
        # get batch date
        batch_dt = sync_local.get_batch_date()

        # set process name
        hostname = socket.gethostname()
        process_name = 'SYNC|%s|%s|%s|%s' % (hostname, sync_sys, sync_tbl, sync_dt)
        setproctitle.setproctitle(process_name)
        self.log.info("table: %s sync Process Name: %s" % (sync_tbl, process_name))

        # When start sync process job, we must update process pid at first time
        self.log.info("table: %s sync PID: %s" % (sync_tbl, str(os.getpid())))
        pid_ruler = {"$set": {"proc_info": hostname + "," + str(os.getpid())}}
        db.sync_update(self.local_cs, self.local_cl, pid_ruler, loc_cond)

        # make sure table is running or not
        self.log.info("initialize local record: %s" % sync_local.get_sync_all_info())
        if 'running' == sync_st or 'failed' == sync_st or 'success' == sync_st:
            self.log.warn("table %s have run, info: %s."
                          "please run next table!" % (sync_tbl, sync_local.get_sync_all_info()))
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
        is_sync_meta = sync_config.get_is_sync_meta()
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
                        self.log.error('table: %s rely table: %s is failed' % (sync_tbl, rely_tbl))
                        sync.update_sync_status("failed")
                        return
                    elif sync_local.get_sync_status() is None:
                        print 'table: %s rely table: %s is failed, ' \
                              'sync status: %s' % (sync_tbl, rely_tbl,
                                                   sync_local.get_sync_status())
                        self.log.error('table: %s rely table: %s is failed, '
                                       'sync status: %s' % (sync_tbl, rely_tbl,
                                                            sync_local.get_sync_status()))
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
        if is_sync_meta != "false":
            is_update_meta = sync.sync_initialize_metatbl(sync_dt)
            if is_update_meta is False:
                sync.update_sync_status("failed")
                self.log.error("table %s synchronous data failed" % sync_tbl)
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
        retry_transcode_times = 5
        retry_tran_time = 0
        while True:
            transcode_path = transcode_paths[int(self.process_num)]
            is_transcode_ok = sync.file_parse_transcd(transcode_path)
            if is_transcode_ok is False and retry_transcode_times <= retry_tran_time:
                sync.update_sync_status("failed")
                self.log.error("Table %s's file transcode is failed!" % sync_tbl)
                return
            retry_tran_time += 1
            if is_transcode_ok is True:
                break
            else:
                # 根据转码的情况，将元数据同步根据当前日期向后同步，适配ODS元数据结构变更问题
                add_date = retry_tran_time
                sync.sync_initialize_metatbl(sync_dt, add_date)

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


def get_local_unique_dirs(db):
    local_host = socket.gethostname()
    local_run_cond = {"proc_info": {"$regex": "^%s.*" % local_host}}
    local_run_selector = {"tran_file": ""}
    loc_records = db.sync_query(sync_local_cs,
                                sync_local_cl,
                                condition=local_run_cond,
                                selector=local_run_selector,
                                order_by={}, hint={}, skip=0L,
                                num_to_return=-1L)
    local_unique_dirs = []
    for loc_record in loc_records:
        tran_dir = os.path.dirname(loc_record.get("tran_file"))
        if tran_dir not in local_unique_dirs:
            local_unique_dirs.append(tran_dir)

    return local_unique_dirs



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
    parser.add_option("", "--run_mode",
                      dest='run_mode',
                      default='batch',
                      help='run synchronous mode, batch/single')
    parser.add_option("", "--sync_date",
                      dest='sync_date',
                      default='',
                      help='run synchronous date, mode: YYYYMMDD')

    (options, args) = parser.parse_args()
    # get input options
    sync_tables = options.sync_tables
    run_proc_number = options.run_proc_number
    run_mode = options.run_mode
    op_sync_date= options.sync_date
    # connection
    hostname = host_name
    svcport = server_port
    username = user_name
    passwd = password
    local_cs = sync_local_cs
    local_cl = sync_local_cl
    config_cs = sync_config_cs
    config_cl = sync_config_cl

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

            # if system name equal CLEAR, table name equal CLEAR.ALL
            if "CLEAR" == sync_sys and "CLEAR.ALL" == tbl_name:
                """
                logging file
                """
                # get nearly date in local table
                selector = {"sync_dt": ""}
                orderby = {"sync_dt": -1}
                num_to_return = 1L
                loc_records = db.sync_query(sync_local_cs,
                                            sync_local_cl,
                                            condition={},
                                            selector=selector,
                                            order_by=orderby,
                                            hint={}, skip=0L,
                                            num_to_return=num_to_return)
                clear_sync_date = loc_records[0].get("sync_dt")
                
                tbl_cond = {'tbl_name': tbl_name}
                log_connect = {'HostName': hostname, 'ServerPort': svcport,
                               'UserName': username, 'Password': password, 
                               'CsName': sync_log_cs, 'ClName': sync_log_cl}
                sync_date = datetime.datetime.now().strftime("%Y%m%d") 
                print '[sync_date]: %s' % sync_date
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

                # clear all temp file
                log.info("begin to clear temp files")
                sync_clear = SyncClear(sync_file=None, sync_date=None, log_handler=log)
                unique_dirs = get_local_unique_dirs(db)
                sync_clear.run_clear_temp_file(transcode_paths, upsert_home,
                                               store_data_dirs, delta_day_clear,
                                               clear_sync_date, unique_dirs)
                log.info("finish clear all temp file in host: %s" % socket.gethostname())
                return

            table_cond = {"tbl_name": tbl_name}
            sync_data = SyncData(sync_queue, run_proc_number)
            try:
                # when single mode, init table
                local_rd = db.sync_get_count(local_cs, local_cl, condition=table_cond)
                config_rd = db.sync_query(config_cs, config_cl, condition=table_cond, selector={"sync_type": "--"})
                if 0 == local_rd and run_mode == "single":
                    if '' == op_sync_date:
                        print "Please give synchronose date, mode YYYYMMDD"
                        return
                    else:
                        sync_type = config_rd[0]["sync_type"]
                        init_info = {"sync_sys": sync_sys, "tbl_name": tbl_name, "sync_dt": op_sync_date,
                                     "sync_st": "wait", "sync_type": sync_type}
                        # init insert
                        db.sync_insert(local_cs, local_cl, init_info)
                else:
                    if '' != op_sync_date:
                        update_cond = {"$set": {"sync_st": "wait", "sync_dt": op_sync_date}}
                        # init update
                        db.sync_update(local_cs, local_cl, update_cond, table_cond)


                """
                logging file
                """
                tbl_cond = {'tbl_name': tbl_name}
                log_connect = {'HostName': hostname, 'ServerPort': svcport,
                               'UserName': username, 'Password': password, 
                               'CsName': sync_log_cs, 'ClName': sync_log_cl}
                sync_date = sync_data.sync_date
                print '[sync_date]: %s' % sync_date
                log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}
                log = logging.getLogger("sync_data_main")
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
