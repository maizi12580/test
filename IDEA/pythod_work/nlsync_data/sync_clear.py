#!/usr/bin/python
#coding=utf-8

from config.global_config import *
from sync_sdb import *
import os
import shutil
from datetime import timedelta
import datetime
from sync_os import *


class SyncClear:
    def __init__(self, sync_file=None, sync_date=None, log_handler=None):
        self.sync_file = sync_file
        self.sync_date = sync_date
        #self.sync_os = sync_os
        self.log = log_handler
        self.data_num = int(sync_data_num)
        #self.data_home_dir = sync_data_dir
        self.sync_os = SyncOS(self.log)

    def get_clear_dirs(self):
        # clear file date
        sync_dt = self.sync_date
        date_fm = '%Y%m%d'
        time_delta = timedelta(days=self.data_num)
        # Here we decide clear data, if backup
        clear_dt = datetime.datetime.strptime(sync_dt, date_fm)
        clear_date = (clear_dt - time_delta).strftime(date_fm)
        remove_dirs = []
        data_home_dir = self.sync_file.split(self.sync_date)[0]
        if os.path.isdir(data_home_dir) is True:
            data_dirs = os.listdir(data_home_dir)
            for data_dir in data_dirs:
                try:
                    date_dir = int(data_dir)
                    clear_date_dir = int(clear_date)
                    if date_dir <= clear_date_dir:
                        remove_dir = '%s/%s' % (data_home_dir, date_dir)
                        remove_dirs.append(remove_dir)

                except ValueError, e:
                    print '[The file "%s" is not regular date synchronous directory]' % data_dir
        else:
            if self.log is None:
                print '[Clear directory Don\'t exists: %s]' % (data_home_dir)
            else:
                self.log.info('[Clear directory Don\'t exists: %s]' % (data_home_dir))

        return remove_dirs

    def clear_data(self):
        clear_dirs = self.get_clear_dirs()
        if self.log is not None:
            self.log.info('clear directorys: %s' % clear_dirs)
        else:
            print 'clear directorys: %s' % clear_dirs
        file_suffix = self.sync_file.split(self.sync_date)[-1]
        for clear_dir in clear_dirs:
            abs_data_file = "%s/%s" % (clear_dir, file_suffix)
            # remove data file
            self.sync_os.remove_file(abs_data_file)
            # remove ok file
            abs_ok_file = abs_data_file.split(".")[0] + ".ok"
            self.sync_os.remove_file(abs_ok_file)
            if self.log is not None:
                self.log.info('clear data file: %s, ok file: %s' % (abs_data_file, abs_ok_file))
            else:
                print 'clear data file: %s, ok file: %s' % (abs_data_file, abs_ok_file)

    # here clear sync temp file
    def run_clear_temp_file(self, transcode_paths, upsert_home, store_data_dirs,
                            delta_day_clear, clear_sync_date, run_unique_dirs):
        sync_os = SyncOS(self.log)
        # 1. transcode path
        for transcode_path in transcode_paths:
            for trans_file in os.listdir(transcode_path):
                trans_file = "%s/%s" % (transcode_path, trans_file)
                if "upsert_rec" == trans_file or "import_rec" == trans_file:
                    continue
                print "trans_file: %s" % trans_file
                sync_os.remove_file(trans_file)
                self.log.info("clear transcode temp file and log file, file: %s" % trans_file)

        # 2. sdbupsert path
        log_conf_file = '%s/conf/log4j.properties' % upsert_home
        sdbupsert_log_dirs = []
        log_conf_fd = open(log_conf_file)
        for log_conf in log_conf_fd:
            if -1 != log_conf.find("log4j.appender.err.File") or \
               -1 != log_conf.find("log4j.appender.rec.File") or \
               -1 != log_conf.find("log4j.appender.log.File"):
                file = log_conf.split("=")[1]
                dir = os.path.dirname(file)
                if dir not in sdbupsert_log_dirs:
                    sdbupsert_log_dirs.append(dir)

        for upsert_log_dir in sdbupsert_log_dirs:
            for upsert_log_file in os.listdir(upsert_log_dir):
                upsert_log_file = "%s/%s" % (upsert_log_dir, upsert_log_file)
                print "upsert_log_file: %s" % upsert_log_file
                sync_os.remove_file(upsert_log_file)
                self.log.info("clear sdbupsert log file, file: %s" % upsert_log_file)

        # 3. sdbimport.log and sdbexport.log file clear
        for run_dir in run_unique_dirs:
            sdbimport_log = "%s/sdbimport.log" % run_dir
            sync_os.remove_file(sdbimport_log)
            sdbexport_log = "%s/sdbexport.log" % run_dir
            sync_os.remove_file(sdbexport_log)
        self.log.info("clear sdbimport.log and sdbexport.log in "
                      "director: %s" % run_unique_dirs)

        # 4. etldata store path
        defaut_num = '31'
        date_fmt = "%Y%m%d"
        if int(delta_day_clear) < int(defaut_num):
            data_num = defaut_num
        else:
            data_num = delta_day_clear         # global_conf
        sync_file_dirs = store_data_dirs   # global_conf
        time_delta = timedelta(days=int(data_num))
        clear_date = (datetime.datetime.strptime(clear_sync_date, date_fmt) - time_delta).strftime(date_fmt)
        print "clear date: %s" % clear_date
        self.log.info("clear date: %s" % clear_date)
        if type(sync_file_dirs) is list:
            for sync_file_dir in sync_file_dirs:
                if os.path.isdir(sync_file_dir) is True:
                    data_dirs = os.listdir(sync_file_dir)
                    for data_dir in data_dirs:
                        try:
                            date_dir = int(data_dir)
                            clear_date_dir = int(clear_date)
                            if date_dir <= clear_date_dir:
                                remove_dir = '%s/%s' % (sync_file_dir, date_dir)
                                print '>>>Begin to clear file: %s' % (remove_dir)
                                self.log.info('>>>Begin to clear file: %s' % (remove_dir))
                                sync_os.remove_directory(remove_dir)
                                print '<<<Success to clear file: %s' % (remove_dir)
                                self.log.info('<<<Success to clear file: %s' % (remove_dir))
                        except ValueError, e:
                            print '[The file "%s" is not regular date synchronous directory]' % data_dir
                            self.log.info('[The file "%s" is not regular date synchronous directory]' % data_dir)
                else:
                    print '[Clear directory Don\'t exists: %s]' % (sync_file_dir)
                    self.log.info('[Clear directory Don\'t exists: %s]' % (sync_file_dir))

if __name__ == '__main__':
    #sync_file = '/asrly/etldata/odsdata/20180327/SMC/GCPOPLT_ALL.dat'
    #sync_date = '20180327'
    #sync_clear = SyncClear(sync_file, sync_date)
    #sync_clear.clear_data()
    hostname = host_name
    svcport = server_port
    username = user_name
    password = password
    db_hosts = hosts
    connect_hosts = []
    for db_host in db_hosts.split(','):
        host_info = db_host.split(':')
        connect_info = {'host': host_info[0], 'service': host_info[1]}
        connect_hosts.append(connect_info)
    db = SCSDB(hostname, svcport, username, password, connect_hosts)
    tbl_cond = {}
    select_cond = {'sync_file': 1, 'sync_dt': 1}
    loc_records = db.sync_query(sync_local_cs, sync_local_cl, tbl_cond, select_cond)
    #print loc_records
    for loc_record in loc_records:
        sync_file = loc_record.get('sync_file')
        sync_date = loc_record.get('sync_dt')
        print 'sync file: %s, sync date: %s' % (sync_file, sync_date)
        sync_clear = SyncClear(sync_file, sync_date)
        sync_clear.clear_data()
    
    


