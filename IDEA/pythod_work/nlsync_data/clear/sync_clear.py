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
    def __init__(self, sync_file, sync_date, log_handler=None):
        self.sync_file = sync_file
        self.sync_date = sync_date
        #self.sync_os = sync_os
        self.log = log_handler
        self.data_num = int(sync_data_num)
        self.data_home_dir = sync_data_dir
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
        if os.path.isdir(self.data_home_dir) is True:
            data_dirs = os.listdir(self.data_home_dir)
            for data_dir in data_dirs:
                try:
                    date_dir = int(data_dir)
                    clear_date_dir = int(clear_date)
                    if date_dir <= clear_date_dir:
                        remove_dir = '%s/%s' % (self.data_home_dir, date_dir)
                        remove_dirs.append(remove_dir)

                except ValueError, e:
                    print '[The file "%s" is not regular date synchronous directory]' % data_dir
        else:
            if self.log is None:
                print '[Clear directory Don\'t exists: %s]' % (self.data_home_dir)
            else:
                self.log.info('[Clear directory Don\'t exists: %s]' % (self.data_home_dir))

        return remove_dirs

    def clear_data(self):
        clear_dirs = self.get_clear_dirs()
        file_suffix = self.sync_file.split(self.sync_date)[-1]
        for clear_dir in clear_dirs:
            abs_data_file = "%s/%s" % (clear_dir, file_suffix)
            # remove data file
            self.sync_os.remove_file(abs_data_file)
            # remove ok file
            abs_ok_file = abs_data_file.split(".")[0] + ".ok"
            self.sync_os.remove_file(abs_ok_file)

if __name__ == '__main__':
    sync_file = '/data03/etldata/odsdata/20170302/CBE/BPTFHISA.dat'
    sync_date = '20170302'
    sync_clear = SyncClear(sync_file, sync_date)
    sync_clear.clear_data()


