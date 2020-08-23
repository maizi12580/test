#!/usr/bin/python
#coding=utf-8

from config.global_config import *
from sync_sdb import *
import os
import shutil
from datetime import timedelta
import datetime


def make_directory(dirname):
    """
    make directory
    Args:
        dirname: the making directory's name

    Returns:

    """
    try:
        if False == os.path.isdir(dirname):
            os.mkdir(dirname)
    except OSError, e:
        print "make directory: %s failed! error info: %s" % (dirname, e)
        raise

def remove_file(filename):
    """
    remove file
    Args:
        filename: the remove file's name

    Returns:

    """
    try:
        if True == os.path.isfile(filename):
            os.remove(filename)
    except OSError, e:
        print "remove file: %s failed! error info: %s" % (filename, e)
        raise

def remove_directory(dirname):
    """
    remove directory
    Args:
        dirname: remove direcotry's name

    Returns:

    """
    try:
        if True == os.path.isdir(dirname):
            shutil.rmtree(dirname)
    except OSError, e:
        print "remove file: %s failed! error info: %s" % (dirname, e)
        raise

def move_directory(filename, dest_directory):
    """
    move file
    Args:
        filename: the moving file's name

    Returns:

    """
    try:
        if True == os.path.isdir(filename) and True == os.path.isdir(dest_directory):
            shutil.move(filename, dest_directory)
    except IOError, e:
        print "remove file: %s failed! error info: %s" % (filename, e)
        raise

def get_sync_dt():
    db = SCSDB(host_name, server_port, user_name, password)
    config_cs = sync_config_cs
    config_cl = sync_config_cl
    local_cs = sync_local_cs
    local_cl = sync_local_cl
    success_cond = {'sync_st': 'success'}
    config_cond = {'is_sync': {'$ne': 'false'}}
    agg_arg = []
    agg_arg1 = {"$group": {"_id": {"sync_dt": "$sync_dt"}}}
    agg_arg2 = {"$project": {"sync_dt": 1}}
    agg_arg3 = {"$sort": {"sync_dt": -1}}
    agg_arg.append(agg_arg1)
    agg_arg.append(agg_arg2)
    agg_arg.append(agg_arg3)
    
    config_cnt = db.sync_get_count(config_cs, config_cl, config_cond)
    success_cnt = db.sync_get_count(local_cs, local_cl, success_cond)
    # when the sum of running success and running failed is equal total config data, we run spark over!
    sync_date = []
    print 'sync_date: %s, sync_date2: %s' % (config_cnt, success_cnt)
    if config_cnt == success_cnt:
        local_rds = db.sync_aggregate(sync_local_cs, sync_local_cl, agg_arg)
        for rd in local_rds:
            sync_date.append(rd['sync_dt'])

    print 'get synchronous data\'s date: %s' % sync_date
    return sync_date

def clear_data():
    # clear file date
    data_num = int(sync_data_num)
    data_home_dir = sync_data_dir
    date_fm = '%Y%m%d'
    sync_dt = get_sync_dt()
    if 0 == len(sync_dt):
        print '<<<No Clear Data!>>>'
        return
    
    time_delta = timedelta(days=data_num)
    # Here we decide clear data, if backup
    if 0 < data_num:
        clear_dt = datetime.datetime.strptime(sync_dt[0], date_fm)
        clear_date = (clear_dt - time_delta).strftime(date_fm)
        if os.path.isdir(data_home_dir) is True:
            data_dirs = os.listdir(data_home_dir)
            for data_dir in data_dirs:
                try:
                    date_dir = int(data_dir)
                    clear_date_dir = int(clear_date)
                    if date_dir <= clear_date_dir:
                        remove_dir = '%s/%s' % (data_home_dir, date_dir)
                        print '>>>Begin to clear file: %s' % (remove_dir)
                        remove_directory(remove_dir)
                        print '<<<Success to clear file: %s' % (remove_dir)

                except ValueError, e:
                    print '[The file "%s" is not regular date synchronous directory]' % data_dir
        else:
            print '[Clear directory Don\'t exists: %s]' % (data_home_dir)
    elif 0 == data_num:
        if os.path.isdir(data_home_dir) is True:
            data_dirs = os.listdir(data_home_dir)
            for data_dir in data_dirs:
                for rm_data_dir in sync_dt:
                    try:
                        rm_date_dir = int(rm_data_dir)
                        date_dir = int(data_dir)
                        if date_dir == rm_date_dir:
                            remove_dir = '%s/%s' % (data_home_dir, date_dir)
                            print '>>>Begin to clear file: %s' % (remove_dir)
                            remove_directory(remove_dir)
                            print '<<<Success to clear file: %s' % (remove_dir)

                    except ValueError, e:
                        print 'The file "%s" is not regular date synchronous directory' % data_dir
        else:
            print '[Clear directory Don\'t exists: %s]' % (data_home_dir)
            
    #print 'Success to clear file in synchronous date: %s' 

def backup_file():
    backup_data_num = 1
    backup_data_dir = 1
    syncdir_num = sync_data_num
    syncdir = sync_data_dir

    clear_date = get_sync_dt()

    date_fm = '%Y%m%d'
    sync_dtm = datetime.datetime.strptime(clear_date, date_fm)
    backup_data_delta = timedelta(days=int(backup_data_num))
    syncdir_delta = timedelta(days=int(syncdir_num))
    clear_dir_dtm = sync_dtm - (backup_data_delta + syncdir_delta)
    move_dir_dtm = sync_dtm - syncdir_delta


    # 1.move sync directory to backup directory
    move_data_dir = move_dir_dtm.strftime(date_fm)
    # list backup directory
    if False == os.path.isdir(backup_data_dir):
        make_directory(backup_data_dir)
    dir_files = os.listdir(syncdir)
    for dir_file in dir_files:
        try:
            abs_data_dir = '%s/%s' % (syncdir, dir_file)
            bak_data_dir = '%s/%s' % (backup_data_dir, dir_file)
            # if <bak_data_dir> is directory,  we remove it; if <bak_data_dir> is file, we backup it
            if os.path.isdir(bak_data_dir) is True: 
                remove_directory(bak_data_dir)
            elif os.path.isfile(bak_data_dir) is True:
                move_directory(bak_data_dir, bak_data_dir + '.bak') 
            else:
                print 'Don\'t have file or directory: %s' % bak_data_dir

            if os.path.isdir(abs_data_dir) is True and int(dir_file) <= int(move_data_dir):
                move_directory(abs_data_dir, backup_data_dir)
                print 'Now sync date is: %s! Success to move directory: %s to %s' % (clear_date,
                                                                                     abs_data_dir,
                                                                                     backup_data_dir)
        except ValueError, e:
            print 'The file "%s" is not regular date synchronous directory' % dir_file

    # 2.clear data when backup directory have greater than backup data number
    clear_data_dir = clear_dir_dtm.strftime(date_fm)
    # list backup directory
    dir_files = os.listdir(backup_data_dir)
    for dir_file in dir_files:
        try:
            abs_data_dir = '%s/%s' % (backup_data_dir, dir_file)
            if os.path.isdir(abs_data_dir) is True and int(dir_file) <= int(clear_data_dir):
                remove_directory(abs_data_dir)
                print 'Now sync date is: %s! Success to remove directory: %s' % (clear_date, abs_data_dir)
        except ValueError, e:
            print 'The file "%s" is not regular date synchronous directory' % dir_file

    print 'Success to Complete synchronous data!'

if __name__ == '__main__':
    # 1.backup data directory: the number of synchronous directory and  backup direcotry according to user specify
    #if '' != backup_dir and '' != backup_num:
    #    backup_file()

    # 2.clear file first: clear the data type is 'full' every day. The clear file date directory is less than synchronous date
    clear_data()

