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
from sync_os import *
from sync_logger import *
from pexpect import pxssh
import pexpect

class SyncGetFile:
    def __init__(self, log_handler=None):
        self.log = log_handler
        self.sync_os = SyncOS(self.log)
        # sleep time
        self.wait_sleep_time = 1
        self.total_times = 3
        self.get_file_timeout = 7200
        self.ssh_timeout = 7200
        self.no_such_file = 'No such file or directory'

    # 获取文件
    def sftp_get_file(self, frm_host, frm_file, frm_user,
                      frm_passwd, local_file, timeout=7200):
        get_success = False
        # get file
        frmok_file = frm_file.split('.')[0] + '.ok'
        syncok_file = local_file.split('.')[0] + '.ok'
        # get remote file
        data_ret = self.sync_os.sftp_get(frm_host, frm_file, frm_user, frm_passwd,
                                         local_file, timeout)
        # get remote ok file
        ok_ret = self.sync_os.sftp_get(frm_host, frmok_file, frm_user, frm_passwd,
                                       syncok_file, timeout)
        # here need to check
        if int(data_ret[0]) == 0 and int(ok_ret[0]) == 0:
            get_success = True

        self.log.info("sftp get data file, info: %s, get ok file, "
                      "info: %s" % (data_ret, ok_ret))

        return get_success


    def sync_get_file(self, frm_host, frm_file,
                      frm_user, sync_file):
        # get ok file
        get_file_status = False
        frmok_file = frm_file.split('.')[0] + '.ok'
        syncok_file = sync_file.split('.')[0] + '.ok'
        get_data_file_cmd = 'scp %s@%s:%s %s' % (frm_user, frm_host, frm_file, sync_file)
        get_ok_file_cmd = 'scp %s@%s:%s %s' % (frm_user, frm_host, frmok_file, syncok_file)
        data_file_ret = self.sync_os.cmd_run(get_data_file_cmd)
        if 0 != data_file_ret[0]:
            if self.log is None:
                print "failed to get data file, run command: %s" \
                      ", error info: %s" % (get_data_file_cmd, data_file_ret)
            else:
                self.log.info("failed to get data file, run command: %s"
                              ", error info: %s" % (get_data_file_cmd, data_file_ret))
        ok_file_ret = self.sync_os.cmd_run(get_ok_file_cmd)
        if 0 != ok_file_ret:
            if self.log is None:
                print "failed to get data ok file, run command: %s" \
                      ", error info: %s" % (get_ok_file_cmd, data_file_ret)
            else:
                self.log.info("failed to get data ok file, run command: %s"
                              ", error info: %s" % (get_ok_file_cmd, data_file_ret))

        if 0 == ok_file_ret[0] and 0 == data_file_ret[0]:
            get_file_status = True

        return get_file_status

    def formate_remote_list_ret(self, remote_file_result):
        """
        Args:
            remote_file_result: such as [-rwxrwxrwx 1 sdbadmin sdbadmin_group 532057 Oct 27 15:56 /data03/etldata/odsdata/20170302/CBE/BPCOFHSH_EX.dat]

        Returns:

        """
        list_ret = []
        result = remote_file_result.split('\r\n')[1]
        list_info = result.split(' ')
        list_ret.append(result)
        list_ret.append(list_info[4])

        return list_ret

    def check_remote_file(self, frm_host, frm_file, frm_user,
                          frm_passwd, timeout=7200):
        check_file_size = None
        # get ok file
        frmok_file = frm_file.split('.')[0] + '.ok'
        check_ok_file_cmd = 'ls -al %s' % frmok_file
        check_ok_ret = self.sync_os.sync_ssh(frm_host, frm_user, frm_passwd,
                                             check_ok_file_cmd, timeout)
        ok_ret = self.formate_remote_list_ret(check_ok_ret)
        if self.log is None:
            print "check ok file return: %s" % check_ok_ret
        else:
            self.log.info("check ok file return: %s" % ok_ret)
        check_data_file_cmd = 'ls -al %s' % frm_file
        check_data_ret = self.sync_os.sync_ssh(frm_host, frm_user, frm_passwd,
                                               check_data_file_cmd)
        data_ret = self.formate_remote_list_ret(check_data_ret)
        if self.log is None:
            print "check data file return: %s" % check_data_ret
        else:
            self.log.info("check data file return: %s" % data_ret)

        if ok_ret[0].find(self.no_such_file) == -1 and data_ret[0].find(self.no_such_file) == -1:
            check_file_size = int(data_ret[1])

        self.log.info("file: %s size: %s" % (frm_file, data_ret[1]))

        return check_file_size

    def check_local_file(self, sync_file, sync_ok_file=None, remote_file_size=None):
        file_ready = False
        #print "input check local file: %s -- %s" % (sync_file, sync_ok_file)
        if sync_ok_file is None:
            ok_file = sync_file.split('.')[0] + '.ok'
        else:
            ok_file = sync_ok_file
        ret_data = self.sync_os.file_is_exists(sync_file)
        ret_ok = self.sync_os.file_is_exists(ok_file)
        local_file_size = None
        if ret_data is True and ret_ok is True:
            local_file_size = self.sync_os.get_file_size(sync_file)
        if remote_file_size is None:
            if ret_data is True and ret_ok is True:
                file_ready = True
        else:
            if ret_data is True and ret_ok is True and \
               local_file_size == remote_file_size:
                file_ready = True

        #print ">>>>check local file: %s -- %s, %s -- %s, %s -- %s" % (sync_file, ok_file, ret_data, ret_ok, local_file_size, remote_file_size)

        return file_ready
        
    def get_file(self, frm_host, frm_file, frm_user, sync_file,
                 frm_password, timeout=7200):
        local_host = self.sync_os.get_local_host()
        remote_host = self.sync_os.get_remote_host(frm_host)
        retry_times = 0
        # when story data host is not equal local host, we need get file from remote host
        file_ready = self.check_local_file(sync_file)
        self.log.warn('file:%s is not ready, file_ready: %s' % (sync_file, file_ready))
        # get file from remot
        while not file_ready:
            # when retry times greater than total times, we break loop
            if retry_times >= self.total_times:
                self.log.error("failed to get remote file: %s from host: %s, and retry "
                               "times: %s in host: %s!" % (frm_file, remote_host,
                                                           retry_times, local_host))
                break
            # retry time counter
            retry_times += 1
            check_file_size = self.check_remote_file(frm_host, frm_file,
                                                     frm_user, frm_password, timeout)
            if check_file_size is None:
                time.sleep(self.wait_sleep_time)
                self.log.warn("remote file: %s in host: %s is not "
                              "ready" % (frm_file, frm_host))
                continue
            # run get file here
            dirname = self.sync_os.get_dirname(sync_file)
            self.sync_os.make_directory(dirname)
            # get_file_ret = self.sync_get_file(frm_host, frm_file, frm_user, sync_file)
            get_file_ret = self.sftp_get_file(frm_host, frm_file, frm_user,
                                              frm_password, sync_file)
            # create directory for sync file and give high autority for directory
            if get_file_ret:
                self.sync_os.give_highest_authority(sync_file)
                # self.sync_os.give_highest_authority(sync_ok_file)
                self.log.info("finish to get file: %s from host: %s" % (frm_file, frm_host))
            else:
                continue
            # check file is get correct or not
            file_ready = self.check_local_file(sync_file, None, check_file_size)

        return file_ready

    def prefetch_file(self, pref_file, prefetch_ok_file, sync_file):
        retry_times = 0
        # when story data host is not equal local host, we need get file from remote host
        file_ready = self.check_local_file(sync_file)
        # get file from remote
        print ">>>OK file: %s - %s" % (pref_file, prefetch_ok_file)
        while not file_ready:
            if retry_times >= self.total_times:
                self.log.error("failed to copy file: %s, and retry "
                               "times: %s! " % (pref_file, retry_times))
                break
            # retry time counter
            retry_times += 1
            check_status = self.check_local_file(pref_file, prefetch_ok_file) 
            if not check_status:
                self.log.warn("source file: %s is not ready" % pref_file)
                time.sleep(self.wait_sleep_time)
                continue
            # run get file here
            dirname = self.sync_os.get_dirname(sync_file)
            self.sync_os.make_directory(dirname)
            # copy data file
            self.sync_os.copy_file(pref_file, sync_file)
            # copy ok file
            pref_ok_file = prefetch_ok_file 
            sync_ok_file = sync_file.split('.')[0] + '.ok'
            src_file_size = self.sync_os.get_file_size(pref_file)
            print "ok file: %s - %s" % (pref_ok_file, sync_ok_file)
            self.sync_os.copy_file(pref_ok_file, sync_ok_file)
            # create directory for sync file and give high autority for directory
            self.sync_os.give_highest_authority(sync_file)
            self.sync_os.give_highest_authority(sync_ok_file)
            # check file is get correct or not
            file_ready = self.check_local_file(sync_file, None,
                                               src_file_size)

        return file_ready

def sync_get_file_main():
    hostname = "T-DSJ-HISDB01"
    svcport = 31810
    username = "sdbapp"
    password = "a2ZwdFNEQjIwMTY="
    tbl_name = 'cbe.dctiaccy'
    db_hosts = 'T-DSJ-HISDB01:31810,T-DSJ-HISDB02:31810,T-DSJ-HISDB03:31810'
    connect_hosts = []
    for db_host in db_hosts.split(','):
        host_info = db_host.split(':')
        connect_info = {'host': host_info[0], 'service': host_info[1]}
        connect_hosts.append(connect_info)
    db = SCSDB(hostname, svcport, username, password, connect_hosts)

    """
    logging file
    """
    tbl_cond = {'tbl_name': tbl_name}
    log_connect = {'HostName': hostname, 'ServerPort': svcport,
                   'UserName': username, 'Password': password,
                   'CsName': sync_log_cs, 'ClName': sync_log_cl}
    cnf_records = db.sync_query(sync_config_cs, sync_config_cl, tbl_cond)
    if 0 == len(cnf_records):
        print 'the table: %s have not in sync.config when init class' % tbl_name
        raise
    sync_sys = cnf_records[0]['sync_sys']
    sync_date = "20170224"
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
    # ******************************************************************************* #
    sync_get_file = SyncGetFile(log)
    frm_host = 'T-DSJ-HISDB02'
    frm_file = '/data03/etldata/odsdata/20170223/CBE/DCTIACCY.dat'
    sync_file = '/data03/etldata/odsdata/20170223/CBE/DCTIACCY.dat'
    frm_user = 'sdbadmin'
    frm_password = 'kfptSDB2016!'
    print "===============GET FILE FROM REMOTE HOST============="
    #file_status = sync_get_file.get_file(frm_host, frm_file, frm_user, sync_file, frm_password)
    #print "remote get sync get status: %s" % file_status
    print "===============GET FILE FROM LOCAL HOST============="
    pref_file = '/data03/odsdata/20130424/RCS_BUSINESS_LOANBACKSTATUS_20130424.dat'
    sync_file1 = '/data03/etldata/odsdata/20130424/RCS/BUSINESS_LOANBACKSTATUS.dat'
    copy_status = sync_get_file.prefetch_file(pref_file, sync_file1)
    print "copy sync get status: %s" % copy_status

if __name__ == '__main__':
    sync_get_file_main()
