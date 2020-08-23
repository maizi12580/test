#!/usr/bin/python
#coding=utf-8


from multiprocessing import Process, Queue, Pipe, Lock, Value, Array, Manager
from sync_sdb import *
import time
from config.global_config import *
from sync_os import *
from sync_logger import *
import os
import shutil
import base64


class SyncUpsert:
    def __init__(self, cs_name, cl_name, upsert_home, upsert_jar, rec_file, log_handler=None):
        self.sync_os = SyncOS(log_handler)
        self.log = log_handler
        self.cs_name = cs_name
        self.cl_name = cl_name
        self.upsert_home = upsert_home
        self.upsert_tool = upsert_jar
        self.rec_file = rec_file

    def sync_upsert(self, upsert_config, upsert_file, retry_times=0):
        """
        Desc: this function upsert table from json file by running command with sdbupsert.jar
        Args:
            upsert_home: the sdbupsert.jar file's directory
            upsert_config: the table's configure file.
            upsert_file: the json file include absolute directory

        Returns:

        """
        retry_cnt = 0
        upsert_ret_info = None
        upsert_ret = []
        while True:
            # when sdbupsert retry, we need to remove log file, rec file and error file last time
            if upsert_ret_info is not None:
                # remove upsert log file
                self.sync_os.remove_file(upsert_ret_info.get("LogFile"))
                # remove upsert rec file
                self.sync_os.remove_file(upsert_ret_info.get("RecFile"))
                # remove upsert error file
                self.sync_os.remove_file(upsert_ret_info.get("ErrorFile"))
                # clean upsert return info
                upsert_ret = []

            # get import rec file and then run sdbupsert
            run_file = upsert_file
            upsert_cmd = 'cd %s; java -jar %s %s %s' % \
                         (self.upsert_home, self.upsert_tool, upsert_config, run_file)
            retval = self.sync_os.cmd_run(upsert_cmd)
            if self.log is None:
                print 'sdbupsert command line: %s, execute return value: %s' % (upsert_cmd, retval)
            else:
                self.log.info('sdbupsert command line: %s, execute return value: %s' % (upsert_cmd, retval))

            upsert_ret_info = self.format_upsert_info(retval[1])
            # upsert_ret[0]==0 mean upsert success, else failed,
            # upsert_ret[1] is map store upsert info
            if upsert_ret_info.get("TotalRead") == upsert_ret_info.get("Success") and \
               '0' == upsert_ret_info.get("Fail"):
                # remove import rec file
                self.sync_os.remove_file(run_file)
                # remove upsert log file
                self.sync_os.remove_file(upsert_ret_info.get("LogFile"))
                # remove upsert rec file
                self.sync_os.remove_file(upsert_ret_info.get("RecFile"))
                # remove upsert error file
                self.sync_os.remove_file(upsert_ret_info.get("ErrorFile"))
                if self.log is None:
                    print "sdbupsert success.Return: %s" % upsert_ret_info
                else:
                    self.log.info("sdbupsert success.Return: %s" % upsert_ret_info)
                # upsert status
                upsert_ret.append(0)
                upsert_ret.append(upsert_ret_info)
                break
            else:
                upsert_ret_info["RunFile"] = run_file
                if self.log is None:
                    print "sdbupsert failed.Return: %s" % upsert_ret_info
                else:
                    self.log.error("sdbupsert failed.Return: %s" % upsert_ret_info)

                if 0 != retval[0]:
                    if self.log is None:
                        print 'sdbupsert execute failed, return value: %s' % retval
                    else:
                        self.log.error('sdbupsert execute failed, return value: %s' % retval)
                # upsert status
                upsert_ret.append(-1)
                upsert_ret.append(upsert_ret_info)

            if retry_cnt >= retry_times:
                if self.log is None:
                    print "failed!sdbupsert tool have retry %s times" % retry_times
                else:
                    self.log.warn("failed!sdbupsert tool have retry %s times" % retry_times)
                break

            # counter
            retry_cnt += 1

        return upsert_ret

    def format_upsert_info(self, upsert_info_ret):
        upsert_infos = upsert_info_ret.split('\n')
        upsert_info_map = {}
        for upsert_info in upsert_infos:
            upsert_rets = upsert_info.split(',')
            for upsert_ret in upsert_rets:
                upsert_field_infos = upsert_ret.split(':')
                if 'Total Read' == upsert_field_infos[0]:
                    upsert_info_map["TotalRead"] = upsert_field_infos[1].strip()
                elif 'Success' == upsert_field_infos[0]:
                    upsert_info_map["Success"] = upsert_field_infos[1].strip()
                elif 'Fail' == upsert_field_infos[0]:
                    upsert_info_map["Fail"] = upsert_field_infos[1].strip()
                elif 'Encode Error' == upsert_field_infos[0]:
                    upsert_info_map["EncodeError"] = upsert_field_infos[1].strip()
                elif 'Time' == upsert_field_infos[0]:
                    upsert_info_map["Time"] = upsert_field_infos[1].strip()
                elif 'Avg' == upsert_field_infos[0]:
                    upsert_info_map["Avg"] = upsert_field_infos[1].strip()
                elif 'Log File' == upsert_field_infos[0]:
                    upsert_info_map["LogFile"] = upsert_field_infos[1].strip()
                elif 'Rec File' == upsert_field_infos[0]:
                    upsert_info_map["RecFile"] = upsert_field_infos[1].strip()
                elif 'Error File' == upsert_field_infos[0]:
                    upsert_info_map["ErrorFile"] = upsert_field_infos[1].strip()

        return upsert_info_map


    def move_rec_file(self):
        """
        Desc: move data home directory's data into data/upsert_rec dirctory
        Args:
            cs_name: the collection space name for table upsert
            cl_name: the collection name for table upsert
            data_home: the data's home directory where ODS put data in

        Returns: get new upsert file

        """
        data_home = self.sync_os.get_dirname(self.rec_file)
        import_rec_dir = '%s/import_rec' % data_home
        upsert_rec_dir = '%s/upsert_rec' % data_home
        # make directory for import failed file, file type is csv
        self.sync_os.make_directory(import_rec_dir)
        # make directory for import failed file, file type is json
        self.sync_os.make_directory(upsert_rec_dir)
        # move files
        if self.log is None:
            print 'sdbupsert file: %s, sdbupsert file story directory: %s' % (self.rec_file, upsert_rec_dir)
        else:
            self.log.info('sdbupsert file: %s, sdbupsert file story directory: %s' % (self.rec_file, upsert_rec_dir))
        self.sync_os.move_file(self.rec_file, upsert_rec_dir)
        new_upsert_file = '%s/%s' % (upsert_rec_dir, self.sync_os.get_basename(self.rec_file))

        return new_upsert_file

    def autogen_upsert_config(self):
        """
        Desc: auto generate configure file for table upsert
        Args:
            cs_name: the collection space name for table upsert
            cl_name: the collection name for table upsert

        Returns: configure file

        """
        # source replace string
        conf_cs = 'collectionSpace='
        conf_cl = 'collection='
        conf_condfds = 'condFields='
        conf_hintidx = 'hint='
        conf_user = 'user='
        conf_passwd = 'password='
        conf_coords = 'croodAddrs='

        # destname replace string
        conf_replace_cs = conf_cs + self.cs_name
        conf_replace_cl = conf_cl + self.cl_name
        conf_replace_user = conf_user + user_name
        conf_replace_passwd = conf_passwd + base64.decodestring(password)
        # get unique index keys
        db = SCSDB(host_name, server_port, user_name, password)
        uniq_idx = db.sync_get_uniqidx(self.cs_name, self.cl_name)
        if 0 == len(uniq_idx):
            return
        conf_replace_condfds = conf_condfds
        cnt = 0
        for key in uniq_idx[1]:
            if 0 == cnt:
                conf_replace_condfds = conf_replace_condfds + key
            else:
                conf_replace_condfds = conf_replace_condfds + ',' + key
            cnt += 1

        # get unique index name
        conf_replace_hintidx = conf_hintidx + uniq_idx[0]

        # get coord address
        conf_replace_coords = '%s%s' % (conf_coords, db.get_coord_address())

        # global file
        src_conf_file = '%s/conf/%s' % (upsert_home, upsert_property)
        dst_conf_file = '%s/conf/%s_%s.conf' % (upsert_home, self.cs_name, self.cl_name)
        log_conf_file = '%s/conf/log4j.properties' % upsert_home
        src_fd = open(src_conf_file)
        dst_fd = open(dst_conf_file, 'w+')
        log_fd = open(log_conf_file)
        # change log file story directory name
        for log_line in log_fd:
            log_line = log_line.replace('opt', upsert_logdir)

        log_fd.close()

        for line_context in src_fd:
            try:
                if -1 != line_context.find(conf_cs):
                    line = line_context.replace(conf_cs, conf_replace_cs)
                elif -1 != line_context.find(conf_cl):
                    line = line_context.replace(conf_cl, conf_replace_cl)
                elif -1 != line_context.find(conf_condfds):
                    line = line_context.replace(conf_condfds, conf_replace_condfds)
                elif -1 != line_context.find(conf_hintidx):
                    line = line_context.replace(conf_hintidx, conf_replace_hintidx)
                elif -1 != line_context.find(conf_user):
                    line = line_context.replace(conf_user, conf_replace_user)
                elif -1 != line_context.find(conf_passwd):
                    line = line_context.replace(conf_passwd, conf_replace_passwd)
                elif -1 != line_context.find(conf_coords):
                    line = line_context.replace(conf_coords, conf_replace_coords)
                else:
                    line = line_context
            except UnicodeDecodeError, e:
                print line


            # put line into destnation file
            dst_fd.write(line)

        src_fd.close()
        dst_fd.close()
        # for relative directory
        dst_conf_file = os.path.basename(dst_conf_file)
        if self.log is None:
            print 'auto generate sdbupsert file: %s' % dst_conf_file
        else:
            self.log.info('auto generate sdbupsert file: %s' % dst_conf_file)

        return dst_conf_file

def test_main():
    host_name = "T-DSJ-HISDB01"
    svc_port = "31810"
    user_name = "sdbapp"
    password = "kfptSDB2016"
    hosts = "T-DSJ-HISDB01:31810,T-DSJ-HISDB02:31810,T-DSJ-HISDB03:31810"
    """
    logging file
    """
    sync_log_cs = "nlsync"
    sync_log_cl = "log"
    sync_sys = "CBE"
    cs_name = "cbe"
    cl_name = "bptfhist"
    tbl_name = "cbe.bptfhist"
    tbl_cond = {'tbl_name': tbl_name}
    log_connect = {'HostName': host_name, 'ServerPort': svc_port,
                   'UserName': user_name, 'Password': password,
                   'CsName': sync_log_cs, 'ClName': sync_log_cl}

    sync_date = "20170226"
    log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}
    log = logging.getLogger("sync_migrate_test")
    log.setLevel(logging.INFO)
    logfile_name = logfile_dir
    fh = SCFileHandler(logfile_name, log_table, log_connect)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    log.addHandler(fh)
    # options
    cs_name = "cbe"
    cl_name = "bptfhist"
    upsert_home = "/home/sequoiadb/synchronous_data/nlsync_data/sdbupsert"
    upsert_jar = "sdbupsert.jar"
    upsert_config = "cbe_bptnhist.conf"
    rec_file = "/data03/etldata/odsdata/20170226/CBE/cbe_bptfhist_import_4441.rec"
    sync_upsert = SyncUpsert(cs_name, cl_name, upsert_home, upsert_jar, upsert_config,
                             rec_file, log)
    # 1.auto generate config file
    sync_upsert.autogen_upsert_config()
    # 2.move rec file
    upsert_file = sync_upsert.move_rec_file()
    # 3.upsert
    up_file = sync_upsert.sync_upsert(upsert_file)
    print 'upsert_file: %s' % up_file

if __name__ == '__main__':
    test_main()
