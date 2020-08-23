#!/opt/soft/python-2.7.11/bin/python
#coding=utf-8

import datetime
from config.global_config import *
from sync_sdb import *
from sync_os import *
import socket
import setproctitle
import collections
import time
import traceback


def init_sync_queue(db, queue_rds):
    # from table nlsync.config get sync system name
    for queue_rd in queue_rds:
        init_rd = {}
        init_rd['sync_sys'] = queue_rd['sync_sys']
        init_rd['tbl_name'] = queue_rd['tbl_name']
        init_rd['sync_dt'] = queue_rd['sync_dt']
        init_rd['batch_dt'] = queue_rd['batch_dt']
        init_rd['sync_type'] = queue_rd['sync_type']
        init_rd['sync_st'] = 'wait'
        init_rd['_id'] = ObjectId()
        if queue_rd.get('rely_tbl') is not None:
            init_rd['rely_tbl'] = queue_rd.get('rely_tbl')
        init_rd['check_file_num'] = queue_rd.get('check_file_num')
        db.sync_insert(sync_local_cs, sync_local_cl, init_rd)


class SyncScheduling:
    def __init__(self, log_handler=None):
        self.config_cs = sync_config_cs
        self.config_cl = sync_config_cl
        self.local_cs = sync_local_cs
        self.local_cl = sync_local_cl
        self.history_cs = sync_history_cs
        self.history_cl = sync_history_cl
        self.sparkloc_cs = sync_sparkloc_cs
        self.sparkloc_cl = sync_sparkloc_cl
        self.sync_hosts = sync_hosts
        self.process_num = int(sync_thread_num)
        self.log = log_handler

        self.check_rcwait_time = check_rcwait_time
        # connect hosts infomation
        self.host_name = host_name
        self.server_port = server_port
        self.user_name = user_name
        self.password = password
        # data disk
        self.transcode_paths = transcode_paths

        # SequoiaDB connection
        self.db = SCSDB(self.host_name, self.server_port, self.user_name, self.password)

    def is_normal_process(self, sync_hostname, sync_username, sync_password, sync_pid):
        is_run = False
        ssh_run = SyncOS()
        command_line = 'ls /proc/%s | wc -l' % sync_pid
        proc_ret = ssh_run.sync_ssh(sync_hostname, sync_username,
                                    sync_password, command_line)
        try:
            proc_infos = proc_ret.split("\r\n")
        except AttributeError, e:
            print "Traceback[%s], proc return: %s" % (traceback.format_exc(), proc_ret)
            self.log.warn("Traceback[%s], proc return: %s" % (traceback.format_exc(), proc_ret))
        if proc_infos[1].isdigit():
            if int(proc_infos[1]) > 0:
                is_run = True
            else:
                print 'Get is normal proccess infomations: %s, ' \
                      'process number: %s' % (proc_ret, sync_pid)
                self.log.warn('Get is normal proccess infomations: %s'
                              'process number: %s' % (proc_ret, sync_pid))
        else:
            print 'Get is normal proccess infomations: %s, ' \
                  'process number: %s' % (proc_ret, sync_pid)
            self.log.warn('Get is normal proccess infomations: %s'
                          'process number: %s' % (proc_ret, sync_pid))

        return is_run

    def inspect_process_status(self):
        inspect_cond = {"$or": [{"sync_st": "running"},
                                {"sync_st": "wait", "tran_file": {"$exists": 1}, "proc_info": {"$exists": 1}}]}
        config_cond = {'is_sync': {'$ne': 'false'}}   # here field 'is_sync' make sure table run or not
        success_cond = {'sync_st': 'success'}
        failed_cond = {'sync_st': 'failed'}
        # set process name
        process_name = 'SYNC|%s|SyncSchedulingInspect|%s' % (socket.gethostname(),
                                                             datetime.datetime.now().strftime("%Y%m%d"))
        setproctitle.setproctitle(process_name)

        while True:
            config_cnt = self.db.sync_get_count(self.config_cs, self.config_cl, config_cond)
            success_cnt = self.db.sync_get_count(self.local_cs, self.local_cl, success_cond)
            failed_cnt = self.db.sync_get_count(self.local_cs, self.local_cl, failed_cond)
            sum_success_failed = success_cnt + failed_cnt
            # when the sum of running success and running failed is equal total config data, we run spark over!
            if config_cnt == sum_success_failed:
                print 'synchronous data over, total: %s, success: %s, ' \
                      'failed: %s' % (sum_success_failed, success_cnt, failed_cnt)
                self.log.info('synchronous data over, total: %s, success: %s, '
                              'failed: %s' % (sum_success_failed, success_cnt, failed_cnt))
                break

            # inspect sync status is running
            time.sleep(self.check_rcwait_time)
            run_tables = self.db.sync_query(self.local_cs, self.local_cl, inspect_cond)
            self.log.info("scheduling inspect running table's info: %s" % (run_tables))

            sync_host_infos = self.sync_hosts.split(',')
            for run_table in run_tables:
                tbl_name = run_table.get('tbl_name')
                proc_info = run_table.get('proc_info')
                if proc_info is None:
                    print "get run tables: %s" % run_tables
                    continue
                sync_hostname = proc_info.split(',')[0]
                sync_username = None
                sync_password = None
                sync_pid = proc_info.split(',')[1]

                # get username and password
                for sync_host_info in sync_host_infos:
                    host_infos = sync_host_info.split(':')
                    if sync_hostname == host_infos[0]:
                        sync_username = host_infos[1]
                        sync_password = host_infos[2]
                        break

                if '' == sync_pid:
                    is_run = False
                else:
                    is_run = self.is_normal_process(sync_hostname, sync_username,
                                                    sync_password, sync_pid)
                if is_run is True:
                    continue
                else:
                    tbl_cond = {'tbl_name': tbl_name}
                    tbl_run = self.db.sync_query(self.local_cs, self.local_cl, tbl_cond)
                    tbl_sync_st = tbl_run[0].get('sync_st')
                    if 'running' == tbl_sync_st or ('wait' == tbl_sync_st and '' == sync_pid):
                        tbl_update_cond = {'$set': {'sync_st': 'failed'}}
                        print 'RUNNING FAILED TABLE: %s, sync_st: %s, sync_pid: %s' % (tbl_name, tbl_sync_st, sync_pid)
                        self.log.error('inspect scheduling status running failed tables: %s' % tbl_name)
                        self.db.sync_update(self.local_cs, self.local_cl, tbl_update_cond, tbl_cond)

    def get_run_host_disk(self, full_gen_host_disks, sync_host_runs):
        full_sync_host_disks = full_gen_host_disks[:]
        for sync_host_run in sync_host_runs:
            # 获取当前批次中同步任务的数量
            sync_hosts = sync_host_run.split(":")
            sync_hostname = sync_hosts[0]
            run_cond = {"$and": [{"$or": [{"sync_st": "running"}, {"sync_st": "wait"}]},
                                 {"tran_file": {"$exists": 1}},
                                 {"proc_info": {"$regex": "^%s.*" % sync_hostname}}]}
            file_selector = {}
            order_by = {"tran_file": 1}
            run_tables = self.db.sync_query(self.local_cs, self.local_cl,
                                                run_cond, file_selector, order_by)

            sync_host_disk_num = -1
            for full_sync_host_disk in full_sync_host_disks:
                sync_host_disk_num += 1
                if "None" != full_sync_host_disk:
                    full_hosts = full_sync_host_disk[0].split(":")
                    full_hostname = full_hosts[0]
                    full_transcode_path = full_sync_host_disk[1]
                    for run_table in run_tables:
                        tran_file = run_table.get("tran_file")
                        try:
                            if full_hostname == sync_hostname and tran_file is not None and -1 != tran_file.find(full_transcode_path):
                                full_sync_host_disks[sync_host_disk_num] = "None"
                        except Exception,e :
                            print "Sync hostname : %s,Transcode path : %s ,Exception Detail information: %s" % (sync_hostname,full_transcode_path,str(e))
                            self.log.error("Sync hostname : %s,Transcode path : %s ,Exception Detail infomation : %s" % (sync_hostname,full_transcode_path,str(e)))

        return full_sync_host_disks

    def get_sort_queue(self, nearly_date=5L):
        # according local table's average time get sorted queue and then running it
        local_cond = {"sync_st": "wait", "proc_info": {"$exists": 0}}
        local_selector = {"tbl_name": "", "sync_sys": "", "rely_tbl": "", "batch_dt": ""}
        local_sort = {"tbl_name": -1}
        # 获取等待执行的数据表
        wait_run_tbls = self.db.sync_query(self.local_cs, self.local_cl, local_cond,
                                           local_selector, local_sort)

        # 获取数据表当前批次日期最近5天的历史数据，并计算其跑的平均值
        his_delta_dt = timedelta(days=int(nearly_date))
        loc_batch_dt = wait_run_tbls[0].get("batch_dt")
        loc_batch_dtfmt = datetime.datetime.strptime(wait_run_tbls[0].get("batch_dt"), '%Y%m%d')
        his_min_date = (loc_batch_dtfmt - his_delta_dt).strftime("%Y%m%d")
        history_aggr_cond = []
        history_matcher = {"$match": {"batch_dt": {"$gte": his_min_date, "$lte": loc_batch_dt},
                                      "total_tm": {"$exists": 1}}}
        history_aggr_cond.append(history_matcher)
        history_group = {"$group": {"_id": "$tbl_name",
                                    "tbl_name": {"$first": "$tbl_name"},
                                    "average_tm": {"$avg": "$total_tm"}}}
        history_aggr_cond.append(history_group)
        history_orderby = {"$sort": {"average_tm": -1}}
        history_aggr_cond.append(history_orderby)
        history_sort_tbls = self.db.sync_aggregate(self.history_cs, self.history_cl,
                                                   history_aggr_cond)
        print "history sort tables: %s" % history_sort_tbls
        self.log.info("history sort tables: %s" % history_sort_tbls)
        # 当history表中无total_tm字段时，需要进行处理
        if history_sort_tbls[0].get("tbl_name") is None:
            history_sort_tbls = []

        # 将数据表组装存入队列列表
        run_lists = []
        for history_sort_tbl in history_sort_tbls:
            wait_run_tbl_pos = -1
            for wait_run_tbl in wait_run_tbls:
                wait_run_tbl_pos += 1
                if history_sort_tbl.get("tbl_name") == wait_run_tbl.get("tbl_name"):
                    run_dict = (history_sort_tbl.get("average_tm"),
                                history_sort_tbl.get("tbl_name"),
                                wait_run_tbl.get("rely_tbl"),
                                wait_run_tbl.get("sync_sys"),)
                    run_lists.append(run_dict)
                    # 当history表和local表相等时，将此表从wait_run_tbls列表中进行删除，并且中止循环
                    del wait_run_tbls[wait_run_tbl_pos]
                    break

        # 当history中没有的数据表，但是local表的等待队列中有，则需要将此数据表添加入执行队列中
        for wait_run_tbl in wait_run_tbls:
            run_dict = (wait_run_tbl.get("average_tm"),
                        wait_run_tbl.get("tbl_name"),
                        wait_run_tbl.get("rely_tbl"),
                        wait_run_tbl.get("sync_sys"),)
            run_lists.append(run_dict)

        # 根据字段rely_tbl进行排序
        self.log.info("begin to sort by field rely_tbl. run lists: %s" % run_lists)
        run_lists = sorted(run_lists, cmp=lambda x, y: cmp(x[2], y[2]))
        self.log.info("finish to sort by field rely_tbl. run lists: %s" % run_lists)

        return run_lists

    def multi_process_sync(self):
        #tbl_queue_cond = {"sync_st": "wait", "tran_file": {"$exists": 0}, "proc_info": {"$exists": 0}}
        tbl_queue_cond = {"sync_st": "wait", "proc_info": {"$exists": 0}}
        # set process name
        process_name = 'SYNC|%s|SyncScheduling|%s' % (socket.gethostname(),
                                                      datetime.datetime.now().strftime("%Y%m%d"))
        setproctitle.setproctitle(process_name)

        sync_host_info = self.sync_hosts.split(',')
        sync_os = SyncOS()

        # 获取根据历史跑批数据排序的队列
        tbl_queue = self.get_sort_queue()
        print "get sort queue: %s" % tbl_queue
        self.log.info("get sort queue: %s" % tbl_queue)
        # 根据同步主机、进程数量和转码数生成数据同步主机-转码目录列表
        full_host_disks = []
        sync_run_hosts = self.sync_hosts.split(",")
        full_host_disks_num = len(sync_run_hosts) * self.process_num
        for disk_num in range(self.process_num):
            disk_pos = disk_num % len(self.transcode_paths)
            for sync_host in sync_run_hosts:
                sync_host_disk = (sync_host, self.transcode_paths[disk_pos],)
                full_host_disks.append(sync_host_disk)
        #总数
        total_cond = {"is_sync":"true"}
        total_cnt = self.db.sync_get_count(self.config_cs, self.config_cl, total_cond)
        #成功的数量
        success_cnt = 0
        #失败的数量
        failed_cnt = 0
        retry_times = 5
        failed_retry_map = {}
        success_cond = {'sync_st':'success'}
        failed_cond = {'sync_st':'failed'}
        while total_cnt != (success_cnt + failed_cnt):
            sync_host_disk_infos = self.get_run_host_disk(full_host_disks, sync_run_hosts)
            success_cnt = self.db.sync_get_count(self.local_cs,self.local_cl,success_cond)
            failed_cnt = self.db.sync_get_count(self.local_cs,self.local_cl,failed_cond)
            if full_host_disks_num == str(sync_host_disk_infos).count("None"):
                time.sleep(2)
                continue
            print "get run host disk list: %s" % sync_host_disk_infos
            #print "total_cnt = %s, success_cnt = %s, failed_cnt = %s, tb_queue_cnt = %s" % (total_cnt,success_cnt,failed_cnt,len(tbl_queue))
            self.log.info("get run host disk list: %s" % sync_host_disk_infos)
            for sync_host_info in sync_host_disk_infos:
                if "None" != sync_host_info:
                    host_info = sync_host_info[0]
                    transcode_path = sync_host_info[1]
                    # 解析主机信息
                    sync_hosts = host_info.split(":")
                    sync_hostname = sync_hosts[0]
                    sync_username = sync_hosts[1]
                    sync_password = sync_hosts[2]
                    sync_path = sync_hosts[3]

                    # 根据系统名和表名构建同步任务进程传递参数，如：CBE.cbe.bptfhist
                    if 0 < len(tbl_queue):
                        run_queue = tbl_queue[0]
                        system_name = run_queue[3]
                        table_name = run_queue[1]
                        run_table = '%s:%s' % (system_name,
                                               table_name)
                        # 因为同步任务进程是远程异步发起的，所以需要先将信息更新至分布式表中
                        sync_info_update = {'$set': {'tran_file': transcode_path}}
                        sync_info_cond = {'tbl_name': table_name}
                        self.db.sync_update(self.local_cs, self.local_cl,
                                            sync_info_update, sync_info_cond)
                        sync_command = 'nohup python %s/sync_data.py ' \
                                       '--sync_tables="%s" ' \
                                       '--run_proc_number="%s" > /dev/null ' \
                                       '&' % (sync_path, run_table,
                                              self.transcode_paths.index(transcode_path))
                        print '[begin date time: %s]host name: %s, synchronous command ' \
                              'line: %s' % (datetime.datetime.now(), sync_hostname, sync_command)
                        sync_ret = sync_os.sync_ssh(sync_hostname, sync_username,
                                                    sync_password, sync_command)
                        self.log.info('host name: %s, synchronous command '
                                      'line: %s, return info: %s' % (sync_hostname, sync_command, sync_ret))
                        # when remote call sync program, we set the proc_info have hostname and process number
                        if sync_ret is not None:
                            process_num = sync_ret.split(']')[1].replace(' ', '').replace('\r\n', '')
                            proc_info = '%s,%s' % (sync_hostname, process_num)
                            sync_info_update = {'$set': {'proc_info': proc_info}}
                            self.log.info("table: %s, parse process number: %s, "
                                          "sync info update: %s" % (run_table, process_num, sync_info_update))
                            self.db.sync_update(self.local_cs, self.local_cl,
                                                sync_info_update, sync_info_cond)

                            # 远程调起执行的表需要将其从队列中进行删除
                            del tbl_queue[0]
                            print '[end date time: %s]host name: %s, synchronous command ' \
                                  'line: %s' % (datetime.datetime.now(), sync_hostname, sync_command)
                        else:
                            # process_num = "None"
                            self.log.warn("return is None, try again")
                            
            """
              检查队列中是否还有表，如果没有，则检查local表是否存在失败的表
              如果存在失败的表，则初始化后加入队列，每张表都只有5次机会，防止陷入死循环
            """
            #如果队列为空且存在重试次数
            if 0 == len(tbl_queue):
                time.sleep(60)
                success_cnt = self.db.sync_get_count(self.local_cs,self.local_cl,success_cond)
                failed_cnt = 0
                #查询local表是否存在失败的记录
                failed_queue = self.db.sync_query(self.local_cs,self.local_cl,failed_cond)
                if 0 != len(failed_queue):
                    #print "=================failed tables: %s " % str(failed_queue)
                    self.log.info("failed tables: %s " % str(failed_queue))
                    retry_queue = []
                    # print "==============failed_retry_map = %s " % str(failed_retry_map)
                    for failed_record in failed_queue:
                        failed_tbl_name = failed_record.get("tbl_name")
                        if failed_tbl_name not in failed_retry_map.keys():
                            failed_retry_map.setdefault(failed_tbl_name,retry_times)
                            self.log.info("add failed table into failed_queue : %s" % failed_tbl_name)
                            #print "=================add failed table into failed_queue : %s" % failed_tbl_name
                        else:
                            failed_retry_map[failed_tbl_name] = failed_retry_map[failed_tbl_name] - 1
                        if failed_retry_map[failed_tbl_name] > 0:
                            rm_condition = {'tbl_name' : failed_tbl_name}
                            self.db.sync_remove(self.local_cs,self.local_cl,rm_condition)
                            retry_queue.append(failed_record)
                        else:
                            failed_cnt = failed_cnt + 1
                            self.log.info("the last failed table : %s" % failed_tbl_name)
                            #print("=================the last failed table : %s" % failed_tbl_name)
                    if len(retry_queue) > 0:
                        self.log.info("add failed_queue into tbl_queue : %s " % str(retry_queue))
                        #print "=================add failed_queue into tbl_queue : %s " % str(retry_queue)
                        init_sync_queue(self.db,retry_queue)
                        tbl_queue = self.get_sort_queue()
def main():
    host = 'A-DSJ-NLSDB02'
    user = 'sdbadmin'
    password = 'kfptSDB2016!'
    #command = 'python /home/sdbadmin/synchronous_data/sync_data/sync_get_file.py'
    print 'BEGIN: %s' % datetime.datetime.now()
    #cmd_run = start_remote_sync(host, user, password, command)

    #print cmd_run.before
    print 'END: %s' % datetime.datetime.now()

if __name__ == '__main__':
    main()
