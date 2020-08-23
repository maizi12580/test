#!/usr/bin/python
#coding=utf-8

from multiprocessing import Process, Queue, Pipe, Lock, Value, Array, Manager
# noinspection PyInterpreter
from util import *
from sync_sdb import *
import time
from config.global_config import *
from bson.objectid import ObjectId
import socket
from sync_scheduling import *
import base64
from sync_check_ret import *
from sync_spark_job import *
from sync_data import *
import setproctitle
import json


def get_sync_date(db, tbl_name):
    tbl_cond = {'tbl_name': tbl_name}
    cnf_records = db.sync_query(sync_config_cs, sync_config_cl, tbl_cond)
    if 0 == len(cnf_records):
        print 'the table: %s have not in sync.config when get sync data' % tbl_name
        #self.log.error('the table: %s have not in sync.config when get sync data' % self.tbl_name)
        raise ('the table: %s have not in sync.config when get sync data')

    # the delta time how long data synchronize to SDB
    dt_delta = cnf_records[0]['dt_delta']
    # synchronous date and batch date
    his_selector = {'sync_dt': 1, 'batch_dt': 1}
    his_orderby = {'sync_dt': -1}
    num_to_return = 1L
    his_records = db.sync_query(sync_history_cs,
                                sync_history_cl,
                                condition=tbl_cond,
                                selector=his_selector,
                                order_by=his_orderby,
                                hint={}, skip=0L,
                                num_to_return=num_to_return)

    delta_dt = timedelta(days=int(dt_delta))
    add_dt = timedelta(days=int(1))          # every day add one day data
    if 0 == len(his_records):
        # auto generate synchronous date time, sync date
        local_time = datetime.datetime.now() - abs(delta_dt)
        local_date = local_time.strftime("%Y%m%d")
        sync_last_dt = datetime.datetime.strptime(str(local_date), '%Y%m%d')
        sync_local_dt = sync_last_dt
        sync_tomor_dt = sync_local_dt + abs(add_dt)
        # auto generate batch date time, batch date
        last_batch_dt = sync_last_dt
        loc_batch_dt = sync_local_dt
        tomor_batch_dt = sync_tomor_dt
    else:
        # get sync date time from table sync.history
        sync_last_dt = datetime.datetime.strptime(his_records[0]['sync_dt'], '%Y%m%d')
        sync_local_dt = sync_last_dt + abs(add_dt)
        sync_tomor_dt = sync_local_dt + abs(add_dt)
        # get batch date time from table sync.history
        last_batch_dt = datetime.datetime.strptime(his_records[0]['batch_dt'], '%Y%m%d')
        loc_batch_dt = last_batch_dt + abs(add_dt)
        tomor_batch_dt = loc_batch_dt + abs(add_dt)
    date_arr = []
    sync_date_arr = []
    sync_date_arr.append(sync_local_dt.strftime("%Y%m%d"))
    sync_date_arr.append(sync_tomor_dt.strftime("%Y%m%d"))
    sync_date_arr.append(sync_last_dt.strftime("%Y%m%d"))
    date_arr.append(sync_date_arr)
    batch_date_arr = []
    batch_date_arr.append(loc_batch_dt.strftime("%Y%m%d"))
    batch_date_arr.append(tomor_batch_dt.strftime("%Y%m%d"))
    batch_date_arr.append(last_batch_dt.strftime("%Y%m%d"))
    date_arr.append(batch_date_arr)
    return date_arr
    
def local_copy2_history(db, log=None, orderby_field=''):
    # from table nlsync.config get sync system name
    init_records = []
    success_cond = {'sync_st': 'success'}
    failed_cond = {'sync_st': {'$ne': 'success'}}
    config_cond = {'is_sync': {'$ne': 'false'}}
    fileter = {}
    order_by = {}
    if '' != orderby_field:
        order_by[orderby_field] = 1
    cnf_count = db.sync_get_count(sync_config_cs, sync_config_cl, config_cond)
    loc_ok_count = db.sync_get_count(sync_local_cs, sync_local_cl, success_cond)
    loc_count = db.sync_get_count(sync_local_cs, sync_local_cl)
    loc_fail_count = db.sync_get_count(sync_local_cs, sync_local_cl, failed_cond)

    # the synchronous data is success
    if 0 == loc_fail_count and loc_ok_count >= cnf_count:
        ####################################
        # nlsync.local -> nlsync.history
        # local 
        loc_selector = {'_id': {'$include': 0}}
        loc_records = db.sync_query(sync_local_cs, sync_local_cl, {}, loc_selector, order_by)
        #  when init ok, we copy local to history and remove local
        for loc_record in loc_records:
            #print 'tbl_info: %s' % loc_record
            db.sync_insert(sync_history_cs, sync_history_cl, loc_record)

        # clean collection 
        db.sync_truncate(sync_local_cs, sync_local_cl)
        # init synchronous local collection
        cnf_records = db.sync_query(sync_config_cs, sync_config_cl, config_cond, loc_selector, order_by)
        for loc_record in loc_records:
            for cnf_record in cnf_records:
                init_rd = {}
                if cnf_record.get("tbl_name") == loc_record.get("tbl_name"):
                    init_rd['sync_sys'] = cnf_record['sync_sys']
                    init_rd['tbl_name'] = cnf_record['tbl_name']
                    # sync date
                    sync_date = datetime.datetime.strptime(loc_record['sync_dt'], "%Y%m%d")
                    sync_dt = (sync_date + timedelta(1)).strftime("%Y%m%d")
                    init_rd['sync_dt'] = sync_dt
                    # batch date
                    if loc_record['batch_dt'] is None:
                        init_rd['batch_dt'] = datetime.datetime.now().strftime("%Y%m%d")
                    else:
                        batch_date = datetime.datetime.strptime(loc_record['batch_dt'], "%Y%m%d")
                        batch_dt = (batch_date + timedelta(1)).strftime("%Y%m%d")
                        init_rd['batch_dt'] = batch_dt
                    init_rd['sync_type'] = cnf_record['sync_type']
                    rely_tbl = cnf_record.get("rely_tbl")
                    if rely_tbl is not None:
                        init_rd['rely_tbl'] = rely_tbl
                    if cnf_record.get('check_file_num') == 'Y':
                        init_rd['check_file_num'] = 'Y'
                    else:
                        init_rd['check_file_num'] = 'N'
                    init_records.append(init_rd)
                    # when match the data, we remove it in array
                    cnf_records.remove(cnf_record)


        ####################################
        # nlsync.sparkloc -> nlsync.sparkhis
        # sparkloc
        sparkloc_cond = {"$or": [{"sprk_st": "success"}, {"sprk_st": "failed"}]}
        sparkloc_selector = {"_id": {"$include": 0}}
        sparkloc_rds = db.sync_query(sync_sparkloc_cs, sync_sparkloc_cl,
                                     sparkloc_cond, sparkloc_selector)
        for sys_tbl in sparkloc_rds:
            db.sync_insert(sync_sparkhis_cs, sync_sparkhis_cl, sys_tbl)
            # clean collection when sync status is success or failed
            rm_cond = {"tbl_name": sys_tbl.get("tbl_name")}
            db.sync_remove(sync_sparkloc_cs, sync_sparkloc_cl, rm_cond)

    else:
        # remove data
        remove_cond = {'sync_st': {'$ne': 'success'}}
        rm_records = db.sync_query(sync_local_cs, sync_local_cl, remove_cond, fileter, order_by)
        if log is None:
            print "last synchronization failed tables: %s" % rm_records
        else:
            log.warn("last synchronization failed tables: %s" % rm_records)
        for rm_record in rm_records:
            rm_condition = {"tbl_name": rm_record.get("tbl_name")}
            db.sync_remove(sync_local_cs, sync_local_cl, rm_condition)
            db.sync_remove(sync_sparkloc_cs, sync_sparkloc_cl, rm_condition)

        cnf_records = db.sync_query(sync_config_cs, sync_config_cl, config_cond, fileter, order_by)
        success_cond = {'sync_st': 'success'}
        loc_records = db.sync_query(sync_local_cs, sync_local_cl, success_cond, fileter, order_by)
        for cnf_record in cnf_records:
            is_tbl_ok = False
            
            for loc_record in loc_records:
                if loc_record.get("tbl_name") == cnf_record.get("tbl_name"):
                    is_tbl_ok = True
                    break

            # when data is ok, we don't need retry sync
            if is_tbl_ok is True:
                continue

            init_rd = {}
            init_rd['sync_sys'] = cnf_record.get('sync_sys')
            init_rd['tbl_name'] = cnf_record.get('tbl_name')
            sync_date_arr = get_sync_date(db, cnf_record.get("tbl_name"))
            sync_dt = sync_date_arr[0][0]
            batch_dt = sync_date_arr[1][0]
            init_rd['sync_dt'] = sync_dt
            init_rd['batch_dt'] = batch_dt
            init_rd['sync_type'] = cnf_record.get('sync_type')
            init_rd['rely_tbl'] = cnf_record.get('rely_tbl')
            if cnf_record.get('check_file_num') == 'Y':
                init_rd['check_file_num'] = 'Y'
            else:
                init_rd['check_file_num'] = 'N'
            init_records.append(init_rd)

    return init_records


def process_mutex_check():
    sync_os = SyncOS()
    local_host = socket.gethostname()
    main_process_name = 'SYNC|%s|MAIN|%s' % (socket.gethostname(),
                                        datetime.datetime.now().strftime("%Y%m%d"))
    command = "ps -ef | grep '%s' | awk '{print $8}' | grep -v 'grep'" % main_process_name
    result = sync_os.cmd_run(command)
    print "excute command : %s " % command 
    output_lines = result[1].split("\n")
    for output_line in output_lines:
        # 切出来的字段中有main_process_name表示主进程已经运行
        if main_process_name == output_line:
            return True
    return False


def table_backup(db,log,bak_tbl_list,bak_path,max_files=3,max_file_size=256):
    """
    bak_tbl_list:需要备份的表的列表
    bak_path:备份的路径
    max_file_size:每个备份文件的最大大小
    max_files : 备份文件的数量
    """
    condition = {"sync_st":{"$ne":"success"}}
    # 开始和结束行，%s嵌入当前实现，%d嵌入备份表的数量
    beging_line = "\n************begin:%s*********************count:%d*************\n"
    end_line = "************end:%s***********************count:%d*************\n"
     
    if os.path.exists(bak_path) is False:
        os.makedirs(bak_path)
        print "backup directory not exists,make directory: %s" % bak_path
        log.info("backup directory not exists,make directory: %s" % bak_path)
    else:
        print "backup directory:%s exists,don't need to create" % bak_path
        log.info("backup directory:%s  exists,don't need to create" % bak_path)
    
    selector = {'_id':{'$include':0}}
    bak_files = os.listdir(bak_path)
    for bak_tbl_item in bak_tbl_list:
        csname = bak_tbl_item.split(".")[0]
        clname = bak_tbl_item.split(".")[1]
        file_basename = "%s_%s" % (csname,clname)
        dest_file = "%s/%s.bak0" % (bak_path,file_basename)
        print "begin backup table: %s" % bak_tbl_item
        log.info("begin backup table: %s" % bak_tbl_item)
        # 确定当前备份表的最后修改的文件
        for bak_file in bak_files:
            if bak_file.find(file_basename) != -1:
               # 获取最后使用的备份文件
               if os.path.getmtime(dest_file) < os.path.getmtime("%s/%s" % (bak_path,bak_file)):
                   dest_file = "%s/%s" % (bak_path,bak_file)
        fsize = 0
        # 如果该文件已经存在，判断大小是否超过最大值
        if os.path.exists(dest_file) is True:
            fsize = os.path.getsize(dest_file) / (1024*1024)
        if fsize >= max_file_size:
           file_index = int(dest_file[-1])
           # 如果超过了文件数量限制，那么就以写入方式写入bak0（不追加）,否则写入下一个
           if file_index == (max_files - 1):
               fp = open("%s/%s.bak0" % (bak_path,file_basename),"w")
               print "backup into file : %s/%s.bak0,mode 'w'"  % (bak_path,file_basename)
               log.info("backup into file : %s/%s.bak0,mode 'w'"  % (bak_path,file_basename))
           else:
               file_index += 1
               fp = open("%s/%s.bak%d" % (bak_path,file_basename,file_index),"w") 
               print "backup into file : %s/%s.bak%d,mode 'w'"  % (bak_path,file_basename,file_index)
               log.info("backup into file : %s/%s.bak%d,mode 'w'"  % (bak_path,file_basename,file_index))
        else:
           # 如果目标文件还没有达到大小限制，则以追加形式写入
           fp = open(dest_file,"a")
           print "backup into file : %s,mode 'a'" % dest_file
           log.info("backup into file : %s,mode 'a'" % dest_file)
        # 将信息写入文件
        records = db.sync_query(csname,clname,condition={},selector=selector) 
        fp.write(beging_line % (datetime.datetime.now(),len(records)))
        for record in records:
            fp.write(json.dumps(record,indent=1,ensure_ascii=False).decode("utf8") + "," + "\n")
        fp.write(end_line % (datetime.datetime.now(),len(records)))
        print "backup table %s successs" % bak_tbl_item
        log.info("backup table %s successs" % bak_tbl_item)
    print "backup over!"
    log.info("backup over")



def table_backup2(db,log,is_first_run): 
    cond = {"prop_type":"backup"}
    prop_rd = db.sync_query(sync_prop_cs, sync_prop_cl, cond)
    prop_info = prop_rd[0]["info"]
    # 获取备份的时间点，年-月-日 时:分
    backup_tm = time.strftime("%Y-%m-%d %H:%M")
    backup_tbls = []
    for backup_tbl in prop_info:
        # 如果不是当天第一次执行，并且配置为first_run那么在备份时跳过这些表
        if is_first_run is False and backup_tbl["cnf_desc"] == "first_run":
            continue
        origin_cs = backup_tbl["cnf_key"].split(".")[0]
        origin_cl = backup_tbl["cnf_key"].split(".")[1]
        target_cs = backup_tbl["cnf_value"].split(".")[0]
        target_cl = backup_tbl["cnf_value"].split(".")[1]
        origin_rds = db.sync_query(origin_cs,origin_cl)
        for origin_rd in origin_rds:
            origin_rd["backup_tm"] = backup_tm
            db.sync_insert(target_cs,target_cl,origin_rd)
        log.info("Backup Table From %s.%s To %s.%s SUCCESS!" % (origin_cs,origin_cl,target_cs,target_cl))
        print "Backup Table From %s.%s To %s.%s SUCCESS!" % (origin_cs,origin_cl,target_cs,target_cl)


def is_fail4file(failed_rds):
    flag = False
    sync_os = SyncOS()
    for failed_rd in failed_rds:
        tbl_name = failed_rd[1]
        sync_file = failed_rd[2]
        sync_dt = failed_rd[3]
        file_path = eval(sync_file)[0]['sync_file']
        real_file_path = file_path % sync_dt
        real_ok_path = real_file_path.split('.')[0] + '.ok'
        is_file_ready = sync_os.file_is_exists(real_file_path)
        is_ok_ready = sync_os.file_is_exists(real_ok_path)
        if not (is_file_ready is True and is_ok_ready is True):
            print "TABLE: %s ,If data file ready ? %s . If ok file ready ? %s ." % (tbl_name,is_file_ready,is_ok_ready)
            flag = True
            break
    return flag
              
    

def sync_main(retry_times=0):
    hostname = host_name
    svcport = server_port
    username = user_name
    sdb_password = password
    config_cs = sync_config_cs
    config_cl = sync_config_cl
    local_cs = sync_local_cs
    local_cl = sync_local_cl
    sparkloc_cs = sync_sparkloc_cs
    sparkloc_cl = sync_sparkloc_cl
    sync_run_hosts = sync_hosts
    sync_sys = "MANAGE"
    tbl_name = "MDM.MANAGE"
    # SDB collection connection
    db_hosts = hosts
    connect_hosts = []
    for db_host in db_hosts.split(','):
        host_info = db_host.split(':')
        connect_info = {'host': host_info[0], 'service': host_info[1]}
        connect_hosts.append(connect_info)
    db = SCSDB(hostname, svcport, username, sdb_password, connect_hosts)
    print "hostname: %s, svcport: %s" % (hostname, svcport)

    """
    logging file
    """
    tbl_cond = {'tbl_name': tbl_name}
    log_connect = {'HostName': hostname, 'ServerPort': svcport,
                   'UserName': username, 'Password': password,
                   'CsName': sync_log_cs, 'ClName': sync_log_cl}
    sync_date = datetime.datetime.now().strftime("%Y%m%d")
    print '[sync_date]: %s' % sync_date
    sync_date = sync_date
    log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}
    log = logging.getLogger("sync_manager")
    log.setLevel(logging.INFO)
    logfile_name = logfile_dir
    just_write_table = True
    fh = SCFileHandler(logfile_name, log_table, log_connect, just_write_table)
    fh.setLevel(logging.DEBUG)
    #设置log格式
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # 为了避免多个handler，在设置之前将handler置为空
    log.handlers = []
    log.addHandler(fh)

    # clear before
    sync_check = SyncCheck(db)
    get_fail_tbls = sync_check.check_sync_result()
    log.info("last running failed tables: %s" % get_fail_tbls)
    sync_os = SyncOS()
    is_first_run = False
    # when all success, we do global clean
    if 0 == len(get_fail_tbls):
        # 数据表备份
        # bak_tbl_list,bakfile_path,max_bakfiles,max_bakfile_size ，都是在config/global_config中配置的
        # table_backup(db,log,bak_tbl_list,bakfile_path,max_bakfiles,max_bakfile_size)
        is_first_run = True
        sync_host_info = sync_run_hosts.split(',')
        for sync_host_run in sync_host_info:
            sync_host = sync_host_run.split(":")
            sync_hostname = sync_host[0]
            sync_username = sync_host[1]
            sync_password = sync_host[2]
            sync_path = sync_host[3]
            print "host: %s, user: %s, passwrod: %s, path: %s" % (sync_hostname, sync_username, sync_password, sync_path)
            sync_command = 'nohup python %s/sync_data.py ' \
                           '--sync_tables="CLEAR:CLEAR.ALL" ' \
                           '--run_proc_number="0" > /dev/null &' % sync_path
            sync_ret = sync_os.sync_ssh(sync_hostname, sync_username,
                                        sync_password, sync_command)
            log.info("clear invalid file in host: %s. command: %s!"
                     "clear info: %s" % (sync_hostname, sync_command, sync_ret))
            print "clear invalid file in host: %s. command: %s!" \
                  "clear info: %s" % (sync_hostname, sync_command, sync_ret)
    try:
        table_backup2(db,log,is_first_run)
    except Exception,e:
        print "failed to backup table, exception %s" % str(e)
        log.error("failed to backup table, exception %s" % str(e))
    # sync_master is host who run spark and let remote host run sync data
    local_host = socket.gethostname()
    multi_host = sync_master['HostName']
    # set process name
    process_name = 'SYNC|%s|MAIN|%s' % (socket.gethostname(),
                                        datetime.datetime.now().strftime("%Y%m%d"))
    setproctitle.setproctitle(process_name)
    log.info("synchronise data times: %s, process name: %s" % (retry_times, process_name))

    # global doing
    if multi_host == local_host:
        # check up sync success or not
        global_run_tbls = local_copy2_history(db, log)
        print 'global run tables: %s' % global_run_tbls

        # initialize synchronous data queue
        init_sync_queue(db, global_run_tbls)
        log.info("global run table queues: %s" % global_run_tbls)

        ######################################
        # scheduling inspect process
        ######################################
        sync_pstatus = SyncScheduling(log)
        p_status = Process(target=sync_pstatus.inspect_process_status, args=())
        log.info("begin to run scheduling inspect process.")
        p_status.start()

        ######################################
        # scheduling process
        ######################################
        sync_scheduling = SyncScheduling(log)
        p_schedu = Process(target=sync_scheduling.multi_process_sync, args=())
        log.info("begin to run scheduling process.")
        p_schedu.start()
        ######################################

        ######################################
        # spark job process
        ######################################
        if "true" == is_spark_run:
            spark_job = SyncSpark()
            p_spark = Process(target=spark_job.run_spark, args=())
            p_spark.start()
            print 'SPARK Process Running'
            log.info("spark job process run")
            # Spark Process Run Over, if spark is error and nlsync.sparkloc
            # table having running table, we need to set it failed
            while True:
                if p_spark.is_alive():
                    time.sleep(sparksql_wait)
                    continue
                else:
                    # update running and wait to failed when spark process exit
                    time.sleep(spark_restart_wait_time)
                    update_cond = {"sprk_st": {"$ne": "success"}, "sync_sys": {"$ne": ""}}
                    set_ruler = {"$set": {"sprk_st": "failed"}}
                    print 'Spark Job Process exit'
                    log.warn("spark job process exit with exception")
                    db.sync_update(sync_sparkloc_cs, sync_sparkloc_cl, set_ruler, update_cond)
                    break

            # scheduling sync task retry
            restart_times = 5
            cnt = 0
            while True:
                if p_schedu.is_alive():
                    time.sleep(sparksql_wait)
                else:
                    # update running and wait to failed when spark process exit
                    print "status: %s, exist code: %s" % (p_schedu.is_alive(), p_schedu.exitcode)
                    if 0 == p_schedu.exitcode or cnt == restart_times:
                        break
                    else:
                        log.info("restart the run scheduling process. "
                                 "last time exit code: %s" % p_schedu.exitcode)
                        sync_scheduling = SyncScheduling(log)
                        p_schedu = Process(target=sync_scheduling.multi_process_sync, args=())
                        log.info("retry, begin to run scheduling process again.")
                        p_schedu.start()
                        cnt += 1

            p_schedu.join()
            print 'Multiprocess SYNC OVER'
            log.info("finish to run scheduling process.")

            p_status.join()
            log.info("finish to run scheduling inspect process.")
            print 'INSPECT Multiprocess SYNC OVER'

            p_spark.join()
            log.info("finish to run spark job process.")
            print 'SPARK Process Running'
        else:
            # scheduling sync task retry
            restart_times = 5
            cnt = 0
            while True:
                if p_schedu.is_alive():
                    time.sleep(sparksql_wait)
                else:
                    # update running and wait to failed when spark process exit
                    print "status: %s, exist code: %s" % (p_schedu.is_alive(), p_schedu.exitcode)
                    if 0 == p_schedu.exitcode or cnt == restart_times:
                        break
                    else:
                        log.info("restart the run scheduling process. "
                                 "last time exit code: %s" % p_schedu.exitcode)
                        sync_scheduling = SyncScheduling(log)
                        p_schedu = Process(target=sync_scheduling.multi_process_sync, args=())
                        log.info("retry, begin to run scheduling process again.")
                        p_schedu.start()
                        cnt += 1

            p_schedu.join()
            print 'Multiprocess SYNC OVER'
            log.info("finish to run scheduling process.")

            p_status.join()
            print 'INSPECT Multiprocess SYNC OVER'
            log.info("finish to run scheduling inspect process.")

    # check synchronous status
    run_cond = {"$or": [{"sync_st": "running"},
                        {"sync_st": "wait", "tran_file": {"$exists": 1},
                         "proc_info": {"$exists": 1}}]}
    total_wait = 1800
    wait_time = 60
    total_time = 0
    while True:
        run_ret = db.sync_query(local_cs, local_cl, run_cond)
        if len(run_ret) == 0:
            break
        if total_time >= total_wait:
            log.warn("still running tables: %s" % run_ret)
            break

        time.sleep(wait_time)
        total_time += wait_time

    # check end

    sync_check = SyncCheck(db)
    get_fail_tbls = sync_check.check_sync_result()
    log.info("running failed tables: %s" % get_fail_tbls)

    # flush disk after synchronous data
    print '>>>Begin to Flush Disk: %s' % datetime.datetime.now()
    log.info("begin to flush disk")
    db.flush_disk()
    log.info("finish to flush disk")
    print '>>>End to Flush Disk: %s' % datetime.datetime.now()

    return get_fail_tbls

if __name__ == '__main__':
    is_main_running = process_mutex_check()
    if is_main_running is True:
        raise Exception("MAIN PROCESS is RUNNING !!")
    begin_tm = time.time()
    retry_num = sync_retry_times
    # 数据同步运行时间的最大值,sync_total_hours在config/global_config.py中配置，单位为：小时
    sync_total_seconds = sync_total_hours * 3600
    # 执行重拉动作等待的时长，restart_wait_minutes在config/global_config.py中配置，单位为：分钟
    restart_wait_seconds = restart_wait_minutes * 60
    cnt = 0
    while True:
        cnt += 1
        # run_ret 返回增加了一个属性local表中的sync_flag
        run_ret = sync_main(retry_times=cnt)
        end_tm = time.time()
        # 如果全部成功则跳出来
        if 0 == len(run_ret):
            break
        # 如果存在重试次数则马上进行重试
        if cnt < retry_num:
            continue

        is_retry = is_fail4file(run_ret) 
        if is_retry is False or (end_tm - begin_tm) > sync_total_seconds:
            break
        print "have table failed because file not exists,running synchronous  time : %d " % (end_tm - begin_tm) 
        time.sleep(restart_wait_seconds)

        



