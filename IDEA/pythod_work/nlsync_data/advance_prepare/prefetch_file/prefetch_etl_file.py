#!/usr/bin/python
#coding=utf-8

import sys
import os
sys.path.append(os.path.abspath(os.path.join(sys.path[0], "..")))
from multiprocessing import Process, Queue, Pipe, Lock, Value, Array, Manager
import time
import setproctitle
import datetime
from datetime import timedelta 
import shutil
import socket
import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)

from prefile_config import *
from sync_logger import *


class PrefetchETLFile:
    def __init__(self, log_handler):
        self.hostname = host_name
        self.svcport = server_port
        self.username = user_name
        self.password = password
        self.config_cs = sync_config_cs
        self.config_cl = sync_config_cl
        self.mdm_metahis_cs = mdm_metahis_cs
        self.mdm_metahis_cl = mdm_metahis_cl
        self.log = log_handler
        self.prefetch_process_num = prefetch_process_num
        self.prefetch_one_day = prefetch_one_day
        self.config_rd = dict()
        self.total_times = 3
        self.wait_sleep_time = 1

        # SDB collection connection
        self.db_hosts = hosts
        self.connect_hosts = []
        for db_host in self.db_hosts.split(','):
            host_info = db_host.split(':')
            connect_info = {'host': host_info[0], 'service': host_info[1]}
            self.connect_hosts.append(connect_info)

        # make sure the connect is correct
        self.db = client(self.hostname, self.svcport, self.username, base64.decodestring(self.password))
        if '' != self.db_hosts:
            self.db.connect_to_hosts(self.connect_hosts, user=self.username, password=base64.decodestring(self.password))
            
        # set read from primary
        attri_options = {'PreferedInstance': 'M'}
        self.db.set_session_attri(attri_options)

    def is_num_by_except(self, num):
        try:
            int(num)
            return True
        except ValueError:
            return False 

    def is_vaild_date(self,str):
        try:
            datetime.datetime.strptime(str,"%Y%m%d")
            return True
        except ValueError:
            return False


    def check_tbl_name(self,prefile_batch_dt):
        file_queue_cond = {"$and": [{"pref_file": {"$ne": ""}},
                                    {"pref_file": {"$exists": 1}},
                                    {"is_sync": "true"}]}

        file_queue_selector = {"tbl_name": 1}
        file_queue_sort = {"sync_sys": 1}
        conf_records = self.sync_query(self.config_cs, self.config_cl, file_queue_cond, file_queue_selector, file_queue_sort)

        metahis_prefile_cod = {"etl_status": "success","prefile_batch_dt": prefile_batch_dt}
        metahis_prefile_selector = {"tbl_name": 1}
        metahis_prefile_sort = {"sync_sys": 1}
        metahis_records = self.sync_query(self.mdm_metahis_cs, self.mdm_metahis_cl, metahis_prefile_cod, metahis_prefile_selector, metahis_prefile_sort)

        is_prefile = False 
        check_info_arr = []
        if 0 == len(conf_records) or conf_records is None:
            self.log.warn("do not needs to fetch file on the config table, so the param conf_records is %s" %(conf_records)) 
            return check_info_arr 

        if 0 == len(metahis_records) or metahis_records is None:
            self.log.warn("the param metahis_records is %s" %(metahis_records)) 
            return check_info_arr 
         
        self.log.info("begin to check tbl_name from metahis table") 
        for conf_record in conf_records:
            conf_tbl_name = conf_record["tbl_name"]
            is_prefile = False
            for metahis_record in metahis_records:
                metahis_tbl_name = metahis_record["tbl_name"]
                if conf_tbl_name == metahis_tbl_name:
                    is_prefile = True
                    break;
            if not is_prefile:
                is_polling = "false"
                check_info_arr.append(prefile_batch_dt)
                check_info_arr.append(is_polling) 
                return check_info_arr 
            
        is_polling = "true"
        add_dt = timedelta(days=int(1))          # every day add one day data            
        next_prefile_batch_dt_str = datetime.datetime.strptime(prefile_batch_dt, '%Y%m%d') + abs(add_dt)
        next_prefile_batch_dt = next_prefile_batch_dt_str.strftime("%Y%m%d")
        check_info_arr.append(next_prefile_batch_dt)
        check_info_arr.append(is_polling)

        return check_info_arr 



    def check_prefile_batch_dt(self):
        check_config_Obtain_file_cond = {"$and": [{"pref_file": {"$ne": ""}},
                                    {"pref_file": {"$exists": 1}},
                                    {"is_sync": "true"}]}
        iS_need_Obtain_file_count = self.sync_get_count(self.config_cs,
                                                        self.config_cl,
                                                        condition=check_config_Obtain_file_cond)

        newest_prefile_batch_dt_cond = {"etl_status": "success"}
        newest_prefile_batch_dt_selectors = {"prefile_batch_dt": "1"}
        newest_prefile_batch_dt_orderby = {"prefile_batch_dt": -1}
        newest_prefile_batch_dt_num_to_return = 1L
        newest_prefile_batch_dt_rd = self.sync_query(self.mdm_metahis_cs,self.mdm_metahis_cl,
                                                  condition=newest_prefile_batch_dt_cond,
                                                  selector=newest_prefile_batch_dt_selectors,
                                                  order_by=newest_prefile_batch_dt_orderby,
                                                  hint={},skip=0L,
                                                  num_to_return=newest_prefile_batch_dt_num_to_return)
        
        check_info_arr = []
        is_polling = "false"
        if newest_prefile_batch_dt_rd is None or 0 == len(newest_prefile_batch_dt_rd):
            is_polling = "true"
            next_prefile_batch_dt = datetime.datetime.now().strftime("%Y%m%d") 
            check_info_arr.append(next_prefile_batch_dt)
            check_info_arr.append(is_polling) 
            self.log.warn("the metahis is not exists prefile_batch_dt fields!")
        else:
            newest_prefile_batch_dt = newest_prefile_batch_dt_rd[0]["prefile_batch_dt"]
            if newest_prefile_batch_dt is not None and self.is_vaild_date(newest_prefile_batch_dt):
                check_metahis_akready_acquired_cond = {"etl_status": "success",
                                                       "prefile_batch_dt": newest_prefile_batch_dt
                                                       }
                check_metahis_akready_acquired_count = self.sync_get_count(self.mdm_metahis_cs,
                                                                           self.mdm_metahis_cl,
                                                                           condition=check_metahis_akready_acquired_cond)

                if iS_need_Obtain_file_count is not None and  0 != iS_need_Obtain_file_count \
                        and check_metahis_akready_acquired_count is not None \
                        and 0 != check_metahis_akready_acquired_count \
                        and iS_need_Obtain_file_count == check_metahis_akready_acquired_count:
                    add_dt = timedelta(days=int(1))
                    newest_prefile_batch_dt_str = datetime.datetime.strptime(newest_prefile_batch_dt, '%Y%m%d')
                    next_prefile_batch_dt_str = newest_prefile_batch_dt_str + abs(add_dt)
                    next_prefile_batch_dt = next_prefile_batch_dt_str.strftime("%Y%m%d")
                    is_polling = "true"
                    check_info_arr.append(next_prefile_batch_dt)
                    check_info_arr.append(is_polling)
                elif iS_need_Obtain_file_count is not None and  0 != iS_need_Obtain_file_count \
                        and check_metahis_akready_acquired_count is not None \
                        and 0 != check_metahis_akready_acquired_count \
                        and iS_need_Obtain_file_count > check_metahis_akready_acquired_count:
                    is_polling = "false"
                    check_info_arr.append(newest_prefile_batch_dt)
                    check_info_arr.append(is_polling)
                else:
                    self.log.warn("the config total count is less metahis by prefile_batch_dt")
                    check_info_arr = self.check_tbl_name(newest_prefile_batch_dt)
                    self.log.error("the param check_info_arr is %s" %(check_info_arr)) 
                    #raise
            else:
                is_polling = "true"
                next_prefile_batch_dt = datetime.datetime.now().strftime("%Y%m%d") 
                check_info_arr.append(next_prefile_batch_dt)
                check_info_arr.append(is_polling) 
                #self.log.error("the newest prefile_batch_dt is null or is not string number on the metahis!")
                self.log.warn("the newest prefile_batch_dt is null or is not string number on the metahis!")
                #raise
        
        return check_info_arr
        

    def get_prefetch_date(self, tbl_name,prefile_batch_dt):
        date_arr = [] * 2
        tbl_cond = {"tbl_name": tbl_name}
        cnf_records = self.sync_query(sync_config_cs, sync_config_cl, tbl_cond)
        if 0 == len(cnf_records):
            print 'the table: %s have not in sync.config when get sync data' % tbl_name
            self.log.error('the table: %s have not in sync.config when get sync data' % tbl_name)
            raise

        # sync.history
        his_selector = {'sync_dt': 1,'prefile_batch_dt': "1"}
        his_orderby = {'sync_dt': -1}
        num_to_return = 1L
        his_cond = {"tbl_name": tbl_name, "etl_status": "success"}
        his_records = self.sync_query(self.mdm_metahis_cs,
                                         self.mdm_metahis_cl,
                                         condition=his_cond,
                                         selector=his_selector,
                                         order_by=his_orderby,
                                         hint={}, skip=0L,
                                         num_to_return=num_to_return)

        if 0 == len(his_records):
            his_cond = {"tbl_name": tbl_name, "prefile_batch_dt": prefile_batch_dt}
            his_records = self.sync_query(self.mdm_metahis_cs,self.mdm_metahis_cl,condition=his_cond)
            if 0 == len(his_records):
                # the delta time how long data synchronize to SDB
                dt_delta = cnf_records[0]['dt_delta']
                # auto generate date time
                delta_dt = timedelta(days=int(dt_delta))
                local_time = datetime.datetime.now() - abs(delta_dt)
                local_date = local_time.strftime("%Y%m%d")
                next_sync_dt = local_date 
                next_prefile_batch_dt = prefile_batch_dt
                self.config_rd = cnf_records[0]
            else:
                next_sync_dt = his_records[0]['sync_dt']
                next_prefile_batch_dt = prefile_batch_dt
                self.config_rd = cnf_records[0]
        else:
            last_his_prefile_batch_dt = his_records[0]["prefile_batch_dt"]
            if last_his_prefile_batch_dt is not None:
                if self.is_vaild_date(last_his_prefile_batch_dt):
                    if last_his_prefile_batch_dt == prefile_batch_dt:
                        check_his_cond = {"tbl_name": tbl_name, "prefile_batch_dt": prefile_batch_dt}
                        check_his_count_by_prefile_batch_dt = self.sync_get_count(self.mdm_metahis_cs,self.mdm_metahis_cl,condition=check_his_cond)
                        if 1 == check_his_count_by_prefile_batch_dt:
                            next_sync_dt = his_records[0]['sync_dt'] 
                            next_prefile_batch_dt = prefile_batch_dt
                            self.config_rd = cnf_records[0]
                        else:
                            self.log.error("the table %s have smae the prefetch data, the prefile_batch_dt is %s" %(check_his_count_by_prefile_batch_dt,prefile_batch_dt))
                    elif int(last_his_prefile_batch_dt) < int(prefile_batch_dt):
                        # get sync date time from table sync.history
                        sync_last_dt = datetime.datetime.strptime(his_records[0]['sync_dt'], '%Y%m%d')
                        add_dt = timedelta(days=int(1))          # every day add one day data            
                        sync_local_dt = sync_last_dt + abs(add_dt)
                        next_sync_dt = sync_local_dt.strftime("%Y%m%d")
                        next_prefile_batch_dt = prefile_batch_dt
                        self.config_rd = cnf_records[0]
                    else:
                        self.log.error("the prefile_batch_dt than less metahis table last prefile_batch_dt for the table %s, it is failed!" %(tbl_name))
                        raise
                else:
                    his_cond = {"tbl_name": tbl_name, "prefile_batch_dt": prefile_batch_dt}
                    is_exists_rd = self.sync_query(self.mdm_metahis_cs,self.mdm_metahis_cl,condition=his_cond)
                    if 0 != len(is_exists_rd):
                        is_sync_dt = is_exists_rd[0]["sync_dt"]
                        newest_his_sync_dt = his_records[0]['sync_dt']
                        if int(is_sync_dt) >= int(newest_his_sync_dt):
                            next_sync_dt = is_exists_rd[0]["sync_dt"]
                            next_prefile_batch_dt = prefile_batch_dt
                            self.config_rd = cnf_records[0]
                        else:
                            self.log.error("the newest sync_dt is less the newest prefile_batch_dt sync_dt!") 
                            
                    else:
                        sync_last_dt = datetime.datetime.strptime(his_records[0]['sync_dt'], '%Y%m%d')
                        add_dt = timedelta(days=int(1))          # every day add one day data            
                        sync_local_dt = sync_last_dt + abs(add_dt)
                        next_sync_dt = sync_local_dt.strftime("%Y%m%d")
                        next_prefile_batch_dt = prefile_batch_dt
                        self.config_rd = cnf_records[0]
            else:
                self.log.error("the newest prefile_batch_dt is null on the  mdm.metahis , the tbl_name is %s, it is failed!" %(tbl_name))
                raise
                

        date_arr.append(next_sync_dt)
        date_arr.append(next_prefile_batch_dt)        
        return date_arr        
        
    
    def generate_file_queue(self,prefetch_file_queue,prefile_batch_dt):
        file_queue_cond = {"$and": [{"pref_file": {"$ne": ""}},
                               {"pref_file": {"$exists": 1}},
                               {"is_sync": "true"}]}

        file_queue_selector = {"tbl_name": 1}
        file_queue_sort = {"sync_sys": 1}
        conf_records = self.sync_query(self.config_cs, self.config_cl, file_queue_cond, file_queue_selector, file_queue_sort)
        prefetch_file_queue_array = []
        out_file_queue_infos = []
        for conf_record in conf_records:
            prefetch_file_array = []
            tbl_name = conf_record.get("tbl_name")
            cs_name = tbl_name.split(".")[0]
            cl_name = tbl_name.split(".")[1]
            tbl_name = cs_name + "." + cl_name
            sync_generate_file_infos = self.get_prefetch_date(tbl_name,prefile_batch_dt)
            if self.prefetch_one_day == "true":
                if sync_generate_file_infos is None or 0 == len(sync_generate_file_infos):
                    self.log.error("get the prefetch_date is failed , the result sync_generate_file_infos is null!")
                    raise
                else:
                    prefetch_date = sync_generate_file_infos[0] 

                    prefetch_file = self.get_prefetch_file() % (prefetch_date, prefetch_date)
                    prefetch_ok_file = self.get_prefetch_ok_file() % (prefetch_date,prefetch_date)
                    sync_file = self.get_table_sync_file() % (prefetch_date)
                    
                    prefetch_file_array.append(prefetch_file)
                    prefetch_file_array.append(prefetch_ok_file)
                    prefetch_file_array.append(sync_file)
                    prefetch_file_array.append(tbl_name)
                    prefetch_file_array.append(prefetch_date)
                    prefetch_file_queue.put(prefetch_file_array)
                    prefetch_file_queue_array.append(prefetch_file_array)
                    
                    out_file_infos = prefetch_file + "--> " + sync_file
                    out_file_queue_infos.append(out_file_infos)
            
            else:
                self.log.warn("Do not need to sync prefetch file! the para self.prefetch_one_day is %s!" %(self.prefetch_one_day))
    
        #print "generate file queue is: %s" % prefetch_file_queue_array
        self.log.info("generate file  is: %s" % (out_file_queue_infos))
        return prefetch_file_queue_array

    def init_prefile_batch_dt(self,prefetch_file_queue_array_infos,prefile_batch_dt,is_poll):
        metahis_exists_tbl_cnd = {"etl_status": "success","prefile_batch_dt": prefile_batch_dt}
        metahis_exists_selector = {"tbl_name": 1}
        exist_his_rds = self.sync_query(self.mdm_metahis_cs,self.mdm_metahis_cl,condition=metahis_exists_tbl_cnd,selector=metahis_exists_selector)
        
        if 0 != len(prefetch_file_queue_array_infos):
            for prefetch_file_array in prefetch_file_queue_array_infos:
                tbl_name = prefetch_file_array[3]
                if exist_his_rds is not None:
                    if 0 == len(exist_his_rds) and "true" == is_poll:
                        sync_dt = prefetch_file_array[4]
                        pref_file = prefetch_file_array[0]
                        matcher = {"tbl_name": tbl_name, "sync_dt": sync_dt}
                        ruler = {"$set": {"etl_file": pref_file,"prefile_batch_dt": prefile_batch_dt}}
                        #print "ruler: %s, matcher: %s" %(ruler,matcher) 
                        self.log.info("initialized table %s -->ruler: %s, matcher: %s" %(tbl_name,ruler,matcher))
                        self.sync_upsert(self.mdm_metahis_cs, self.mdm_metahis_cl, ruler, matcher)
                    elif 0 < len(exist_his_rds) and "false" == is_poll:
                        init_flag = True 
                        for exist_his_rd in exist_his_rds:
                            exist_tbl_name = exist_his_rd["tbl_name"] 
                            if tbl_name == exist_tbl_name:
                                init_flag = False
                                break 

                        if not init_flag:
                            self.log.info("That table %s has been initialized!" %(exist_tbl_name))
                        else:
                            self.log.info("That table %s needs to be be initialized!" %(exist_tbl_name))
                            sync_dt = prefetch_file_array[4]
                            pref_file = prefetch_file_array[0]
                            matcher = {"tbl_name": tbl_name, "sync_dt": sync_dt}
                            ruler = {"$set": {"etl_file": pref_file,"prefile_batch_dt": prefile_batch_dt}}
                            #print "ruler: %s, matcher: %s" %(ruler,matcher) 
                            self.log.info("initialized table %s -->ruler: %s, matcher: %s" %(tbl_name,ruler,matcher))
                            self.sync_upsert(self.mdm_metahis_cs, self.mdm_metahis_cl, ruler, matcher)
                
                    else:
                        self.log.error("Initialized is failed !")
                        raise
                else:
                    self.log.error("the exist_his_rds is null!")
                    raise

        else:
            print "the prefetch_file_queue_array_infos is null ! "
            prefetch_etl.log.error("the prefetch_file_queue_array_infos is null on the init_prefile_batch_dt method!")
            raise
   
        time.sleep(10) 


    def generate_type_run(self,prefetch_file_queue):
        check_prefile_batch_dt_infos = self.check_prefile_batch_dt()
        if check_prefile_batch_dt_infos is not None and 0 != len(check_prefile_batch_dt_infos):
            is_poll = check_prefile_batch_dt_infos[1]
            if "true" == is_poll:
                prefile_batch_dt = check_prefile_batch_dt_infos[0]
                self.log.info("Begin to all flop from next day  %s" %(prefile_batch_dt))
                print 'prefile_batch_dt ---------- %s' % prefile_batch_dt
                prefetch_file_queue_array_infos = self.generate_file_queue(prefetch_file_queue,prefile_batch_dt)
                self.init_prefile_batch_dt(prefetch_file_queue_array_infos,prefile_batch_dt,is_poll)
            else:
                prefile_batch_dt = check_prefile_batch_dt_infos[0]
                self.log.info("Not all completed yet  on that day %s" %(prefile_batch_dt))
                print 'prefile_batch_dt ---------- %s' % prefile_batch_dt
                prefetch_file_queue_array_infos = self.generate_file_queue(prefetch_file_queue,prefile_batch_dt)
                self.init_prefile_batch_dt(prefetch_file_queue_array_infos,prefile_batch_dt,is_poll)

            prefetch_file_queue_array_infos.append(check_prefile_batch_dt_infos[0])
        else:
            self.log.error("the check prefile_batch_dt is failed!,the result check_prefile_batch_dt_infos is null!")
            prefetch_file_queue_array_infos = []
        
        return prefetch_file_queue_array_infos    
    
    def sync_get_cscl(self, csname, clname):
        try:
            #print "--0--BEGIN to get table: %s.%s's connect handler" % (csname, clname)
            cs = self.db.get_collection_space(csname)
            #print "--1--BEGIN to get table: %s.%s's connect handler" % (csname, clname)
            cl = cs.get_collection(clname)
            #print "--2--BEGIN to get table: %s.%s's connect handler" % (csname, clname)

            return cl
        except (SDBBaseError, SDBTypeError), e:
            print "failed to get table: %s.%s's connect handler, error code: %s" % (csname, clname, e.code)


    def sync_get_count(self, cs_name, cl_name, condition=None):
        try:
            cl = self.sync_get_cscl(cs_name, cl_name)
            count = cl.get_count(condition)

            return count
        except (SDBBaseError, SDBTypeError), e:
            print e.code


    def sync_query(self, cs_name, cl_name, condition={},
                   selector={}, order_by={},
                   hint={}, skip=0L, num_to_return=-1L):
        try:
            cl = self.sync_get_cscl(cs_name, cl_name)
            cursor = cl.query(condition=condition, selector=selector,
                              order_by=order_by, hint=hint,
                              skip=skip, num_to_return=num_to_return)
            records = []
            while True:
                try:
                    record = cursor.next()
                    records.append(record)
                except SDBEndOfCursor:
                    break
                except SDBBaseError:
                    raise

            return records
        except (SDBBaseError, SDBTypeError), e:
            print e.code

    def get_table_sync_file(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[0].get("sync_file")

    def get_prefetch_file(self):
        if self.config_rd.get("pref_file") is not None and "" != self.config_rd.get("pref_file"):
            return self.config_rd.get("pref_file").split(",")[0]


    def get_prefetch_ok_file(self):
        if self.config_rd.get("pref_file") is not None and "" != self.config_rd.get("pref_file"):
            return self.config_rd.get("pref_file").split(",")[1]

    def sync_upsert(self, cs_name, cl_name, ruler, matcher):
        try:
            cl = self.sync_get_cscl(cs_name, cl_name)
            cl.upsert(rule=ruler, condition=matcher)
        except (SDBBaseError, SDBTypeError), e:
            print "failed to upsert: %s, condition: %s, ruler: %s" % (e.code, matcher, ruler)
            raise
            

    def get_dirname(self, absolute_file):
        try:
            return os.path.dirname(absolute_file)
        except OSError, e:
            raise

    def file_is_exists(self, file_name):
        try:
            return os.path.isfile(file_name.strip())
        except IOError, e:
            if self.log is None:
                print "failed to inspect file: %s is exists or not! " \
                      "error info: %s" % (file_name, e)
            else:
                self.log.error("failed to inspect file: %s is exists or not! "
                              "error info: %s" % (file_name, e))
            raise

    def get_file_size(self, file_name):
        try:
            if os.path.isfile(file_name):
                return os.path.getsize(file_name)
            else:
                return None
        except IOError, e:
            if self.log is None:
                print "failed to get file  %s's size!error info: %s" % (file_name, e)
            else:
                self.log.error("failed to get file  %s's size!"
                               "error info: %s" % (file_name, e))
            raise            

    def cmd_run(self, command_line):
        try:
            run_status = []
            (status, output) = commands.getstatusoutput(command_line)
            run_status.append(status)
            run_status.append(output)
            return run_status
        except OSError, e:
            raise


    def make_directory(self, dirname):
        """
        make directory
        Args:
            dirname: the making directory's name

        Returns:

        """
        try:
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
                # give new director high autority, how to give autority of parent directory
                cmd = 'chmod 777 %s -R' % dirname
                self.cmd_run(cmd)
        except OSError, e:
            if self.log is None:
                print "make directory: %s failed! error info: %s" % (dirname, e)
            else:
                self.log.warn("make directory: %s failed! error info: %s" % (dirname, e))
            if 17 == e.errno and os.path.isdir(dirname) is True:
                if self.log is None:
                    print 'File directory is exists: %s' % e.errno
                else:
                    self.log.warn('File directory is exists: %s' % e.errno)
            else:
                if self.log is None:
                    print 'File exists: %s and File is directory: %s' % (e.errno, os.path.isdir(dirname))
                else:
                    self.log.error('File exists: %s and File is directory: %s' % (e.errno, os.path.isdir(dirname)))
                raise

    def copy_file(self, source_file, destination_file):
        try:
            return shutil.copy(source_file, destination_file)
        except (OSError,IOError), e:
            print "failed to copy source file: to destination file: %s!" % e.errno
            """
            if 2 != e.errno:
                if self.log is None:
                    print "failed to copy source file: %s to destination file: %s !" \
                          "error info: %s" % (source_file, destination_file, e)
                else:
                    self.log.error("failed to copy source file: %s to destination file: %s !"
                                   "error info: %s" % (source_file, destination_file, e))
                raise
            """

    def give_highest_authority(self, filename):
        try:
            command_line = 'mkdir -p %s; chmod 777 %s;' \
                           'chmod 777 %s;chmod 777 %s' % (self.get_dirname(filename),
                                             os.path.abspath(os.path.join(self.get_dirname(filename), "..")),
                                             self.get_dirname(filename),
                                             filename)
            run_ret = self.cmd_run(command_line)
            return run_ret
        except IOError, e:
            raise            
            
    def check_local_file(self, sync_file, sync_ok_file=None, remote_file_size=None):
        file_ready = False
        #print "input check local file: %s -- %s" % (sync_file, sync_ok_file)
        if sync_ok_file is None:
            ok_file = sync_file.split('.')[0] + '.ok'
        else:
            ok_file = sync_ok_file
        ret_data = self.file_is_exists(sync_file)
        ret_ok = self.file_is_exists(ok_file)
        local_file_size = None
        if ret_data is True and ret_ok is True:
            local_file_size = self.get_file_size(sync_file)
        if remote_file_size is None:
            if ret_data is True and ret_ok is True:
                file_ready = True
        else:
            if ret_data is True and ret_ok is True and \
               local_file_size == remote_file_size:
                file_ready = True

        #print ">>>>check local file: %s -- %s, %s -- %s, %s -- %s" % (sync_file, ok_file, ret_data, ret_ok, local_file_size, remote_file_size)

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
            dirname = self.get_dirname(sync_file)
            self.make_directory(dirname)
            # copy data file
            self.copy_file(pref_file, sync_file)
            # copy ok file
            pref_ok_file = prefetch_ok_file
            sync_ok_file = sync_file.split('.')[0] + '.ok'
            src_file_size = self.get_file_size(pref_file)
            print "ok file: %s - %s" % (pref_ok_file, sync_ok_file)
            self.copy_file(pref_ok_file, sync_ok_file)
            # create directory for sync file and give high autority for directory
            self.give_highest_authority(sync_file)
            self.give_highest_authority(sync_ok_file)
            # check file is get correct or not
            file_ready = self.check_local_file(sync_file, None,
                                               src_file_size)

        return file_ready
            
            
    def prefectch_etl_file(self,prefetch_queue, prefetch_lock, prefile_batch_dt):
        while True:
            prefetch_lock.acquire()
            if prefetch_queue.empty() and 0 == prefetch_queue.qsize():
                print "queue is empty and queue size is 0"
                self.log.info("prefectch ETL file queue is empty. prefectch all ETL file success")
                prefetch_lock.release()
                break
            print 'queue size: %s' % prefetch_queue.qsize()
            queue_data = prefetch_queue.get()
            prefetch_lock.release()
            pref_file = queue_data[0]
            pref_ok_file = queue_data[1]
            sync_file = queue_data[2]
            tbl_name = queue_data[3]
            prefetch_date = queue_data[4]
            process_name = 'SYNC-Prefetch|%s|%s:%s->%s|%s' \
                           % (socket.gethostname(), tbl_name, pref_file,
                              sync_file, datetime.datetime.now().strftime("%Y%m%d"))
            setproctitle.setproctitle(process_name)
            print "PID: %s, process name: %s" % (os.getpid(), process_name)
            prefetch_ok = self.prefetch_file(pref_file, pref_ok_file, sync_file)
            # update infomation into table
            ruler = {"$set": {"etl_file": pref_file, "etl_status": "success", "prefile_batch_dt": prefile_batch_dt}}
            matcher = {"tbl_name": tbl_name, "sync_dt": prefetch_date}
            print "ruler: %s, matcher: %s" %(ruler,matcher) 
            print "preftech file: %s, status: %s" % (pref_file, prefetch_ok)
            if prefetch_ok:
                print "success to preftech file: %s" % pref_file
                prefetch_lock.acquire()
                self.sync_upsert(self.mdm_metahis_cs, self.mdm_metahis_cl, ruler, matcher)
                prefetch_lock.release()
                self.log.info("success to preftech file: %s, PID: %s, process name: %s" % (pref_file, os.getpid(), process_name))
            else:
                print "faile to preftech file: %s" % pref_file
                prefetch_lock.acquire()
                queue_data = prefetch_queue.put(queue_data)
                prefetch_lock.release()



def sync_prefile_log():
    hostname = host_name
    svcport = server_port
    username = user_name
    pfpassword = password
    log_cs = sync_log_cs
    log_cl = sync_log_cl

    """
    logging file
    """
    # table name
    sync_sys = "PREFETCH"
    tbl_name = "PREFETCH_FILE"
    log_connect = {'HostName': hostname, 'ServerPort': svcport,
                   'UserName': username, 'Password': password,
                   'CsName': log_cs, 'ClName': log_cl}
    # prefetch file's sync date using local date
    sync_date = datetime.datetime.now().strftime("%Y%m%d")
    print '[sync_date]: %s' % sync_date
    #sync_date = sync_date[0]
    log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}

    log = logging.getLogger("sync_prefetch")
    log.setLevel(logging.INFO)
    logfile_name = logfile_dir
    just_write_table = True
    fh = SCFileHandler(logfile_name, log_table, log_connect, just_write_table)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    log.addHandler(fh)
    return log

def sync_main():
    #logging file
    log = sync_prefile_log()
    # set process name
    sync_date = sync_date = datetime.datetime.now().strftime("%Y%m%d")
    process_name = 'SYNC|%s|PrefetchMain|%s' % (socket.gethostname(),
                                                sync_date)
    setproctitle.setproctitle(process_name)
    # global doing
    lock = Lock()
    prefetch_etl = PrefetchETLFile(log)
    prefetch_queue = Queue()
    prefetch_array = prefetch_etl.generate_type_run(prefetch_queue)
    if 0 != len(prefetch_array):
        prefile_batch_dt = prefetch_array[len(prefetch_array) - 1]
    else:
        #print "the prefetch_array is null ! ",prefetch_array
        prefetch_etl.log.error("the prefetch_array is null !")
        raise
    #print "get prefetch_queue: %s" % prefetch_queue
    process_num = prefetch_process_num
    # check up sync success or not
    p_etl = []
    log.info('Begin to Prefetch file in date: %s. Prefetch queue: %s' % (sync_date, prefetch_array))

    queue_size = prefetch_queue.qsize()
    join_flag = 0
    print "queue_size: ",queue_size
    for i in range(process_num):
        if queue_size == 0:
            break
        #p_pref_etl = Process(target=prefetch_etl.prefectch_etl_file, args=(prefetch_queue, lock, prefile_batch_dt))
        p_pref_etl = Process(target=prefetch_etl.prefectch_etl_file, args=(prefetch_queue, lock, prefile_batch_dt))
        p_pref_etl.start()
        p_etl.append(p_pref_etl)
        queue_size = queue_size - 1
        join_flag = join_flag + 1


    #for i in range(process_num):
    for i in range(join_flag):
        p_etl[i].join()

    join_flag = 0
    queue_size = prefetch_queue.qsize()
    prefetch_queue_arr = [] * queue_size
    while prefetch_queue.qsize():
        prefetch_queue_arr.append(prefetch_queue.get())

    #print 'Finish to Prefetch file in date: %s.Prefetch queue: %s' % (sync_date, prefetch_queue_arr)
    log.info('Finish to Prefetch file in date: %s.Prefetch queue: %s' % (sync_date, prefetch_queue_arr))

if __name__ == '__main__':
    sync_main()
