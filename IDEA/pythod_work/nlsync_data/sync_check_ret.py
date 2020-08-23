#!/usr/bin/python
#coding=utf-8

from sync_sdb import *
from config.global_config import *
from sync_orm import *
import socket

class SyncCheck:
    def __init__(self, db_connect_handle, log_handler=None):
        # self.fail_bak_file = sync_bakfail_file don't need this
        self.log = log_handler
        self.db = db_connect_handle

    def check_sync_result(self):
        # from table nlsync.config get sync system name
        aggre_cond = []
        aggre1 = {'$group': {'_id': {'sync_sys': '$sync_sys'}}}
        aggre2 = {'$project': {'sync_sys': 1}}
        aggre_cond.append(aggre1)
        aggre_cond.append(aggre2)
        sys_nms = self.db.sync_aggregate(sync_config_cs, sync_config_cl,
                                         aggre_cond)
        if 0 == len(sys_nms):
            self.log.info("synchronos config table have no sync system name: %s" % sys_nms)
            raise Exception;
        # according everyone system to inspect sync table
        local_fail_tbls = []
        sys_selector = {'_id': {'$include': 0}}
        tbl_orderby = {'tbl_name': 1}
        for sys_nm in sys_nms:
            sync_sys = sys_nm['sync_sys']
            loc_sys_condition = {'sync_sys': sync_sys}
            # config table must inspect is_sync field
            cnf_sys_condition = {'sync_sys': sync_sys, 'is_sync': {'$ne': 'false'}}
            loc_records = self.db.sync_query(sync_local_cs, sync_local_cl,
                                             loc_sys_condition, sys_selector, tbl_orderby)
            cnf_records = self.db.sync_query(sync_config_cs, sync_config_cl,
                                             cnf_sys_condition, sys_selector, tbl_orderby)

            for cnf_rd in cnf_records:
                is_equal = False
                sync_dt = ''
                for loc_rd in loc_records:
                    if loc_rd.get('tbl_name') == cnf_rd.get('tbl_name') and \
                       loc_rd.get("sync_st") == 'success':
                        is_equal = True
                        break
                    elif loc_rd.get('tbl_name') == cnf_rd.get('tbl_name') and \
                       loc_rd.get("sync_st") == 'failed' and loc_rd.get('sync_dt') is not None:
                        sync_dt =  loc_rd.get('sync_dt')   

                if is_equal is False:
                    cnf_info = [] * 4
                    cnf_info.append(cnf_rd.get('sync_sys'))
                    cnf_info.append(cnf_rd.get('tbl_name'))
                    cnf_info.append(cnf_rd.get('sync_file'))
                    cnf_info.append(sync_dt)
                    local_fail_tbls.append(cnf_info)
        if self.log:
            self.log.error("failed to synchonouse data! failed tables: %s" % local_fail_tbls)
        else:
            print "failed to synchonouse data! failed tables: %s" % local_fail_tbls
        return local_fail_tbls

if __name__ == '__main__':
    SYNC_OK = 0
    SYNC_ERR = 1
    db = SCSDB(host_name, server_port, user_name, password)
    sync_check = SyncCheck(db)
    sync_check.check_sync_result()

