#!/usr/bin/python
#coding=utf-8

import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)

import collections
import datetime
from datetime import timedelta
import time
import base64
import commands
from bson.objectid import ObjectId


class SCSDB:
    def __init__(self, hostname='localhost', svcport='11810',
                 username='', password='', connect_hosts='', log_handler=None):
        self.hostname = hostname
        self.svcport = svcport
        self.username = username
        self.password = password
        self.db = client(self.hostname, self.svcport, self.username, base64.decodestring(self.password))
        # make sure the connect is correct
        if '' != connect_hosts:
            self.db.connect_to_hosts(connect_hosts, user=self.username, password=base64.decodestring(self.password))
        # set read from primary
        attri_options = {'PreferedInstance': 'M'}
        self.db.set_session_attri(attri_options)
        self.log = log_handler
        self.SDB_SNAP_CATALOG = 8

    def sync_create_cscl(self, cs_name, cl_name, cs_options={}, cl_options={},
                         is_crt_cs=False, is_crt_cl=False):
        retry_times = 5
        retry_wait_time = 60
        run_number = 0
        cl = None
        while True:
            # 1. 创建SDB集合空间
            try:
                cs = self.db.create_collection_space(cs_name, cs_options)
            except SDBError, e:
                if -33 == e.code and is_crt_cs is False:
                    cs = self.db.get_collection_space(cs_name)
                elif -33 == e.code and is_crt_cs is True:
                    self.db.drop_collection_space(cs_name)
                    cs = self.db.create_collection_space(cs_name, cs_options)
                else:
                    # retry create collection space
                    if run_number <= retry_times:
                        print "Retry, table %s.%s createCS failed, error: %s" % (cs_name,
                                                                                 cl_name,
                                                                                 e.code)
                        run_number += 1
                        time.sleep(retry_wait_time)
                        continue
                    else:
                        print "table %s.%s createCS failed, error: %s" % (cs_name,
                                                                          cl_name,
                                                                          e.code)
                        raise

            # 2. 创建SDB集合
            try:
                cl = cs.create_collection(cl_name, cl_options)
            except SDBBaseError, e:
                if -22 == e.code and is_crt_cl is False:
                    cl = cs.get_collection(cl_name)
                elif -22 == e.code and is_crt_cl is True:
                    cs.drop_collection(cl_name)
                    cl = cs.create_collection(cl_name, cl_options)
                else:
                    # retry create collection
                    if run_number <= retry_times:
                        print "Retry, table %s.%s createCL failed, options: %s, " \
                              "error code: %s" % (cs_name, cl_name, cl_options, e.code)
                        run_number += 1
                        time.sleep(retry_wait_time)
                        continue
                    else:
                        print "table %s.%s createCL failed, error: %s" % (cs_name,
                                                                          cl_name,
                                                                          e.code)
                        raise

            # break from while loop
            break

        print "table: %s.%s create collection space and collection success" % (cs_name,
                                                                               cl_name)
        return cl

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

    def sync_remove(self, cs_name, cl_name, condition={}):
        try:
            cl = self.sync_get_cscl(cs_name, cl_name)
            print "table %s.%s RM condition: %s, count: %s" % (cs_name,
                                                               cl_name,
                                                               condition,
                                                               cl.get_count(condition=condition))
            cl.delete(condition=condition)
        except (SDBBaseError, SDBTypeError), e:
            print "table %s.%s RM condition: %s, error code: %s" % (cs_name,
                                                                    cl_name,
                                                                    condition,
                                                                    e.code)

    def sync_insert(self, cs_name, cl_name, record):
        try:
            object_id = {"_id": ObjectId()}
            #print "get object id: %s" % object_id
            record.update(object_id)
            cl = self.sync_get_cscl(cs_name, cl_name)
            cl.insert(record)
            #print "%sinsert data: %s, cs: %s, cl: %s" % (cl, record, cs_name, cl_name)
        except (SDBBaseError, SDBTypeError), e:
            # if have table in sync.local
            print "failed to insert data: %s, error code: %s" % (record, e.code)
            if -38 != e.code:
                raise

    def sync_truncate(self, cs_name, cl_name):
        try:
            cl = self.sync_get_cscl(cs_name, cl_name)
            cl.truncate()
        except (SDBBaseError, SDBTypeError), e:
            print e.code
            raise

    def sync_update(self, cs_name, cl_name, ruler, matcher):
        try:
            cl = self.sync_get_cscl(cs_name, cl_name)
            cl.update(rule=ruler, condition=matcher)

        except (SDBBaseError, SDBTypeError), e:
            print "failed to update: %s, condition: %s, ruler: %s" % (e.code, matcher, ruler)
            raise

    def sync_upsert(self, cs_name, cl_name, ruler, matcher):
        try:
            cl = self.sync_get_cscl(cs_name, cl_name)
            cl.upsert(rule=ruler, condition=matcher)
        except (SDBBaseError, SDBTypeError), e:
            print "failed to upsert: %s, condition: %s, ruler: %s" % (e.code, matcher, ruler)
            raise

    def sync_get_catalog_snapshot(self, main_tbl):
        try:

            snap_cond = '{"Name": "%s"}' % main_tbl
            snap_cursor = self.db.get_snapshot(self.SDB_SNAP_CATALOG,
                                               condition=eval(snap_cond))
            record = None
            while True:
                try:
                    record = snap_cursor.next()
                except SDBEndOfCursor:
                    break
                except SDBBaseError:
                    raise

            return record
        except (SDBTypeError, SDBBaseError), e:
            if self.log is None:
                print "failed to get catalog snapshot of table: %s, " \
                      "error code: %s" % (main_tbl, e.code)
            else:
                self.log.error("failed to get catalog snapshot of table: %s, "
                               "error code: %s" % (main_tbl, e.code))
            raise

    def sync_detach_cl(self, cs_name, cl_name, detach_tbl):
        try:
            mcs = self.db.get_collection_space(cs_name)
            mcl = mcs.get_collection(cl_name)
            mcl.detach_collection(detach_tbl)
        except (SDBTypeError, SDBBaseError), e:
            pysequoiadb._print(e)

    def sync_attach_cl(self, cs_name, cl_name, attach_tbl, attach_bound={}):
        try:
            print "csname: %s, clname: %s, attach_tbl: %s, attach_bound: %s" % (cs_name, cl_name, attach_tbl, attach_bound)
            error_code = 0
            cs = self.db.get_collection_space(cs_name)
            cl = cs.get_collection(cl_name)
            cl.attach_collection(attach_tbl, attach_bound)
            return error_code
        except (SDBTypeError, SDBBaseError), e:
            if self.log is None:
                print "table: %s.%s attach sub table: %s failed, bound is %s." \
                      "error code: %s" % (cs_name, cl_name, attach_tbl,
                                          attach_bound, e.code)
            else:
                self.log.info("table: %s.%s attach sub table: %s failed, bound is %s."
                              "error code: %s" % (cs_name, cl_name, attach_tbl,
                                                  attach_bound, e.code))
            error_code = e.code

            return error_code

    def create_index(self, cs_name, cl_name, indexes_objstr):
        try:
            index_infos = self.sync_format_idxkey(indexes_objstr)
            cl = self.sync_create_cscl(cs_name, cl_name)
            is_unique = False
            is_enforced = False
            for index_info in index_infos:
                index_name = index_info[0]
                index_key = index_info[1]
                cl.create_index(index_key, index_name, is_unique, is_enforced)
                print "Create index %s OK, full cl: %s.%s, index definition: %s" % (
                      index_name, cs_name, cl_name, index_key)
        except (SDBTypeError, SDBBaseError), e:
            print 'error code: %s' % e.code
            pysequoiadb._print(e)
            raise

    def create_indexEE(self, cs_name, cl_name, index_def1, **kwargs):
        sdb = self.db
        if not sdb.is_valid():
            print "Sdb connection is invalid"
            exit(1)

        is_unique = False
        is_enforced = False
        buffer_size = 64
        index_def = eval(index_def1)
        #index_def = index_def1
        #print 'index_def: %s' % index_def
        if not isinstance(index_def, list):
            raise SDBTypeError("Index definition must be an instance of tuple")

        if "is_unique" in kwargs and isinstance(kwargs.get("is_unique"), bool):
            is_unique = kwargs.get("is_unique")

        if "is_enforced" in kwargs and isinstance(kwargs.get("is_enforced"), bool):
            is_enforced = kwargs.get("is_enforced")

        if "buffer_size" in kwargs and isinstance(kwargs.get("buffer_size"), int):
            buffer_size = kwargs.get("buffer_size")

        for i in range(len(index_def)):
            if not isinstance(index_def[i], dict):
                print "Every object in index definition must be an instance of dict"
                print "Please check parameter: %s" % index_def[i]
                continue

            index_name = ''
            for key in index_def[i]:
                index_name = index_name + key
                index_name = index_name + '_'
            index_name = index_name + 'Idx'

            try:
                cs = sdb.get_collection_space( cs_name )
                cl = cs.get_collection( cl_name )
                cl.create_index(index_def[i], index_name, is_unique, is_enforced, buffer_size)
                print "Create index %s OK, full cl: %s.%s, index definition: %s" % (
                    index_name, cs_name, cl_name, index_def[i])
            except (SDBTypeError, SDBBaseError), e:
                pysequoiadb._print(e)

    def sync_get_subtbl(self, maincs_name, maincl_name, attach_bound=[]):
        try:
            main_tbl = '%s.%s' % (maincs_name, maincl_name)
            cs = self.db.get_collection_space(maincs_name)
            cl = cs.get_collection(maincl_name)
            snap8 = self.sync_get_catalog_snapshot(main_tbl)
            cata_infos = snap8['CataInfo']
            shardkey = snap8['ShardingKey']
            key_idx = shardkey.keys()[0]
            subtbl_name = ''
            for i in range(len(cata_infos)):
                cata_info_dict = cata_infos[i]
                try:
                    low_bound = cata_info_dict['LowBound'][key_idx]
                except:
                    low_bound = cata_info_dict['LowBound']['']
                    print 'get shard: %s' % key_idx

                if 0 != len(attach_bound):
                    low_attach = attach_bound[0]
                    if low_bound == low_attach:
                        subtbl_name = cata_info_dict['SubCLName']
                else:
                    if '' == subtbl_name:
                        subtbl_name = cata_info_dict['SubCLName']
                        break

            return subtbl_name
            
        except (SDBTypeError, SDBBaseError), e:
            if self.log is None:
                print 'failed to get table: %s.%s\'s sub table, ' \
                      'error code: %s' % (maincs_name, maincl_name, e.code)
            else:
                self.log.error('failed to get table: %s.%s\'s sub table, ' \
                               'error code: %s' % (maincs_name, maincl_name, e.code))
            pysequoiadb._print(e)
            raise

    def sync_format_idxkey(self, indexes_objstr):
        index_infos = []
        print 'keys: %s' % indexes_objstr
        indexes_obj = eval(indexes_objstr)
        for index_list in indexes_obj:
            index_info = []
            index_name = ''
            index_key = collections.OrderedDict()
            for index in index_list:
                # here dict in list only have one
                #print 'keys: %s' % index.keys()[0]
                index_name = index_name + index.keys()[0] + '_'
                index_key[index.keys()[0]] = index[index.keys()[0]]

            index_name = index_name + 'Idx'
            index_info.append(index_name)
            index_info.append(index_key)
            index_infos.append(index_info)

        return index_infos

    def get_up_subtbl(self, main_tbl, lowbound_date):
        snap8 = self.sync_get_catalog_snapshot(main_tbl)
        cata_infos = snap8['CataInfo']
        shardkey = snap8['ShardingKey']
        key_idxs = shardkey.keys()
        tbls_lowbd = []
        next_updt = None
        sub_tbl = ''
        for cata_info in cata_infos:
            # lowbound
            for key_idx in key_idxs:
                try:
                    low_bound = cata_info['LowBound'][key_idx]
                except:
                    low_bound = cata_info['LowBound']['']
                    print 'get shard: %s' % key_idx
                # upbound
                try:
                    up_bound = cata_info['UpBound'][key_idx]
                except:
                    up_bound = cata_info['UpBound']['']
                    print 'get shard: %s' % key_idx
                if low_bound == lowbound_date:
                    sub_tbl = cata_info['SubCLName']
                    tbls_lowbd.append(sub_tbl)
                elif up_bound == lowbound_date:
                    next_updt = low_bound
                    sub_tbl = cata_info['SubCLName']
                    tbls_lowbd.append(sub_tbl)
                else:
                    continue

        # end append up low bound table
        tbls_lowbd.append(next_updt)
        return tbls_lowbd

    def sync_hisub_drpidx(self, main_cs_name, main_cl_name, indexes_objstr, date_fm, etl_date='', dropidx_num=2):
        try:
            main_tbl = '%s.%s' % (main_cs_name, main_cl_name)
            snap8 = self.sync_get_catalog_snapshot(main_tbl)
            cata_infos = snap8['CataInfo']
            if len(cata_infos) <= (dropidx_num + 2):
                print 'drop index from main cl: %s.%s' % (main_cs_name, main_cl_name)
                index_infos = self.sync_format_idxkey(indexes_objstr)
                cl = self.sync_create_cscl(main_cs_name, main_cl_name)
                for index_info in index_infos:
                    index_name = index_info[0]
                    cl.drop_index(index_name)

                return True

            shardkey = snap8['ShardingKey']
            key_idx = shardkey.keys()[0]
            print ' ShardingKey: ' + key_idx
            # get local sub table
            #now = datetime.datetime.strptime(etl_date, date_fm) - timedelta(days=1)
            att_fm = '%s01' % date_fm[0:(len(date_fm)-2)]
            etl_dtfm = datetime.datetime.strptime(etl_date, "%Y%m%d")
            low_bound = etl_dtfm.strftime(att_fm)
            sub_tbls = []
            drop_subnum = 0
            if dropidx_num < 2:
                drop_subnum = dropidx_num
            else:
                drop_subnum = dropidx_num/2

            for i in range(drop_subnum):
                sub_tbls = self.get_up_subtbl(main_tbl, low_bound)
                low_bound = sub_tbls[-1]
                sub_tbls.remove(low_bound)
                for sub_tbl in sub_tbls:
                    sub_cs = sub_tbl.split('.')[0]
                    sub_cl = sub_tbl.split('.')[1]
                    cl = self.sync_create_cscl(sub_cs, sub_cl)
                    index_infos = self.sync_format_idxkey(indexes_objstr)
                    for index_info in index_infos:
                        index_name = index_info[0]
                        # drop index
                        try:
                            print '[DROP INDEX]Sub Table: %s, Index:%s' % (sub_tbl, index_name)
                            cl.drop_index( index_name )
                        except (SDBTypeError, SDBBaseError), e:
                            pysequoiadb._print("error code: %s, error: %s" % (e.code, e))
        except (SDBTypeError, SDBBaseError), e:
            pysequoiadb._print("error code: %s, error: %s" % (e.code, e))

    def sync_hisub_crtidx(self, main_cs_name, main_cl_name, indexes_objstr, date_fm, etl_date='', dropidx_num=2):
        try:
            main_tbl = '%s.%s' % (main_cs_name, main_cl_name)
            snap8 = self.sync_get_catalog_snapshot(main_tbl)
            cata_infos = snap8['CataInfo']
            if len(cata_infos) <= (dropidx_num + 2):
                print 'drop index from main cl: %s.%s' % (main_cs_name, main_cl_name)
                index_infos = self.sync_format_idxkey(indexes_objstr)
                cl = self.sync_create_cscl(main_cs_name, main_cl_name)
                for index_info in index_infos:
                    index_name = index_info[0]
                    cl.drop_index(index_name)

                return True

            shardkey = snap8['ShardingKey']
            key_idx = shardkey.keys()[0]
            print ' ShardingKey: ' + key_idx
            # get local sub table
            #now = datetime.datetime.strptime(etl_date, date_fm) - timedelta(days=1)
            att_fm = '%s01' % date_fm[0:(len(date_fm)-2)]
            etl_dtfm = datetime.datetime.strptime(etl_date, "%Y%m%d")
            low_bound = etl_dtfm.strftime(att_fm)
            sub_tbls = []
            is_unique = False
            is_enforced = False
            drop_subnum = 0
            if dropidx_num < 2:
                drop_subnum = dropidx_num
            else:
                drop_subnum = dropidx_num/2

            for i in range(drop_subnum):
                sub_tbls = self.get_up_subtbl(main_tbl, low_bound)
                low_bound = sub_tbls[-1]
                sub_tbls.remove(low_bound)
                for sub_tbl in sub_tbls:
                    sub_cs = sub_tbl.split('.')[0]
                    sub_cl = sub_tbl.split('.')[1]
                    cl = self.sync_create_cscl(sub_cs, sub_cl)
                    index_infos = self.sync_format_idxkey(indexes_objstr)
                    for index_info in index_infos:
                        index_name = index_info[0]
                        index_key = index_info[1]
                        # drop index
                        try:
                            print '[CREATE INDEX]Sub Table: %s, Index:%s' % (sub_tbl, index_name)
                            cl.create_index(index_key, index_name, is_unique, is_enforced)
                            print "Create index %s OK, full cl: %s.%s, index definition: %s" % (index_name, sub_cs, sub_cl, index_key)
                        except (SDBTypeError, SDBBaseError), e:
                            pysequoiadb._print("error code: %s, error: %s" % (e.code, e))
        except (SDBTypeError, SDBBaseError), e:
            pysequoiadb._print("error code: %s, error: %s" % (e.code, e))

    def sync_aggregate(self, cs_name, cl_name, sdbop):
        try:
            cs = self.db.get_collection_space(cs_name)
            cl = cs.get_collection(cl_name)
            agg_cursor = cl.aggregate(sdbop)
            records = []
            while True:
                try:
                    record = agg_cursor.next()
                    records.append(record)
                except SDBEndOfCursor:
                    break
                except SDBBaseError:
                    raise

            return records
        except (SDBBaseError, SDBTypeError), e:
            print e.code

    def disconnect(self):
        try:
            self.db.disconnect()
        except (SDBTypeError, SDBBaseError), e:
            pysequoiadb._print(e)

    def sync_get_uniqidx(self, cs_name, cl_name):
        try:
            cs = self.db.get_collection_space(cs_name)
            cl = cs.get_collection(cl_name)
            uniq_idx_key = []
            uniq_arr = []
            shard_key = []
            idx_cursor = cl.get_indexes()
            main_tbl = '%s.%s' % (cs_name, cl_name)
            while True:
                try:
                    idx_info = []
                    record = idx_cursor.next()
                    #print 'index record: %s' % record
                    sub_tbl = self.sync_get_subtbl(cs_name, cl_name)
                    snap8 = self.sync_get_catalog_snapshot(sub_tbl)
                    #print 'sub_tbl: %s, snapshot: %s' % (sub_tbl, snap8)
                    cata_infos = snap8['CataInfo']
                    shardkey = snap8['ShardingKey']
                    keys = shardkey.keys()
                    keys.sort()
                    shard_key.append(keys)

                    """
                    keys = shardkey.keys()
                    print 'keys: %s, key type: %s' % (keys, type(keys))
                    keys.sort()
                    shard_key.append(keys)
                    if '$shard' == record['IndexDef']['name']:
                        shard_key.append(record['IndexDef']['name'])
                        keys = record['IndexDef']['key'].keys()
                        print 'keys: %s, key type: %s' % (keys, type(keys))
                        keys.sort()
                        shard_key.append(keys)
                    """
                    if True == record['IndexDef']['unique'] and '$id' != record['IndexDef']['name']:
                        idx_info.append(record['IndexDef']['name'])
                        keys = record['IndexDef']['key'].keys()
                        #print 'keys: %s, key type: %s' % (keys, type(keys))
                        keys.sort()
                        idx_info.append(keys)
                        uniq_arr.append(idx_info)
                except SDBEndOfCursor:
                    break
                except SDBBaseError:
                    raise

            #print 'unique array: %s' % uniq_arr
            #print 'sharding key: %s' % shard_key
            for idx in uniq_arr:
                # only support have same sort type, such as 1: asc, -1 desc
                if str(idx[1]) == str(shard_key[1]):
                    uniq_idx_key.append(idx[0])   # index name
                    uniq_idx_key.append(idx[1])   # index key

            print 'unique idx: <%s>' % uniq_idx_key
            return uniq_idx_key
        except (SDBBaseError, SDBTypeError), e:
            print e.code
            raise

    def get_coord_address(self):
        try:
            rg_cursor = self.db.list_replica_groups()
            coord_rg_obj = {}
            while True:
                try:
                    rg_info = []
                    record = rg_cursor.next()
                    if 'SYSCoord' == record['GroupName']:
                        coord_rg_obj = record
                except SDBEndOfCursor:
                    break
                except SDBBaseError:
                    raise
            #print 'coord : %s' % coord_rg_obj
            group_infos = coord_rg_obj["Group"]
            coord_addrs = ''
            for group_info in group_infos:
                host_name = group_info["HostName"]
                service_port = ''
                service_infos = group_info["Service"]
                for service_info in service_infos:
                    if 0 == service_info["Type"]:
                        service_port = service_info["Name"]

                if '' == coord_addrs:
                    coord_addrs += '%s:%s' % (host_name, service_port)
                else:
                    coord_addrs += ',%s:%s' % (host_name, service_port)

            print 'coord address: <%s>' % coord_addrs
            return coord_addrs
        except (SDBBaseError, SDBTypeError), e:
            print e.code
            raise


    def flush_disk(self, options={}):
        try:
            # the next line code just can use in SequoiaDB greate than 2.8.1
            #cmd = "sdb 'db = new Sdb(\"%s\", \"%s\", \"%s\", \"%s\");db.sync();db.close()'" % (self.hostname, self.svcport, self.username, base64.decodestring(self.password))
            #self.db.sync(options)
            cmd = 'echo "<<<SequoiaDB 2.6.2, No Flush Disk>>>"'
            (status, output) = commands.getstatusoutput(cmd)
            #if 0 != status:
            #    raise output
        except (SDBBaseError, SDBTypeError), e:
            print e.code
            raise

    def __del__(self):
        self.db.disconnect()


def main():
    hostname = 'localhost'
    svcport = 11810
    username = 'sdbapp'
    password = 'a2ZwdFNEQjIwMTY='
    #cs_name = 'sync'
    #cl_name = 'history'
    cs_name = 'smc'
    cl_name = 'cpolsd'

    #创建表
    db = SCSDB(hostname, svcport, username, password)
    print 'uniq index: %s: ' % db.sync_get_uniqidx(cs_name, cl_name)
    """
    sync_idx_arr = "[[{'agt_modif_num': 1}, {'agt_num': 1}], [{'cert_type_cd': 1}, {'cert_info_content': 1}], [{'concat_agt': 1}], [{'etl_date': 1}]]"
    main_tbl = '%s.%s' % (cs_name, cl_name)
    lowbound_date = '20170322'
    db.sync_hisub_drpidx(cs_name, cl_name, sync_idx_arr, '%Y%m%d', lowbound_date, dropidx_num=2)
    db.sync_hisub_crtidx(cs_name, cl_name, sync_idx_arr, '%Y%m%d', lowbound_date, dropidx_num=2)
    up_subtbl = db.get_up_subtbl(main_tbl, lowbound_date)
    #print up_subtbl
    #print lowbound_date
    indexes_objstr = "[[{'agt_num':1}, {'agt_modif_num': 1}], [{'concat_agt': 1}], [{'etl_date': 1}]]"
    index_infos = db.sync_format_idxkey(indexes_objstr)
    print '===%s' % index_infos
    condition = {'tbl_name': 'tpch01.customer'}
    selector = {'tbl_name': '--', 'sync_dt': '--'}
    orderby = {'sync_dt': -1}
    hint = {}
    skip = 0L
    returnN = 1L
    his_records = db.sync_query(cs_name,
                                cl_name,
                                condition=condition,
                                selector=selector,
                                order_by=orderby,
                                hint=hint,
                                skip=skip,
                                num_to_return=returnN)
    """

if __name__ == '__main__':
    main()
