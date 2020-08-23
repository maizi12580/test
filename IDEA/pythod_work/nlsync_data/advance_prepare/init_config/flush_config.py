#!/usr/bin/python
#coding=utf-8

from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)
from sync_config import *
import collections
from bson.min_key import MinKey
from bson.max_key import MaxKey

global hostname
hostname = sdb_connection['hostname']
global svcport
svcport = sdb_connection['svcport']
global username
username = sdb_connection['username']
global password
password = sdb_connection['password']


# 如何检查flush的配置文件, 如字典对象之类变成了字符串(String)
def create_sync_managetbl():
    try:
        db = client(hostname, svcport, username, password)
        cs_domain = {'Domain': sync_manage_domain['domain_name']}
        for sync_tbl in sdb_sync_tbls:
            # 1. create main collection space
            try:
                cs = db.create_collection_space(sync_tbl['cs_name'], cs_domain)
            except (SDBBaseError, SDBTypeError), e:
                print 'ERROR TO CREATE MAIN CS: %s' % e.code
                cs = db.get_collection_space(sync_tbl['cs_name'])
            # 2. create main collection
            maincl_options = sync_tbl_options['tbl_options']
            try:
                maincl = cs.create_collection(sync_tbl['cl_name'], maincl_options)
            except (SDBBaseError, SDBTypeError), e:
                print 'ERROR TO CREATE MAIN CL: %s' % e.code
                maincl = cs.get_collection(sync_tbl['cl_name'])

            # 3. create sub collection space
            try:
                cs = db.create_collection_space(sync_tbl['subcs_name'], cs_domain)
            except (SDBBaseError, SDBTypeError), e:
                print 'ERROR TO CREATE SUB CS: %s' % e.code
                cs = db.get_collection_space(sync_tbl['subcs_name'])
            # 4. create sub collection
            subcl_options = []
            subcl_options = collections.OrderedDict()
            subcl_options['ShardingKey'] = sync_tbl_options['subtbl_options']['ShardingKey']
            subcl_options['ShardingType'] = sync_tbl_options['subtbl_options']['ShardingType']
            subcl_options['Compressed'] = sync_tbl_options['subtbl_options']['Compressed']
            subcl_options['CompressionType'] = sync_tbl_options['subtbl_options']['CompressionType']
            subcl_options['AutoSplit'] = sync_tbl_options['subtbl_options']['AutoSplit']
            try:
                print subcl_options
                cl = cs.create_collection(sync_tbl['subcl_name'], subcl_options)
            except (SDBBaseError, SDBTypeError), e:
                print 'ERROR TO CREATE SUB CL: %s' % e.code
                cl = cs.get_collection(sync_tbl['subcl_name'])
            # 5. attach sub collection
            sub_tbl = sync_tbl['subcs_name'] + '.' + sync_tbl['subcl_name']
            attach_bound = collections.OrderedDict()
            low_bound = {}
            up_bound = {}
            sync_vkeys = sync_tbl_options['tbl_options']['ShardingKey'].keys()
            print 'verkey: %s' % sync_vkeys
            repo_tbl_vkey = sync_vkeys[0]
            low_bound[repo_tbl_vkey] = MinKey()
            up_bound[repo_tbl_vkey] = MaxKey()
            attach_bound['LowBound'] = low_bound
            attach_bound['UpBound'] = up_bound
            print attach_bound
            try:
                maincl.attach_collection(sub_tbl, attach_bound)
            except (SDBBaseError, SDBTypeError), e:
                print e.code


    except (SDBBaseError, SDBTypeError), e:
        raise


def flush_main():
    # 集合空间与集合
    cs_name = 'nlsync'
    cl_name = 'config'

    # 1.根据配置文件sync_config.py内的信息将其刷入同步批次初始化配置表(sync.config)
    try:
        # create synchronize data
        create_sync_managetbl()
        db = client(hostname, svcport, username, password)
        try:
            cs = db.get_collection_space(cs_name)
        except (SDBBaseError, SDBTypeError), e:
            print 'error code: %s' % e.code
            raise

        try:
            cl = cs.get_collection(cl_name)
        except (SDBBaseError, SDBTypeError), e:
            print 'error code: %s' % e.code
            raise

        for cnf in sync_tblconf_infos:
            tbl_name = cnf['tbl_name']
            matcher = {'tbl_name': tbl_name}
            # 先删除再插入, upsert方法使用存在问题(抱-6操作)
            cl.delete(condition=matcher)
            cl.insert(cnf)

    except (SDBBaseError, SDBTypeError), e:
        print e.code
        raise

    # 2.将配置文件sync_config.py进行备份
    """
    local_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d-%H:%M:%S')
    ret = os.rename('sync_config.py', 'bak.sync_config.py.%s' % (local_time))
    if 0 == ret:
        print 'success to backup'
    else:
        print 'failed to backup'
    """


if __name__ == '__main__':
    flush_main()
