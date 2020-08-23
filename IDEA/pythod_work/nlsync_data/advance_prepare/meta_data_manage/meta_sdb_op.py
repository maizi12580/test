#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author: Vector
Desc: 封装SDB接口, 用于元数据的各种处理操作
"""

import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)



class MSdb:
    def __init__(self, hostname='localhost', srvport=11810,
                 user='sdbapp', passwd='kfptSDB2016'):
        self.hostname = hostname
        self.srvport = srvport
        self.user = user
        self.passwd = passwd
        self.db = client(hostname, srvport, user, passwd)
        self.domain = 'mdm_domain'

    def create_cs_cl(self, csname, clname):
        try:
            cs = self.db.create_collection_space(csname, {'Domain': self.domain})
        except (SDBBaseError, SDBTypeError), e:
            #print e.code
            cs = self.db.get_collection_space(csname)
        try:
            cl = cs.create_collection(clname, {'ShardingKey': {'_id': 1}, 'ShardingType': 'hash',
                                               'Compressed': True})
        except (SDBBaseError, SDBTypeError), e:
            #print e.code
            cl = cs.get_collection(clname)

        return cl

    def insert(self, cs_name, cl_name, map_record):
        try:
            cl = self.create_cs_cl(cs_name, cl_name)
            cl.insert(map_record)
        except (SDBBaseError, SDBTypeError), e:
            print e.code

    def upsert(self, cs_name, cl_name, map_record):
        try:
            cl = self.create_cs_cl(cs_name, cl_name)
            #ruler_str = '{"$set": %s}' % map_record
            ruler_str = '{"$set": {"desc": "testing testing"}}'
            ruler = eval(ruler_str)
            matcher_str = '{"tbl_name": "%s"}' % map_record["tbl_name"]
            matcher = eval(matcher_str)
            print ruler
            print matcher
            #cl.upsert(rule=ruler, condition=matcher)
            # 因为 upsert 方法总是报 -6 错误, 暂时不清楚是什么原因导致, 后续验证数据
            cl.delete(condition=matcher)
            cl.insert(map_record)
            print '==================================='
        except (SDBBaseError, SDBTypeError), e:
            print e.code

    def aggregate(self, cs_name, cl_name, sdbop):
        try:
            cl = self.create_cs_cl(cs_name, cl_name)
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

    def query(self, cs_name, cl_name, condition):
        try:
            cl = self.create_cs_cl(cs_name, cl_name)
            cursor = cl.query(condition=condition)
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

    def get_hosts(self):
        try:
            cursor = self.db.get_list(7)
            hosts = ''
            while True:
                try:
                    record = cursor.next()
                    print record['GroupName']
                    if 'SYSCoord' == record['GroupName']:
                        for j in range(len(record["Group"])):
                            hostname = record["Group"][j]["HostName"]
                            svcport = record["Group"][j]["Service"][0]["Name"]
                            if '' == hosts:
                                hosts = hosts + hostname + ':' + svcport
                            else:
                                hosts = hosts + ',' + hostname + ':' + svcport

                except SDBEndOfCursor:
                    break
                except SDBBaseError:
                    raise
            return hosts
        except (SDBBaseError, SDBTypeError), e:
            print e.code

def main():
    msdb = MSdb()

if __name__ == "__main__":
    main()
