#!/usr/bin/python
#coding=utf-8

from sync_sdb import *
import collections


class SyncDetachAttach:
    def __init__(self, hostname='localhost', svcport='11810',
                 username='', password='', connect_hosts='', log_handler=None):
        # connect to SDB
        self.hostname = hostname
        self.svcport = svcport
        self.username = username
        self.password = password
        self.connect_hosts = connect_hosts
        self.db = SCSDB(self.hostname, self.svcport, self.username,
                        self.password, self.connect_hosts)
        self.log = log_handler

    def get_sub_table_bound(self, main_tbl, sub_tbl):
        catalog_infos = self.db.sync_get_catalog_snapshot(main_tbl)
        cata_infos = catalog_infos.get("CataInfo")
        sub_table_bound = collections.OrderedDict()
        for cata_info in cata_infos:
            if sub_tbl == cata_info.get("SubCLName"):
                sub_table_bound["LowBound"] = cata_info.get("LowBound")
                sub_table_bound["UpBound"] = cata_info.get("UpBound")

        return sub_table_bound

    def get_split_shardingkey(self, tbl_name):
        cata_infos = self.db.sync_get_catalog_snapshot(tbl_name)
        shardkey_obj = collections.OrderedDict()
        if 1 == len(cata_infos.get("ShardingKey").keys()):
            shardkey_obj = cata_infos.get("ShardingKey")
        else:
            shardkey_obj = None

        return shardkey_obj

def test_main():
    print "here we test"

if __name__ == "__main__":
    test_main()
