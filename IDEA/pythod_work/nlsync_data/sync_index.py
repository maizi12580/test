#!/user/bin/python
#coding=utf-8

from sync_sdb import *
from sync_os import *


class SyncIndex:
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
        self.sync_os = SyncOS(log_handler)
        # constanc value
        self.full_type = "full"
        self.append_cus_type = "append_cus"
        self.append_his_type = "append_his"
        self.append_his_update_type = "append_his_update"
        self.append_his_insert_type = "append_his_insert"
        self.append_his_dp_type = "append_his_dp"
        self.append_his_dprepo_type = "append_his_dprepo"

    def format_index_key(self, index_keys):
        indexes_obj = eval(index_keys)
        index_infos = []
        for index_list in indexes_obj:
            index_info = collections.OrderedDict()
            index_name = ''
            index_key = collections.OrderedDict()
            for index in index_list:
                # here dict in list only have one
                index_name += index.keys()[0] + '_'
                index_key[index.keys()[0]] = index[index.keys()[0]]

            index_name += 'Idx'
            index_info["IndexName"] = index_name
            index_info["IndexKey"] =index_key
            index_infos.append(index_info)

        return index_infos

    def create_index(self, cs_name, cl_name, index_keys):
        index_infos = self.format_index_key(index_keys)
        for index_info in index_infos:
            index_name = index_info[0]
            index_key = index_info[1]
            self.db.create_index(cs_name, cl_name, index_key, index_name)
            if self.log is None:
                print "the table: %s create index success!index name: %s, " \
                      "index key: %s" % (self.tbl_name, index_name, index_key)
            else:
                self.log.info("the table: %s create index success!index name: %s, " \
                              "index key: %s" % (self.tbl_name, index_name, index_key))

    def get_sub_table(self, main_csname, main_clname, count_cond):
        main_tbl = '%s.%s' % (main_csname, main_clname)
        snap8 = self.db.sync_get_catalog_snapshot(main_tbl)
        cata_infos = snap8['CataInfo']
        sub_table_info = []
        for cata_info in cata_infos:
            sub_table_count = {}
            sub_table = cata_info.get("SubCLName")
            count_subcs = sub_table.split(".")[0]
            count_subcl = sub_table.split(".")[1]
            sub_count = self.db.sync_get_count(count_subcs, count_subcl, count_cond)
            if 0 < sub_count:
                sub_table_count["Name"] = sub_table
                sub_table_count["Number"] = sub_count
                sub_table_info.append(sub_table_count)

        def listKey(key):
            return key.get("Number")
        reversed_sort_table = sorted(sub_table_info, key=listKey, reverse=True)

        return reversed_sort_table

    def drop_sub_table_index(self, main_csname, main_clname, index_keys,
                             date_fm, etl_date_cond, drop_index_num=2):
        sub_tables = self.get_sub_table(main_csname, main_clname, etl_date_cond)
        rebuild_sub_tables = sub_tables[drop_index_num:]
        index_infos = self.format_index_key(index_keys)
        drop_index_tables = []
        for rebuild_sub_table in rebuild_sub_tables:
            sub_table = rebuild_sub_table.get("Name")
            drop_index_tables.append(sub_table)
            sub_cs = sub_table.split(".")[0]
            sub_cl = sub_table.split(".")[1]
            for index_info in index_infos:
                self.db.drop_index(sub_cs, sub_cl, index_info.get("IndexName"))

        if self.log is None:
            print "the sub tables: %s of main table: %s.%s have dropped index!" \
                  "index infomations: %s" % (rebuild_sub_tables, main_csname,
                                             main_clname, index_infos)
        else:
            self.log.info("the sub tables: %s of main table: %s.%s have dropped index!"
                          "index infomations: %s" % (rebuild_sub_tables, main_csname,
                                                     main_clname, index_infos))
        return drop_index_tables

    def create_sub_table_index(self, main_csname, main_clname, index_keys,
                               drop_index_tables):
        for create_index_table in drop_index_tables:
            sub_cs = create_index_table.split(".")[0]
            sub_cl = create_index_table.split(".")[1]
            self.create_index(sub_cs, sub_cl, index_keys)

        if self.log is None:
            print "the sub tables: %s of main table: %s.%s have created index!" \
                  "index infomations: %s" % (drop_index_tables, main_csname,
                                             main_clname, index_keys)
        else:
            self.log.info("the sub tables: %s of main table: %s.%s have created index!"
                          "index infomations: %s" % (drop_index_tables, main_csname,
                                                     main_clname, index_keys))
    def get_sub_table_sharding_keys(self, main_csname, main_clname):
        main_tbl = "%s.%s" % (main_csname, main_clname)
        snapshot_info = self.db.sync_get_catalog_snapshot(main_tbl)
        snap_cata_info = snapshot_info.get("CataInfo")
        sub_table_sharding_key = collections.OrderedDict()
        if snap_cata_info is not None and snapshot_info.get("IsMainCL") is True:
            sub_table = snap_cata_info[0].get("SubCLName")
            sub_table_snapshot = self.db.sync_get_catalog_snapshot(sub_table)
            sub_table_sharding_key = sub_table_snapshot.get("ShardingKey")

        return sub_table_sharding_key

    def get_unique_index(self, cs_name, cl_name):
        index_infos = self.db.get_indexes(cs_name, cl_name)
        sub_table_sharding_key = self.get_sub_table_sharding_keys()
        unique_index_infos = collections.OrderedDict()
        for index_info in index_infos:
            unique_index_info = []
            index_keys = index_info.get("IndexDef").get("key")
            index_name = index_info.get("IndexDef").get("name")
            index_unique = index_info.get("IndexDef").get("unique")
            if 0 == cmp(index_keys, sub_table_sharding_key) and \
               "$id" != index_name and index_unique is True:
                unique_index_info["IndexName"] = index_name
                unique_index_info["IndexKey"] = index_keys
                unique_index_infos.apppend(unique_index_info)

        return unique_index_infos

def test_main():
    print "test"

if __name__ == '__main__':
   test_main()