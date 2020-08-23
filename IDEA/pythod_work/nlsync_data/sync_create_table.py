#!/usr/bin/python
#coding=utf-8

from sync_os import *

class SyncCrtTbl:
    def __init__(self, hostname='localhost', svcport='11810',
                 username='', password='', connect_hosts='', log_handler=None):
        # connect to SDB
        self.hostname = hostname
        self.svcport = svcport
        self.username = username
        self.password = password
        self.connect_hosts = connect_hosts
        self.db = SCSDB(self.hostname, self.svcport, self.username, self.password, self.connect_hosts)
        self.log = log_handler
        # SDB Collection Space options
        self.cs_default_options = collections.OrderedDict()
        self.cs_default_options["PageSize"] = 65536
        self.cs_default_options["Domain"] = None
        self.cs_default_options["LobPageSize"] = 262144
        # SDB Collection options
        self.cl_default_options = collections.OrderedDict()
        self.cl_default_options["ShardingKey"] = {"_id": 1}
        self.cl_default_options["ShardingType"] = "hash"
        self.cl_default_options["Compressed"] = True
        self.cl_default_options["IsMainCL"] = False
        # Prefer Variable Name
        self.pref_repo_name = "repo_"
        self.sync_dt_fmt = "%Y%m%d"

    def sub_table_loop(self, modify_name, loop_cnt, delta=1):
        suffix = modify_name[-1]
        cscl_name = modify_name[0:(len(modify_name)-1)]
        range_len = loop_cnt
        suffix_sum = int(suffix) + delta
        if int(suffix_sum) > range_len:
            suffix = suffix_sum % range_len
        else:
            suffix = int(suffix_sum)

        cscl_name += str(suffix)
            
        return cscl_name

    # create full sub table for product table
    def initialize_full_table(self, main_csname, main_clname, sync_domain,
                              prod_tbl_vkey, prod_tbl_hkey, cs_pref=''):
        tbl_name = "%s.%s" % (main_csname, main_clname)
        snap8_info = self.db.sync_get_catalog_snapshot(tbl_name)
        sub_tbl = None
        sub_table_list = []
        sub_tbl_truncate = False
        formate_cs = cs_pref + main_csname
        formate_cl = main_clname + "_trn"
        if '' != snap8_info and snap8_info is not None:
            sub_tbl = self.db.sync_get_subtbl(main_csname, main_clname)
            sub_tbl_split = sub_tbl.split('.')
            if formate_cs != sub_tbl_split[0] or formate_cl != sub_tbl_split[1][0:(len(sub_tbl_split[1]) - 1)]:
                fmt_cs_name = formate_cs
                fmt_cl_name = main_clname + "_trn3"
            else:
                fmt_cs_name = sub_tbl_split[0]
                fmt_cl_name = sub_tbl_split[1]
                sub_tbl_truncate = True
        else:
            fmt_cs_name = formate_cs
            fmt_cl_name = main_clname + "_trn3"

            # create product main table
            self.cs_default_options["Domain"] = sync_domain
            self.cl_default_options['ShardingKey'] = prod_tbl_vkey
            self.cl_default_options['ShardingType'] = 'range'
            self.cl_default_options['IsMainCL'] = True
            self.db.sync_create_cscl(main_csname, main_clname, self.cs_default_options,
                                     self.cl_default_options, is_crt_cs=False,
                                     is_crt_cl=False)

        # get new sub table. loop times is 3 times(3 sub tables loop)
        loop_times = 3
        sub_cs_name = fmt_cs_name
        sub_cl_name = self.sub_table_loop(fmt_cl_name, loop_times)
        next_sub_tbl = '%s.%s' % (sub_cs_name, sub_cl_name)
        next_sub_snap = self.db.sync_get_catalog_snapshot(next_sub_tbl)
        """
        if sub_tbl_truncate and '' != next_sub_snap and next_sub_snap is not None:
            # truncate new sub table
            self.db.sync_truncate(sub_cs_name, sub_cl_name)
        else:
        """
        # create product sub table collection space and collection
        self.cs_default_options["Domain"] = sync_domain
        self.cl_default_options['ShardingKey'] = prod_tbl_hkey
        self.cl_default_options['ShardingType'] = 'hash'
        self.cl_default_options['IsMainCL'] = False
        self.cl_default_options["AutoSplit"] = True
        self.cl_default_options["EnsureShardingIndex"] = False
        self.cl_default_options["CompressionType"] = "lzw"
        if self.log is None:
            print 'main table: %s.%s, dest table: %s.%s' % (main_csname, main_clname,
                                                            sub_cs_name, sub_cl_name)
        else:
            self.log.info('main table: %s.%s, dest table: %s.%s' % (main_csname, main_clname,
                                                                   sub_cs_name, sub_cl_name))
        self.db.sync_create_cscl(sub_cs_name, sub_cl_name, self.cs_default_options,
                                 self.cl_default_options, is_crt_cs=False, is_crt_cl=True)

        dst_sub_tbl = sub_cs_name + '.' + sub_cl_name
        sub_table_list.append(sub_tbl)
        sub_table_list.append(dst_sub_tbl)

        return sub_table_list

    # create full sub table for product table
    def create_full_subtbl(self, main_csname, main_clname, sync_new, sync_domain,
                           prod_tbl_vkey, prod_tbl_hkey, subtbl_bd=None):
        tbl_name = "%s.%s" % (main_csname, main_clname)
        snap8 = self.db.sync_get_catalog_snapshot(tbl_name)
        sub_tbl = None
        sub_table_list = []
        print "===========snap8 info: %s" % snap8
        if '' != snap8 and snap8 is not None:
            if 0 != len(snap8['CataInfo']):
                if subtbl_bd is not None:
                    get_sub_table = False
                    # get sub table according to sub table bound in config
                    for cata_info in snap8['CataInfo']:
                        print "cata_info dict: %s, subtbl_bd dict: %s" % (cata_info.get("LowBound"), subtbl_bd.get("LowBound"))
                        if 0 == cmp(subtbl_bd.get("LowBound"), cata_info.get("LowBound")) and \
                           0 == cmp(subtbl_bd.get("UpBound"), cata_info.get("UpBound")):
                            sub_tbl = cata_info.get("SubCLName")
                            get_sub_table = True
                            break
                    if get_sub_table is False:
                        if self.log is None:
                            print "sub table bound: %s is not equal with config " \
                                  "bound: %s" % (cata_info, subtbl_bd)
                        else:
                            self.log.error("sub table bound: %s is not equal with "
                                            "config bound: %s" % (cata_info, subtbl_bd))
                        # here need to check config
                        raise
                elif 1 == len(snap8['CataInfo']):
                    sub_tbl = snap8['CataInfo'][0]['SubCLName']
                else:
                    if self.log is None:
                        print "cannot to get sub table: %s of table: %s.%s" % (sub_tbl,
                                                                               main_csname,
                                                                               main_clname)
                    else:
                        self.log.error("cannot to get sub table: %s "
                                       "of table: %s.%s" % (sub_tbl, main_csname, main_clname))
                    raise
                sub_tbl_split = sub_tbl.split('.')
                sub_cs = sub_tbl_split[0]
                sub_cl = sub_tbl_split[1]
                pref_cs = sub_cs[0:(len(sub_cs)-1)]
                # inspect for new synchronous code or not
                if 'true' == sync_new:
                    fm_cl_name = main_csname + '_' + main_clname + '_'
                else:
                    fm_cl_name = main_clname
                if pref_cs == fm_cl_name:
                    sub_name = sub_cs
                else:
                    if 'true' == sync_new:
                        sub_name = main_csname + '_' + main_clname + '_3'
                    else:
                        sub_name = main_clname + '3'
            else:
                if 'true' == sync_new:
                    sub_name = main_csname + '_' + main_clname + '_3'
                else:
                    sub_name = main_clname + '3'
        else:
            if 'true' == sync_new:
                sub_name = main_csname + '_' + main_clname + '_3'
            else:
                sub_name = main_clname + '3'

            # create product main table
            self.cs_default_options["Domain"] = sync_domain
            self.cl_default_options['ShardingKey'] = prod_tbl_vkey
            self.cl_default_options['ShardingType'] = 'range'
            self.cl_default_options['IsMainCL'] = True
            self.db.sync_create_cscl(main_csname, main_clname, self.cs_default_options,
                                     self.cl_default_options, is_crt_cs=False,
                                     is_crt_cl=False)

        # get new sub table. loop times is 3 times(3 sub tables loop)
        loop_times = 3
        sub_cs_name = self.sub_table_loop(sub_name, loop_times)
        sub_cl_name = sub_cs_name
        # create product sub table collection space and collection
        self.cs_default_options["Domain"] = sync_domain
        self.cl_default_options['ShardingKey'] = prod_tbl_hkey
        self.cl_default_options['ShardingType'] = 'hash'
        self.cl_default_options['IsMainCL'] = False
        self.cl_default_options["AutoSplit"] = True
        self.cl_default_options["EnsureShardingIndex"] = False
        self.cl_default_options["CompressionType"] = "lzw"
        if self.log is None:
            print 'main table: %s.%s, dest table: %s.%s' % (main_csname, main_clname,
                                                            sub_cs_name, sub_cl_name)
        else:
            self.log.info('main table: %s.%s, dest table: %s.%s' % (main_csname, main_clname,
                                                                   sub_cs_name, sub_cl_name))
        self.db.sync_create_cscl(sub_cs_name, sub_cl_name, self.cs_default_options,
                                 self.cl_default_options, is_crt_cs=True, is_crt_cl=True)

        dst_sub_tbl = sub_cs_name + '.' + sub_cl_name
        sub_table_list.append(sub_tbl)
        sub_table_list.append(dst_sub_tbl)

        return sub_table_list

    # create repository table
    def create_repository_table(self, maincs_name, maincl_name, sync_domain, repo_tbl_vkey,
                                repo_tbl_hkey, repo_dt_type, dt_delta, sync_dt):
        repo_maincs = "%s%s" % (self.pref_repo_name, maincs_name)
        repo_maincl = "%s%s" % (self.pref_repo_name, maincl_name)
        repo_maintbl = "%s.%s" % (repo_maincs, repo_maincl)
        repo_table_list = []
        # 1.create repository main table
        self.cs_default_options["Domain"] = sync_domain
        self.cl_default_options['ShardingKey'] = repo_tbl_vkey
        self.cl_default_options['ShardingType'] = 'range'
        self.cl_default_options['IsMainCL'] = True
        self.db.sync_create_cscl(repo_maincs, repo_maincl, self.cs_default_options,
                                 self.cl_default_options, is_crt_cs=False,
                                 is_crt_cl=False)
        # 2.create repository sub table. the sync date time according before over date
        delta_dt = timedelta(days=int(dt_delta))
        sync_last_dt = datetime.datetime.strptime(sync_dt, self.sync_dt_fmt)
        sync_local_dt = sync_last_dt
        sync_tomor_dt = sync_local_dt + abs(delta_dt)
        sync_repo_dt = sync_local_dt.strftime(repo_dt_type)
        tomo_repo_dt = sync_tomor_dt.strftime(repo_dt_type)
        # repository sub table's collection space name is equal to
        # main table's collection name
        repo_subcs = repo_maincl + "1"
        repo_subcl = repo_subcs + sync_repo_dt
        self.cs_default_options["Domain"] = sync_domain
        self.cl_default_options['ShardingKey'] = repo_tbl_hkey
        self.cl_default_options['ShardingType'] = 'hash'
        self.cl_default_options['Compressed'] = True
        self.cl_default_options['CompressionType'] = 'lzw'
        self.cl_default_options['AutoSplit'] = True
        self.cl_default_options['IsMainCL'] = False
        self.cl_default_options["EnsureShardingIndex"] = False
        self.db.sync_create_cscl(repo_subcs, repo_subcl, self.cs_default_options,
                                 self.cl_default_options, is_crt_cs=False, is_crt_cl=False)
        if self.log is None:
            print 'repo_sub_tbl: %s.%s' % (repo_subcs, repo_subcl)
        else:
            self.log.info('repo_sub_tbl: %s.%s' % (repo_subcs, repo_subcl))
        # 3.attach repository sub table under repository main table
        attach_bound = collections.OrderedDict()
        low_bound = {}
        up_bound = {}
        repo_vkeys = repo_tbl_vkey.keys()
        repo_tbl_vkey = repo_vkeys[0]
        low_bound[repo_tbl_vkey] = sync_repo_dt
        up_bound[repo_tbl_vkey] = tomo_repo_dt
        attach_bound['LowBound'] = low_bound
        attach_bound['UpBound'] = up_bound
        # print attach_bound
        repo_subtbl = repo_subcs + '.' + repo_subcl
        ret = self.db.sync_attach_cl(repo_maincs, repo_maincl, repo_subtbl, attach_bound)
        if -235 == ret:
            if self.log is None:
                print 'truncate table: %s.%s' % (repo_subcs, repo_subcl)
            else:
                self.log.info('truncate table: %s.%s' % (repo_subcs, repo_subcl))
            self.db.sync_truncate(repo_subcs, repo_subcl)

        repo_table_list.append(repo_maintbl)
        repo_table_list.append(repo_subtbl)

        return repo_table_list

def test_main():
    print "here we test"

if __name__ == "__main__":
    test_main()
