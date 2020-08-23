#!/usr/bin/python
#coding=utf-8

from sync_sdb import *
from sync_logger import *
from bson.min_key import MinKey
from bson.max_key import MaxKey

class SyncConfig:
    def __init__(self, hostname='localhost', svcport='11810',
                 username=None, password=None, connect_hosts=None,
                 cs_name=None, cl_name=None, log_handler=None):
        # config collection space and collection
        config_csname = "nlsync"
        config_clname = "config"
        self.log = log_handler

        # table sync.local's primary key is tbl_name
        tbl_name = {"tbl_name": "%s.%s" % (cs_name, cl_name)}
        # get db connection
        db = SCSDB(hostname, svcport, username, password, connect_hosts)
        self.config_rd = dict()
        config_rds = db.sync_query(config_csname, config_clname, tbl_name)
        if len(config_rds) == 1:
            self.config_rd = config_rds[0]
        elif len(config_rds) < 1:
            if self.log is None:
                print "WARNNING, don't have table %s.%s config" % (cs_name, cl_name)
            else:
                self.log.warn("WARNNING, don't have table %s.%s config" % (cs_name, cl_name))
        else:
            if self.log is None:
                print "WARNNING, table %s.%s have two configs" % (cs_name, cl_name)
            else:
                self.log.warn("WARNNING, table %s.%s have two configs" % (cs_name, cl_name))

    # sync_sys
    def get_sync_sys(self):
        if self.config_rd.get("sync_sys") is not None and '' != self.config_rd.get("sync_sys"):
            return self.config_rd.get("sync_sys")

    # tbl_name
    def get_table_name(self):
        if self.config_rd.get("tbl_name") is not None and '' != self.config_rd.get("tbl_name"):
            return self.config_rd.get("tbl_name")

    def get_cs_name(self):
        if self.config_rd.get("tbl_name") is not None and '' != self.config_rd.get("tbl_name"):
            return self.config_rd.get("tbl_name").split(".")[0]

    def get_cl_name(self):
        if self.config_rd.get("tbl_name") is not None and '' != self.config_rd.get("tbl_name"):
            return self.config_rd.get("tbl_name").split(".")[1]

    # sync_dom
    def get_table_domain(self):
        if self.config_rd.get("sync_dom") is not None and '' != self.config_rd.get("sync_dom"):
            return self.config_rd.get("sync_dom")

    # dt_delta
    def get_delta_date(self):
        if self.config_rd.get("dt_delta") is not None and '' != self.config_rd.get("dt_delta"):
            return self.config_rd.get("dt_delta")

    # main_dtp
    def get_date_format_list(self):
        if self.config_rd.get("main_dtp") is not None and '' != self.config_rd.get("main_dtp"):
            return eval(self.config_rd.get("main_dtp"))

    def get_product_vertical_date_format(self):
        if self.config_rd.get("main_dtp") is not None and '' != self.config_rd.get("main_dtp"):
            return eval(self.config_rd.get("main_dtp"))[0]

    def get_repository_vertical_date_format(self):
        if self.config_rd.get("main_dtp") is not None and '' != self.config_rd.get("main_dtp"):
            return eval(self.config_rd.get("main_dtp"))[1]

    # sync_type
    def get_table_sync_type(self):
        if self.config_rd.get("sync_type") is not None and '' != self.config_rd.get("sync_type"):
            return self.config_rd.get("sync_type")

    # sync_file
    def get_table_sync_host(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[0].get("sync_host")

    def get_table_sync_file(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[0].get("sync_file")

    def get_table_sync_file_endcode_format(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[0].get("encode_fmt")

    def get_table_sync_file_remote_mode(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[1].get("get_mode")

    def get_table_sync_file_remote_host(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[1].get("frm_ip")

    def get_table_sync_file_remote_file(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[1].get("frm_file")

    def get_table_sync_file_remote_user(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[1].get("frm_user")

    def get_table_sync_file_remote_password(self):
        if self.config_rd.get("sync_file") is not None and '' != self.config_rd.get("sync_file"):
            sync_file = eval(self.config_rd.get("sync_file"))
            return sync_file[1].get("frm_passwd")

    # idx_field
    def get_table_index_field(self):
        if self.config_rd.get("idx_field") is not None and '' != self.config_rd.get("idx_field"):
            return self.config_rd.get("idx_field")

    def get_table_index_field_list(self):
        if self.config_rd.get("idx_field") is not None and '' != self.config_rd.get("idx_field"):
            return eval(self.config_rd.get("idx_field"))

    # shard_key
    def get_product_shard_key(self):
        if self.config_rd.get("shard_key") is not None and '' != self.config_rd.get("shard_key"):
            return eval(self.config_rd.get("shard_key"))

    def get_product_vertical_key_map(self):
        if self.config_rd.get("shard_key") is not None and '' != self.config_rd.get("shard_key"):
            return eval(self.config_rd.get("shard_key"))[0].get("vertical_key")

    def get_product_horizontal_key_map(self):
        if self.config_rd.get("shard_key") is not None and '' != self.config_rd.get("shard_key"):
            return eval(self.config_rd.get("shard_key"))[1].get("horizontal_key")

    # rp_sd_key
    def get_repository_shard_key(self):
        if self.config_rd.get("rp_sd_key") is not None and '' != self.config_rd.get("rp_sd_key"):
            return eval(self.config_rd.get("rp_sd_key"))

    def get_repository_vertical_key_map(self):
        if self.config_rd.get("rp_sd_key") is not None and '' != self.config_rd.get("rp_sd_key"):
            return eval(self.config_rd.get("rp_sd_key"))[0].get("vertical_key")

    def get_repository_horizontal_key_map(self):
        if self.config_rd.get("rp_sd_key") is not None and '' != self.config_rd.get("rp_sd_key"):
            return eval(self.config_rd.get("rp_sd_key"))[1].get("horizontal_key")

    # iprt_cmd
    def get_table_import_options_map(self):
        if self.config_rd.get("iprt_cmd") is not None and '' != self.config_rd.get("iprt_cmd"):
            return eval(self.config_rd.get("iprt_cmd"))

    def get_table_import_options_csname(self):
        if self.config_rd.get("iprt_cmd") is not None and '' != self.config_rd.get("iprt_cmd"):
            return eval(self.config_rd.get("iprt_cmd")).get("csname")

    def get_table_import_options_clname(self):
        if self.config_rd.get("iprt_cmd") is not None and '' != self.config_rd.get("iprt_cmd"):
            return eval(self.config_rd.get("iprt_cmd")).get("clname")

    # upst_cmd
    def get_table_upsert_options_map(self):
        if self.config_rd.get("upst_cmd") is not None and '' != self.config_rd.get("upst_cmd"):
            return eval(self.config_rd.get("upst_cmd"))

    # expt_cmd
    def get_table_export_options_map(self):
        if self.config_rd.get("expt_cmd") is not None and '' != self.config_rd.get("expt_cmd"):
            return eval(self.config_rd.get("expt_cmd"))

    def get_table_export_options_file(self):
        if self.config_rd.get("expt_cmd") is not None and '' != self.config_rd.get("expt_cmd"):
            return eval(self.config_rd.get("expt_cmd")).get("file")

    def get_table_export_options_fields(self):
        if self.config_rd.get("expt_cmd") is not None and '' != self.config_rd.get("expt_cmd"):
            return eval(self.config_rd.get("expt_cmd")).get("fields")

    # etldt_key
    def get_table_etl_date_key(self):
        if self.config_rd.get("etldt_key") is not None and '' != self.config_rd.get("etldt_key"):
            return self.config_rd.get("etldt_key")

    # dpist_num
    def get_drop_index_sub_table_num(self):
        if self.config_rd.get("dpist_num") is not None and '' != self.config_rd.get("dpist_num"):
            return self.config_rd.get("dpist_num")

    # rely_tbl
    def get_rely_table_list(self):
        if self.config_rd.get("rely_tbl") is not None and '' != self.config_rd.get("rely_tbl"):
            return self.config_rd.get("rely_tbl").split(",")

    # sync_new
    def get_table_sync_new(self):
        if self.config_rd.get("sync_new") is not None and '' != self.config_rd.get("sync_new"):
            return self.config_rd.get("sync_new")

    # is_sync
    def get_table_is_sync(self):
        if self.config_rd.get("is_sync") is not None and '' != self.config_rd.get("is_sync"):
            return self.config_rd.get("is_sync")

    # nopr_rplc
    def get_transcode_replace_char_list(self):
        if self.config_rd.get("nopr_rplc") is not None and '' != self.config_rd.get("nopr_rplc"):
            return eval(self.config_rd.get("nopr_rplc"))

    # subtbl_bd, cannot support nest dict
    def get_sub_table_bound(self):
        if self.config_rd.get("subtbl_bd") is not None and '' != self.config_rd.get("subtbl_bd"):
            sub_tbl_bound = eval(self.config_rd.get("subtbl_bd"))
            # get LowBound
            for sub_bound in sub_tbl_bound.get("LowBound"):
                if 'MinKey()' == sub_tbl_bound.get("LowBound").get(sub_bound):
                    sub_tbl_bound["LowBound"][sub_bound] = MinKey()  
            # get UpBound
            for sub_bound in sub_tbl_bound.get("UpBound"):
                if 'MaxKey()' == sub_tbl_bound.get("UpBound").get(sub_bound):
                    sub_tbl_bound["UpBound"][sub_bound] = MaxKey()  

            return sub_tbl_bound

    # dest_tbl
    def get_destination_table(self):
        if self.config_rd.get("dest_tbl") is not None and "" != self.config_rd.get("dest_tbl"):
            return self.config_rd.get("dest_tbl")

    def get_destination_csname(self):
        if self.config_rd.get("dest_tbl") is not None and "" != self.config_rd.get("dest_tbl"):
            return self.config_rd.get("dest_tbl").split(".")[0]

    def get_destination_clname(self):
        if self.config_rd.get("dest_tbl") is not None and "" != self.config_rd.get("dest_tbl"):
            return self.config_rd.get("dest_tbl").split(".")[1]

    def get_prefetch_file(self):
        if self.config_rd.get("pref_file") is not None and "" != self.config_rd.get("pref_file"):
            return self.config_rd.get("pref_file").split(",")[0]

    def get_prefetch_ok_file(self):
        if self.config_rd.get("pref_file") is not None and "" != self.config_rd.get("pref_file"):
            return self.config_rd.get("pref_file").split(",")[1]

    # sync_meta
    def get_is_sync_meta(self):
        if self.config_rd.get("sync_meta") is not None and '' != self.config_rd.get("sync_meta"):
            return self.config_rd.get("sync_meta")

    # running SQLnn

    # add_fields
    def get_add_fields_list(self):
        if self.config_rd.get("add_fields") is not None and "" != self.config_rd.get("add_fields"):
            return self.config_rd.get("add_fields").split(",")



class SyncLocal:
    def __init__(self, hostname='localhost', svcport='11810',
                 username=None, password=None, connect_hosts=None,
                 cs_name=None, cl_name=None, log_handler=None):
        # config collection space and collection
        self.local_csname = "nlsync"
        self.local_clname = "local"
        self.log = log_handler

        # table sync.local's primary key is tbl_name
        self.cs_name = cs_name
        self.cl_name = cl_name
        self.tbl_name_cond = {"tbl_name": "%s.%s" % (self.cs_name, self.cl_name)}
        # get db connection
        self.db = SCSDB(hostname, svcport, username, password, connect_hosts)
        self.local_rd = dict()
        local_rds = self.db.sync_query(self.local_csname, self.local_clname, self.tbl_name_cond)
        if len(local_rds) == 1:
            self.local_rd = local_rds[0]
        elif len(local_rds) < 1:
            if self.log is None:
                print "WARNNING, don't have table %s.%s restore local info." % (cs_name, cl_name)
            else:
                self.log.warn("WARNNING, don't have table %s.%s restore local info." % (cs_name, cl_name))
        else:
            if self.log is None:
                print "WARNNING, table %s.%s have restore local info." % (cs_name, cl_name)
            else:
                self.log.warn("WARNNING, table %s.%s have restore local info." % (cs_name, cl_name))

    # update local table
    def update_local(self, update_value):
        update_ruler = {"$set": update_value}
        self.db.sync_update(self.local_csname, self.local_clname, update_ruler, self.tbl_name_cond)

    # get all local info
    def get_sync_all_info(self):
        if self.local_rd is not None:
            return self.local_rd

    # sync_sys
    def get_table_system(self):
        if self.local_rd.get("sync_sys") is not None and '' != self.local_rd.get("sync_sys"):
            return self.local_rd.get("sync_sys")

    def set_table_system(self, value):
        update_value = {"sync_sys": value}
        self.update_local(update_value)

    # tbl_name
    def get_table_name(self):
        if self.local_rd.get("tbl_name") is not None and '' != self.local_rd.get("tbl_name"):
            return self.local_rd.get("tbl_name")

    def get_cs_name(self):
        if self.local_rd.get("tbl_name") is not None and '' != self.local_rd.get("tbl_name"):
            return self.local_rd.get("tbl_name").split(".")[0]

    def get_cl_name(self):
        if self.local_rd.get("tbl_name") is not None and '' != self.local_rd.get("tbl_name"):
            return self.local_rd.get("tbl_name").split(".")[1]

    # sync_dt
    def get_sync_date(self):
        if self.local_rd.get("sync_dt") is not None and '' != self.local_rd.get("sync_dt"):
            return self.local_rd.get("sync_dt")

    # batch_dt
    def get_batch_date(self):
        if self.local_rd.get("batch_dt") is not None and '' != self.local_rd.get("batch_dt"):
            return self.local_rd.get("batch_dt")

    # sync_type
    def get_table_sync_type(self):
        if self.local_rd.get("sync_type") is not None and '' != self.local_rd.get("sync_type"):
            return self.local_rd.get("sync_type")

    # proc_info
    def get_process_info(self):
        if self.local_rd.get("proc_info") is not None and '' != self.local_rd.get("proc_info"):
            return self.local_rd.get("proc_info")

    def get_process_info_host(self):
        if self.local_rd.get("proc_info") is not None and '' != self.local_rd.get("proc_info"):
            proc_info = self.local_rd.get("proc_info").split(",")
            return proc_info[0]

    def get_process_info_pid(self):
        if self.local_rd.get("proc_info") is not None and '' != self.local_rd.get("proc_info"):
            proc_info = self.local_rd.get("proc_info").split(",")
            return proc_info[1]

    # sync_flag
    def get_table_sync_flag_list(self):
        if self.local_rd.get("sync_flag") is not None and '' != self.local_rd.get("sync_flag"):
            return eval(self.local_rd.get("sync_flag"))

    # sync_file
    def get_table_sync_file(self):
        if self.local_rd.get("sync_file") is not None and '' != self.local_rd.get("sync_file"):
            return self.local_rd.get("sync_file")

    # getfl_tm
    def get_file_geted_time(self):
        if self.local_rd.get("getfl_tm") is not None and '' != self.local_rd.get("getfl_tm"):
            return self.local_rd.get("getfl_tm")

    # sub_tbl
    def get_product_sub_table(self):
        if self.local_rd.get("sub_tbl") is not None and '' != self.local_rd.get("sub_tbl"):
            return self.local_rd.get("sub_tbl")

    def get_product_sub_cs(self):
        if self.local_rd.get("sub_tbl") is not None and '' != self.local_rd.get("sub_tbl"):
            return self.local_rd.get("sub_tbl").split(".")[0]

    def get_product_sub_cl(self):
        if self.local_rd.get("sub_tbl") is not None and '' != self.local_rd.get("sub_tbl"):
            return self.local_rd.get("sub_tbl").split(".")[1]

    # ctbl_tm
    def get_table_create_index_time(self):
        if self.local_rd.get("ctbl_tm") is not None and '' != self.local_rd.get("ctbl_tm"):
            return self.local_rd.get("ctbl_tm")

    # repo_mtbl
    def get_repository_main_table(self):
        if self.local_rd.get("repo_mtbl") is not None and '' != self.local_rd.get("repo_mtbl"):
            return self.local_rd.get("repo_mtbl")

    def get_repository_main_cs(self):
        if self.local_rd.get("repo_mtbl") is not None and '' != self.local_rd.get("repo_mtbl"):
            return self.local_rd.get("repo_mtbl").split(".")[0]

    def get_repository_main_cl(self):
        if self.local_rd.get("repo_mtbl") is not None and '' != self.local_rd.get("repo_mtbl"):
            return self.local_rd.get("repo_mtbl").split(".")[1]

    # repo_stbl
    def get_repository_sub_table(self):
        if self.local_rd.get("repo_stbl") is not None and '' != self.local_rd.get("repo_stbl"):
            return self.local_rd.get("repo_stbl")

    def get_repository_sub_cs(self):
        if self.local_rd.get("repo_stbl") is not None and '' != self.local_rd.get("repo_stbl"):
            return self.local_rd.get("repo_stbl").split(".")[0]

    def get_repository_sub_cl(self):
        if self.local_rd.get("repo_stbl") is not None and '' != self.local_rd.get("repo_stbl"):
            return self.local_rd.get("repo_stbl").split(".")[1]

    # dst_tbl
    def get_destination_table(self):
        if self.local_rd.get("dst_tbl") is not None and '' != self.local_rd.get("dst_tbl"):
            return self.local_rd.get("dst_tbl")

    def get_destination_cs(self):
        if self.local_rd.get("dst_tbl") is not None and '' != self.local_rd.get("dst_tbl"):
            return self.local_rd.get("dst_tbl").split(".")[0]

    def get_destination_cl(self):
        if self.local_rd.get("dst_tbl") is not None and '' != self.local_rd.get("dst_tbl"):
            return self.local_rd.get("dst_tbl").split(".")[1]

    # file_num
    def get_file_num(self):
        if self.local_rd.get("file_num") is not None and '' != self.local_rd.get("file_num"):
            if type(self.local_rd.get("file_num")) is not int:
                return int(self.local_rd.get("file_num"))
            else:
                return self.local_rd.get("file_num")

    # trok_num
    def get_transcode_ok_num(self):
        if self.local_rd.get("trok_num") is not None and '' != self.local_rd.get("trok_num"):
            if type(self.local_rd.get("trok_num")) is not int:
                return int(self.local_rd.get("trok_num"))
            else:
                return self.local_rd.get("trok_num")

    # tran_tm
    def get_transcode_time(self):
        if self.local_rd.get("tran_tm") is not None and '' != self.local_rd.get("tran_tm"):
            return self.local_rd.get("tran_tm")

    # tran_file
    def get_transcode_file(self):
        if self.local_rd.get("tran_file") is not None and '' != self.local_rd.get("tran_file"):
            return self.local_rd.get("tran_file")

    # iprt_num
    def get_import_num(self):
        if self.local_rd.get("iprt_num") is not None and '' != self.local_rd.get("iprt_num"):
            if type(self.local_rd.get("iprt_num")) is not int:
                return int(self.local_rd.get("iprt_num"))
            else:
                return self.local_rd.get("iprt_num")

    # iprt_tm
    def get_import_time(self):
        if self.local_rd.get("iprt_tm") is not None and '' != self.local_rd.get("iprt_tm"):
            return self.local_rd.get("iprt_tm")

    # upst_tm
    def get_upsert_time(self):
        if self.local_rd.get("upst_tm") is not None and '' != self.local_rd.get("upst_tm"):
            return self.local_rd.get("upst_tm")

    # expt_file
    def get_export_file(self):
        if self.local_rd.get("expt_file") is not None and '' != self.local_rd.get("expt_file"):
            return self.local_rd.get("expt_file")

    # expt_num
    def get_export_num(self):
        if self.local_rd.get("expt_num") is not None and '' != self.local_rd.get("expt_num"):
            if type(self.local_rd.get("expt_num")) is not int:
                return int(self.local_rd.get("expt_num"))
            else:
                return self.local_rd.get("expt_num")

    # expt_tm
    def get_upsert_time(self):
        if self.local_rd.get("expt_tm") is not None and '' != self.local_rd.get("expt_tm"):
            return self.local_rd.get("expt_tm")

    # updt_bnum
    def get_update_begin_num(self):
        if self.local_rd.get("updt_bnum") is not None and '' != self.local_rd.get("updt_bnum"):
            if type(self.local_rd.get("updt_bnum")) is not int:
                return int(self.local_rd.get("updt_bnum"))
            else:
                return self.local_rd.get("updt_bnum")

    # updt_tm
    def get_update_time(self):
        if self.local_rd.get("updt_tm") is not None and '' != self.local_rd.get("updt_tm"):
            return self.local_rd.get("updt_tm")

    # updt_enum
    def get_update_end_num(self):
        if self.local_rd.get("updt_enum") is not None and '' != self.local_rd.get("updt_enum"):
            if type(self.local_rd.get("updt_enum")) is not int:
                return int(self.local_rd.get("updt_enum"))
            else:
                return self.local_rd.get("updt_enum")

    # cidx_tm
    def get_create_index_time(self):
        if self.local_rd.get("cidx_tm") is not None and '' != self.local_rd.get("cidx_tm"):
            return self.local_rd.get("cidx_tm")

    # dtat_tm
    def get_detach_attach_time(self):
        if self.local_rd.get("dtat_tm") is not None and '' != self.local_rd.get("dtat_tm"):
            return self.local_rd.get("dtat_tm")

    # sync_st
    def get_sync_status(self):
        if self.local_rd.get("sync_st") is not None and '' != self.local_rd.get("sync_st"):
            return self.local_rd.get("sync_st")

    # begin_num
    def get_sync_begin_num(self):
        if self.local_rd.get("begin_num") is not None and '' != self.local_rd.get("begin_num"):
            if type(self.local_rd.get("begin_num")) is not int:
                return int(self.local_rd.get("begin_num"))
            else:
                return self.local_rd.get("begin_num")

    # end_num
    def get_sync_end_num(self):
        if self.local_rd.get("end_num") is not None and '' != self.local_rd.get("end_num"):
            if type(self.local_rd.get("end_num")) is not int:
                return int(self.local_rd.get("end_num"))
            else:
                return self.local_rd.get("end_num")

    # begin_tm
    def get_sync_begin_time(self):
        if self.local_rd.get("begin_tm") is not None and '' != self.local_rd.get("begin_tm"):
            return self.local_rd.get("begin_tm")

    # end_tm
    def get_sync_end_time(self):
        if self.local_rd.get("end_tm") is not None and '' != self.local_rd.get("end_tm"):
            return self.local_rd.get("end_tm")



class SyncMeta:
    def __init__(self, hostname='localhost', svcport='11810',
                 username=None, password=None, connect_hosts=None,
                 cs_name=None, cl_name=None, log_handler=None):
        # config collection space and collection
        mdm_csname = "mdm"
        mdm_clname = "metatbl"
        self.metatbl_cs = mdm_csname
        self.metatbl_cl = mdm_clname
        self.log = log_handler

        # table sync.local's primary key is tbl_name
        self.cs_name = cs_name
        self.cl_name = cl_name
        self.tbl_cond = {"tbl_name": "%s.%s" % (self.cs_name, self.cl_name)}
        # get db connection
        self.db = SCSDB(hostname, svcport, username, password, connect_hosts)
        meta_rds = self.db.sync_query(mdm_csname, mdm_clname, self.tbl_cond)
        self.meta_rd = dict()
        if len(meta_rds) == 1:
            self.meta_rd = meta_rds[0]
        elif len(meta_rds) < 1:
            if self.log is None:
                print "WARNNING, don't have table %s.%s metatbl" % (self.cs_name, self.cl_name)
            else:
                self.log.warn("WARINNING, don't have table %s.%s metatbl" % (self.cs_name, self.cl_name))
        else:
            if self.log is None:
                print "WARNNING, table %s.%s have two metatbl" % (self.cs_name, self.cl_name)
            else:
                self.log.warn("WARNNING, table %s.%s have two metatbl" % (self.cs_name, self.cl_name))

    def get_meta_ret(self):
        return self.meta_rd

    def get_fields_count(self):
        meta_record = self.get_meta_ret()
        tbl_meta_rd = meta_record

        # get table's meta keys and sort by field name, such as "field01", "field02"...
        meta_keys = tbl_meta_rd.keys()
        field_keys = []
        for meta_key in meta_keys:
            if 'field' == meta_key[:5]:
                field_keys.append(meta_key)
        # sort
        field_keys.sort()
        return len(field_keys)

    def get_formate_fields(self):
        meta_record = self.get_meta_ret()
        meta_keys = meta_record.keys()
        formate_fields = []
        for meta_key in meta_keys:
            if 'field' == meta_key[:5]:
                field_value = meta_record[meta_key]
                field_value_arr = field_value.split("|")
                field_map = collections.OrderedDict()
                field_map["field_no"] = meta_key[5:]
                field_map["field_info"] = field_value_arr[0]
                field_map["field_type"] = field_value_arr[1]
                field_map["field_desc"] = field_value_arr[2]
                formate_fields.append(field_map)

        fmt_fields = sorted(formate_fields, key=lambda formate_field : formate_field["field_no"])
        return fmt_fields

    def get_primary_key(self):
        if self.meta_rd.get("prim_key") is not None and '' != self.meta_rd.get("prim_key"):
            return self.meta_rd.get("prim_key")

    def get_system_name_ch(self):
        if self.meta_rd.get("sysnm_ch") is not None and '' != self.meta_rd.get("sysnm_ch"):
            return self.meta_rd.get("sysnm_ch")

    def get_system_name_en(self):
        if self.meta_rd.get("sysnm_en") is not None and '' != self.meta_rd.get("sysnm_en"):
            return self.meta_rd.get("sysnm_en")

    def get_table_name_ch(self):
        if self.meta_rd.get("tblnm_ch") is not None and '' != self.meta_rd.get("tblnm_ch"):
            return self.meta_rd.get("tblnm_ch")

    def get_table_name_en(self):
        if self.meta_rd.get("tblnm_en") is not None and '' != self.meta_rd.get("tblnm_en"):
            return self.meta_rd.get("tblnm_en")

    def get_meta_sync_date(self):
        if self.meta_rd.get("sync_dt") is not None and '' != self.meta_rd.get("sync_dt"):
            return self.meta_rd.get("sync_dt")

    def remove_metatbl_info(self):
        matcher = self.tbl_cond
        self.db.sync_remove(self.metatbl_cs, self.metatbl_cl, matcher)

    def upsert_metatbl_info(self, metatbl_info):
        ruler = {"$set": metatbl_info}
        matcher = self.tbl_cond
        self.db.sync_upsert(self.metatbl_cs, self.metatbl_cl, ruler, matcher)

class SyncMetaHis:
    def __init__(self, hostname='localhost', svcport='11810',
                 username=None, password=None, connect_hosts=None,
                 cs_name=None, cl_name=None, sync_date=None, log_handler=None):
        # config collection space and collection
        mdm_csname = "mdm"
        mdm_clname = "metahis"
        self.log = log_handler

        # table sync.local's primary key is tbl_name
        self.cs_name = cs_name
        self.cl_name = cl_name
        self.tbl_cond = {"tbl_name": "%s.%s" % (self.cs_name, self.cl_name),
                         "sync_dt": sync_date}
        # get db connection
        self.db = SCSDB(hostname, svcport, username, password, connect_hosts)
        meta_rds = self.db.sync_query(mdm_csname, mdm_clname, self.tbl_cond)
        self.meta_rd = dict()
        if len(meta_rds) == 1:
            self.meta_rd = meta_rds[0]
        elif len(meta_rds) < 1:
            if self.log is None:
                print "WARNNING, don't have table %s.%s metatbl" % (self.cs_name, self.cl_name)
            else:
                self.log.warn("WARINNING, don't have table %s.%s metatbl" % (self.cs_name, self.cl_name))
        else:
            if self.log is None:
                print "WARNNING, table %s.%s have two metatbl" % (self.cs_name, self.cl_name)
            else:
                self.log.warn("WARNNING, table %s.%s have two metatbl" % (self.cs_name, self.cl_name))

    def get_meta_ret(self):
        return self.meta_rd

    def get_fields_count(self):
        meta_record = self.get_meta_ret()
        tbl_meta_rd = meta_record

        # get table's meta keys and sort by field name, such as "field01", "field02"...
        meta_keys = tbl_meta_rd.keys()
        field_keys = []
        for meta_key in meta_keys:
            if 'field' == meta_key[:5]:
                field_keys.append(meta_key)
        # sort
        field_keys.sort()
        return len(field_keys)

    def get_formate_fields(self):
        metahis_record = self.get_meta_ret()
        metahis_keys = metahis_record.keys()
        formate_fields = []
        for metahis_key in metahis_keys:
            if 'field' == metahis_key[:5]:
                field_value = metahis_record[metahis_key]
                field_value_arr = field_value.split("|")
                field_map = collections.OrderedDict()
                field_map["field_no"] = metahis_key[5:]
                field_map["field_info"] = field_value_arr[0]
                field_map["field_type"] = field_value_arr[1]
                field_map["field_desc"] = field_value_arr[2]
                formate_fields.append(field_map)

        fmt_fields = sorted(formate_fields, key=lambda formate_field : formate_field["field_no"])
        return fmt_fields

    def get_primary_key(self):
        if self.meta_rd.get("prim_key") is not None and '' != self.meta_rd.get("prim_key"):
            return self.meta_rd.get("prim_key")

    def get_system_name_ch(self):
        if self.meta_rd.get("sysnm_ch") is not None and '' != self.meta_rd.get("sysnm_ch"):
            return self.meta_rd.get("sysnm_ch")

    def get_system_name_en(self):
        if self.meta_rd.get("sysnm_en") is not None and '' != self.meta_rd.get("sysnm_en"):
            return self.meta_rd.get("sysnm_en")

    def get_table_name_ch(self):
        if self.meta_rd.get("tblnm_ch") is not None and '' != self.meta_rd.get("tblnm_ch"):
            return self.meta_rd.get("tblnm_ch")

    def get_table_name_en(self):
        if self.meta_rd.get("tblnm_en") is not None and '' != self.meta_rd.get("tblnm_en"):
            return self.meta_rd.get("tblnm_en")

    def get_meta_sync_date(self):
        if self.meta_rd.get("sync_dt") is not None and '' != self.meta_rd.get("sync_dt"):
            return self.meta_rd.get("sync_dt")

def test_main():
    host_name = "T-DSJ-HISDB01"
    svc_port = "31810"
    user_name = "sdbapp"
    password = "a2ZwdFNEQjIwMTY="
    hosts = "T-DSJ-HISDB01:31810,T-DSJ-HISDB02:31810,T-DSJ-HISDB03:31810"
    connect_hosts = [{"host": "T-DSJ-HISDB01", "service":31810},{"host": "T-DSJ-HISDB02", "service":31810},{"host": "T-DSJ-HISDB03", "service":31810}]
    """
    logging file
    """
    sync_log_cs = "nlsync"
    sync_log_cl = "log"
    sync_sys = "CBE"
    cs_name = "cbe"
    cl_name = "bptparm"
    tbl_name = "cbe.bptparm"
    tbl_cond = {'tbl_name': tbl_name}
    log_connect = {'HostName': host_name, 'ServerPort': svc_port,
                   'UserName': user_name, 'Password': password,
                   'CsName': sync_log_cs, 'ClName': sync_log_cl}

    sync_date = "20170226"
    log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}
    log = logging.getLogger("sync_migrate_test")
    log.setLevel(logging.INFO)
    logfile_name = logfile_dir
    fh = SCFileHandler(logfile_name, log_table, log_connect)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    log.addHandler(fh)

    # begin test config
    sync_config = SyncConfig(host_name, svc_port, user_name, password,
                             connect_hosts, cs_name, cl_name, log)

    print "=================TESTING nlsync.config FUNCTION=========================="
    print "sync_sys: %s" % sync_config.get_sync_sys()
    print "tbl_name: %s" % sync_config.get_table_name()
    print "tbl_name cs: %s" % sync_config.get_cs_name()
    print "tbl_name cl: %s" % sync_config.get_cl_name()
    print "sync_dom: %s" % sync_config.get_table_domain()
    print "dt_delta: %s" % sync_config.get_delta_date()
    print "main_dtp: %s" % sync_config.get_date_format_list()
    print "main_dtp product: %s" % sync_config.get_product_vertical_date_format()
    print "main_dtp repository: %s" % sync_config.get_repository_vertical_date_format()
    print "sync_type: %s" % sync_config.get_table_sync_type()
    print "sync_file sync_host: %s" % sync_config.get_table_sync_host()
    print "sync_file sync_file: %s" % sync_config.get_table_sync_file()
    print "sync_file get_mode: %s" % sync_config.get_table_sync_file_remote_mode()
    print "sync_file frm_ip: %s" % sync_config.get_table_sync_file_remote_host()
    print "sync_file frm_file: %s" % sync_config.get_table_sync_file_remote_file()
    print "sync_file frm_user: %s" % sync_config.get_table_sync_file_remote_user()
    print "sync_file frm_password: %s" % sync_config.get_table_sync_file_remote_password()
    print "idx_field: %s" % sync_config.get_table_index_field()
    print "idx_field list: %s" % sync_config.get_table_index_field_list()
    print "shard_key product: %s" % sync_config.get_product_vertical_key_map()
    print "shard_key repository: %s" % sync_config.get_product_horizontal_key_map()
    print "rp_sd_key vertical: %s" % sync_config.get_repository_vertical_key_map()
    print "rp_sd_key horizontal: %s" % sync_config.get_repository_horizontal_key_map()
    print "iprt_cmd map: %s" % sync_config.get_table_import_options_map()
    print "upst_cmd map: %s" % sync_config.get_table_upsert_options_map()
    print "expt_cmd map: %s" % sync_config.get_table_export_options_map()
    print "expt_cmd export_file: %s" % sync_config.get_table_export_options_file()
    print "expt_cmd export_fileds: %s" % sync_config.get_table_export_options_fields()
    print "etldt_key: %s" % sync_config.get_table_etl_date_key()
    print "dpist_num: %s" % sync_config.get_drop_index_sub_table_num()
    print "rely_tbl list: %s" % sync_config.get_rely_table_list()
    print "sync_new: %s" % sync_config.get_table_sync_new()
    print "is_sync: %s" % sync_config.get_table_is_sync()
    print "nopr_rplc: %s" % sync_config.get_transcode_replace_char_list()
    print "========================================================================="

    # begin test config
    sync_local = SyncLocal(host_name, svc_port, user_name, password,
                           connect_hosts, cs_name, cl_name, log)

    print "=================TESTING nlsync.local FUNCTION==========================="
    print "sync_sys: %s" % sync_local.get_table_system()
    print "tbl_name: %s" % sync_local.get_table_name()
    print "tbl_name cs: %s" % sync_local.get_cs_name()
    print "tbl_name cl: %s" % sync_local.get_cl_name()
    print "sync_dt: %s" % sync_local.get_sync_date()
    print "sync_type: %s" % sync_local.get_table_sync_type()
    print "proc_info: %s" % sync_local.get_process_info()
    print "proc_info host: %s" % sync_local.get_process_info_host()
    print "proc_info pid: %s" % sync_local.get_process_info_pid()
    print "sync_flag: %s" % sync_local.get_table_sync_flag_list()
    print "sync_file: %s" % sync_local.get_table_sync_file()
    print "getfl_tm: %s" % sync_local.get_file_geted_time()
    print "sub_tbl: %s" % sync_local.get_product_sub_table()
    print "sub_tbl cs: %s" % sync_local.get_product_sub_cs()
    print "sub_tbl cl: %s" % sync_local.get_product_sub_cl()
    print "ctbl_tm: %s" % sync_local.get_table_create_index_time()
    print "repo_mtbl: %s" % sync_local.get_repository_main_table()
    print "repo_mtbl cs: %s" % sync_local.get_repository_main_cs()
    print "repo_mtbl cl: %s" % sync_local.get_repository_main_cl()
    print "repo_stbl: %s" % sync_local.get_repository_sub_table()
    print "repo_stbl cs: %s" % sync_local.get_repository_sub_cs()
    print "repo_stbl cl: %s" % sync_local.get_repository_sub_cl()
    print "dst_tbl: %s" % sync_local.get_destination_table()
    print "dst_tbl cs: %s" % sync_local.get_destination_cs()
    print "dst_tbl cl: %s" % sync_local.get_destination_cl()
    print "file_num: %s" % sync_local.get_file_num()
    print "trok_num: %s" % sync_local.get_transcode_ok_num()
    print "tran_tm: %s" % sync_local.get_transcode_time()
    print "tran_file: %s" % sync_local.get_transcode_file()
    print "iprt_num: %s" % sync_local.get_import_num()
    print "iprt_tm: %s" % sync_local.get_import_time()
    print "upst_tm: %s" % sync_local.get_upsert_time()
    print "expt_file: %s" % sync_local.get_export_file()
    print "expt_num: %s" % sync_local.get_export_num()
    print "updt_bnum: %s" % sync_local.get_update_begin_num()
    print "updt_tm: %s" % sync_local.get_update_time()
    print "updt_enum: %s" % sync_local.get_update_end_num()
    print "cidx_tm: %s" % sync_local.get_create_index_time()
    print "dtat_tm: %s" % sync_local.get_detach_attach_time()
    print "sync_st: %s" % sync_local.get_sync_status()
    print "begin_num: %s" % sync_local.get_sync_begin_num()
    print "end_num: %s" % sync_local.get_sync_end_num()
    print "begin_tm: %s" % sync_local.get_sync_begin_time()
    print "end_tm: %s" % sync_local.get_sync_end_time()
    print "========================================================================="

if __name__ == '__main__':
    test_main()
