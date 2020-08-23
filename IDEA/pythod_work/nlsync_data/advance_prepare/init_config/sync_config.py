#!/usr/bin/python
#coding=utf-8

"""
Description: sdb_connection == the connection to sdb,
             nlsync_manage_domain == the domain of nlsynchronize data manage,
             nlsync_tbl_options == the main table and sub table opitions for manage table,
             sdb_nlsync_tbls == the name of nlsynchronize data main and sub tables

Date: 2017-02-14
"""
sdb_connection = {
    "hostname": "localhost",
    "svcport": 11810,
    "username": "sdbapp",
    "password": "kfptSDB2016"
}

# python API have no create domain function, so create domain cannot do on PYTHON API
sync_manage_domain = {
    "domain_name": "nlsync_domain",
    "group": ["rg1", "rg2", "rg3", "rg4", "rg5", "rg6", "rg7", "rg8", "rg9", "rg10", "rg11", "rg12"],
    "AutoSplit": True
}

sync_tbl_options = {
    "tbl_options": {
        "ShardingKey": {"_id": 1},
        "ShardingType": "range",
        "IsMainCL": True
    },
    "subtbl_options": {
        "ShardingKey": {"tbl_name": 1},
        "ShardingType": "hash",
        "Compressed": True,
        "CompressionType": "lzw",
        "AutoSplit": True
    }
}

sdb_sync_tbls = [
    {
        "cs_name": "nlsync",
        "cl_name": "config",
        "subcs_name": "nlsync_config1",
        "subcl_name": "nlsync_config1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "local",
        "subcs_name": "nlsync_local1",
        "subcl_name": "nlsync_local1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "history",
        "subcs_name": "nlsync_history1",
        "subcl_name": "nlsync_history1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "sparkloc",
        "subcs_name": "nlsync_sparkloc1",
        "subcl_name": "nlsync_sparkloc1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "sparkhis",
        "subcs_name": "nlsync_sparkhis1",
        "subcl_name": "nlsync_sparkhis1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "log",
        "subcs_name": "nlsync_log1",
        "subcl_name": "nlsync_log1"
    }
]


"""
Description: sync_tblconf_infos == the synchronize data tables information,

Date: 2017-02-14
"""
sync_tblconf_infos = (
# import store table
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bptfhist_yuedp",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_his_dprepo",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': ''}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'ci_no': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bpcofhsh",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/BPCOFHSH.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPCOFHSH.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},

# import table
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citcdm",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITCDM.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITCDM.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citfdm",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITFDM.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITFDM.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.ddtmstr",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DDTMSTR.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DDTMSTR.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'ci_no': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.lntcont",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/LNTCONT.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/LNTCONT.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'ci_no': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.lntguar",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/LNTGUAR.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/LNTGUAR.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'ci_no': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.pntbcc",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/PNTBCC.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/PNTBCC.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'ci_no': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.pntdft",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_import",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/PNTDFT.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/PNTDFT.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'ci_no': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
},
#full
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citnam",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITNAM.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITNAM.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ci_no': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctiamst",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTIAMST_MAS.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTIAMST_MAS.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.ibtmst",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/IBTMST.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/IBTMST.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'nostro_code': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctbinpm",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTBINPM.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTBINPM.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'bin': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bptparm",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/BPTPARM.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPTPARM.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'typ': 1}, {'cd': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['all']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citcnt",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITCNT.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITCNT.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ci_no': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citadr",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITADR.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITADR.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ci_no': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citads",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITADS.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITADS.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ci_no': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.ddtccy",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "full",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DDTCCY.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DDTCCY.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ac': 1}, {'ccy': 1}, {'ccy_type': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": ""
},

# append customer table
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctiaccy",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTIACCY.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTIACCY.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'crt_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_dctiaccy where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.dctiaccy a left join repo_cbe.repo_dctiaccy b on a.via_ac = b.via_ac and a.ccy = b.ccy and a.ccy_type = b.ccy_type and b.tx_date = '%s' where b.via_ac is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctacbnd",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTACBND.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTACBND.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'crt_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_dctacbnd where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.dctacbnd a left join repo_cbe.repo_dctacbnd b on a.ii_ac = b.ii_ac and a.i_ac = b.i_ac and a.i_ac_bnk = b.i_ac_bnk and b.tx_date = '%s' where b.ii_ac is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.ddtbalh",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DDTBALH.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DDTBALH.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_ddtbalh where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.ddtbalh a left join repo_cbe.repo_ddtbalh b on a.ac = b.ac and a.ccy = b.ccy and a.ccy_type = b.ccy_type and a.str_date = b.str_date and b.tx_date = '%s' where b.ac is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citagent",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITAGENT.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITAGENT.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_citagent where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.citagent a left join repo_cbe.repo_citagent b on a.jrn_no = b.jrn_no and a.ac_dt = b.ac_dt and b.tx_date = '%s' where b.jrn_no is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.aitcmib",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/AITCMIB.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/AITCMIB.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'itm': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_aitcmib where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.aitcmib a left join repo_cbe.repo_aitcmib b on a.gl_book = b.gl_book and a.br = b.br and a.itm = b.itm and a.seq = b.seq and b.tx_date = '%s' where b.gl_book is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.aitmib",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/AITMIB.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/AITMIB.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_aitmib where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.aitmib a left join repo_cbe.repo_aitmib b on a.gl_book = b.gl_book and a.br = b.br and a.itm_no = b.itm_no and a.seq = b.seq and a.ccy = b.ccy and b.tx_date = '%s' where b.gl_book is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctspac",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTSPAC.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTSPAC.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'eff_flg': 1}, {'std_ac': 1}], [{'eff_flg': 1}, {'free_ac': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_dctspac where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.dctspac a left join repo_cbe.repo_dctspac b on a.free_ac = b.free_ac and b.tx_date = '%s' where b.free_ac is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.ddtvsabi",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DDTVSABI.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DDTVSABI.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_ddtvsabi where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.ddtvsabi a left join repo_cbe.repo_ddtvsabi b on a.vs_ac = b.vs_ac and a.ccy = b.ccy and a.ccy_typ = b.ccy_typ and b.tx_date = '%s' where b.vs_ac is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctnocrd",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTNOCRD.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTNOCRD.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'new_card_no': 1}, {'old_card_no': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_dctnocrd where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.dctnocrd a left join repo_cbe.repo_dctnocrd b on a.new_card_no = b.new_card_no and b.tx_date = '%s' where b.new_card_no is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citreln",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITRELN.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITRELN.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ci_no': 1}], [{'ci_no': 1}, {'cirel_cd': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_citreln where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.citreln a left join repo_cbe.repo_citreln b on a.ci_no = b.ci_no and a.seq = b.seq and b.tx_date = '%s' where b.ci_no is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citpdm",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITPDM.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITPDM.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ci_no': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_citpdm where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.citpdm a left join repo_cbe.repo_citpdm b on a.ci_no = b.ci_no and b.tx_date = '%s' where b.ci_no is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citacr",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITACR.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITACR.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'agr_no':1}], [{'enty_typ': 1}, {'agr_no': 1}, {'frm_app': 1}], [{'enty_typ': 1}, {'ci_no': 1}, {'frm_app': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'agr_no': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_citacr where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.citacr a left join repo_cbe.repo_citacr b on a.agr_no = b.agr_no and b.tx_date = '%s' where b.agr_no is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.citbas",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/CITBAS.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/CITBAS.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ci_nm': 1}], [{'ci_no': 1}], [{'id_no': 1}], [{'tx_date': 1}, {'ci_no': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_citbas where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.citbas a left join repo_cbe.repo_citbas b on a.ci_no = b.ci_no and b.tx_date = '%s' where b.ci_no is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctaclnk",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTACLNK.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTACLNK.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'card_no': 1}], [{'via_ac': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'card_no': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_dctaclnk where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.dctaclnk a left join repo_cbe.repo_dctaclnk b on a.card_no = b.card_no and b.tx_date = '%s' where b.card_no is null ;"
},


{
    "sync_sys": "CBE",
    "tbl_name": "cbe.dctiaacr",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DCTIAACR.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DCTIAACR.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'sub_ac': 1}], [{'via_ac': 1}], [{'via_ac': 1}, {'seq': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'via_ac': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_dctiaacr where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.dctiaacr a left join repo_cbe.repo_dctiaacr b on a.via_ac = b.via_ac and a.seq = b.seq and b.tx_date = '%s' where b.via_ac is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.ddtmst",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/DDTMST.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/DDTMST.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ac': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'ac': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_ddtmst where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.ddtmst a left join repo_cbe.repo_ddtmst b on a.ac = b.ac and b.tx_date = '%s' where b.ac is null ;"
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.tdtsmst",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_cus",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/TDTSMST.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/TDTSMST.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ac': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'ac': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into %s select distinct * from repo_cbe.repo_tdtsmst where tx_date = '%s';",
    "run_sql02": "SparkSQL|insert into %s select distinct a.* from cbe.tdtsmst a left join repo_cbe.repo_tdtsmst b on a.ac = b.ac and b.tx_date = '%s' where b.ac is null ;"
},





# append history table
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bptfhisa",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_his",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/BPTFHISA.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPTFHISA.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ac': 1}], [{'ac_dt': 1}, {'ac': 1}], [{'ac': 1}, {'ac_dt': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'ac_dt': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into cbe.bptfhisa select * from repo_cbe.repo_bptfhisa where tx_date = '%s' ;",
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bptfhist",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_his_dp",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/BPTFHIST.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPTFHIST.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ac': 1}], [{'ac': 1}, {'ac_dt': 1}], [{'ac': 1}, {'ac_dt': 1}, {'print_flg': 1}], [{'ac_dt': 1}, {'ac': 1}], [{'ac_dt': 1}, {'jrnno': 1}, {'jrn_seq': 1}], [{'jrnno': 1}, {'ac_dt': 1}, {'print_flg': 1}], [{'tx_date': 1}], [{'tx_tlr': 1}, {'tx_br': 1}, {'ac_dt': 1}], [{'tx_tlr': 1}, {'tx_br': 1}, {'ac_dt': 1}, {'print_flg': 1}], [{'tx_tool': 1}, {'ac_dt': 1}, {'print_flg': 1}]]",
    "shard_key": "[{'vertical_key': {'ac_dt': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|create table if not exists repo_bptfhist.repo_bptfhist[MAINDT-1](ac string, ac_dt int, jrnno bigint, jrn_seq int, vchno string, tx_br int, tx_dp int, tx_dt int, tx_tm int, tx_chnl string, tx_req_chnl string, tx_chnl_dtl string, tx_reqfm string, app_mmo string, tx_cd string, drcrflg string, prod_cd string, com_prod string, prdmo_cd string, ref_no string, bv_code string, head_no string, bv_no string, ci_no string, tx_tool string, oth_ac string, oth_tx_tool string, rlt_ac string, rlt_ac_name string, rlt_tx_tool string, rlt_bank string, rlt_ref_no string, rlt_ccy string, tx_ccy string, tx_ccy_type string, tx_amt double, tx_mmo string, tx_sts string, tx_val_dt int, sumup_flg string, print_flg string, remark string, narrative string, tx_tlr string, maker string, sup1 string, sup2 string, tx_rev_dt int, org_ac_dt int, org_jrnno bigint, update_dt int, fmt_id string, fmt_cd string, fmt_len bigint, fmt_data string, ts string, his_update_ind string, tx_date string) USING com.sequoiadb.spark OPTIONS  (host 'A-DSJ-HISDB01:11810,A-DSJ-HISDB02:11810,A-DSJ-HISDB03:11810,A-DSJ-HISDB04:11810,A-DSJ-HISDB05:11810,A-DSJ-HISDB06:11810,A-DSJ-HISDB07:11810,A-DSJ-HISDB08:11810,A-DSJ-HISDB09:11810,A-DSJ-HISDB10:11810,A-DSJ-HISDB11:11810,A-DSJ-HISDB12:11810,A-DSJ-HISDB13:11810,A-DSJ-HISDB14:11810,A-DSJ-HISDB15:11810,A-DSJ-HISDB16:11810' , collectionspace 'repo_bptfhist1', collection 'repo_bptfhist1[MAINDT-2]', username 'sdbapp', password 'kfptSDB2016');",
    "run_sql02": "SparkSQL|create table if not exists repo_bpcofhsh.repo_bpcofhsh[MAINDT-1](data_flg string, tx_dt int, ac_dt int, jrnno bigint, jrn_seq int, ac string, ref_no string, tx_tool string, app_mmo string, tx_cd string, tx_br int, tx_dp int, tx_tlr string, sup1 string, sup2 string, tx_chnl string, tx_req_chnl string, tx_chnl_dtl string, ci_no string, drcr_flg string, tx_ccy string, ccy_type string, tx_amt double, cur_bal double, tx_val_dt int, prod_cd string, sts string, tx_tm int, remark string, maker_tlr string, narrative string, tx_mmo string, rel_ac string, rel_ci_name string, rel_ac_br string, rel_ac_br_desc string, bv_code string, head_no string, bv_no string, com_prod string, ac_ci_name string, ac_br int, ac_br_desc string, his_update_ind string, tx_date string) USING com.sequoiadb.spark OPTIONS  (host 'A-DSJ-HISDB01:11810,A-DSJ-HISDB02:11810,A-DSJ-HISDB03:11810,A-DSJ-HISDB04:11810,A-DSJ-HISDB05:11810,A-DSJ-HISDB06:11810,A-DSJ-HISDB07:11810,A-DSJ-HISDB08:11810,A-DSJ-HISDB09:11810,A-DSJ-HISDB10:11810,A-DSJ-HISDB11:11810,A-DSJ-HISDB12:11810,A-DSJ-HISDB13:11810,A-DSJ-HISDB14:11810,A-DSJ-HISDB15:11810,A-DSJ-HISDB16:11810' , collectionspace 'repo_bpcofhsh1', collection 'repo_bpcofhsh1[MAINDT-2]', username 'sdbapp', password 'kfptSDB2016');",
    "run_sql03": "SparkSQL|insert into repo_cbe.repo_bptfhist_yuedp[CHECK-cbe.bptfhist_yuedp-] select case when a.tx_date is not null then a.ac else b.ac end, case when a.tx_date is not null then a.ac_dt else b.ac_dt end, case when a.tx_date is not null then a.jrnno else b.jrnno end, case when a.tx_date is not null then a.jrn_seq else b.jrn_seq end, case when a.tx_date is not null then a.vchno end, case when a.tx_date is not null then a.tx_br else b.tx_br end, case when a.tx_date is not null then a.tx_dp else b.tx_dp end, case when a.tx_date is not null then a.tx_dt else b.tx_dt end, case when a.tx_date is not null then a.tx_tm else b.tx_tm end, case when a.tx_date is not null then a.tx_chnl else b.tx_chnl end, case when a.tx_date is not null then a.tx_req_chnl else b.tx_req_chnl end, case when a.tx_date is not null then a.tx_chnl_dtl else b.tx_chnl_dtl end, case when a.tx_date is not null then a.tx_reqfm end, case when a.tx_date is not null then a.app_mmo else b.app_mmo end, case when a.tx_date is not null then a.tx_cd else b.tx_cd end, case when a.tx_date is not null then a.drcrflg else b.DRCR_FLG end, case when a.tx_date is not null then a.prod_cd else b.prod_cd end, case when a.tx_date is not null then a.com_prod else b.com_prod end, case when a.tx_date is not null then a.prdmo_cd end, case when a.tx_date is not null then a.ref_no else b.ref_no end, case when a.tx_date is not null then a.bv_code else b.bv_code end, case when a.tx_date is not null then a.head_no else b.head_no end, case when a.tx_date is not null then a.bv_no else b.bv_no end, case when a.tx_date is not null then a.ci_no else b.ci_no end, case when a.tx_date is not null then a.tx_tool else b.tx_tool end, case when a.tx_date is not null then a.oth_ac end, case when a.tx_date is not null then a.oth_tx_tool end, case when a.tx_date is not null then a.rlt_ac end, case when a.tx_date is not null then a.rlt_ac_name end, case when a.tx_date is not null then a.rlt_tx_tool end, case when a.tx_date is not null then a.rlt_bank end, case when a.tx_date is not null then a.rlt_ref_no end, case when a.tx_date is not null then a.rlt_ccy end, case when a.tx_date is not null then a.tx_ccy else b.tx_ccy end, case when a.tx_date is not null then a.tx_ccy_type end, case when a.tx_date is not null then a.tx_amt else b.tx_amt end, case when a.tx_date is not null then a.tx_mmo else b.tx_mmo end, case when a.tx_date is not null then a.tx_sts else b.STS end, case when a.tx_date is not null then a.tx_val_dt else b.tx_val_dt end, case when a.tx_date is not null then a.sumup_flg end, case when a.tx_date is not null then a.print_flg end, case when a.tx_date is not null then a.remark else b.remark end, case when a.tx_date is not null then a.narrative else b.narrative end, case when a.tx_date is not null then a.tx_tlr else b.tx_tlr end, case when a.tx_date is not null then a.maker else b.MAKER_TLR end, case when a.tx_date is not null then a.sup1 else b.sup1 end, case when a.tx_date is not null then a.sup2 else b.sup2 end, case when a.tx_date is not null then a.tx_rev_dt end, case when a.tx_date is not null then a.org_ac_dt end, case when a.tx_date is not null then a.org_jrnno end, case when a.tx_date is not null then a.update_dt end, case when a.tx_date is not null then a.fmt_id end, case when a.tx_date is not null then a.fmt_cd end, case when a.tx_date is not null then a.fmt_len end, case when a.tx_date is not null then a.fmt_data end, case when a.tx_date is not null then a.ts end, case when a.tx_date is not null then a.his_update_ind else b.his_update_ind end, case when a.tx_date is not null then a.tx_date else b.tx_date end, b.ac_br, b.ac_br_desc, b.ac_ci_name, b.ccy_type, b.cur_bal, b.data_flg, b.rel_ac, b.rel_ci_name, b.rel_ac_br, b.rel_ac_br_desc from repo_bptfhist.repo_bptfhist[MAINDT-1] a full join repo_bpcofhsh.repo_bpcofhsh[MAINDT-1][CHECK-cbe.bpcofhsh-] b on a.ac_dt=b.ac_dt and a.jrnno=b.jrnno and a.jrn_seq=b.jrn_seq;",
    "run_sql04": "SparkSQL|insert into cbe.bptfhist_repo_di[TRUNCATE-bptfhist_repo_di.bptfhist_repo_di-] select * from repo_cbe.repo_bptfhist_yuedp where tx_date = '[MAINDT-2]' and not (substr(tx_date, 1, 4) = substr(cast(ac_dt as string), 1, 4) and substr(tx_date, 6, 2) = substr(cast(ac_dt as string), 5, 2) and substr(tx_date, 9, 2) = substr(cast(ac_dt as string), 7, 2) );",
    "run_sql05": "PostgreSQL|insert into bptfhist_repo_dicz[TRUNCATE-bptfhist_repo_dicz.bptfhist_repo_dicz-] select a.ac, a.ac_dt, a.jrnno, a.jrn_seq, a.vchno, a.tx_br, a.tx_dp, a.tx_dt, a.tx_tm, a.tx_chnl, a.tx_req_chnl, a.tx_chnl_dtl, a.tx_reqfm, a.app_mmo, a.tx_cd, a.drcrflg, a.prod_cd, a.com_prod, a.prdmo_cd, a.ref_no, a.bv_code, a.head_no, a.bv_no, a.ci_no, a.tx_tool, a.oth_ac, a.oth_tx_tool, a.rlt_ac, a.rlt_ac_name, a.rlt_tx_tool, a.rlt_bank, a.rlt_ref_no, a.rlt_ccy, a.tx_ccy, a.tx_ccy_type, a.tx_amt, a.tx_mmo, a.tx_sts, a.tx_val_dt, a.sumup_flg, a.print_flg, a.remark, a.narrative, a.tx_tlr, a.maker, a.sup1, a.sup2, a.tx_rev_dt, a.org_ac_dt, a.org_jrnno, a.update_dt, a.fmt_id, a.fmt_cd, a.fmt_len, a.fmt_data, a.ts, a.his_update_ind, a.tx_date, b.ac_br, b.ac_br_desc, b.ac_ci_name, b.ccy_type, b.cur_bal, b.data_flg, b.rel_ac, b.rel_ci_name, b.rel_ac_br, b.rel_ac_br_desc from bptfhist_repo_di a, bptfhist b where a.ac_dt = b.ac_dt and a.jrnno = b.jrnno and a.jrn_seq = b.jrn_seq;",
    "run_sql06": "SparkSQL|insert into cbe.bptfhist select * from repo_cbe.repo_bptfhist_yuedp where tx_date = '[MAINDT-2]' and substr(tx_date, 1, 4) = substr(cast(ac_dt as string), 1, 4) and substr(tx_date, 6, 2) = substr(cast(ac_dt as string), 5, 2) and substr(tx_date, 9, 2) = substr(cast(ac_dt as string), 7, 2);",
    "run_sql07": "PostgreSQL|delete from bptfhist a where exists (select 1 from bptfhist_repo_di b where a.ac_dt = b.ac_dt and a.jrnno = b.jrnno and a.jrn_seq = b.jrn_seq);",
    "run_sql08": "PostgreSQL|insert into bptfhist select * from bptfhist_repo_dicz;",
    "run_sql09": "PostgreSQL|insert into bptfhist select a.* from bptfhist_repo_di a left join bptfhist_repo_dicz b on a.ac_dt = b.ac_dt and a.jrnno = b.jrnno and a.jrn_seq = b.jrn_seq where b.ac_dt is null;",
    "run_sql10": "SparkSQL|insert into cbe.tbbrhp[TRUNCATE] select distinct b.cdesc,substr(b.cd,4,6),reverse(substr(reverse(val), 0, 6)) from cbe.bptparm b where b.typ='ORGM' ;"
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bptnhist",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_his",
    "sync_file": "[{'sync_host': 'A-DSJ-HISDB07', 'sync_file': '/etldata1/odsdata/%s/CBE/BPTNHIST.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.2.2.25', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPTNHIST.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ac': 1}], [{'ac_dt': 1}, {'ac': 1}], [{'tx_date': 1}]]",
    "shard_key": "[{'vertical_key': {'ac_dt': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "iprt_cmd": "",
    "run_sql01": "SparkSQL|insert into cbe.bptnhist_repo_di select * from repo_cbe.repo_bptnhist where tx_date = '%s' and not (substr(tx_date, 1, 4) = substr(cast(ac_dt as string), 1, 4) and substr(tx_date, 6, 2) = substr(cast(ac_dt as string), 5, 2) and substr(tx_date, 9, 2) = substr(cast(ac_dt as string), 7, 2) );",
    "run_sql02": "PostgreSQL|delete from bptnhist a where exists (select 1 from bptnhist_repo_di b where a.ac_dt = b.ac_dt and a.jrnno = b.jrnno and a.jrn_seq = b.jrn_seq);",
    "run_sql03": "SparkSQL|insert into cbe.bptnhist select * from repo_cbe.repo_bptnhist where tx_date = '%s';",
},




               )
