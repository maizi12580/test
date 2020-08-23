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
    "username": "nlsdbapp",
    "password": "kfptNLSDB2017"
}



# python API have no create domain function, so create domain cannot do on PYTHON API
sync_manage_domain = {
    "domain_name": "nl_domain_1",
    "group": ["dg1", "dg2", "dg3", "dg4", "dg5", "dg6", "dg7", "dg8", "dg9", "dg10", "dg11", 
              "dg12", "dg13", "dg14", "dg15", "dg16", "dg17", "dg18", "dg19", "dg20", "dg21", "dg22", "dg23", "dg24", "dg25", 
              "dg26", "dg27", "dg28", "dg29", "dg30", "dg31", "dg32", "dg33", "dg34", "dg35", "dg36", "dg37", "dg38", "dg39", 
              "dg40", "dg41", "dg42", "dg43", "dg44", "dg45", "dg46", "dg47", "dg48", "dg49", "dg50", "dg51", "dg52", "dg53", 
              "dg54", "dg55", "dg56", "dg57", "dg58", "dg59", "dg60", "dg61", "dg62", "dg63", "dg64", "dg65", "dg66", "dg67", 
              "dg68", "dg69", "dg70", "dg71", "dg72", "dg73", "dg74", "dg75", "dg76", "dg77", "dg78", "dg79", "dg80", "dg81", 
              "dg82", "dg83", "dg84", "dg85", "dg86", "dg87", "dg88", "dg89", "dg90", "dg91", "dg92", "dg93", "dg94", "dg95", 
              "dg96", "dg97", "dg98", "dg99", "dg100", "dg101", "dg102", "dg103", "dg104", "dg105", "dg106", "dg107", "dg108", 
              "dg109", "dg110", "dg111", "dg112", "dg113", "dg114", "dg115", "dg116", "dg117", "dg118", "dg119", "dg120"],
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
        "subcs_name": "nlsync_config_1",
        "subcl_name": "nlsync_config_1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "local",
        "subcs_name": "nlsync_local_1",
        "subcl_name": "nlsync_local_1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "history",
        "subcs_name": "nlsync_history_1",
        "subcl_name": "nlsync_history_1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "sparkloc",
        "subcs_name": "nlsync_sparkloc_1",
        "subcl_name": "nlsync_sparkloc_1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "sparkhis",
        "subcs_name": "nlsync_sparkhis_1",
        "subcl_name": "nlsync_sparkhis_1"
    },
    {
        "cs_name": "nlsync",
        "cl_name": "log",
        "subcs_name": "nlsync_log_1",
        "subcl_name": "nlsync_log_1"
    }
]
"""
Description: sync_tblconf_infos == the synchronize data tables information,

Date: 2017-02-14
"""
sync_tblconf_infos = (
# import store table
{
  "dt_delta": "-1",
  "iprt_cmd": "",
  "idx_field": "[[{'tx_date': 1}]]",
  "sync_file": "[{'sync_host': 'a-hqp-nlsdb03', 'sync_file': '/etldata/odsdata/%s/CBE/BPTFHIST.dat'}, {'get_mode': 'sftp', 'frm_ip': '21.6.1.13', 'frm_file': '/etldata/odsdata/%s/CBE/BPTFHIST.dat', 'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
  "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
  "sync_dom": "cbe_domain",
  "nopr_rplc": "['']",
  "sync_type": "append_his_update",
  "shard_key": "[{'vertical_key': {'ac_dt': 1}},{'horizontal_key': {'_id': 1}}]",
  "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
  "rely_tbl": "cbe.bptfhist",
  "tbl_name": "cbe.bptfhist_tst",
  "sync_new": "true",
  "sync_sys": "CBE"
},
{
  "dt_delta": "-1",
  "iprt_cmd": "",
  "idx_field": "[[{'tx_date': 1}]]",
  "sync_file": "[{'sync_host': 'a-hqp-nlsdb03', 'sync_file': '/etldata/odsdata/%s/CBE/BPCOFHSH.dat'}, {'get_mode': 'sftp', 'frm_ip': '21.6.1.13', 'frm_file': '/etldata/odsdata/%s/CBE/BPCOFHSH.dat', 'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
  "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
  "sync_dom": "cbe_domain",
  "nopr_rplc": "['']",
  "rely_tbl": "cbe.bpcofhsh",
  "sync_type": "append_his_insert",
  "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
  "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
  "tbl_name": "cbe.bpcofhsh_tst",
  "sync_new": "true",
  "sync_sys": "CBE",
  "expt_cmd": "{'delchar': '0x1d', 'delfield': '0x1b', 'fields': 'ac_dt,jrnno,jrn_seq,his_update_ind,tx_date,ac_br,ac_br_desc,ac_ci_name,ccy_type,cur_bal,data_flg,rel_ac,rel_ci_name,rel_ac_br,rel_ac_br_desc', 'file': '/etldata/odsdata/%s/CBE/BPCOFHSH_TST_EX.dat'}"
},
{
  "dt_delta": "-1",
  "iprt_cmd": "{'delchar': '29', 'delfield': '27', 'csname': 'cbe', 'clname': 'bptfhist_tst'}",
  "idx_field": "[]",
  "rely_tbl": "cbe.bptfhist_tst,cbe.bpcofhsh_tst",
  "sync_file": "[{'sync_host': 'a-hqp-nlsdb03', 'sync_file': '/etldata/odsdata/%s/CBE/BPCOFHSH_TST_EX.dat', 'encode_fmt': 'utf-8'}, {'get_mode': 'sftp', 'frm_ip': '21.6.1.13', 'frm_file': '/etldata/odsdata/%s/CBE/BPCOFHSH_TST_EX.dat', 'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
  "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
  "sync_dom": "cbe_domain",
  "nopr_rplc": "['']",
  "sync_type": "append_his_update",
  "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
  "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
  "tbl_name": "cbe.bpcofhsh_tst_ex",
  "sync_new": "true",
  "sync_sys": "CBE",
  "expt_cmd": ""
},




"""
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bpcofhsh_ex",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_his_update",
    "sync_file": "[{'sync_host': 'a-hqp-nlsdb03', 'sync_file': '/etldata1/odsdata/%s/CBE/BPCOFHSH_EX.dat', 'encode_fmt': 'utf-8'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.6.1.13', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPCOFHSH_EX.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "rely_tbl": "cbe.bptfhist,cbe.bpcofhsh",
    "shard_key": "[{'vertical_key': {'ci_no': 1}},{'horizontal_key': {'ci_no': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "sync_new": "true",
    "iprt_cmd": "{'delchar': '29', 'delfield': '27', 'csname': 'cbe', 'clname': 'bptfhist'}",
    "expt_cmd": "",
  
},

{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bpcofhsh",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_his_insert",
    "sync_file": "[{'sync_host': 'a-hqp-nlsdb03', 'sync_file': '/etldata1/odsdata/%s/CBE/BPCOFHSH.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.6.1.13', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPCOFHSH.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[]",
    "shard_key": "[{'vertical_key': {'_id': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "sync_new": "true",
    "iprt_cmd": "",
    "expt_cmd": "{'delchar': '0x1d', 'delfield': '0x1b', 'fields': 'ac_dt,jrnno,jrn_seq,his_update_ind,tx_date,ac_br,ac_br_desc,ac_ci_name,ccy_type,cur_bal,data_flg,rel_ac,rel_ci_name,rel_ac_br,rel_ac_br_desc', 'file': '/etldata1/odsdata/%s/CBE/BPCOFHSH_EX.dat'}",
},
{
    "sync_sys": "CBE",
    "tbl_name": "cbe.bptfhist",
    "sync_dom": "cbe_domain",
    "dt_delta": "-1",
    "main_dtp": "['%Y%m%d', '%Y-%m-%d']",
    "sync_type": "append_his_update",
    "sync_file": "[{'sync_host': 'a-hqp-nlsdb03', 'sync_file': '/etldata1/odsdata/%s/CBE/BPTFHIST.dat'}, " +
                 "{'get_mode': 'sftp', 'frm_ip': '21.6.1.13', " +
                 "'frm_file': '/etldata1/odsdata/%s/CBE/BPTFHIST.dat', " +
                 "'frm_user': 'sdbadmin', 'frm_passwd': ''}]",
    "idx_field": "[[{'ac': 1}], [{'ac': 1}, {'ac_dt': 1}], [{'ac': 1}, {'ac_dt': 1}, {'print_flg': 1}], [{'ac_dt': 1}, {'ac': 1}], [{'ac_dt': 1}, {'jrnno': 1}, {'jrn_seq': 1}], [{'jrnno': 1}, {'ac_dt': 1}, {'print_flg': 1}], [{'tx_date': 1}], [{'tx_tlr': 1}, {'tx_br': 1}, {'ac_dt': 1}], [{'tx_tlr': 1}, {'tx_br': 1}, {'ac_dt': 1}, {'print_flg': 1}], [{'tx_tool': 1}, {'ac_dt': 1}, {'print_flg': 1}]]",
    "shard_key": "[{'vertical_key': {'ac_dt': 1}},{'horizontal_key': {'_id': 1}}]",
    "rp_sd_key": "[{'vertical_key': {'tx_date': 1}},{'horizontal_key': {'_id': 1}}]",
    "nopr_rplc": "['']",
    "sync_new": "true",
    "iprt_cmd": "",
},
"""


               )
