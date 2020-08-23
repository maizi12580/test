#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author: Vector
Desc: 根据生成的元数据信息管理表结构生成Spark SQL/Postgre SQL/SDB Import Cmd
      ----------------------------------------------------------
      SDB/Spark/PG 三者之间对应关系:
      SDB:   collectionsapce.collection,   如: gjf.cbshispf
      Spark: database.table,               如: gjf.cbshispf
      PG:    tablePref_tableEnd,           如: gjf_cbshispf
      ----------------------------------------------------------
      1. 创建Spark SQL 表:
      create table tep.template(
      field1   string,
      field2   string,
      field3   string,
      field4   int,
      field5   bigint,
      field6   double,
      ...
      )USING com.sequoiadb.spark OPTIONS (host 'TST-SDB01:11810, TST_SDB02:11810, TST-SDB03:11810',
      collectionspace 'tep', collection 'template', username 'sdbadmin', password 'sdbadmin');

      2. 创建Postgre SQL 表:
      create foreign table cbe_template(
      field1   text,
      field2   text,
      field3   text,
      field4   integer,
      field5   numeric,
      field6   double precision,
      ...
      )server sdb_server options (collectionspace 'tep', collection 'template');

      3. 创建SDB Import Command Line:
      /opt/sequoiadb/bin/sdbimprt -s 'localhost' -p '11810' -u '' -w '' -c 'cts' -l 'template2' --file 'import_file' --type 'csv' -a '\29' -e '\27' --fields '
      field1                        int,
      field2                        double,
      field3                        double,
      field4                        int,
      field5                        double,
      ...
      ' -n '1000' -j '16' --trim 'both'
"""

from optparse import OptionParser
from meta_sdb_op import *
import time
import os

class CmdSQL:
    def __init__(self, connectOptions):
        self.hostname = connectOptions["hostname"]
        self.svcport = connectOptions["svcport"]
        self.username = connectOptions["username"]
        self.password = connectOptions["password"]
        self.meta_csname = connectOptions["meta_cs"]
        self.meta_clname = connectOptions["meta_cl"]
        self.spark_pkg = connectOptions["spark_pkg"]
        self.pg_srv = connectOptions["pg_srv"]

    """
    DESC: 创建Spark表
    必须指定的参数变量:
    模式一:
    create table <Spark 表名>(
    <字段名   字段类型,>
    )USING <包名-default=com.sequoiadb.spark> OPTIONS(host "<主机名:端口>", collectionspace "<SDB集合空间>",
    collection "<SDB集合>", username "<SDB用户名>", password "<SDB用户密码>");
    模式二:
    create table <Spark 表名>(
    <字段名   字段类型,>
    <)USING 包名 OPTIONS(host "主机名:端口", collectionspace "SDB集合空间",
    collection "SDB集合", username "SDB用户名", password "SDB用户密码")>;
    """
    def gen_spark_sql(self, meta_info):
        spark_head = 'create table %s(\n'
        spark_body = '%s'
        spark_end = ')USING %s OPTIONS (host "%s", ' + \
                    'collectionspace "%s", collection "%s", username "%s", password "%s"); \n'
        spark_end1 = '%s'

        msdb = MSdb(self.hostname, self.svcport, self.username, self.password)
        hosts = msdb.get_hosts()
        # 如果创建的SPARK表名存在-和_时,需要将其替换掉
        full_name = meta_info["tbl_name"]
        cs_name = full_name.split(".")[0]
        cl_name = full_name.split(".")[1]

        fields = ""
        # 因为一个表的字段定义最长为1000个, field001~field999
        for i in range(1000):
            if 0 == i:
                continue
            # 格式化字段
            field_num ='{0:0>3}'.format(str(i))
            if False == meta_info.has_key("field"+field_num):
                print "last field: field" + field_num
                break
            field_info = meta_info["field" + field_num].split("|")
            field_name = field_info[0]
            field_name_fm = '{0:<30}'.format(field_name)
            field_type = field_info[1]
            if 'long' == field_type:
                field_type = 'bigint'
            if i != len(meta_info.keys()) - 1:
                fields = fields + "%s%s,\n" % (field_name_fm, field_type)
            else:
                fields = fields + "%s%s\n" % (field_name_fm, field_type)

        #print "fields: %s" % fields
        crt_tbl = (spark_head % full_name) + \
                  (spark_body % fields) + \
                  (spark_end % (self.spark_pkg, hosts, cs_name, cl_name,
                                self.username, self.password))

        return crt_tbl

    """
    DESC: 创建PostgreSQL表
    必须指定的参数变量:
    模式一:
    create foreign table <PG 表名>(
    <字段名   字段类型,>
    )server <服务名-default=sdb_server> options(collectionspace "<SDB集合空间>",
    collection "<SDB集合>", username "<SDB用户名>", password "<SDB用户密码>");
    模式二:
    create table <Spark 表名>(
    <字段名   字段类型,>
    <)USING 包名 OPTIONS(host "主机名:端口", collectionspace "SDB集合空间",
    collection "SDB集合", username "SDB用户名", password "SDB用户密码")>;
    """
    def gen_pg_sql(self, meta_info):
        spark_head = 'create foreign table %s(\n'
        spark_body = '%s'
        spark_end = ') server %s options(collectionspace "%s", collection "%s", ' + \
                    'username "%s", password "%s"); \n'
        spark_end1 = '%s'

        # 如果创建的SPARK表名存在-和_时,需要将其替换掉
        full_name = meta_info["tbl_name"]
        cs_name = full_name.split(".")[0]
        cl_name = full_name.split(".")[1]

        fields = ""
        # 因为一个表的字段定义最长为1000个, field001~field999
        for i in range(1000):
            if 0 == i:
                continue
            # 格式化字段
            field_num ='{0:0>3}'.format(str(i))
            if False == meta_info.has_key("field"+field_num):
                print "last field: field" + field_num
                break
            field_info = meta_info["field" + field_num].split("|")
            field_name = field_info[0]
            field_name_fm = '{0:<30}'.format(field_name)
            field_type = field_info[1]
            if 'int' == field_type:
                field_type = 'integer'
            if 'long' == field_type:
                field_type = 'bigint'
            if 'string' == field_type:
                field_type = 'text'
            if 'double' == field_type:
                field_type = 'double precision'

            if fields != "":
                fields = fields + "%s%s,\n" % (field_name_fm, field_type)
            else:
                fields = fields + "%s%s\n" % (field_name_fm, field_type)

        #print "fields: %s" % fields
        crt_tbl = (spark_head % full_name) + \
                  (spark_body % fields) + \
                  (spark_end % (self.pg_srv, cs_name, cl_name, self.username, self.password))

        return crt_tbl

    """
    DESC: 生成SDB Import Command 命令行
          1. 主机名(-s)、服务端口(-p)、用户名(-u)、密码(-p)、
             集合空间(-c)、集合(-l)、字段名(--fields)作为参数变量传递使用;
          2. 除1外的其余变量均采取固定变量值, 与需要不符的, 需要在文本中进行批量替换即可
          --file     根据集合空间+日期.sql, 如cbe.20161121.sql<表示新核心系统的数据>
          --type     默认: csv
          -a         默认: \\29, 不可见字符"^]"
          -e         默认: \\27, 不可见字符"^["
          -n         默认: 1000
          -j         默认: 16
          --trim     默认: both
    """
    def gen_import_cmd(self, meta_info):
        impr_type = 'csv'
        impr_strdel = '\\29'
        impr_flddel = '\\27'
        impr_insnum = '1000'
        impr_jobs = '16'
        impr_trim = 'both'
        import_cmd = '/opt/sequoiadb/bin/sdbimprt -s \'%s\' -p \'%s\' -u \'%s\' -w \'%s\' ' + \
                     '-c \'%s\' -l \'%s\' --file \'%s\' --type \'%s\' -a \'%s\' -e \'%s\' ' + \
                     '--fields \'\n%s\' -n \'%s\' -j \'%s\' --trim \'%s\''

        # 如果创建的SPARK表名存在-和_时,需要将其替换掉
        full_name = meta_info["tbl_name"]
        cs_name = full_name.split(".")[0]
        cl_name = full_name.split(".")[1]

        impr_tm = time.strftime("%Y%m%d%H%M%S")
        impr_file = cs_name + '.' + impr_tm + '.csv'

        fields = ""
        for i in range(1000):
            if 0 == i:
                continue
            # 格式化字段
            field_num ='{0:0>3}'.format(str(i))
            if False == meta_info.has_key("field"+field_num):
                print "last field: field" + field_num
                break
            field_info = meta_info["field" + field_num].split("|")
            field_name = field_info[0]
            field_name_fm = '{0:<30}'.format(field_name)
            field_type = field_info[1]
            if "string" == field_type:
                field_type = 'string default ""'

            if fields != "":
                fields = fields + "%s%s,\n" % (field_name_fm, field_type)
            else:
                fields = fields + "%s%s\n" % (field_name_fm, field_type)

        crt_tbl = import_cmd % (self.hostname, self.svcport, self.username,
                                self.password, cs_name, cl_name,
                                impr_file, impr_type, impr_strdel, impr_flddel,
                                fields, impr_insnum, impr_jobs, impr_trim)

        return crt_tbl

def main():
    spark_sql_file = os.path.abspath('./autogen_cmd_line/spark_create_table.sql')
    pg_sql_file = os.path.abspath('./autogen_cmd_line/pg_create_table.sql')
    import_cmd_file = os.path.abspath('./autogen_cmd_line/import_cmd_script.sql')
    parser = OptionParser()
    parser.add_option("-i", "--hostname",
                      dest='hostname',
                      default='localhost',
                      help='host name or IP address, default: localhost')
    parser.add_option("-s", "--svcport",
                      dest='svcport',
                      default='11810',
                      help='service name, default: 11810')
    parser.add_option("-u", "--user",
                      dest='username',
                      default='',
                      help='user name of sdb')
    parser.add_option("-w", "--password",
                      dest='password',
                      default='',
                      help='password of sdb')
    parser.add_option("", "--meta_cs",
                      dest='meta_cs',
                      default='mdm',
                      help='meta data struct cs name')
    parser.add_option("", "--meta_cl",
                      dest='meta_cl',
                      default='metatbl',
                      help='meta data struct cl name')
    parser.add_option("", "--spark_pkg",
                      dest='spark_pkg',
                      default='com.sequoiadb.spark',
                      help='spark package for spark connector')
    parser.add_option("", "--pg_srv",
                      dest='pg_srv',
                      default='sdb_server',
                      help='postgreSQL server for postgreSQL connector')

    (options, args) = parser.parse_args()
    # get options
    hostname = options.hostname
    svcport = options.svcport
    username = options.username
    password = options.password
    meta_cs = options.meta_cs
    meta_cl = options.meta_cl
    spark_pkg = options.spark_pkg
    pg_srv = options.pg_srv
    connectOptions = {'hostname': hostname, 'svcport': svcport, 'username': username, 'password': password,
                      'meta_cs': meta_cs, 'meta_cl': meta_cl,
                      'spark_pkg': spark_pkg, 'pg_srv': pg_srv}

    # 生成 Spark Table 创建语句
    spark_handle = open(spark_sql_file, 'w+')
    pg_handle = open(pg_sql_file, 'w+')
    import_cmd_handle = open(import_cmd_file, 'w+')

    # 查询
    sdb = MSdb(hostname, svcport, username, password)
    gensql = CmdSQL(connectOptions)
    meta_infos = sdb.query(meta_cs, meta_cl, {})
    for i in range(len(meta_infos)):
        sparkcrt = gensql.gen_spark_sql(meta_infos[i])
        pgcrt = gensql.gen_pg_sql(meta_infos[i])
        importcmdcrt = gensql.gen_import_cmd(meta_infos[i])
        spark_handle.write(sparkcrt)
        pg_handle.write(pgcrt)
        import_cmd_handle.write(importcmdcrt)
        spark_handle.write('\n\n')
        pg_handle.write('\n\n')
        import_cmd_handle.write('\n\n')

    # 关闭 spark 文件句柄
    spark_handle.close()
    pg_handle.close()
    import_cmd_handle.close()

if __name__ == "__main__":
    main()
