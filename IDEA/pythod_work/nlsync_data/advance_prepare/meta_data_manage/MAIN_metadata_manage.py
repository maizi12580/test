#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author: Vector
Desc: 生产源表数据结构导入
"""

from optparse import OptionParser
from meta_sdb_op import *
from meta_cmd_sql import *
from meta_data_format import *
from meta_excel_import import *

"""
元数据据管理表结构和输出的定义
"""
# 1. 定义存储集合空间的集合空间和集合(mdm: Meta Data Manage)
# {tbpool: Table Pool[Import From Excel]; metatbl: Meta Table[Format From Table Pool]}
global tpl_csname
tpl_csname = 'mdm'
global tpl_clname
tpl_clname = 'tbpool'
global mt_csname
mt_csname = 'mdm'
global mt_clname
mt_clname = 'metatbl'
# 2. 定义输出的Spark SQL、Postgre SQL、SDB Import Command这三类文件的输出
spark_sql_file = os.path.abspath('./autogen_cmd_line/spark_create_table.sql')
pg_sql_file = os.path.abspath('./autogen_cmd_line/pg_create_table.sql')
import_cmd_file = os.path.abspath('./autogen_cmd_line/import_cmd_script.sql')


def main():
    """
    参数选项说明:
    hostname 主机名/IP：连接SDB的地址
    svcname  服务端口： 连接SDB的协调端口
    username 用户名：  连接SDB的用户名
    password 密码：    连接SDB的密码
    filename 文件名：  存储元数据结构的EXCEL文件[唯一输入]
    """
    parser = OptionParser()
    parser.add_option("", "--hostname",
                      dest='hostname',
                      default='localhost',
                      help='host name or IP address, default: localhost')
    parser.add_option("", "--svcname",
                      dest='svcname',
                      default='11810',
                      help='service name, default: 11810')
    parser.add_option("", "--user",
                      dest='username',
                      default='nlsdbapp',
                      help='user name of sdb, default: ""')
    parser.add_option("", "--password",
                      dest='password',
                      default='kfptNLSDB2017',
                      help='password of sdb, default: ""')
    parser.add_option("", "--filename",
                      dest='filename',
                      default='[Template]Data_Structure.xlsx',
                      help='source data table structure in EXCEL, default: [Template]Data_Structure.xlsx')
    parser.add_option("", "--meta_cs",
                      dest='meta_cs',
                      default='mdm',
                      help='meta data struct cs name, default: mdm')
    parser.add_option("", "--meta_cl",
                      dest='meta_cl',
                      default='metatbl',
                      help='meta data struct cl name, default: meta_cl')
    parser.add_option("", "--spark_pkg",
                      dest='spark_pkg',
                      default='com.sequoiadb.spark',
                      help='spark package for spark connector, default: com.sequoiadb.spark')
    parser.add_option("", "--pg_srv",
                      dest='pg_srv',
                      default='sdb_server',
                      help='PostgreSQL server for postgreSQL connector, default: sdb_server')
    """
    parser.add_option("", "--pgaddr",
                      dest='pgaddr',
                      default='/opt/sequoiasql/bin/psql -p 5432 foo -c ',
                      help='PostgreSQL Connect Address')
    parser.add_option("", "--sparkaddr",
                      dest='sparkaddr',
                      default='./bin/spark-sql -e',
                      help='SparkSQL Connect Address')
    """


    (options, args) = parser.parse_args()
    # get input options
    hostname = options.hostname
    svcport = options.svcname
    username = options.username
    password = options.password
    filename = options.filename
    meta_cs = options.meta_cs
    meta_cl = options.meta_cl
    spark_pkg = options.spark_pkg
    pg_srv = options.pg_srv

    connectOptions = {'hostname': hostname, 'svcport': svcport, 'username': username, 'password': password,
                      'meta_cs': meta_cs, 'meta_cl': meta_cl,
                      'spark_pkg': spark_pkg, 'pg_srv': pg_srv}

    sdb = MSdb(hostname, svcport, username, password)

    """
    1. 将EXCEL文件进行解析处理,然后存入mdm.tbpool
    """
    # 解析文件
    file = os.path.abspath('./source_excel/%s' % filename)
    parse = ParseFile(file)
    records_list = parse.parse_excel()

    # 插入记录
    for i in range(len(records_list)):
        try:
            sdb.insert(tpl_csname, tpl_clname, eval(records_list[i]))
        except (SDBTypeError, SDBBaseError), e:
            pysequoiadb._print(e)


    """
    2. 将从EXCEL文件中解析处理得到的记录存入库表mdm.metatbl(mdm: Meta Data Manage; meta table)
    """
    # 用聚集做字段分组处理所需要的条件
    agg_arg = []
    agg_arg1 = {"$group": {"_id": {"field1": u"$系统英文名", "field2": u"$系统中文名",
                                   "field3": u"$表英文名", "field4": u"$表中文名",
                                   "field5": u"$SDB存储表",}}}
    agg_arg2 = {"$project": {u"系统英文名": 1, u"系统中文名": 1, u"表英文名": 1,
                             u"表中文名": 1, u"SDB存储表": 1}}
    agg_arg.append(agg_arg1)
    agg_arg.append(agg_arg2)

    # 聚集
    sdb = MSdb(hostname, svcport, username, password)
    meta = MetaStore()
    agg_records = sdb.aggregate(tpl_csname, tpl_clname, agg_arg)
    for query in agg_records:
        # 查询表所有的记录
        fields_list = sdb.query(tpl_csname, tpl_clname, query)
        # 格式化元数据表
        meta_record = meta.format(fields_list)
        # 元数据插入
        #sdb.insert(mt_csname, mt_clname, meta_record)
        sdb.upsert(mt_csname, mt_clname, meta_record)


    """
    3. 自动生成SparkSQL/PostgreSQL和SDB Import Cmd命令等文件
    """
    # 生成 Spark Table 创建语句
    """
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
    """

if __name__ == '__main__':
    main()
