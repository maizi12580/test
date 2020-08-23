#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author: Vector
Desc: 将导入SDB的原表信息进行整理(每表多个字段一条记录), 将多条记录根据表存储为一条管理信息。
      最后的SDB元数据信息管理表结构为:
      {
         "sysnm_ch": "测试系统",
         "sysnm_en": "CTS2",
         "tblnm_en": "TEMPLATE1",
         "tblnm_ch": "模板测试表1",
         "tbl_name": "cts2.template1",
         "prim_key": "field1,field2,field3",
         "field001": "field1|int|'字段1'",
         "field002": "field2|long|'字段2'",
         "field003": "field3|string|'字段3'",
         "field004": "field4|int|'字段4'",
         "field005": "field5|int|'字段5'",
         "field999": "field999|int|'字段999'",
         "timestamp": "2016-11-30 10:19:35,335"
      }
      其中表的每个字段均放成一个字段的原因是为了三表(SDB-SPARK-SSQL)统一,这样处理之后可以在SPARK或者SSQL上进行查询:
      SPARL SQL 格式化查询为:
         create database mdm;
         drop table mdm.metatbl;
         create table mdm.metatbl(
         sysnm_ch    string,
         sysnm_en    string,
         tblnm_en    string,
         tblnm_ch    string,
         tbl_name    string,
         prim_key    string,
         field001    string,
         field002    string,
         field003    string,
         field004    string,
         field005    string,
         field999    string,
         timestamp   string
         )USING com.sequoiadb.spark OPTIONS(host 'CentOS65H1:11810,CentOS65H2:11810,CentOS65H3:11810', collectionspace 'mdm', collection 'metatbl', username '', password '');

         select
         explode(split(concat_ws(',', field001, field002, field003, field004, field005, field999), ',')) from mdm.metatbl;
         0: jdbc:hive2://localhost:10000> select
         0: jdbc:hive2://localhost:10000> explode(split(concat_ws(',', field001, field002, field003, field004, field005, field999), ',')) from mdm.metatbl;
         +-----------------------+--+
         |          col          |
         +-----------------------+--+
         |   |
         | field2|long|'字段2'     |
         | field3|string|'字段3'   |
         | field4|int|'字段4'      |
         | field5|int|'字段5'      |
         | field999|int|'字段999'  |
         +-----------------------+--+
         6 rows selected (1.194 seconds)
"""



from optparse import OptionParser
import time
from meta_sdb_op import *


class MetaStore:

    def format(self, tbl_fields):
        tbl_format = {
            "sysnm_en": "TEP",
            "sysnm_ch": "模板库系统",
            "tblnm_en": "TEMPLATE",
            "tblnm_ch": "模板信息表",
            "tbl_name": "tep.template",
            "prim_key": "",
            "timestamp": "2016-09-08 01:01:01.534"
        }

        primary_keys = ""
        #print "length: %s" % len(tbl_fields)
        fields = []
        tbl_fields.sort()
        for field in tbl_fields:
            field_obj = field
            num = int(field_obj[u"字段序号"])
            print "number: %s -- %s " % (num, field_obj[u"SDB存储表"])
            print "number: %s -- %s " % (num, type(field_obj[u"字段序号"]))
            if 1 == num:
                tbl_format["sysnm_en"] = field_obj[u"系统英文名"]
                tbl_format["sysnm_ch"] = field_obj[u"系统中文名"]
                tbl_format["tblnm_en"] = field_obj[u"表英文名"]
                tbl_format["tblnm_ch"] = field_obj[u"表中文名"]
                tbl_format["tbl_name"] = field_obj[u"SDB存储表"]

            field_num ='{0:0>3}'.format(field_obj[u"字段序号"])
            field_str = '%s|%s|%s' % (field_obj[u"字段名称"], field_obj[u"字段类型"], field_obj[u"中文注释"])
            tbl_format["field" + field_num] = field_str

            if "Y" == field_obj[u"主键"]:
                if "" == primary_keys:
                    primary_keys = field_obj[u"字段名称"]
                else:
                    primary_keys = primary_keys + "," + field_obj[u"字段名称"]

        #primary_keys.sort()
        tbl_format["prim_key"] = primary_keys
        #fields.sort(key=lambda k:(k.get("num", 1)))
        #tbl_format = fields
        tbl_format["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return tbl_format

def main():
    parser = OptionParser()
    parser.add_option("-i", "--hostname",
                      dest='hostname',
                      default='localhost',
                      help='host name or IP address, default: localhost')
    parser.add_option("-s", "--svcname",
                      dest='svcname',
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
    parser.add_option("", "--dstcsname",
                      dest='dstcsname',
                      default='mdm',
                      help='collection space name')
    parser.add_option("", "--dstclname",
                      dest='dstclname',
                      default='metatbl',
                      help='collection name')
    parser.add_option("", "--srcsname",
                      dest='srcsname',
                      default='mdm',
                      help='collection space name')
    parser.add_option("", "--srclname",
                      dest='srclname',
                      default='tbpool',
                      help='collection name')

    (options, args) = parser.parse_args()
    # get options
    hostname = options.hostname
    svcport = options.svcname
    username = options.username
    password = options.password
    dstcsname = options.dstcsname
    dstclname = options.dstclname
    srcsname = options.srcsname
    srclname = options.srclname

    # 用聚集做字段分组处理所需要的条件
    agg_arg = []
    agg_arg1 = {"$group": {"_id": {"field1": u"$系统英文名", "field2": u"$系统中文名", \
                                   "field3": u"$表英文名", "field4": u"$表中文名", \
                                   "field5": u"$SDB存储表",}}}
    agg_arg2 = {"$project": {u"系统英文名": 1, u"系统中文名": 1, u"表英文名": 1, \
                             u"表中文名": 1, u"SDB存储表": 1}}
    agg_arg.append(agg_arg1)
    agg_arg.append(agg_arg2)

    # 聚集
    sdb = MSdb(hostname, svcport, username, password)
    meta = MetaStore()
    agg_records = sdb.aggregate(srcsname, srclname, agg_arg)
    for query in agg_records:
        # 查询表所有的记录
        fields_list = sdb.query(srcsname, srclname, query)
        # 格式化元数据表
        meta_record = meta.format(fields_list)
        # 元数据插入
        sdb.insert(dstcsname, dstclname, meta_record)

if __name__ == "__main__":
    main()
