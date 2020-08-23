#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author: Vector
Desc: 将业务系统各表的数据结构以记录的形式存入SequoiaDB中。脚本分两步进行：
      一、解析 XXX.XLSX 或者XXX.CSV 文件(暂不支持), 转成记录；
      二、将解析转存的记录插入 SDB 中;
DifPro:
       1.csv格式文件解析, 中文字符解析失败；
       2.WIN 10 command line(CMD)上执行此脚本, 中文PRINT乱码
"""

import os
import xlrd
import csv
from optparse import OptionParser
from meta_sdb_op import *



class ParseFile:
    def __init__(self, parse_file):
        self.parse_file = parse_file

    def parse_excel(self):
        data = xlrd.open_workbook(self.parse_file)
        table = data.sheet_by_name(u'Sheet1')
        nrows = table.nrows
        ncols = table.ncols
        fields_name = table.row_values(0)

        # 生成记录，存于字典
        records = range(nrows-1)             #第一行不算写入的记录
        for i in range(nrows):
            if 0 == i:
                continue
            colnames = table.row_values(i)
            record = '{'
            for j in range(len(colnames)):
                if isinstance(fields_name[j], float):
                    field_name = str(int(fields_name[j]))
                else:
                    field_name = fields_name[j].strip()
                if isinstance(colnames[j], float):
                    field_value = str(int(colnames[j]))
                else:
                    field_value = colnames[j].strip().replace("'", "")

                if 0 == j:
                    record = record + '\'%s\':\'%s\'' % (field_name, field_value)
                else:
                    record = record + ', \'%s\':\'%s\'' % (field_name, field_value)

            record = record + '}'
            records[i-1] = record
        return records

    """
    遗留问题:
       解析CSV格式文件时，解析中文字符存在问题，平台WIN10
    """
    def parse_csv(self):
        csv_read = csv.reader(open(self.parse_file, 'rb'))
        cnt = 0
        records = []
        for line in csv_read:
            #print line
            if 0 == cnt:
                fields_name = line
                cnt = cnt + 1
                continue

            record = '{'
            for j in range(len(line)):
                if isinstance(fields_name[j], float):
                    field_name = str(int(fields_name[j]))
                else:
                    field_name = fields_name[j].strip()
                if isinstance(line[j], float):
                    field_value = str(int(line[j]))
                else:
                    field_value = line[j].strip()
                if 0 == j:
                    record = record + '\'%s\':\'%s\'' % (field_name, field_value)
                else:
                    record = record + ', \'%s\':\'%s\'' % (field_name, field_value)

            record = record + '}'
            records.append(record)
            #print record
            cnt = cnt + 1


        return records





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
    parser.add_option("-c", "--csname",
                      dest='csname',
                      default='mdm',
                      help='collection space name')
    parser.add_option("-l", "--clname",
                      dest='clname',
                      default='tbpool',
                      help='collection name')
    parser.add_option("-f", "--file",
                      dest='filename',
                      default='[Template]Data_Structure.xlsx',
                      help='write report to FILE',
                      metavar="FILE")

    (options, args) = parser.parse_args()
    # get options
    hostname = options.hostname
    svcport = options.svcname
    username = options.username
    password = options.password
    csname = options.csname
    clname = options.clname
    filename = options.filename

    sdb = MSdb(hostname, svcport, username, password)
    fileformat = filename.split('.')[-1]

    abs_filename = os.path.abspath('./source_excel/%s' % filename)
    # 解析文件
    if 'xlsx' == fileformat:
        parse = ParseFile(abs_filename)
        records_list = parse.parse_excel()
    else:
        print "请输入格式为 XXXX.xlsx 的EXCEL文件"
        return

    # 插入记录
    for i in range(len(records_list)):
        try:
            sdb.insert(csname, clname, eval(records_list[i]))
        except (SDBTypeError, SDBBaseError), e:
            pysequoiadb._print(e)

if __name__ == "__main__":
    main()

