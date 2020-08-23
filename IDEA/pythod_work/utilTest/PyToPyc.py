#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""文件编译"""



def One_py_compile():
    import py_compile
    import  compileall
    #单文件编译
    py_compile.compile('.py','.pyc')
    #多文件编译
    compileall.compile_dir(r'D:\workspace\pythod_work\utilTest')

"""命令行解析"""
def optParseTest():
    from optparse import OptionParser

    usage = "我就是一个说明文档，可以不带进去"
    parser = OptionParser(usage) #可以带usage显示帮助信息的参数
    parser.add_option("-q","--quit",
                      dest="verbose",
                      default=True )
    parser.add_option("-u","--username",    #长短参数名
                      dest='username',          #
                      default='maizi',          #默认参数内容
                      help='username of ...')
    parser.add_option("-p","--password",
                      dest="password",      #调用参数的ID名
                      default="12580")
    parser.add_option("-f","--filename",
                      dest="filename",      #等等用于解析excel
                      default="[..].xlsx")
    args = ["-p","maizi12580"]
    (options, args)=parser.parse_args(args)
    print options.password
    parser.set_default(password="12580")    #设置属性默认值
    filename = options.filename

    """获取当前路径下的excel并解析"""
    import xlrd         #用于excel的读写，需要另外安装
    import xlwt
    import os
    file = os.path.abspath('./%s' % filename)
    data = xlrd.open_workbook(file)         #打开excel文件
    table = data.sheet_by_name(u'Sheet1')   #默认第一个表
    nrows = table.nrows                     #按行分配
    ncols = table.ncols                     #按列分配
    fields = table.row_values(1)[:10]       #获取表的前十行
    records = range(0,10,3)                 #range(start, stop[, step]),[0,3,6,9]

def process_mutex_check():
    import socket
    import datetime
    local_host = socket.gethostname();
    main_process_name = 'SYNC|%s|MAIN|%s' % (socket.gethostname(),datetime.datetime.now().strftime("%Y%m%d"))
    print main_process_name
    command = "ps -ef | grep '%s' | awk '{print $8}' | grep -v 'grep'" % main_process_name
    print command
if __name__ == '__main__':
    is_main_running = process_mutex_check()