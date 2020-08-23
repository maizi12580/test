#!/usr/bin/python
#coding=utf-8
#@author maizi
#2019/10/11
import sys
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
#自由輸入
str = raw_input("请开始输入:")
print "你输入的内容是:" + str
#open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None)
fo = open("D:\\1.txt", "ab+")
fo.write(str)
# 把指针再次重新定位到文件开头
position = fo.seek(0, 0)
while True:
    line = fo.readline()
    if len(line) == 0:
        break
    print (line)
fo.close()

print "文件名: ", fo.name
print "是否已关闭 : ", fo.closed
print "访问模式 : ", fo.mode
print "末尾是否强制加空格 : ", fo.softspace
fo.close()