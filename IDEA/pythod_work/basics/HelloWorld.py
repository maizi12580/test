#!/usr/bin/python
# -*- coding: UTF-8 -*-

import random

print random.choice([1,2,3])
print dir(random)

x, y,z = 1,1.2,"jia"
s = "maizi"
s1 = {'name':'maizi','name1':'maizi1','name2':'maizi2'}
print x
print y
print s*2,s[1],s[-1]
print s1,s1.keys(),s1.values(),s1['name']
print (s in s1.values())

# 自定义函数
def more(str):
    print str
    return

# 调用函数
more("来吧")