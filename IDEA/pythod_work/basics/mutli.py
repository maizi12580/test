#!/usr/bin/python
# -*- coding: UTF-8 -*-
#@author maizi
#2019/10/15

#乘法口诀
for i in range(1, 10):
    print
    for j in range(1, i+1):
        print "%d*%d=%d" % (i, j, i*j),

#素数
h,leap = 0,1
from math import sqrt
from sys import stdout
for m in range(101,201):
    k = int(sqrt(m + 1))
    for i in range(2,k+1):
        if m%i==0:
            leap = 0
            break
    if leap == 1:
        print '%-4d' % m
        h +=1
        if h%10 == 0:
            print ''
    leap = 1
print 'The total is %d' % h

#因式分解
def reduceNum(n):
    print '{} = '.format(n),
    if not isinstance(n, int) or n<=0:
        print '请输入一个数字'
        exit(0)
    elif n in [1]:
        print '{}'.format(n)
    while n not in [1]:
        for index in xrange(2, n + 1) :
            if n % index == 0:
                n /= index # n 等于 n/index
                if n == 1:
                    print index
                else : # index 一定是素数
                    print '{} *'.format(index),
                break
reduceNum(90)

#将数组逆序输出
if __name__ == '__main__':
    a = [9,6,5,4,1]
    N = len(a)
    print a
    for i in range(len(a)/2):
        a[i],a[N-i-1] = a[N-i-1],a[i]
    print a

#矩形相加
x = [[12,7,5],[4,5,6]]
y = [[2,34,5],[4,2,3]]
result = [[0, 0, 0],[0, 0, 0]]
for i in range(len(x)):
    for j in range(len(x[0])):
        result[i][j] = x[i][j] + y[i][j]
print result

#加密
from sys import stdout
if __name__ == '__main__':
    a = int(raw_input('输入四个数字:\n'))
    aa = []
    aa.append(a % 10)
    aa.append(a % 100 / 10)
    aa.append(a % 1000 / 100)
    aa.append(a / 1000)
    for i in range(4):
        aa[i] += 5
        aa[i] %= 10
    for i in range(2):
        aa[i],aa[3 - i] = aa[3 - i],aa[i]
    for i in range(3,-1,-1):
        stdout.write(str(aa[i]))