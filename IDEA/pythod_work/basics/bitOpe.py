#!/usr/bin/python
# -*- coding: UTF-8 -*-
#@author maizi
#2019/10/15

#按位异或^:0|0=0; 0|1=1; 1|0=1; 1|1=1
if __name__ == '__main__':
    a = 077
    b = a ^ 3
    print 'The a ^ 3 = %d' % b
    b ^= 7
    print 'The a ^ b = %d' % b

#学习使用按位或 | :0^0=0; 0^1=1; 1^0=1; 1^1=0
if __name__ == '__main__':
    a = 077
    b = a | 3
    print 'a | b is %d' % b
    b |= 7
    print 'a | b is %d' % b
#取一个整数a从右端开始的4〜7位
if __name__ == '__main__':
    a = int(raw_input('input a number:\n'))
    b = a >> 4
    c = ~(~0 << 4)
    d = b & c
    print '%o\t%o' %(a,d)