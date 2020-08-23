#!/usr/bin/python
# -*- coding: UTF-8 -*-
# @author maizi
# 2019/10/14

import threading
import time

list = [0,0,0,0,0,0,0]
class myThread(threading.Thread):
    def __init__(self, threadId, name, counter):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name
        self.counter = counter
    def run(self):
        print "Starting:" + self.name
        #使用锁，timeout不填会一直阻塞到获得锁定
        threadLock.acquire()
        print_time(self.name, self.counter, list.__len__())
       #释放锁
        threadLock.release()
    def __del__(self):
        print self.name,"线程结束"

def print_time(threadName, delay, counter):
    while counter:
        time.sleep(delay)
        list[counter-1] += 1
        print "[%s] %s 修改第 %d 个值，修改后值为:%d" % (time.ctime(time.time()),threadName,counter,list[counter-1])
        counter -= 1
#给线程增加一个锁print "exit"
threadLock = threading.Lock()
threads = []
# 创建新线程
thread1 = myThread(1, "Thread-1", 1)
thread2 = myThread(2, "Thread-2", 2)
thread1.start()
thread2.start()
#添加进入进程组
threads.append(thread1)
threads.append(thread2)
#等待所有进程完成
for t in threads:
    t.join()
print "exit"
