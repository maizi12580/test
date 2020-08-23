#!/usr/bin/python
#coding=utf-8

import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)
from bson.objectid import ObjectId
from bson.min_key import MinKey
from bson.max_key import MaxKey
import logging
import sys
import os
import logging.handlers
import datetime
#from time import time, ctime
import time
from datetime import timedelta
import collections
from sync_sdb import *
import socket
from pexpect import pxssh
import getpass
import pexpect
import commands
from sync_logger import *
from config.global_config import *
import re


def start_remote_syncSAMPLE(host, user, password, command, timeout=18000):
    login_new = 'Are you sure you want to continue connecting'
    cmd_run = pexpect.spawn('ssh -l %s %s %s' % (user, host, command), timeout=timeout)
    ret = cmd_run.expect([pexpect.TIMEOUT, login_new, 'password: '])
    # if login time out, exit
    if 0 == ret:
        print 'ERROR RUN COMMAIND: %s' % command
        print cmd_run.before, cmd_run.after
        return None
    if 1 == ret:
        cmd_run.sendline('yes')
        cmd_run.expect('password: ')
        ret = cmd_run.expect([pexpect.TIMEOUT, 'password: '])
        if 0 == ret:
            print 'LOGIN ERROR RUN COMMAIND: %s' % command
            print cmd_run.before, cmd_run.after
            return None

    cmd_run.sendline(password)
    return cmd_run

def start_remote_sync(host, user, password, command):
    try:
        s = pxssh.pxssh(timeout=21600)
        s.login(host, user, password, original_prompt='[$#>]')
        a = s.sendline(command)
        s.prompt()
        ret = s.before
        s.logout()

        return ret
        #
    except pxssh.ExceptionPxssh, e:
        print 'pxssh failed on login.'
        print str(e)
        #


def main():
    host = 'T-DSJ-HISDB03'
    user = 'sdbadmin'
    password = 'kfptSDB2016!'
    #command = 'nohup sh /home/sequoiadb/synchronous_data/nlsync_data/tmp/test.sh &'
    command = 'ps -elf | grep "16767"'
    print 'BEGIN: %s' % datetime.datetime.now()
    cmd_run = start_remote_sync(host, user, password, command)
    process = cmd_run.split("\r\n")
    print 'Command Run: %s|%s' % (cmd_run,process[1])
    #print cmd_run.before
    print 'END: %s' % datetime.datetime.now()

if __name__ == '__main__':
    main()
