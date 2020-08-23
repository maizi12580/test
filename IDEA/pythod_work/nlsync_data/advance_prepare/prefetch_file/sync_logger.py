#!/usr/bin/python
#coding=utf-8
from multiprocessing import Process
import logging
import codecs
import os

import base64
import datetime
from datetime import timedelta
from bson.objectid import ObjectId
from prefile_config import *

import pysequoiadb
from pysequoiadb import client
from pysequoiadb.error import (SDBTypeError,
                               SDBBaseError,
                               SDBError,
                               SDBEndOfCursor)



class SCFileHandler(logging.StreamHandler):
    """
    A handler class which writes formatted logging records to disk files.
    """
    def __init__(self, filename, dblog={}, connect=None, just_write_table=None,
                 mode='a', encoding=None, delay=0):
        """
        Open the specified file and use it as the stream for logging.
        """
        #keep the absolute path, otherwise derived classes which use this
        #may come a cropper when the current directory changes
        if codecs is None:
            encoding = None
        self.baseFilename = os.path.abspath(filename)
        self.mode = mode
        self.encoding = encoding
        self.delay = delay
        self.dblog = dblog
        #self.connect = connect
        self.just_write_table = just_write_table
        print "------------------> mode: %s,encoding: %s,delay: %s" %(mode,encoding,delay)
        if delay:
            #We don't open the stream, but we still need to call the
            #Handler constructor to set level, formatter, lock etc.
            logging.Handler.__init__(self)
            self.stream = None
        else:
            logging.StreamHandler.__init__(self, self._open())

        # SDB collection connection
        #self.connect = connect
        self.hostname = connect['HostName']
        self.serverport = connect['ServerPort']
        self.username = connect['UserName']
        self.password = connect['Password']
        self.cs_name = connect['CsName']
        self.cl_name = connect['ClName']

        self.db_hosts = hosts
        self.connect_hosts = []
        for db_host in self.db_hosts.split(','):
            host_info = db_host.split(':')
            connect_info = {'host': host_info[0], 'service': host_info[1]}
            self.connect_hosts.append(connect_info)

        # make sure the connect is correct
        self.db = client(self.hostname, self.serverport, self.username, base64.decodestring(self.password))
        if '' != self.db_hosts:
            self.db.connect_to_hosts(self.connect_hosts, user=self.username, password=base64.decodestring(self.password))
            
        # set read from primary
        attri_options = {'PreferedInstance': 'M'}
        self.db.set_session_attri(attri_options)


    def close(self):
        """
        Closes the stream.
        """
        self.acquire()
        try:
            if self.stream:
                self.flush()
                if hasattr(self.stream, "close"):
                    self.stream.close()
                self.stream = None
            # Issue #19523: call unconditionally to
            # prevent a handler leak when delay is set
            logging.StreamHandler.close(self)
        finally:
            self.release()

    def _open(self):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        if self.encoding is None:
            stream = open(self.baseFilename, self.mode)
        else:
            stream = codecs.open(self.baseFilename, self.mode, self.encoding)
        return stream

    def emit(self, record):
        """
        Emit a record.

        If the stream was not opened because 'delay' was specified in the
        constructor, open it before calling the superclass's emit.
        """
        if self.stream is None:
            self.stream = self._open()
        if self.just_write_table is None:
            logging.StreamHandler.emit(self, record)

        """
        Description: put the log info in sdb collection
        Parameters: connect   dict   {'HostName': '', 'ServerPort': '', 'UserName': '',
                                      'Password': '', 'CsName': '', 'ClName': ''}
                    dblog     dict   {'sync_sys': '', 'tbl_name': '', 'sync_dt': ''}
        """

        # put the record in sdb collection
        now_time = str(datetime.datetime.now())
        loginfo = {'log_desc': self.format(record), 'up_tm': now_time}
        # print 'SYNC: %s === %s' % (loginfo, self.dblog)
        self.dblog.update(loginfo)
        self.acquire()
        self.sync_insert(self.cs_name, self.cl_name, self.dblog)
        self.release()


    def sync_insert(self, cs_name, cl_name, record):
        try:
            object_id = {"_id": ObjectId()}
            record.update(object_id)
            cl = self.sync_get_cscl(cs_name, cl_name)
            cl.insert(record)
        except (SDBBaseError, SDBTypeError), e:
            # if have table in sync.local
            print "failed to insert data: %s, error code: %s" % (record, e.code)
            if -38 != e.code:
                raise

    def sync_get_cscl(self, csname, clname):
        try:
            #print "--0--BEGIN to get table: %s.%s's connect handler" % (csname, clname)
            cs = self.db.get_collection_space(csname)
            #print "--1--BEGIN to get table: %s.%s's connect handler" % (csname, clname)
            cl = cs.get_collection(clname)
            #print "--2--BEGIN to get table: %s.%s's connect handler" % (csname, clname)

            return cl
        except (SDBBaseError, SDBTypeError), e:
            print "failed to get table: %s.%s's connect handler, error code: %s" % (csname, clname, e.code)



def main():
    connect = {'HostName': '192.168.36.133', 'ServerPort': '11810', 'UserName': '', 'Password': '',
               'CsName': 'sync', 'ClName': 'log'}
    dblob = {'sync_sys': 'TPCH01', 'tbl_name': 'tpch01.customer', 'sync_dt': '20170303'}

    logger = logging.getLogger("sample_log")
    logger.setLevel(logging.INFO)
    fh = SCFileHandler("sample.log", dblob, connect)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    #logr = logging.debug("a %(a)d b %(b)s", {'a': 1, 'b': 2})


    logger.info("creating an instance of auxiliary_module.Auxiliary")
    logger.info("created an instance of auxiliary_module.Auxiliary")
    logger.info("calling auxiliary_module.Auxiliary.do_something")
    logger.info("finish auxiliary_module.Auxiliary.do_something")
    logger.info("calling auxiliary_module.Auxiliary.some_function()")
    logger.info("done with auxiliary_module.some_function()")


if __name__ == '__main__':
    main()
