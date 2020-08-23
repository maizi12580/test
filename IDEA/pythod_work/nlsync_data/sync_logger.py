#!/usr/bin/python
#coding=utf-8
import os
from multiprocessing import Process
import logging
import codecs
from util import *
from sync_sdb import *


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
        self.connect = connect
        self.just_write_table = just_write_table
        if delay:
            #We don't open the stream, but we still need to call the
            #Handler constructor to set level, formatter, lock etc.
            logging.Handler.__init__(self)
            self.stream = None
        else:
            logging.StreamHandler.__init__(self, self._open())

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
        hostname = self.connect['HostName']
        serverport = self.connect['ServerPort']
        username = self.connect['UserName']
        password = self.connect['Password']
        cs_name = self.connect['CsName']
        cl_name = self.connect['ClName']
        db = SCSDB(hostname, serverport, username, password)

        # put the record in sdb collection
        now_time = str(datetime.datetime.now())
        loginfo = {'log_desc': self.format(record), 'up_tm': now_time}
        # print 'SYNC: %s === %s' % (loginfo, self.dblog)
        self.dblog.update(loginfo)
        db.sync_insert(cs_name, cl_name, self.dblog)


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
