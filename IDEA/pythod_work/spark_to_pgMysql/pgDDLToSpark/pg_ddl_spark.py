#! /usr/bin/python
# -*- coding: utf-8 -*-
# Atom
import ConfigParser
import base64
import csv
import logging.config
import random
import sched
import socket
import traceback
import psycopg2
import json

from datetime import datetime

import subprocess
import time
import os
import sys
import re

class CryptoUtil:
    def __init__(self):
        pass

    @classmethod
    def encrypt(cls, source_str):
        random_choice = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()"
        to_encrypt_arr = []
        shift_str = ""
        for char in source_str:
            shift_str = shift_str + chr(ord(char) + 3)
        shift_index = 0
        for index in range(0, len(shift_str) * 3):
            if index % 3 != 0:
                rand_char = random.choice(random_choice)
                to_encrypt_arr.append(rand_char)
            else:
                to_encrypt_arr.append(shift_str[shift_index])
                shift_index = shift_index + 1
        to_encrypt_str = ''.join(to_encrypt_arr)
        encrypt_str = base64.b64encode(to_encrypt_str)
        return encrypt_str

    @classmethod
    def decrypt(cls, encrypt_str):
        decrypt_str = base64.b64decode(encrypt_str)
        shift_str = []
        for index in range(len(decrypt_str)):
            if index % 3 == 0:
                shift_str.append(decrypt_str[index])
        source_arr = []
        for char in shift_str:
            source_arr.append(chr(ord(char) - 3))
        source_str = "".join(source_arr)
        return source_str


class DateUtils:

    def __init__(self):
        pass

    @classmethod
    def get_current_date(cls):
        """get current time of year-month-day format

        :return: time of year-month-day format
        """
        return datetime.now().strftime('%Y-%m-%d')

    @classmethod
    def get_current_time(cls):
        """get current time of year-month-day hour:minute:second.microsecond format

        :return: time of year-month-day hour:minute:second.microsecond format
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    @classmethod
    def timestamp_to_datetime(cls, timestamp):

        local_dt_time = datetime.fromtimestamp(timestamp / 1000000.0)
        return local_dt_time

    @classmethod
    def datetime_to_strtime(cls, datetime_obj, date_format):
        local_str_time = datetime_obj.strftime(date_format)
        return local_str_time

    @classmethod
    def datetime_to_timestamp(cls, datetime_obj):
        local_timestamp = long(time.mktime(datetime_obj.timetuple()) * 1000000.0 + datetime_obj.microsecond)
        return local_timestamp

    @classmethod
    def strtime_to_datetime(cls, timestr, date_format):
        local_datetime = datetime.strptime(timestr, date_format)
        return local_datetime

    @classmethod
    def timestamp_to_strtime(cls, timestamp, date_format):
        return cls.datetime_to_strtime(cls.timestamp_to_datetime(timestamp), date_format)

    @classmethod
    def strtime_to_timestamp(cls, timestr, date_format):
        try:
            local_str_time = cls.datetime_to_timestamp(cls.strtime_to_datetime(timestr, date_format))
            return local_str_time
        except Exception as e:
            return 0

    @classmethod
    def get_file_ctime_timestamp(cls, f):
        return cls.datetime_to_timestamp(datetime.fromtimestamp(os.path.getctime(f)))

    @classmethod
    def get_file_mtime_timestamp(cls, f):
        return cls.datetime_to_timestamp(datetime.fromtimestamp(os.path.getmtime(f)))

    @staticmethod
    def compare_mtime(x, y):
        x_mtime = x["mtime"]
        y_mtime = y["mtime"]
        if x_mtime < y_mtime:
            return -1
        elif x_mtime > y_mtime:
            return 1
        else:
            return 0


class SparkDDLSync:

    def __init__(self, log):
        # 获取当前文件路径
        current_file_path = os.path.split(os.path.realpath(__file__))[0]
        self.config_file = os.path.join(current_file_path, "syn.config")
        self.log = log
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.config_file)

        self.host = self.config.get('psql', 'host')
        self.spark_host = self.config.get('psql', 'spark_host')
        self.port = self.config.get('psql', 'port')
        self.psql_password_type = self.config.get('psql', 'psql_password_type')
        self.psql_user = self.config.get('psql', 'psql_user')
        self.psql_password = self.config.get('psql', 'psql_password')
	self.fromDB = self.config.get('psql','from_db_name')
	self.aimDB = self.config.get('psql','aim_db_name')
        self.interval_time = int(self.config.get('execute', 'interval_time'))
        ignore_option = self.config.get('execute', 'ignore_error')
        if "true" == ignore_option.lower():
            self.ignore_error = True
        else:
            self.ignore_error = False

        self.max_retry_times = int(self.config.get('execute', 'max_retry_times'))
        self.sleep_time = 1
        self.SUCCESS_STATE = 0
        self.level = {"debug": "DEBUG", "info": "INFO", "warning": "WARNING", "error": "ERROR"}
        self.level_priority = {"DEBUG": 4, "INFO": 3, "WARNING": 2, "ERROR": 1}
        self.ignore_file = "syn.ignore.info"
	self.in_metastore_file = "syn.insert_metastore.info"
        self.check_avg()


        self.logger(self.level["info"], "Start pg ddl sync to spark...")
	self.pg_to_spark_fields={"text":"string",\
	"integer":"int",\
	"numeric":"int",\
	"integer":"integer",\
	"bigint":"long",\
	"float":"double",\
	"double":"double",\
	"bigint":"bigint",\
	"decimal":"decimal",\
	"date":"date",\
	"timestamp":"timestamp",\
	"LOB":"LOB"}
	self.current_file_path = os.path.split(os.path.realpath(__file__))[0]
        path=os.path.join(self.current_file_path, self.aimDB)
        isExists=os.path.exists(path)
        if not isExists:
                os.makedirs(path)
                self.logger(self.level["info"], "Create folder successful.")
        else:
                self.logger(self.level["info"], "Folder is exists.")


    def __log_ignore_stmt(self, stmt):
        ignore_file = open(self.ignore_file, "a")
        ignore_file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " " + stmt + "\n")
        ignore_file.close()

    def check_avg(self):

        # if not os.path.exists(self.log_directory):
        #     os.makedirs(self.log_directory)

        if int(self.psql_password_type) == 0:
            encrypt_password = CryptoUtil.encrypt(self.psql_password)
            self.psql_password = encrypt_password
            self.psql_password_type = 1
            self.update_password()

    def update_password(self):
        self.config.set('psql', 'psql_password', self.psql_password)
        self.config.set('psql', 'psql_password_type', self.psql_password_type)
        self.config.write(open(self.config_file, "w"))


    def run_check_task(self):
        self.logger(self.level["info"], "begin to check psql")
	self.fromDB_conn = psycopg2.connect(database=self.fromDB, user=self.psql_user,\
                password=CryptoUtil.decrypt(self.psql_password), host=self.host, port=self.port)
        self.fromDB_cursor = self.fromDB_conn.cursor()
	self.exe()

	self.fromDB_cursor.close()
	self.fromDB_cursor.close()

    def exe(self):
	self.fromDB_cursor.execute("select a.relname ,b.ftoptions  from \"pg_class\"  a ,\"pg_foreign_table\" b where a.relkind='f' and a.relfilenode=b.ftrelid")
	tables = self.fromDB_cursor.fetchall()
        folder=os.path.join(self.current_file_path, self.aimDB)
	for table in tables:
		self.fromDB_cursor.execute("select column_name,data_type from information_schema.\"columns\" where \"table_name\"='{tb}'".format(tb=table[0]))
		fields=self.fromDB_cursor.fetchall()
		sub=''
		for field in fields:
			field_type=field[1].split(" ")[0]
			if(self.pg_to_spark_fields.has_key(field_type)):
				sub+=field[0]+" "+self.pg_to_spark_fields[field_type]+","
			else:
				self.logger(self.level["info"], "error field type  {fie}".format(fie=field[1]))
		cs=table[1][0].split("=")[1]
		cl=table[1][1].split("=")[1]
		pre='drop table if exists  {cs}.{cl};\n'.format(cs=self.aimDB,cl=table[0],sub=sub[:-1])
		head='create table {cs}.{cl} ({sub} )'.format(cs=self.aimDB,cl=table[0],sub=sub[:-1])
		tail='USING com.sequoiadb.spark OPTIONS(host \'{host}\',collectionspace \'{cs}\',collection \'{cl}\',ignoreduplicatekey \'true\');'.format(host=self.spark_host,cs=cs,cl=cl)
		path=os.path.join(folder,table[0])
		with open (path,'w+') as f:
			f.write(pre)
			f.write(head)
			f.write(tail)

    def logger(self, log_level, message):

        if log_level == self.level["error"]:
            self.log.error(message)
        elif log_level == self.level["warning"]:
            self.log.warn(message)
        elif log_level == self.level["info"]:
            self.log.info(message)
        elif log_level == self.level["debug"]:
            self.log.debug(message)


def init_log(log_config_file):
    try:
        # Get the log file path from the log configuration file, and create the directory if it dose not exist.
        config_parser = ConfigParser.ConfigParser()
        files = config_parser.read(log_config_file)
        if len(files) != 1:
            print("Error: Read log configuration file failed")
            return None
        log_file = config_parser.get("handler_rotatingFileHandler", "args").split('\'')[1]
        curr_path = os.path.abspath(os.path.dirname(log_config_file))
        log_file_full_path = os.path.join(curr_path, log_file)
        log_file_parent_dir = os.path.abspath(os.path.join(log_file_full_path, ".."))
        if not os.path.exists(log_file_parent_dir):
            os.makedirs(log_file_parent_dir)

        logging.config.fileConfig(log_config_file)
        log = logging.getLogger("ddlLogger")
        return log
    except BaseException as e:
        print("Error: Initialize logging failed. Error number: " + ". Message: " + e.message)
        return None


def run_task(log):
	sparkDDLSync = SparkDDLSync(log)
	scheduler = sched.scheduler(time.time, time.sleep)  # 定时器
    	while True:
        	scheduler.enter(sparkDDLSync.interval_time, 1, sparkDDLSync.run_check_task, ())
        	scheduler.run()
		break

def main():
    current_file_path = os.path.split(os.path.realpath(__file__))[0]
    pid_file = os.path.join(current_file_path, "APP_ID")
    if os.path.exists(pid_file):
        with open(pid_file, "r") as f:
            pid = str(f.readline())
        if os.path.exists("/proc/{pid}".format(pid=pid)):
            with open("/proc/{pid}/cmdline".format(pid=pid), "r") as process:
                process_info = process.readline()
            if process_info.find(sys.argv[0]) != -1:
                return
    with open(pid_file, "w") as f:
        pid = str(os.getpid())
        f.write(pid)

    log_config_file= os.path.join(current_file_path, "syn.log.config")
    log = init_log(log_config_file)
    if log is None:
        print("Initialize logging failed. Exit...")
        return 1
    run_task(log)


if __name__ == '__main__':
    main()
