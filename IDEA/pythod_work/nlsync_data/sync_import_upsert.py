#!/usr/bin/python
#coding=utf-8


from multiprocessing import Process, Queue, Pipe, Lock, Value, Array, Manager
from sync_sdb import *
import time
from config.global_config import *
from start_remote_sync import *
import os
import shutil
import base64



def make_directory(dirname):
    """
    make directory
    Args:
        dirname: the making directory's name

    Returns:

    """
    try:
        os.mkdir(dirname)
    except OSError, e:
        print "make directory: %s failed! error info: %s" % (dirname, e)
        if 17 == e.errno and os.path.isdir(dirname) is True:
            print 'File directory is exists: %s' % e.errno
        else:
            print 'File exists: %s and File is directory: %s' % (e.errno, os.path.isdir(dirname))
            raise

def remove_file(filename):
    """
    remove file
    Args:
        filename: the remove file's name

    Returns:

    """
    try:
        if True == os.path.isfile(filename):
            os.remove(filename)
    except OSError, e:
        print "remove file: %s failed! error info: %s" % (filename, e)
        raise

def move_file(filename, dest_directory):
    """
    move file
    Args:
        filename: the moving file's name

    Returns:

    """
    try:
        if True == os.path.isfile(filename) and True == os.path.isdir(dest_directory):
            shutil.move(filename, dest_directory)
    except IOError, e:
        print "remove file: %s failed! error info: %s" % (filename, e)
        raise

def import_upsert(upsert_home, upsert_config, upsert_file):
    """
    Desc: this function upsert table from json file by running command with sdbupsert.jar
    Args:
        upsert_home: the sdbupsert.jar file's directory
        upsert_config: the table's configure file.
        upsert_file: the json file include absolute directory

    Returns:

    """
    run_file = upsert_file
    upsert_cmd = 'cd %s;java -jar %s %s %s' % \
                 (upsert_home, upsert_jar, upsert_config, upsert_file)
    (status, output) = commands.getstatusoutput(upsert_cmd)
    if 0 == status:
        remove_file(run_file)
    else:
        print 'Cmd: %s, return code: %s, output: %s' % (upsert_cmd, status, output)
        raise

    
def get_rec_file(cs_name, cl_name, data_home):
    """
    Desc: get json file name for table upsert in data's home directory
    Args:
        cs_name: the collection space name for table upsert
        cl_name: the collection name for table upsert
        data_home: the data's home directory where ODS put data in

    Returns: json file name with absolute directory

    """
    # data_home direcotry
    make_directory(data_home)
    dir_files = os.listdir(data_home)
    rec_files = []
    for dir_file in dir_files:
        rec_file = "%s/%s" % (data_home, dir_file)
        if -1 != rec_file.find("%s_%s" % (cs_name, cl_name)) and -1 != rec_file.find(".rec"):
            rec_files.append(rec_file)

    # data_home/upsert_rec direcotry
    upsert_rec_dir = '%s/upsert_rec' % data_home
    make_directory(upsert_rec_dir)
    dir_files = os.listdir(upsert_rec_dir)
    for dir_file in dir_files:
        rec_file = "%s/%s" % (upsert_rec_dir, dir_file)
        if -1 != rec_file.find("%s_%s" % (cs_name, cl_name)) and -1 != rec_file.find(".rec"):
            rec_files.append(rec_file)

    return rec_files


def move_rec_file(cs_name, cl_name, data_home):
    """
    Desc: move data home directory's data into data/upsert_rec dirctory
    Args:
        cs_name: the collection space name for table upsert
        cl_name: the collection name for table upsert
        data_home: the data's home directory where ODS put data in

    Returns: get new upsert file

    """
    import_rec_dir = '%s/import_rec' % data_home
    upsert_rec_dir = '%s/upsert_rec' % data_home
    # make directory for import failed file, file type is csv
    make_directory(import_rec_dir)
    # make directory for import failed file, file type is json
    make_directory(upsert_rec_dir)
        
    upsert_file = ''
    new_upsert_file = ''
    # before move file, we remove cs_cl_pid.rec file
    dir_files = os.listdir(upsert_rec_dir)
    for dir_file in dir_files:
        #print 'RM2:%s %s %s' % (dir_file, dir_file.find("%s_%s" % (cs_name, cl_name)), dir_file.find(".rec"))
        rm_file = "%s/%s" % (upsert_rec_dir, dir_file)
        if -1 != rm_file.find("%s_%s" % (cs_name, cl_name)) and -1 != rm_file.find(".rec"):
            remove_file(rm_file)

    # move file into directory upsert_rec
    dir_files = os.listdir(data_home)
    rec_files = []
    for dir_file in dir_files:
        mv_file = "%s/%s" % (data_home, dir_file)
        #print 'MV:%s %s %s' % (dir_file, dir_file.find("%s_%s" % (cs_name, cl_name)), dir_file.find(".rec"))
        if -1 != mv_file.find("%s_%s" % (cs_name, cl_name)) and -1 != mv_file.find(".rec"):
            move_file(mv_file, upsert_rec_dir)
            rec_files.append(dir_file)

    if 1 < len(rec_files):
        print 'rec files must have one, but have two. Files is %s !' % (rec_files)
        raise
    elif 0 == len(rec_files):
        return new_upsert_file
    else:
        upsert_file = rec_files[0]

    # here need thinking when synchonous failed, how to retry???
    if '' != upsert_file:
        new_upsert_file = '%s/%s' % (upsert_rec_dir, os.path.basename(upsert_file))
    else:
        new_upsert_file = upsert_file

    return new_upsert_file


def autogen_upsert_config(cs_name, cl_name):
    """
    Desc: auto generate configure file for table upsert
    Args:
        cs_name: the collection space name for table upsert
        cl_name: the collection name for table upsert

    Returns: configure file

    """
    # source replace string
    conf_cs = 'collectionSpace='
    conf_cl = 'collection='
    conf_condfds = 'condFields='
    conf_hintidx = 'hint='
    conf_user = 'user='
    conf_passwd = 'password='
    conf_coords = 'croodAddrs='

    # destname replace string
    conf_replace_cs = conf_cs + cs_name
    conf_replace_cl = conf_cl + cl_name
    conf_replace_user = conf_user + user_name
    conf_replace_passwd = conf_passwd + base64.decodestring(password)
    # get unique index keys
    db = SCSDB(host_name, server_port, user_name, password)
    uniq_idx = db.sync_get_uniqidx(cs_name, cl_name)
    if 0 == len(uniq_idx):
        return
    conf_replace_condfds = conf_condfds
    cnt = 0
    for key in uniq_idx[1]:
        if 0 == cnt:
            conf_replace_condfds = conf_replace_condfds + key
        else:
            conf_replace_condfds = conf_replace_condfds + ',' + key
        cnt += 1

    # get unique index name
    conf_replace_hintidx = conf_hintidx + uniq_idx[0]

    # get coord address
    conf_replace_coords = '%s%s' % (conf_coords, db.get_coord_address())

    # global file
    src_conf_file = '%s/conf/%s' % (upsert_home, upsert_property)
    dst_conf_file = '%s/conf/%s_%s.conf' % (upsert_home, cs_name, cl_name)
    log_conf_file = '%s/conf/log4j.properties' % upsert_home
    src_fd = open(src_conf_file)
    dst_fd = open(dst_conf_file, 'w+')
    log_fd = open(log_conf_file)
    # change log file story directory name
    for log_line in log_fd:
        log_line = log_line.replace('opt', upsert_logdir)

    log_fd.close()

    for line_context in src_fd:
        try:
            if -1 != line_context.find(conf_cs):
                line = line_context.replace(conf_cs, conf_replace_cs)
            elif -1 != line_context.find(conf_cl):
                line = line_context.replace(conf_cl, conf_replace_cl)
            elif -1 != line_context.find(conf_condfds):
                line = line_context.replace(conf_condfds, conf_replace_condfds)
            elif -1 != line_context.find(conf_hintidx):
                line = line_context.replace(conf_hintidx, conf_replace_hintidx)
            elif -1 != line_context.find(conf_user):
                line = line_context.replace(conf_user, conf_replace_user)
            elif -1 != line_context.find(conf_passwd):
                line = line_context.replace(conf_passwd, conf_replace_passwd)
            elif -1 != line_context.find(conf_coords):
                line = line_context.replace(conf_coords, conf_replace_coords)
            else:
                line = line_context
        except UnicodeDecodeError, e:
            print line

            
        # put line into destnation file
        dst_fd.write(line)

    src_fd.close()
    dst_fd.close()

    # for relative directory
    dst_conf_file = os.path.basename(dst_conf_file)
    return dst_conf_file

if __name__ == '__main__':
    #upsert_file = '/home/sequoiadb/upsert_test/test_data/CITBAS.dat'
    #import_upsert(upsert_file)
    dirname = 'xiaojun'
    make_directory(dirname)
    make_directory(dirname)
    make_directory(dirname)
