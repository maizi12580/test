#!/usr/bin/python
#coding=utf-8


from pexpect import pxssh
import pexpect
import os, stat
import commands
import shutil
import traceback
from sync_logger import *


class SyncOS:
    def __init__(self, log_handler=None):
        '''
            使程序的日志在不落盘的情况下自动上传到日志服务,无须写入文件,实时性高,吞吐量大,无须知道主机所在位置,
            自动解析日志中的JSON和KV格式信息无须写入文件,实时性高,吞吐量大,无须知道主机所在位置,自动解析日志中的JSON和KV格式信息
        '''
        self.log = log_handler
        self.pexpect_connect_timeout = 10
        self.pexpect_timeout = 7200
        self.sftp_cmd = 'sftp'

    def sync_ssh(self, host, user, password, command, timeout=3600):
        try:
            #实行与远程ssh的交互
            s = pxssh.pxssh(timeout=timeout)
            s.login(host, user, password, original_prompt='[$#>]')
            s.sendline(command)
            s.prompt()
            command_info = s.before
            s.logout()
            return command_info

        except pxssh.ExceptionPxssh, e:
            if self.log is None:
                print 'failed to execute pxssh(ssh cmd): %s, error: %s' % (command, str(e))
            else:
                self.log.warn('failed to execute pxssh(ssh cmd): %s, error: %s' % (command, str(e)))

    def get_dirname(self, absolute_file):
        try:
            return os.path.dirname(absolute_file)
        except OSError, e:
            raise

    def get_basename(self, absolute_file):
        try:
            return os.path.basename(absolute_file)
        except OSError, e:
            raise

    def cmd_run(self, command_line):
        try:
            run_status = []
            (status, output) = commands.getstatusoutput(command_line)
            run_status.append(status)
            run_status.append(output)
            return run_status
        except OSError, e:
            raise

    def make_directory(self, dirname):
        """
        make directory
        Args:
            dirname: the making directory's name

        Returns:

        """
        try:
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
                # give new director high autority, how to give autority of parent directory
                cmd = 'chmod 777 %s -R' % dirname
                self.cmd_run(cmd)
        except OSError, e:
            if self.log is None:
                print "make directory: %s failed! error info: %s" % (dirname, e)
            else:
                self.log.warn("make directory: %s failed! error info: %s" % (dirname, e))
            if 17 == e.errno and os.path.isdir(dirname) is True:
                if self.log is None:
                    print 'File directory is exists: %s' % e.errno
                else:
                    self.log.warn('File directory is exists: %s' % e.errno)
            else:
                if self.log is None:
                    print 'File exists: %s and File is directory: %s' % (e.errno, os.path.isdir(dirname))
                else:
                    self.log.error('File exists: %s and File is directory: %s' % (e.errno, os.path.isdir(dirname)))
                raise

    def remove_file(self, filename):
        """
        remove file
        Args:
            filename: the remove file's name

        Returns:

        """
        try:
            if os.path.isfile(filename) is True:
                os.remove(filename)
        except OSError, e:
            if self.log is None:
                print "remove file: %s failed! error info: %s" % (filename, e)
            else:
                self.log.warn("remove file: %s failed! error info: %s" % (filename, e))
            raise

    def move_file(self, filename, dest_directory):
        """
        move file
        Args:
            filename: the moving file's name

        Returns:

        """
        try:
            if os.path.isfile(filename) is True and os.path.isdir(dest_directory) is True:
                shutil.move(filename, dest_directory)
        except IOError, e:
            if self.log is None:
                print "move file: %s failed! error info: %s" % (filename, e)
            else:
                self.log.warn("move file: %s failed! error info: %s" % (filename, e))
            raise

    def get_lineno(self, filename):
        try:
            command_line = "wc -l %s" % filename
            run_ret = self.cmd_run(command_line)
            file_num = run_ret[1].split(" ")[0]
            return int(file_num)
        except IOError, e:
            raise

    def touch_ok_file(self, filename):
        try:
            ok_filename = filename.split(".")[0] + ".ok"
            # new ok file
            ok_file = open(ok_filename, "w+")
            ok_file.close()
            # chmod permission
            os.chmod(filename, stat.S_IRWXU)      # .dat file
            os.chmod(ok_filename, stat.S_IRWXU)   # .ok  file
        except IOError, e:
            raise

    def give_highest_authority(self, filename):
        try:
            command_line = 'mkdir -p %s; chmod 777 %s;' \
                           'chmod 777 %s;chmod 777 %s' % (self.get_dirname(filename),
                                             os.path.abspath(os.path.join(self.get_dirname(filename), "..")),
                                             self.get_dirname(filename),
                                             filename)
            run_ret = self.cmd_run(command_line)
            return run_ret
        except IOError, e:
            raise

    def get_local_host(self):
        try:
            return socket.gethostname()
        except IOError, e:
            if self.log is None:
                print "failed to get local host! error info: %s" % e
            else:
                self.log.warn("failed to get local host! error info: %s" % e)
            raise

    def get_remote_host(self, frm_ip):
        try:
            remote_host = None
            remote = socket.gethostbyaddr(frm_ip)
            remote_host = remote[0]
            return remote_host
        except IOError, e:
            if self.log is None:
                print "failed to get remote host! error info: %s" % e
            else:
                self.log.warn("failed to get remote host! error info: %s" % e)
            return remote_host

    def file_is_exists(self, file_name):
        try:
            return os.path.isfile(file_name.strip())
        except IOError, e:
            if self.log is None:
                print "failed to inspect file: %s is exists or not! " \
                      "error info: %s" % (file_name, e)
            else:
                self.log.error("failed to inspect file: %s is exists or not! "
                               "error info: %s" % (file_name, e))
            raise

    def copy_file(self, source_file, destination_file):
        try:
            return shutil.copy(source_file, destination_file)
        except (OSError,IOError), e:
            print "failed to copy source file: to destination file: %s!" % e.errno 
            """
            if 2 != e.errno:
                if self.log is None:
                    print "failed to copy source file: %s to destination file: %s !" \
                          "error info: %s" % (source_file, destination_file, e)
                else:
                    self.log.error("failed to copy source file: %s to destination file: %s !"
                                   "error info: %s" % (source_file, destination_file, e))
                raise
            """

    def get_file_size(self, file_name):
        try:
            if os.path.isfile(file_name):
                return os.path.getsize(file_name)
            else:
                return None
        except IOError, e:
            if self.log is None:
                print "failed to get file  %s's size!error info: %s" % (file_name, e)
            else:
                self.log.error("failed to get file  %s's size!"
                               "error info: %s" % (file_name, e))
            raise

    def sftp_get(self, remote_host, remote_file, remote_user, encd_remote_passwd,
                 local_file, timeout=None):
        connect_timeout = self.pexpect_connect_timeout
        remote_passwd = base64.decodestring(encd_remote_passwd)
        ret_arr = []
        if timeout is None:
            timeout = self.pexpect_timeout
        sftp = pexpect.spawn(self.sftp_cmd, ['%s@%s' % (remote_user, remote_host)],
                             timeout=timeout)
        ret = sftp.expect(['sftp>', '%s@%s\'s password:' % (remote_user, remote_host),
                           pexpect.TIMEOUT, pexpect.EOF], connect_timeout)
        if 0 == ret:
            # get remote file with no password input
            sftp.sendline('get %s %s' % (remote_file, local_file))
            ret = sftp.expect('sftp>')
            if 0 == ret:
                sftp.sendline('ls -atl %s' % remote_file)
            ret = sftp.expect('sftp>')
            ret_arr.append(ret)
            if 0 == ret:
                file_size_str = sftp.before
                file_size = (file_size_str.split("\r\n")[1]).split(' ')[19]
                ret_arr.append(file_size)
            return ret_arr
        elif 1 == ret:
            sftp.sendline(remote_passwd)
            ret = sftp.expect('sftp>')
            # get remote file with password input
            if 0 == ret:
                sftp.sendline('get %s %s' % (remote_file, local_file))
            ret = sftp.expect('sftp>')
            if 0 == ret:
                sftp.sendline('ls -atl %s' % remote_file)
            ret = sftp.expect('sftp>')
            ret_arr.append(ret)
            if 0 == ret:
                file_size_str = sftp.before
                file_size = (file_size_str.split("\r\n")[1]).split(' ')[19]
                ret_arr.append(file_size)
            return ret_arr
        elif 2 == ret:
            self.log.warn("failed to use %s get file: %s from host: %s@%s."
                          "TimeOut!" % (self.sftp_cmd, remote_file, remote_user,
                                        remote_host))
            return 'TimeOut'
        elif 3 == ret:
            self.log.warn("failed to use %s get file: %s from host: %s@%s."
                          "Terminate!" % (self.sftp_cmd, remote_file, remote_user,
                                          remote_host))
            return 'Terminate'
        else:
            self.log.warn("failed to use %s get file: %s from host: %s@%s."
                          "UnknownError!" % (self.sftp_cmd, remote_file, remote_user,
                                          remote_host))
            return 'UnknownError'

    def remove_directory(self, dirname):
        """
        remove directory
        Args:
            dirname: remove direcotry's name

        Returns:

        """
        try:
            if os.path.isdir(dirname) is True:
                shutil.rmtree(dirname)
        except OSError, e:
            if self.log is None:
                print "failed to remove file: %s ! " \
                      "error info: %s" % (dirname, traceback.format_exc())
            else:
                self.log.error("failed to remove file: %s ! "
                         "error info: %s" % (dirname, traceback.format_exc()))
