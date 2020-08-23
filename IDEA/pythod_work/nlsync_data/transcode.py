#!/user/bin/python
#coding=utf-8
from optparse import OptionParser
import datetime
import re
import os

class Transcode:
    def __init__(self, log_handler=None):
        self.fail_trans_count = 0L
        self.error_list = []
        self.del_error_can_not_merge_count = 0L
        self.log = log_handler
        self.ENCODE_UTF8 = "utf-8"
        self.ENCODE_IGNORE = "ignore"
        self.ENCODE_OK_SUFFIX = ".utf8"
        self.ERROR_NOT_MERGE_SUFFIX = ".del_error_can_not_merge"
        self.ERROR_MERGE_SUFFIX = ".del_error_can_merge"
        self.TRAN_ERROR_SUFFIX = ".trans_error"
        self.EXCEPT1_FILE_SUFFIX = ".except1"
        self.APPEND_WRITE_MODE = "w+"
        self.DEFAULT_DECODING = "gb18030"
        self.check_num=10

    def split_transcodes(self, line, src_encoding, src_delfield, except_file_list,
                         trans_error, field_count, string_del=None):
        parm_line = line.split(src_delfield)
        all_parm_list = []
        for parm in parm_line:
            try:
                parm_after_code = parm.decode(src_encoding).encode(self.ENCODE_UTF8)
                all_parm_list.append(parm_after_code)
            except Exception, e:
                if self.log is not None:
                    self.log.warn("transcode split exception 1: %s" % e)
                except_file_list[0].write(line)
                try:
                    parm_after_code = parm.decode(src_encoding,
                                                  self.ENCODE_IGNORE).encode(self.ENCODE_UTF8)
                    all_parm_list.append(parm_after_code)
                except Exception, e:
                    if self.log is not None:
                        self.log.warn("transcode split ignore exception 2 and "
                                      "not need to write into file: %s" % e)
                    # here split field don't need to write into file
                    print 'here split field don\'t need to write into file'

        line_after_code = src_delfield.join(all_parm_list)
        line_field_count = self.field_count_def(line, src_delfield, string_del)
        after_trans_field_count = self.field_count_def(line_after_code,
                                                       src_delfield, string_del)
        if line_field_count == field_count and after_trans_field_count != field_count:
            if self.log is not None:
                self.log.warn('one line field count: %s, '
                              'after transcode field count: %s, '
                              'error line in file: %s' % (line_field_count,
                                                          after_trans_field_count,
                                                          trans_error))
            trans_error.write(line)
            self.fail_trans_count += 1
        if after_trans_field_count != field_count:
            line_after_code = 'error'
        return line_after_code

    def field_count_def(self, line, default_delfield, string_del=None):
        k = m = 0
        list_dot = line.split(default_delfield)
        if string_del is not None:
            string_del_count = line.count(string_del)
        else:
            # hear mean the line don't have string delimiter
            string_del_count = 0
        if string_del_count != 0:
            # print 'string_del_count != 0'
            if string_del_count % 2 == 0:
                # print 'string_del_count%2 == 0'
                while True:
                    # deal with field ["a,b"] in sample ["a,b", "c", "d"]
                    if list_dot[m].count(string_del) == 1:
                        while True:
                            m += 1
                            k += 1
                            if list_dot[m].count(string_del) == 1:
                                break
                    m += 1
                    if m == len(list_dot):
                        break
                field_count = (len(list_dot)-k)
            else:
                field_count = len(list_dot)
        else:
            field_count = len(list_dot)
        # print field_count
        return field_count

    def error_list_flush(self, del_error_can_not_merge):
        if len(self.error_list) > 0:
            for i in self.error_list:
                del_error_can_not_merge.write(i)
                self.del_error_can_not_merge_count += 1
            del_error_can_not_merge.write(120 * "-" + "\n")
            self.error_list = []
    def transcode(self, source_file_name, result_path, field_count,
                  default_delfield='27', string_del=None, file_just_name=None):
        src_delfield = chr(int(default_delfield))
        del_count = field_count - 1
        if file_just_name is None:
           file_just_name = os.path.basename(source_file_name).split(".")[0]
        source_file = open(source_file_name)
        check_line_num=self.check_num
        has_check_line_num = 0
        rel_delcount = 0
        check_file_line_sub=""
        can_transcode=True
        #if os.path.getsize(source_file_name): 
        for line in source_file:
            if check_line_num == 0:
               break
            has_check_line_num+=1
            check_file_line_sub=check_file_line_sub+line
            rel_delcount=check_file_line_sub.count(src_delfield)
            print "rel_delcount: %s , del_count: %s"%(rel_delcount,del_count)
            if rel_delcount==del_count:
               check_file_line_sub=""
               check_line_num=check_line_num-1
            elif rel_delcount > del_count:
               can_transcode=False
               if self.log is not None:
                  self.log.info("line del num is false")
               else:
                  print "line del num is false"
               break             
        source_file.close()
        transcode_ret=[]
        if can_transcode is True:
            if self.log is not None:
               self.log.info("has_check_line_num: %s , need check %s line field count has %s is true begin transcode ..." % (has_check_line_num,self.check_num, (self.check_num -check_line_num))) 
            else:
               print "has_check_line_num: %s , need check %s line field count has %s is true begin transcode ..." % (has_check_line_num,self.check_num, (self.check_num -check_line_num))
            transcode_ret = self.transcode_real(source_file_name, result_path, field_count,default_delfield,string_del,file_just_name)
        else:
            transcode_ret.append(None)
            transcode_ret.append(self.check_num)
            transcode_ret.append(0)
            if self.log is not None:
               self.log.warn("has_check_line_num: %s , need check %s line field count has %s is true can not transcode ..." % (has_check_line_num,self.check_num, (self.check_num -check_line_num))) 
            else:
               print "has_check_line_num: %s , need check %s line field count has %s is true can not transcode ..." % (has_check_line_num,self.check_num, (self.check_num -check_line_num))
        return transcode_ret 

    def transcode_real(self, source_file_name, result_path, field_count, 
                  default_delfield='27', string_del=None, file_just_name=None):
        """
        Args:
            source_file_name:
            result_path:
            default_delfield: decimal of delimiter string, type of int
            field_count:
            string_del:
            file_just_name:

        Returns:

        """
        if file_just_name is None:
            file_just_name = os.path.basename(source_file_name).split(".")[0]
        source_file = open(source_file_name)
        result_file_name = result_path + '/' + file_just_name + self.ENCODE_OK_SUFFIX
        result_file = open(result_file_name, self.APPEND_WRITE_MODE)
        del_error_cannot_merge_file = result_path + '/'+file_just_name + self.ERROR_NOT_MERGE_SUFFIX
        del_error_can_not_merge = open(del_error_cannot_merge_file, self.APPEND_WRITE_MODE)
        del_error_can_merge_file = result_path + '/' + file_just_name + self.ERROR_MERGE_SUFFIX
        del_error_can_merge = open(del_error_can_merge_file, self.APPEND_WRITE_MODE)
        trans_error_file = result_path + '/' + file_just_name + self.TRAN_ERROR_SUFFIX
        trans_error = open(trans_error_file, self.APPEND_WRITE_MODE)
        except_file_1_file = result_path + '/' + file_just_name + self.EXCEPT1_FILE_SUFFIX
        except_file_1 = open(except_file_1_file, self.APPEND_WRITE_MODE)
        except_file_list = []
        except_file_list.append(except_file_1)
        del_count = field_count - 1
        # error_list = []
        default_encoding = self.DEFAULT_DECODING
        src_encoding = default_encoding
        # the default_delfield para is decimal of delimiter, it is int type
        src_delfield = chr(int(default_delfield))
        # fail_trans_count = 0L
        trans_count = 0L
        total_count = 0L
        # del_error_can_not_merge_count = 0L
        del_error_can_merge_count = 0L
        merge_count = 0L
        for line in source_file:
            total_count += 1
            try:
                line_after_code = line.decode(src_encoding).encode("utf-8")
            except Exception, e:
                line_after_code = self.split_transcodes(line, src_encoding,
                                                        src_delfield, except_file_list, 
                                                        trans_error, field_count, string_del)

            if line_after_code.count(src_delfield) == del_count:
                result_file.write(line_after_code)
                trans_count += 1
                self.error_list_flush(del_error_can_not_merge)
            # source del less
            elif line.count(src_delfield) < del_count:
                # print '#source del less'
                self.error_list.append(line)
                if len(self.error_list) > 1:
                    error_line = ""
                    for i in self.error_list:
                        error_line = error_line + i
                    print 'here error_line.count(src_delfield):%s, ' \
                          'del_count:%s, transcode file: %s' % (error_line.count(src_delfield),
                                                                del_count, source_file)
                    if error_line.count(src_delfield) == del_count:
                        error_line = error_line.replace("\n", "")
                        error_line += "\n"
                        try:
                            line_after_code = error_line.decode(src_encoding).encode("utf-8")
                        except Exception, e:
                            line_after_code = self.split_transcodes(error_line, src_encoding,
                                                                    src_delfield, except_file_list,
                                                                    trans_error, field_count, string_del)
                        print 'line line_after_code.count(src_delfield):%s, ' \
                              'del_count:%s, transcode file: %s' % \
                              (line_after_code.count(src_delfield), del_count, source_file)
                        # print line_after_code
                        if line_after_code.count(src_delfield) == del_count:
                            result_file.write(line_after_code)
                            trans_count += 1
                            merge_count += 1
                            if len(self.error_list) > 0:
                                for i in self.error_list:
                                    del_error_can_merge.write(i)
                                    del_error_can_merge_count += 1
                                del_error_can_merge.write(120 * "-" + "\n")
                                self.error_list = []
                        else:
                            line_after_code = self.split_transcodes(error_line, src_encoding,
                                                                    src_delfield, except_file_list,
                                                                    trans_error, field_count, string_del)
                            if line_after_code != 'error':
                                result_file.write(line_after_code)
                                trans_count += 1
                                merge_count += 1
                                if len(self.error_list) > 0:
                                    for i in self.error_list:
                                        del_error_can_merge.write(i)
                                        del_error_can_merge_count += 1
                                    del_error_can_merge.write(120 * "-" + "\n")
                                    self.error_list = []
            # source del good
            elif line.count(src_delfield) == del_count:
                # print '#source del good'
                line_after_code = self.split_transcodes(line, src_encoding, src_delfield,
                                                        except_file_list, trans_error, 
                                                        field_count, string_del)
                if line_after_code != 'error':
                    result_file.write(line_after_code)
                    trans_count += 1
                    self.error_list_flush(del_error_can_not_merge)
            # source del more
            elif line.count(src_delfield) > del_count:
                # print '#source del more'
                # print 'line_after_code:%s' % line_after_code
                actual_field_count = self.field_count_def(line_after_code, src_delfield,
                                                          string_del)
                print 'field_count = %s, actual_field_count = %s, transcode file: %s' % \
                      (field_count, actual_field_count, source_file)
                if field_count == actual_field_count:
                    trans_count += 1
                    result_file.write(line_after_code)
                else:
                    del_error_can_not_merge.write(line)
                    self.del_error_can_not_merge_count += 1
                    del_error_can_not_merge.write(120 * "-" + "\n")
                self.error_list_flush(del_error_can_not_merge)
        self.error_list_flush(del_error_can_not_merge)
        # file handle close
        source_file.close()
        result_file.close()
        del_error_can_not_merge.close()
        del_error_can_merge.close()
        trans_error.close()
        except_file_1.close()
        # when the file is null, we remove it
        self.remove_null_file(del_error_cannot_merge_file)
        self.remove_null_file(del_error_can_merge_file)
        self.remove_null_file(trans_error_file)
        self.remove_null_file(except_file_1_file)
        print 'transcode file: %s, total_count: %s' % (source_file, total_count)
        print 'transcode file: %s, trans_count: %s' % (source_file, trans_count)
        print 'transcode file: %s, fail_trans_count: %s' % (source_file, self.fail_trans_count)
        print 'transcode file: %s, merge count: %s' % (source_file, merge_count)
        print 'transcode file: %s, del_error_can_merge_count: %s' % (source_file,
                                                                     del_error_can_merge_count)
        print 'transcode file: %s, del_error_can_not_merge_count: %s' % (source_file,
                                                                         self.del_error_can_not_merge_count)
        if self.log is not None:
           self.log.info("transcode file: %s, total count: %s, transcode count: %s"
                        "failed transcode count: %s, merge count: %s"
                        "delete error can merge count: %s"
                        "delete error can not merge count: %s" %
                        (result_file, total_count, trans_count, self.fail_trans_count,
                        merge_count, del_error_can_merge_count,
                        self.del_error_can_not_merge_count))
        transcode_ret=[]
        transcode_ret.append(result_file_name)
        transcode_ret.append(total_count)
        transcode_ret.append(trans_count)
        # remomve

        return transcode_ret

    def remove_null_file(self, file_name):
        if os.path.exists(file_name):
            if os.path.getsize(file_name):
                print "file:%s exist and not null" % file_name
                if self.log is not None:
                   self.log.info("file:%s exist and not null" % file_name)
            else:
                os.remove(file_name)
                print "file:%s is null and has remove" % file_name
                if self.log is not None:
                   self.log.info("file:%s is null and has remove" % file_name)
        else:
           print "file:%s is not exists" % file_name

def main():
    parser = OptionParser()
    parser.add_option("-f", "--fileName",
                      dest='fileName',
                      default='file',
                      help=' please input file name or path & file name')
    parser.add_option("-n", "--field",
                      dest='num',
                      default='0',
                      help='please input field number')
    parser.add_option("-d", "--delfield",
                      dest='delfield',
                      default='27',
                      help='please input  = 27')
    (options, args) = parser.parse_args()
    fileName = options.fileName
    num = options.num
    delfield = options.delfield
    begin = datetime.datetime.now()
    source_file = fileName
    delfield = int(delfield)
    num = int(num)
    if num == 0:
        ff = open(source_file)
        for line in ff:
            line_field_count = line.count(chr(delfield)) + 1
            break
        num = line_field_count
    print 'line_field_count:%s' % num
    if fileName == "file":
        print "please input file"
    if fileName.count("/") == 0:
        source_file = "%s/%s" % (os.getcwd(), fileName)
    file_just_name = source_file.split("/").pop().split('.')[0]
    isExists = os.path.exists(file_just_name)
    if not isExists:
        os.mkdir(file_just_name)
    result_path = "%s/%s" % (os.getcwd(), file_just_name)
    string_del = "'"
    trans = Transcode()
    trans.transcode(source_file, result_path, num)
    end = datetime.datetime.now()
    print 'begin:%s' % begin
    print 'end:%s' % end

if __name__ == '__main__':
    main()