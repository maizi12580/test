#!/user/bin/python
#coding=utf-8
from optparse import OptionParser
import datetime
import re
import os
class Transcode:
      def __init__(self,source_encoding,des_encoding,log_handler=None):
          self.fail_trans_count=0L
          self.error_list=[]
          self.del_error_can_not_merge_count=0L
          self.log=log_handler
          self.ENCODE_UTF8=des_encoding
          self.ENCODE_IGNORE="ignore"
          self.ENCODE_OK_SUFFIX=".%s"%des_encoding
          self.ERROR_NOT_MERGE_SUFFIX=".del_error_can_not_merge"
          self.ERROR_MERGE_SUFFIX=".del_error_can_merge"
          self.TRAN_ERROR_SUFFIX=".trans_error"
          self.EXCEPT1_FILE_SUFFIX=".except1"
          self.APPEND_WRITE_MODE="w+"
          self.DEFAULT_DECODING=source_encoding
          self.check_num=10

      def split_transcodes(self,line,src_encoding,src_delfield,except_file_list,trans_error,field_count,string_del=None):
          parm_line=line.split(src_delfield)
          all_parm_list=[]
          for parm in parm_line:
              try:
                  parm_after_code=parm.decode(src_encoding).encode(self.ENCODE_UTF8)
                  all_parm_list.append(parm_after_code)
              except Exception, e:
                     if self.log is not None:
                         self.log.warn("transcode split exception1 : %s" % e)
                     except_file_list[0].write(line)
                     try:
                         parm_after_code=parm.decode(src_encoding,self.ENCODE_IGNORE).encode(self.ENCODE_UTF8)
                         all_parm_list.append(parm_after_code)
                     except Exception,e:
                         if self.log is not None:
                             self.log.warn("transcode split ignore exception2 and don\'t need write into file: %s" % e)
                         print 'ignore is not ok ,don\'t need to write into file ,has write into exception1 file the except :%s'%e
          line_after_code=src_delfield.join(all_parm_list)
          line_field_count=self.field_count_def(line,src_delfield,string_del)
          after_trans_field_count=self.field_count_def(line_after_code,src_delfield,string_del)
          #print 'line_field_count:%s,after_trans_field_count:%s'%(line_field_count,after_trans_field_count)
          if(line_field_count==field_count) and (after_trans_field_count != field_count):
             if self.log is not None:
                 self.log.warn('true field count: %s , src line  field count: %s ,after trans field count: %s' % (field_count,line_field_count,after_trans_field_count))
             trans_error.write(line)
          if(after_trans_field_count!=field_count):
             line_after_code='error'
          return line_after_code

      def field_count_def(self,line,default_delfield,string_del=None):
          k = m =0
          list_dot=line.split(default_delfield)
          if string_del is not None:
             string_del_count=line.count(string_del)
          else:
             #mean don't have  string_del
             string_del_count=0
          if string_del_count!=0:
             #print 'string_del_count!=0'
             if string_del_count%2==0:
                #print 'string_del_count%2==0'
                while True:
                      if list_dot[m].count(string_del)==1:
                         while True:
                               m+=1
                               k+=1
                               if list_dot[m].count(string_del)==1:
                                  break
                      m+=1
                      if m==len(list_dot):
                          break
                field_count=(len(list_dot)-k)
             else:
                 field_count=len(list_dot)
          else:
             field_count=len(list_dot)
          #print field_count
          return field_count

      def error_list_flush(self,del_error_can_not_merge):
          if(len(self.error_list)>0):
             for i in self.error_list:
                 del_error_can_not_merge.write(i)
                 self.del_error_can_not_merge_count+=1
             del_error_can_not_merge.write(120 * "-"+"\n")
             self.error_list=[]

      def transcode(self,source_file_name,result_path,field_count,default_delfield,string_del=None,file_just_name=None):
          src_delfield=default_delfield
          if file_just_name is None:
             file_just_name=os.path.basename(source_file_name).split[0]
          source_file=open(source_file_name)
          check_line_num=self.check_num
          print "we check file first %s line"%check_line_num
          line_num_good=0
          line_num=0
          field_num=0
          for line in source_file:
              if check_line_num<=0:
                 break
              check_line_num=check_line_num-1
              line_num+=1
              #field_num=line.count(src_delfield)
              field_num=self.field_count_def(line,src_delfield,string_del)
              if field_num==field_count:
                  line_num_good+=1
          source_file.close()
          print "line_num_good:%s "%line_num_good
          transcode_ret=[]
          result=""
          if line_num_good == line_num:
             print "file first %s line field count has %s is true begin transcode ..."%(line_num,line_num_good)
             transcode_ret=self.transcode_real(source_file_name,result_path,field_count,src_delfield,string_del,file_just_name)
             if transcode_ret[1]==transcode_ret[2] or (transcode_ret[1] > transcode_ret[2] and transcode_ret[3] > 1 and transcode_ret[4]==0):
                result="%s,%s,%s"%(1,transcode_ret[1],transcode_ret[2])
             else:
                result="%s,%s,%s"%(0,transcode_ret[1],transcode_ret[2])
          else:
             print "file first %s line field count has %s is true cannot  transcode ..."%(line_num,line_num_good)
             transcode_ret.append(None)
             transcode_ret.append(0)
             transcode_ret.append(0)
             transcode_ret.append(0)
             transcode_ret.append(0)
             result="%s,%s,%s"%(0,transcode_ret[1],transcode_ret[2])

          return result


      def transcode_real(self,source_file_name,result_path,field_count,default_delfield,string_del=None,file_just_name=None):
          source_file=open(source_file_name)

          result_file_path_name=result_path+'/'+file_just_name+self.ENCODE_OK_SUFFIX
          result_file=open(result_file_path_name,self.APPEND_WRITE_MODE)

          del_error_can_not_merge_file=result_path+'/'+file_just_name+self.ERROR_NOT_MERGE_SUFFIX
          del_error_can_not_merge=open(del_error_can_not_merge_file,self.APPEND_WRITE_MODE)

          del_error_can_merge_file=result_path+'/'+file_just_name+self.ERROR_MERGE_SUFFIX
          del_error_can_merge=open(del_error_can_merge_file,self.APPEND_WRITE_MODE)

          trans_error_file=result_path+'/'+file_just_name+self.TRAN_ERROR_SUFFIX
          trans_error=open(trans_error_file,self.APPEND_WRITE_MODE)

          except_file_1_file=result_path+'/'+file_just_name+self.EXCEPT1_FILE_SUFFIX
          except_file_1=open(except_file_1_file,self.APPEND_WRITE_MODE)

          except_file_list=[]
          except_file_list.append(except_file_1)
          del_count=field_count-1

          default_encoding=self.DEFAULT_DECODING
          src_encoding=default_encoding
          src_delfield=default_delfield
          
          trans_count=0L
          total_count=0L
          del_error_can_merge_count=0L
          merge_count=0L
          
          for line in source_file:
              total_count+=1
              source_del_count=self.field_count_def(line,src_delfield,string_del)-1
              try:
                 line_after_code=line.decode(src_encoding).encode(self.ENCODE_UTF8)
              except Exception, e:
                 line_after_code=self.split_transcodes(line,src_encoding,src_delfield,except_file_list,trans_error,field_count,string_del)
              if (self.field_count_def(line_after_code,src_delfield,string_del)-1)==del_count:
                  result_file.write(line_after_code)
                  trans_count+=1
                  self.error_list_flush(del_error_can_not_merge)
              #source del less
              elif source_del_count < del_count:
                  #print ' #source del less'
                  self.error_list.append(line)
                  if(len(self.error_list) > 1):
                     error_line=""
                     for i in self.error_list:
                         error_line=error_line+i

                     #print 'here error_line field count: %s,del_count;%s'%(error_line.count(src_delfield)+1,del_count)
                     if ((self.field_count_def(error_line,src_delfield,string_del)-1)==del_count):
                        error_line=error_line.replace("\n","")
                        error_line+="\n"
                        try:
                           line_after_code=error_line.decode(src_encoding).encode(self.ENCODE_UTF8)
                        except Exception, e:
                           line_after_code=self.split_transcodes(error_line,src_encoding,src_delfield,except_file_list,trans_error,field_count,string_del)
                        print 'line_line_after_code.count(src_delfield):%s,del_count;%s'%(self.field_count_def(line_after_code,src_delfield,string_del)-1,del_count)
                        # print line_after_code 
                        if(self.field_count_def(line_after_code,src_delfield,string_del)-1==del_count):
                           result_file.write(line_after_code)
                           trans_count+=1
                           merge_count+=1
                           if(len(self.error_list)>0):
                              for i in self.error_list:
                                 del_error_can_merge.write(i)
                                 del_error_can_merge_count+=1
                              del_error_can_merge.write(120 * "-"+"\n")
                              self.error_list=[]
                        else:
                           line_after_code=self.split_transcodes(error_line,src_encoding,src_delfield,except_file_list,trans_error,field_count,string_del)
                           if line_after_code!='error':
                              result_file.write(line_after_code)
                              trans_count+=1
                              merge_count+=1
                              if(len(self.error_list)>0):
                                 for i in self.error_list:
                                    del_error_can_merge.write(i)
                                    del_error_can_merge_count+=1
                                 del_error_can_merge.write(120 * "-"+"\n")
                                 self.error_list=[]
                           else:
                              self.fail_trans_count+=1
              #source del good
              elif(source_del_count==del_count):
                  #print '#source del good'
                  line_after_code=self.split_transcodes(line,src_encoding,src_delfield,except_file_list,trans_error,field_count,string_del)
                  #print "line_after_code:%s "%line_after_code
                  if line_after_code!='error':
                     result_file.write(line_after_code)
                     trans_count=trans_count+1
                     self.error_list_flush(del_error_can_not_merge)
                  else:
                     self.fail_trans_count+=1
              #source del more
              elif source_del_count>del_count:
                  #print '#source del more'
                  #print 'line_after_code:%s'%line_after_code
                  actual_field_count=self.field_count_def(line_after_code,src_delfield,string_del)
                  print 'field_count=%s  actual_field_count=%s'%(field_count,actual_field_count)
                  if field_count==actual_field_count:
                     trans_count+=1
                     result_file.write(line_after_code)
                  else:
                     self.fail_trans_count+=1
                     del_error_can_not_merge.write(line)
                     self.del_error_can_not_merge_count+=1
                     del_error_can_not_merge.write(120 * "-"+"\n")
                  self.error_list_flush(del_error_can_not_merge)
          self.error_list_flush(del_error_can_not_merge)

          source_file.close()
          result_file.close()
          del_error_can_not_merge.close()
          del_error_can_merge.close()
          trans_error.close()
          except_file_1.close()

          self.remove_null_file(del_error_can_not_merge_file)
          self.remove_null_file(del_error_can_merge_file)
          self.remove_null_file(trans_error_file)
          self.remove_null_file(except_file_1_file)

          print 'total_count:%s'%total_count
          print 'trans_count:%s'%trans_count
          print 'fail_trans_count:%s'%self.fail_trans_count
          print 'merge count:%s'%merge_count
          print 'del_error_can_merge_count:%s'%del_error_can_merge_count
          print 'del_error_can_not_merge_count:%s'%self.del_error_can_not_merge_count

          transcode_ret=[]
          transcode_ret.append(result_file_path_name)
          transcode_ret.append(total_count)
          transcode_ret.append(trans_count)
          transcode_ret.append(del_error_can_merge_count)
          transcode_ret.append(self.del_error_can_not_merge_count)

          return transcode_ret

      def remove_null_file(self,file_name):
          if os.path.exists(file_name):
             if os.path.getsize(file_name):
                print "file: %s exist but not null"%file_name
             else:
                os.remove(file_name)
                print "file: %s is null has remove"%file_name
          else:
             print "file: %s is not exist"%file_name
def main():
    parser = OptionParser()
    parser.add_option("-f", "--fileName",
                      dest='fileName',
                      default=None,
                      help='Necessary please input file name or path & file name')
    parser.add_option("-o", "--resultFileName",
                      dest='resultFileName',
                      default=None,
                      help='please output file  path ,default is current directory')
    parser.add_option("-n", "--num",
                      dest='num',
                      default=None,
                      help='please input field number')
    parser.add_option("-d", "--delfield",
                      dest='delfield',
                      default=None,
                      help='Necessary please input such as 27 ,default is None')
    parser.add_option("-s", "--delstring",
                      dest='delstring',
                      default=None,
                      help="please input string del such as 34  default is None ")
    parser.add_option("-a", "--source_encoding",
                      dest='source_encoding',
                      default="gb18030",
                      help="please input string del such as \"gb18030\" default is gb18030 ")
    parser.add_option("-b", "--des_encoding",
                      dest='des_encoding',
                      default="utf-8",
                      help="please input string del such as \"utf-8\"  default is utf-8 ")

    (options, args) = parser.parse_args()


    string_del= options.delstring
    if string_del is not None:
       string_del=chr(int(string_del))

    delfield=options.delfield
    if delfield is not None:
       delfield=chr(int(delfield))

    source_file = options.fileName

    source_encoding=options.source_encoding
    des_encoding=options.des_encoding
    trans=Transcode(source_encoding,des_encoding)

    num = options.num
    if num is None:
       ff=open(source_file)
       for line in ff:
           line_field_count=trans.field_count_def(line,delfield,string_del)
           break
       num=line_field_count
    else:
       num=int(num)
    print 'line_field_count:%s'%num

    if source_file.count("/")==0:
       source_file="%s/%s"%(os.getcwd(),source_file)
    file_just_name=source_file.split("/").pop().split('.')[0]
    file_name=source_file.split("/").pop()

    resultFileName=options.resultFileName
    if resultFileName is None:
       isExists=os.path.exists(file_just_name)
       if not isExists:
          os.makedirs(file_just_name)
       result_path="%s/%s"%(os.getcwd(),file_just_name)
    else:
       isExists=os.path.exists(resultFileName)
       if not isExists:
          os.makedirs(resultFileName)
       result_path=resultFileName

    begin =datetime.datetime.now()
    result=trans.transcode(source_file,result_path,num,delfield,string_del,file_name)
    end =datetime.datetime.now()
    
    print 'begin:%s '%begin
    print 'end:%s'%end
    print result
    return result
if __name__=='__main__':
    main()