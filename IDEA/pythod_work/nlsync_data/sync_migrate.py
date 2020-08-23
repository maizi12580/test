#!/usr/bin/python
#coding=utf-8
import base64
from sync_os import *
from sync_logger import *
import datetime
import base64


class SyncMigrate:
    def __init__(self, hostname, svcport, username, password, hosts, log_handler):
        self.import_cmd = '/opt/sequoiadb/bin/sdbimprt'
        self.export_cmd = '/opt/sequoiadb/bin/sdbexprt'
        # import and export type is constant
        self.migrate_type = "csv"
        # connection options
        self.host_name = hostname
        self.import_hosts = hosts
        self.svc_name = svcport
        self.user = username
        self.password = base64.decodestring(password)
        # log handler
        self.log = log_handler
        # other options is defaut for import and export
        # import options blow:
        self.ipr_delchar = "\\127\\28\\29\\127\\30\\31\\127"  # string delimiter, must have
        self.ipr_delfield = "\\27"                            # field delimiter, must have
        self.extra= "true"                                    # batch insert records number, must have
        self.insertnum = "5000"                               # batch insert records number, must have
        self.jobs = "16"                                      # importing job number, must have
        self.trim = "both"                                    # trim string, must have
        self.allowkeydup = "false"                            # allow key duplication, must have
        # export options blow:
        self.exp_delfield = "0x1b"
        self.exp_delchar = "0x1d"
        self.included = "false"                               # batch insert records number, must have
        self.filelimit = "30G"                                # importing job number, must have

    def formate_fields(self, meta_record, etldate_key, etldate_fm, sync_date,add_fields_list=None):
        # get options
        etl_date_key = etldate_key
        etl_date_fm = etldate_fm
        loc_sync_dt = sync_date
        tbl_meta_rd = meta_record
        default_value_map = dict()
        # 设置tx_date的默认值
        default_value_map[etldate_key] = datetime.datetime.strptime(loc_sync_dt,'%Y%m%d').strftime(etl_date_fm)
        if add_fields_list is not None and len(add_fields_list) > 0:
            for add_field_item in add_fields_list:
                field_info = add_field_item.split(":")
                # 获取add_fields默认值，导入是设置
                if len(field_info) == 2:
                    if field_info[1].find("%s") != -1:
                        default_value_map[field_info[0].strip()] = field_info[1].strip() % loc_sync_dt
                    else:
                        default_value_map[field_info[0].strip()] = field_info[1].strip()
                # 如果有三个字段，一定要求作为抽数日期字段
                # 如果配置了抽数日期字段，使用配置的字段
                elif len(field_info) == 3:
                    del default_value_map[etldate_key]
                    etlvalue = datetime.datetime.strptime(loc_sync_dt,'%Y%m%d').strftime(field_info[2].strip()) 
                    default_value_map[field_info[0].strip()] = etlvalue

        self.log.info("fields have default value : %s " % str(default_value_map))
        # get table's meta keys and sort by field name, such as "field01", "field02"...
        meta_keys = tbl_meta_rd.keys()
        field_keys = [0] * len(meta_keys)
        for meta_key in meta_keys:
            if 'field' == meta_key[:5]:
                key = int(meta_key.split("field")[1])
                field_keys[key-1]= meta_key
                
        # sort
        #field_keys.sort()
        print "fields: %s" % field_keys

        loc_date_fm = datetime.datetime.strptime(loc_sync_dt, '%Y%m%d')
        loc_date = loc_date_fm.strftime(etl_date_fm)
        fields = ''
        cnt = 0
        for field in field_keys:
            if 0 == field:   
                continue
            field_value = tbl_meta_rd.get(field)
            field_arr = field_value.split('|')
            # don't need care about the length of array, just add extend length
            field_name = ('{:<%s}' % (len(field_arr[0]) + 10)).format(field_arr[0])
            # assignment "" when type is string
            if 'string' == field_arr[1]:       
                # 如果字段在map中就是用map中的value作为默认值
                if field_name.strip() in default_value_map.keys():
                    field_type = field_arr[1] + ' default "%s"' % default_value_map[field_name.strip()]
                else:
                    field_type = field_arr[1] + ' default ""'
            else:
                field_type = field_arr[1]

            # the last key don't have symbol ','
            if cnt == len(field_keys) - 1:
                field_type += '\n'
            else:
                field_type += ',\n'

            field_str = '%s%s' % (field_name, field_type)
            fields += field_str
            cnt += 1

        self.log.info("sync table's fields info: %s" % fields)
        return fields

    def mig_import(self, cs_name, cl_name, file_name, fields, **kwargs):
        """Export data to sdb from file

        Parameters:
            Name         Type     Info:
            cs_name      str      The sdb collection space name
            cl_name      str      The sdb collection name
            file_name    str      The data file name with absolute path
            fields       str      The field name, separated by comma (',')
            **kwargs              Useful option are below
            - s          str      Host name, default: localhost
            - p          str      Service name, default: 11810
            - type       str      The type of record to load,
                                      default: csv (json,csv)
            - e          str      The field delimiter, default: '\27' ( csv only )
            - a          str      The string delimiter, default: '\44' ( csv only )
            - u          str      The sdb username, default: 'sdbapp'
            - w          str      The password, default: 'kfptSDB2016'
            - n          int      The batch insert records number, minimun 1,
                                      maximum 100000, default: 100
            - j          int      The importing job num at once, default: 1
            - trim       str      The trim string (arg: [no|right|left|both]),
                                      default: both
        Exceptions:
             pysequoiadb.error.SDBTypeError
        """
        # import command
        import_cmd = self.import_cmd

        # get options
        if "hostname" in kwargs:
            if not isinstance(kwargs.get("hostname"), basestring):
                raise "hostname must be an instance of string"
            else:
                host_name = kwargs.get("hostname")
        else:
            host_name = self.host_name

        if "svcname" in kwargs:
            if not isinstance(kwargs.get("svcname"), basestring):
                raise "svcname must be an instance of string"
            else:
                svc_name = kwargs.get("svcname")
        else:
            svc_name = self.svc_name

        if "hosts" in kwargs:
            if not isinstance(kwargs.get("hosts"), basestring):
                raise "hosts must be an instance of string"
            else:
                import_hosts = kwargs.get("hosts")
        else:
            import_hosts = self.import_hosts

        if "user" in kwargs:
            if not isinstance(kwargs.get("user"), basestring):
                raise "user must be an instance of string"
            else:
                user_name = kwargs.get("user")
        else:
            user_name = self.user

        if "password" in kwargs:
            if not isinstance(kwargs.get("password"), basestring):
                raise "password must be an instance of string"
            else:
                password = kwargs.get("password")
        else:
            password = self.password

        if "csname" in kwargs:
            if not isinstance(kwargs.get("csname"), basestring):
                raise "csname must be an instance of string"
            else:
                cs_name = kwargs.get("csname")
        else:
            cs_name = cs_name

        if "clname" in kwargs:
            if not isinstance(kwargs.get("clname"), basestring):
                raise "clname must be an instance of string"
            else:
                cl_name = kwargs.get("clname")
        else:
            cl_name = cl_name

        if "type" in kwargs:
            if not isinstance(kwargs.get("type"), int):
                raise "type must be an instance of integer"
            else:
                import_type = kwargs.get("type")
        else:
            import_type = self.migrate_type

        if "file" in kwargs and file_name is None:
            if not isinstance(kwargs.get("file"), basestring):
                raise "file must be an instance of string"
            else:
                file_name = kwargs.get("file")
        else:
            file_name = file_name

        if "delchar" in kwargs:
            if not isinstance(kwargs.get("delchar"), basestring):
                raise "delchar must be an instance of string"
            else:
                del_char = "\\" + kwargs.get("delchar")
        else:
            del_char = self.ipr_delchar

        if "delfield" in kwargs:
            if not isinstance(kwargs.get("delfield"), basestring):
                raise "delfield must be an instance of string"
            else:
                del_field = "\\" + kwargs.get("delfield")
        else:
            del_field = self.ipr_delfield

        if "extra" in kwargs:
            if not isinstance(kwargs.get("extra"), basestring):
                raise "extra must be an instance of string"
            else:
                insert_num = kwargs.get("extra")
        else:
            insert_num = self.extra

        if "insertnum" in kwargs:
            if not isinstance(kwargs.get("insertnum"), basestring):
                raise "insertnum must be an instance of string"
            else:
                insert_num = kwargs.get("insertnum")
        else:
            insert_num = self.insertnum

        if "jobs" in kwargs:
            if not isinstance(kwargs.get("jobs"), basestring):
                raise "jobs must be an instance of string"
            else:
                jobs = kwargs.get("jobs")
        else:
            jobs = self.jobs

        if "trim" in kwargs:
            if not isinstance(kwargs.get("trim"), basestring):
                raise "trim must be an instance of string"
            else:
                trim = kwargs.get("trim")
        else:
            trim = self.trim

        if "allowkeydup" in kwargs:
            if not isinstance(kwargs.get("allowkeydup"), basestring):
                raise "allowkeydup must be an instance of string"
            else:
                allow_keydup = kwargs.get("allowkeydup")
        else:
            allow_keydup = self.allowkeydup

        if "fields" in kwargs and fields is None:
            if not isinstance(kwargs.get("fields"), basestring):
                raise "fields must be an instance of string"
            else:
                fields = kwargs.get("fields")
        else:
            fields = fields

        # the command line must have options
        import_cmd_line = '%s --hosts \'%s\' -c \'%s\' -l \'%s\' --file \'%s\''\
                          ' --fields \'\n%s\' --type \'%s\' -a \'%s\' -e \'%s\''\
                          ' -u \'%s\' -w \'%s\' -j \'%s\' -n \'%s\' --trim \'%s\''\
                          ' --allowkeydup \'%s\''\
                          ' --extra \'%s\'' % (import_cmd, import_hosts, cs_name, cl_name,
                                               file_name, fields, import_type, del_char,
                                               del_field, user_name, password, jobs,
                                               insert_num, trim, allow_keydup, self.extra)

        if "errorstop" in kwargs:
            if not isinstance(kwargs.get("errorstop"), basestring):
                raise "errorstop must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --errorstop ' + kwargs.get("errorstop")

        if "ssl" in kwargs:
            if not isinstance(kwargs.get("ssl"), basestring):
                raise "ssl must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --ssl ' + kwargs.get("ssl")

        if "exec" in kwargs:
            if not isinstance(kwargs.get("exec"), basestring):
                raise "exec must be an instance of string"
            else:
                print "don't support execute out code for sdbimprt, --exec is invalid options"

        if "linepriority" in kwargs:
            if not isinstance(kwargs.get("linepriority"), basestring):
                raise "linepriority must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --linepriority ' + kwargs.get("linepriority")

        if "delrecord" in kwargs:
            if not isinstance(kwargs.get("delrecord"), basestring):
                raise "delrecord must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --delrecord ' + kwargs.get("delrecord")

        if "force" in kwargs:
            if not isinstance(kwargs.get("force"), int):
                raise "force must be an instance of integer"
            else:
                import_cmd_line = import_cmd_line + ' --force ' + kwargs.get("force")

        if "datefmt" in kwargs:
            if not isinstance(kwargs.get("datefmt"), basestring):
                raise "datefmt must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --datefmt ' + kwargs.get("datefmt")

        if "timestampfmt" in kwargs:
            if not isinstance(kwargs.get("timestampfmt"), basestring):
                raise "timestampfmt must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --timestampfmt ' + kwargs.get("timestampfmt")

        if "headerline" in kwargs:
            if not isinstance(kwargs.get("headerline"), basestring):
                raise "headerline must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --headerline ' + kwargs.get("headerline")

        if "sparse" in kwargs:
            if not isinstance(kwargs.get("sparse"), basestring):
                raise "sparse must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --sparse ' + kwargs.get("sparse")

        if "extra" in kwargs:
            if not isinstance(kwargs.get("extra"), basestring):
                raise "extra must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --extra ' + kwargs.get("extra")

        if "cast" in kwargs:
            if not isinstance(kwargs.get("cast"), basestring):
                raise "cast must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --cast ' + kwargs.get("cast")

        if "coord" in kwargs:
            if not isinstance(kwargs.get("coord"), basestring):
                raise "coord must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --coord ' + kwargs.get("coord")

        if "sharding" in kwargs:
            if not isinstance(kwargs.get("sharding"), basestring):
                raise "sharding must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --sharding ' + kwargs.get("sharding")

        if "transaction" in kwargs:
            if not isinstance(kwargs.get("transaction"), basestring):
                raise "transaction must be an instance of string"
            else:
                import_cmd_line = import_cmd_line + ' --transaction ' + kwargs.get("transaction")

        sync_os = SyncOS(self.log)
        file_home = sync_os.get_dirname(file_name)
        full_import_cmdline = 'cd %s; %s' % (file_home, import_cmd_line)
        # command line running
        self.log.info("sdbimport execute command line: %s" % full_import_cmdline)
        ret = sync_os.cmd_run(full_import_cmdline)
        self.log.info("sdbimport return value: %s" % ret)
        rec_file = None
        import_rets = ret[1].split("\n")
        if 7 == len(import_rets):
            rec_file = file_home + "/" + import_rets[6].split(" ")[1]
            self.log.warn("sdbimport failed, rec file: %s" % rec_file)

        return rec_file

    def mig_export(self, cs_name, cl_name, file_name, matcher, **kwargs):
        """Export data to sdb from file

        Parameters:
            Name         Type     Info:
            cs_name      str      The sdb collection space name
            cl_name      str      The sdb collection name
            file_name    str      The data file name with absolute path
            fields       str      The field name, separated by comma (',')
            **kwargs              Useful option are below
            - s          str      Host name, default: localhost
            - p          str      Service name, default: 11810
            - type       str      The type of record to load,
                                      default: csv (json,csv)
            - e          str      The field delimiter, default: '\27' ( csv only )
            - a          str      The string delimiter, default: '\44' ( csv only )
            - u          str      The sdb username, default: 'sdbapp'
            - w          str      The password, default: 'kfptSDB2016'
            - n          int      The batch insert records number, minimun 1,
                                      maximum 100000, default: 100
            - j          int      The importing job num at once, default: 1
            - trim       str      The trim string (arg: [no|right|left|both]),
                                      default: both
        Exceptions:
             pysequoiadb.error.SDBTypeError
        """
        # import command
        export_cmd = self.export_cmd

        # get options
        if "hostname" in kwargs:
            if not isinstance(kwargs.get("hostname"), basestring):
                raise "hostname must be an instance of string"
            else:
                host_name = kwargs.get("hostname")
        else:
            host_name = self.host_name

        if "svcname" in kwargs:
            if not isinstance(kwargs.get("svcname"), basestring):
                raise "svcname must be an instance of string"
            else:
                svc_name = kwargs.get("svcname")
        else:
            svc_name = self.svc_name

        if "user" in kwargs:
            if not isinstance(kwargs.get("user"), basestring):
                raise "user must be an instance of string"
            else:
                user_name = kwargs.get("user")
        else:
            user_name = self.user

        if "password" in kwargs:
            if not isinstance(kwargs.get("password"), basestring):
                raise "password must be an instance of string"
            else:
                password = kwargs.get("password")
        else:
            password = self.password

        if "csname" in kwargs:
            if not isinstance(kwargs.get("csname"), basestring):
                raise "csname must be an instance of string"
            else:
                cs_name = kwargs.get("csname")
        else:
            cs_name = cs_name

        if "clname" in kwargs:
            if not isinstance(kwargs.get("clname"), basestring):
                raise "clname must be an instance of string"
            else:
                cl_name = kwargs.get("clname")
        else:
            cl_name = cl_name

        if "type" in kwargs:
            if not isinstance(kwargs.get("type"), basestring):
                raise "type must be an instance of integer"
            else:
                import_type = kwargs.get("type")
        else:
            import_type = self.migrate_type

        if "file" in kwargs and file_name is None:
            if not isinstance(kwargs.get("file"), basestring):
                raise "file must be an instance of string"
            else:
                file_name = kwargs.get("file")
        else:
            file_name = file_name

        if "delchar" in kwargs:
            if not isinstance(kwargs.get("delchar"), basestring):
                raise "delchar must be an instance of string"
            else:
                del_char = kwargs.get("delchar")
        else:
            del_char = self.exp_delchar

        if "delfield" in kwargs:
            if not isinstance(kwargs.get("delfield"), basestring):
                raise "delfield must be an instance of string"
            else:
                del_field = kwargs.get("delfield")
        else:
            del_field = self.exp_delfield

        if "filelimit" in kwargs:
            if not isinstance(kwargs.get("filelimit"), basestring):
                raise "jobs must be an instance of string"
            else:
                file_limit = kwargs.get("filelimit")
        else:
            file_limit = self.filelimit

        if "fields" in kwargs:
            if not isinstance(kwargs.get("fields"), basestring):
                raise "fields must be an instance of string"
            else:
                fields = kwargs.get("fields")

        if "filter" in kwargs:
            if not isinstance(kwargs.get("filter"), basestring):
                raise "filter must be an instance of string"
            else:
                filter = eval(kwargs.get("filter"))
                if eval(matcher).keys()[0] in filter:
                    filter.update(eval(matcher)) 
                    matcher = str(filter)
                else:
                    matcher = kwargs.get("filter") 
        else:
            matcher = matcher
        if "included" in kwargs:
            if not isinstance(kwargs.get("included"), basestring):
                raise "included must be an instance of string"
            else:
                included = kwargs.get("included")
        else:
            included = self.included
        # the command line must have options
        export_cmd_line = '%s -s \'%s\' -p \'%s\' -c \'%s\' -l \'%s\' --file \'%s\''\
                          ' --fields \'%s\' --type \'%s\' -a \'%s\' -e \'%s\''\
                          ' -u \'%s\' -w \'%s\' --filelimit \'%s\' --filter "%s" '\
                          ' --included \'%s\' --replace true'\
                          % (export_cmd, host_name, svc_name, cs_name, cl_name,
                             file_name, fields, import_type, del_char, del_field,
                             user_name, password, file_limit, matcher, included)

        if "delrecord" in kwargs:
            if not isinstance(kwargs.get("delrecord"), basestring):
                raise "delrecord must be an instance of string"
            else:
                export_cmd_line = export_cmd_line + ' --delrecord ' + kwargs.get("delrecord")

        if "withid" in kwargs:
            if not isinstance(kwargs.get("withid"), basestring):
                raise "withid must be an instance of string"
            else:
                export_cmd_line = export_cmd_line + ' --withid ' + kwargs.get("withid")

        if "errorstop" in kwargs:
            if not isinstance(kwargs.get("errorstop"), basestring):
                raise "errorstop must be an instance of string"
            else:
                export_cmd_line = export_cmd_line + ' --errorstop ' + kwargs.get("errorstop")

        if "ssl" in kwargs:
            if not isinstance(kwargs.get("ssl"), basestring):
                raise "ssl must be an instance of string"
            else:
                export_cmd_line += ' --ssl ' + kwargs.get("ssl")

        if "select" in kwargs:
            if not isinstance(kwargs.get("select"), basestring):
                raise "select must be an instance of string"
            else:
                export_cmd_line += ' --select ' + kwargs.get("select")

        if "sort" in kwargs:
            if not isinstance(kwargs.get("sort"), basestring):
                raise "sort must be an instance of string"
            else:
                export_cmd_line += ' --sort ' + kwargs.get("sort")

        if "skip" in kwargs:
            if not isinstance(kwargs.get("skip"), basestring):
                raise "skip must be an instance of string"
            else:
                export_cmd_line += ' --skip ' + kwargs.get("skip")

        if "limit" in kwargs:
            if not isinstance(kwargs.get("limit"), basestring):
                raise "limit must be an instance of string"
            else:
                export_cmd_line += ' --limit ' + kwargs.get("limit")

        if "cscl" in kwargs:
            if not isinstance(kwargs.get("cscl"), basestring):
                raise "cscl must be an instance of string"
            else:
                print "don't support option --cscl"

        if "excludecscl" in kwargs:
            if not isinstance(kwargs.get("excludecscl"), basestring):
                raise "excludecscl must be an instance of string"
            else:
                print "don't support option --excludecscl"

        if "dir" in kwargs:
            if not isinstance(kwargs.get("dir"), basestring):
                raise "dir must be an instance of string"
            else:
                print "don't support option --dir"

        if "includebinary" in kwargs:
            if not isinstance(kwargs.get("includebinary"), basestring):
                raise "includebinary must be an instance of string"
            else:
                export_cmd_line += ' --includebinary ' + kwargs.get("includebinary")

        if "includeregex" in kwargs:
            if not isinstance(kwargs.get("includeregex"), basestring):
                raise "includebinary must be an instance of string"
            else:
                export_cmd_line += ' --includeregex ' + kwargs.get("includeregex")

        if "force" in kwargs:
            if not isinstance(kwargs.get("force"), int):
                raise "force must be an instance of integer"
            else:
                export_cmd_line += ' --force ' + kwargs.get("force")

        if "kicknull" in kwargs:
            if not isinstance(kwargs.get("kicknull"), int):
                raise "kicknull must be an instance of integer"
            else:
                export_cmd_line += ' --kicknull ' + kwargs.get("kicknull")

        if "conf" in kwargs:
            if not isinstance(kwargs.get("conf"), basestring):
                raise "conf must be an instance of string"
            else:
                print "don't support option --conf"

        if "genconf" in kwargs:
            if not isinstance(kwargs.get("genconf"), basestring):
                raise "genconf must be an instance of string"
            else:
                print "don't support option --genconf"

        if "genfields" in kwargs:
            if not isinstance(kwargs.get("genfields"), basestring):
                raise "genfields must be an instance of string"
            else:
                print "don't support option --genfields"

        sync_os = SyncOS(self.log)
        file_home = sync_os.get_dirname(file_name)
        full_export_cmdline = 'cd %s; %s' % (file_home, export_cmd_line)
        self.log.info("sdbexport execute command line: %s" % full_export_cmdline)
        # command line running
        ret = sync_os.cmd_run(full_export_cmdline)
        self.log.info("sdbexport return info: %s" % ret)

        return ret

def test_main():
    host_name = "a-hqp-nlsdb01"
    svc_port = "11810"
    user_name = "nlsdbapp"
    password = "a2ZwdE5MU0RCMjAxNw=="
    hosts = "a-hqp-nlsdb01:11810,a-hqp-nlsdb02:11810,a-hqp-nlsdb03:11810,a-hqp-nlsdb04:11810,a-hqp-nlsdb05:11810,a-hqp-nlsdb06:11810,a-hqp-nlsdb07:11810,a-hqp-nlsdb08:11810,a-hqp-nlsdb09:11810,a-hqp-nlsdb10:11810,a-hqp-nlsdb11:11810,a-hqp-nlsdb12:11810,a-hqp-nlsdb13:11810,a-hqp-nlsdb14:11810,a-hqp-nlsdb15:11810,a-hqp-nlsdb16:11810,a-hqp-nlsdb17:11810,a-hqp-nlsdb18:11810,a-hqp-nlsdb19:11810,a-hqp-nlsdb20:11810,a-hqp-nlsdb21:11810,a-hqp-nlsdb22:11810,a-hqp-nlsdb23:11810,a-hqp-nlsdb24:11810,a-hqp-nlsdb25:11810,a-hqp-nlsdb26:11810,a-hqp-nlsdb27:11810,a-hqp-nlsdb28:11810,a-hqp-nlsdb29:11810,a-hqp-nlsdb30:11810"
    connect_hosts = [{"host": "a-hqp-nlsdb01", "service":11810},{"host": "a-hqp-nlsdb02", "service":11810},{"host": "a-hqp-nlsdb03", "service":11810}]
    """
    logging file
    """
    sync_log_cs = "nlsync"
    sync_log_cl = "log"
    sync_sys = "SMC"
    #cs_name = "smc"
    #cl_name = "gcpocrd"
    cs_name = "nl999smc"
    cl_name = "gcpocrd_trn1"
    tbl_name = "smc.gcpocrd"
    tbl_cond = {'tbl_name': tbl_name}
    log_connect = {'HostName': host_name, 'ServerPort': svc_port,
                   'UserName': user_name, 'Password': password,
                   'CsName': sync_log_cs, 'ClName': sync_log_cl}

    sync_date = "20180202"
    log_table = {'sync_sys': sync_sys, 'tbl_name': tbl_name, 'sync_dt': sync_date}
    log = logging.getLogger("sync_migrate")
    log.setLevel(logging.INFO)
    logfile_name = logfile_dir
    fh = SCFileHandler(logfile_name, log_table, log_connect)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    log.addHandler(fh)
    sync_mig = SyncMigrate(host_name, svc_port, user_name, password, hosts, log)
    # import test
    file_name = "/sdbdata/data01/etldata/odsdata/transcode/GCPOCRD_ALL.utf8"
    db = SCSDB(host_name, svc_port, user_name, password, connect_hosts)
    meta_records = db.sync_query("mdm", "metatbl", tbl_cond)
    meta_record = meta_records[0]
    fields = sync_mig.formate_fields(meta_record, "tx_date", "%Y-%m-%d", "20180202")
    rec_file = sync_mig.mig_import(cs_name, cl_name, file_name, fields)
    print 'import ------: %s' % rec_file

    # export test
    #export_file = "/data03/etldata/odsdata/20170226/CBE/BPTFHIST_EX.datutf8"
    #export_fields = "ac_dt,jrnno,jrn_seq,vchno,tx_br,his_update_ind,tx_date"
    #matcher = '{"tx_date": "2017-02-26"}'
    #sync_mig.mig_export(cs_name, cl_name, export_file, export_fields, matcher)

if __name__ == "__main__":
    test_main()

