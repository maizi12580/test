package com.sequoiadb.upsert.util;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Enumeration;

/**
 * Created by yang on 2017/4/26.
 */
public class LogUtils {

    private static final String LOGGER_ROOT = "com.sequoiadb.upsert";

    /**
     * 设置日志配置文件使用的环境变量
     * @param value
     * @return
     */
    public static void setEnv(String value) {
        System.setProperty("sdb.cs.cl", value);
    }

    /**
     * * 获取日志文件的绝对路径
     * @param appender appender name
     * @return
     */
    public static String logFileName(String appender) {
        return ((FileAppender) Logger.getLogger(LOGGER_ROOT).getAppender(appender)).getFile();
    }

    /**
     * 处理日志文件
     * 如果有报错，则提示日志文件名
     * 如果无报错，删除空文件
     * @param b 是否
     */
    public static void handleLogFile(boolean b) {
        File err = new File(logFileName("err"));
        File rec = new File(logFileName("rec"));
        if(b) {    //删除空文件
            closeLogger();
            err.delete();
            rec.delete();
        } else {
            System.out.println("see  " + err.getName() + " for upsert failure errors");
            System.out.println("see  " + rec.getName() + " for upsert failure records");
        }
    }

    public static void closeLogger() {
        Enumeration appenders = Logger.getLogger(LOGGER_ROOT).getAllAppenders();
        while (appenders.hasMoreElements()) {
            ((FileAppender)appenders.nextElement()).close();
        }
    }

}
