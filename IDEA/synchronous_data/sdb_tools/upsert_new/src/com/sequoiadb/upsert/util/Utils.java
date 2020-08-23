package com.sequoiadb.upsert.util;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.base.SequoiadbDatasource;
import com.sequoiadb.datasource.ConnectStrategy;
import com.sequoiadb.datasource.DatasourceOptions;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.net.ConfigOptions;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Utils {

	private static SequoiadbDatasource ds = null;
	
	private static String 			collectionSpace		= null; 
	private static String 			collection			= null;
	private static String[] 		condFields 			= null;
	private static String 			hint 				= null;
	private static String 			fileEncoding 		= null;
    private static boolean          checkFileEncoding   = false;

	private static int 				retriesWhenDupKey 	= 0;
	private static int 				threadSize			= 0;
	private static int 				dataCache			= 0;
	private static int 				printLogPerSecond	= 0;

	/**
	 * 读取配置文件，分别赋给对应变量，方便在其它类中获取
	 * @param prop	配置文件位置
	 */
	public static void init(String prop){
    	InputStream in = null;
    	try {
	        in = Utils.class.getClassLoader().getResourceAsStream(prop);
	    	Properties p = new Properties();
			p.load(in);
			
			List<String> addrs = new ArrayList<String>();
	        ConfigOptions nwOpt = new ConfigOptions();			//设置建立连接的各项参数
	        DatasourceOptions dsOpt = new DatasourceOptions();	//设置连接池的各种参数
	        
	        // 获取coord节点地址 
	        String[] addrString = p.getProperty("croodAddrs").split(",");
	        for (String addr : addrString) {
	        	addrs.add(addr);
	        	System.out.println("crood:"+addr);
			}
	        
	        // 设置网络参数
	        nwOpt.setConnectTimeout(Integer.parseInt(p.getProperty("connectTimeout", "500")));                      	// 建连超时时间为500ms
	        nwOpt.setMaxAutoConnectRetryTime(Integer.parseInt(p.getProperty("maxAutoConnectRetryTime", "0")));          // 建连失败后重试时间为0ms
	        // 设置连接池参数
	        dsOpt.setMaxCount(Integer.parseInt(p.getProperty("maxCount", "500")));                            			// 连接池最多能提供500个连接
	        dsOpt.setDeltaIncCount(Integer.parseInt(p.getProperty("deltaIncCount", "20")));                        		// 每次增加20个连接
	        dsOpt.setMaxIdleCount(Integer.parseInt(p.getProperty("maxIdleCount", "20")));                        		// 连接池空闲时，保留20个连接
	        dsOpt.setKeepAliveTimeout(Integer.parseInt(p.getProperty("keepAliveTimeout", "0")));                      	// 池中空闲连接存活时间。单位:毫秒。
            																											// 0表示不关心连接隔多长时间没有收发消息
	        //每隔60秒将连接池中多于MaxIdleCount限定的空闲连接关闭，并将存活时间过长（连接已停止收发超过keepAliveTimeout时间）的连接关闭
	        dsOpt.setCheckInterval(Integer.parseInt(p.getProperty("checkInterval", "6000")));                 			
	        dsOpt.setSyncCoordInterval(Integer.parseInt(p.getProperty("syncCoordInterval", "0")));                      // 向catalog同步coord地址的周期。单位:毫秒。0表示不同步
	        dsOpt.setValidateConnection(Boolean.parseBoolean(p.getProperty("validateConnection", "false")));            // 连接出池时，是否检测连接的可用性，默认不检测
	        dsOpt.setConnectStrategy(ConnectStrategy.valueOf(p.getProperty("connectStrategy", "BALANCE"))); 			// 默认使用coord地址负载均衡的策略获取连接
	        // 建立连接池
	        String user = p.getProperty("user", "sdbadmin");
			String password = p.getProperty("password","");
	        ds = new SequoiadbDatasource(addrs, user, password, nwOpt, dsOpt);
	        
	        
	        collectionSpace    = 	p.getProperty("collectionSpace");
			collection 		   = 	p.getProperty("collection");
			condFields 		   = 	p.getProperty("condFields").split(",");
			hint 			   = 	p.getProperty("hint");

			fileEncoding	   =  	p.getProperty("fileEncoding");
            checkFileEncoding  =    Boolean.parseBoolean(p.getProperty("checkFileEncoding"));

			retriesWhenDupKey  = 	Integer.parseInt(p.getProperty("retriesWhenDupKey"));
			threadSize	 	   = 	Integer.parseInt(p.getProperty("threadSize"));
			dataCache	 	   = 	Integer.parseInt(p.getProperty("dataCache"));
   			printLogPerSecond  = 	Integer.parseInt(p.getProperty("printLogPerSecond"));

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}finally{
			if(in != null){
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
    }

    public static Sequoiadb getConnection() {
        try {
			return ds.getConnection();
		} catch (BaseException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
    }

    public static void disconnect(Sequoiadb sdb) {
//    	sdb.disconnect();
    	ds.releaseConnection(sdb);
    }
    
    public static void closeDataSource(){
    	ds.close();
    }
    
    public static String getCollectionSpace() {
		return collectionSpace;
	}

	public static String getCollection() {
		return collection;
	}

	public static String[] getCondFields() {
		return condFields;
	}

	public static String getHint() {
		return hint;
	}

	public static String getFileEncoding() {
		return fileEncoding;
	}

    public static boolean isCheckFileEncoding() {
        return checkFileEncoding;
    }

    public static int getRetriesWhenDupKey() {
		return retriesWhenDupKey;
	}

	public static int getThreadSize() {
		return threadSize;
	}

	public static int getDataCache() {
		return dataCache;
	}

	public static int getPrintLogPerSecond() {
		return printLogPerSecond;
	}



	/**\
	 * 将字符串转换成二进制字符串，以空格相隔
	 * @param str
	 * @return
	 */
	public static String strToHexStr(String str) {
		char[] strChar = str.toCharArray();
		String result = "";
		for (int i = 0; i < strChar.length; i++) {
			result += Integer.toHexString(strChar[i]) + " ";
		}
		return result;
	}
}
