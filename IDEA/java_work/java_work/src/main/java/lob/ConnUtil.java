package lob;

import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.ConnectStrategy;
import com.sequoiadb.datasource.DatasourceOptions;
import com.sequoiadb.datasource.SequoiadbDatasource;

import java.util.ArrayList;
import java.util.List;

public class ConnUtil {
    /*单个连接*/
    public Sequoiadb getSdbConnect() {
        Sequoiadb sequoiadb = null;
        // addr, port, username, password
        sequoiadb = new Sequoiadb("192.168.30.6", 11810, "sdbadmin", "sdbadmin");
        return sequoiadb;
    }
    /*多个连接*/
    public Sequoiadb getSdbConnects(){
        ConfigOptions configOptions =  new ConfigOptions();
        List<String> connStrings = new ArrayList<String>();
        //connStrings.add("sdb1:11810");
        connStrings.add("192.168.30.6:11810");
        connStrings.add("192.168.30.7:11810");
        connStrings.add("192.168.30.8:11810");
        Sequoiadb sequoiadb = new Sequoiadb(connStrings,"","",configOptions);
        return sequoiadb;
    }
    /*连接池*/
    public SequoiadbDatasource getSdbConnectPoll() {
        ArrayList<String> addrs = new ArrayList<String>();
        String user = "sdbadmin";
        String password = "sdbadmin";
        ConfigOptions nwOpt = new ConfigOptions();
        DatasourceOptions dsOpt = new DatasourceOptions();
        SequoiadbDatasource ds = null;

        // 提供coord节点地址
        addrs.add("192.168.30.6:11810");
        addrs.add("192.168.30.7:11810");
        addrs.add("192.168.30.8:11810");

        // 设置网络参数
        nwOpt.setConnectTimeout(500); // 建连超时时间为500ms。
        nwOpt.setMaxAutoConnectRetryTime(0); // 建连失败后重试时间为0ms。

        // 设置连接池参数
        dsOpt.setMaxCount(500); // 连接池最多能提供500个连接。
        dsOpt.setDeltaIncCount(20); // 每次增加20个连接。
        dsOpt.setMaxIdleCount(20); // 连接池空闲时，保留20个连接。
        dsOpt.setKeepAliveTimeout(0); // 池中空闲连接存活时间。单位:毫秒。
        // 0表示不关心连接隔多长时间没有收发消息。
        dsOpt.setCheckInterval(60 * 1000); // 每隔60秒将连接池中多于
        // MaxIdleCount限定的空闲连接关闭，
        // 并将存活时间过长（连接已停止收发
        // 超过keepAliveTimeout时间）的连接关闭。
        dsOpt.setSyncCoordInterval(0); // 向catalog同步coord地址的周期。单位:毫秒。
        // 0表示不同步。
        dsOpt.setValidateConnection(false); // 连接出池时，是否检测连接的可用性，默认不检测。
        dsOpt.setConnectStrategy(ConnectStrategy.BALANCE); // 默认使用coord地址负载均衡的策略获取连接。
        ds = new SequoiadbDatasource(addrs, user, password, nwOpt, dsOpt);
        return ds;
    }
}
