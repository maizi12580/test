package Test;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.SequoiadbDatasource;
import lob.ConnUtil;
import org.bson.BasicBSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;


public class Util {
    private static class InstanceHolder{
        private final static ConnUtil INSTANCE = new ConnUtil();
    }
    private static ConnUtil getInstance(){
        return InstanceHolder.INSTANCE;
    }

    private  static SequoiadbDatasource sequoiadbDatasource = null;

    public Sequoiadb getConnect(){
        ConnUtil connUtil = getInstance();
        //从连接池获取连接
        sequoiadbDatasource= connUtil.getSdbConnectPoll();
        Sequoiadb sequoiadb  = null;
        try {
            sequoiadb = sequoiadbDatasource.getConnection();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sequoiadb;
    }
    public CollectionSpace makeCS( String csName,String Domain){
        Sequoiadb sequoiadb = null;
        sequoiadb = getConnect();
        CollectionSpace collectionSpace = null ;
        BasicBSONObject csOptions = new BasicBSONObject();
        csOptions.put("PageSize",Sequoiadb.SDB_PAGESIZE_64K);
        csOptions.put("Domain",Domain);
        csOptions.put("LobPageSize", 4096);
        if (sequoiadb.isCollectionSpaceExist(csName)){
            collectionSpace = sequoiadb.getCollectionSpace(csName);
        }else
            collectionSpace = sequoiadb.createCollectionSpace(csName,csOptions);
        //release pool
        sequoiadbDatasource.releaseConnection(sequoiadb);
        return collectionSpace;
    }

    /* make the common cl */
    public void crtComCL(String csName,String clName,String domain){
        Sequoiadb sequoiadb = null;
        sequoiadb = getConnect();
        //make CS
        CollectionSpace collectionSpace = null;
        collectionSpace = makeCS(csName,domain);
        //make CL
        DBCollection collection = null;
        BasicBSONObject clOptions = new BasicBSONObject();
        clOptions.put("ShardingKey", new BasicBSONObject("id", 1));
        clOptions.put("ShardingType", "hash");
        clOptions.put("Compressed", true);
        clOptions.put("CompressionType", "lzw");
        clOptions.put("AutoSplit", true);
        if(collectionSpace.isCollectionExist(clName)){
            collection = collectionSpace.getCollection(clName);
        }else
            collection = collectionSpace.createCollection(clName,clOptions);
        //release pool
        sequoiadbDatasource.releaseConnection(sequoiadb);
    }

    /* make the Main cl */
    public void crtMainCL(String csName,String clName,String domainName){
        Sequoiadb sequoiadb = null;
        sequoiadb = getConnect();
        //make CS
        CollectionSpace collectionSpace = null;
        collectionSpace = makeCS(csName,domainName);
        //make mainCL
        DBCollection collection = null;
        BasicBSONObject mainOptions = new BasicBSONObject();
        // 以字段date作为主集合分区键，此字段为日期类型,单一子表为_id
        mainOptions.put("ShardingKey", new BasicBSONObject("date", 1));
        mainOptions.put("ShardingType", "range");
        mainOptions.put("IsMainCL", true);
        if(collectionSpace.isCollectionExist(clName)){
            collection = collectionSpace.getCollection(clName);
        }else
            collection = collectionSpace.createCollection(clName,mainOptions);
        //release pool
        sequoiadbDatasource.releaseConnection(sequoiadb);
    }
    /*make subCL*/
    public void  crtSubCL(String csName,String clName,String domainName){
        Sequoiadb sequoiadb = getConnect();
        //make CS
        CollectionSpace collectionSpace = null;
        collectionSpace = makeCS(csName,domainName);
        //make SubCL
        DBCollection collection = null;
        BasicBSONObject subOptions = new BasicBSONObject();
        subOptions.put("ShardingKey", new BasicBSONObject("id", 1));
        subOptions.put("ShardingType", "hash");
        if(collectionSpace.isCollectionExist(clName)){
            collection = collectionSpace.getCollection(clName);
        }else
            collection = collectionSpace.createCollection(clName,subOptions);
        //release pool
        sequoiadbDatasource.releaseConnection(sequoiadb);
    }
    /* attach */
    public void attachCL(DBCollection mailCL, DBCollection subCL, String date){
        Sequoiadb sequoiadb = getConnect();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        BasicBSONObject attachOptions  = new BasicBSONObject();
        try {
            attachOptions.put("LowBound", new BasicBSONObject(date, format.parse("2018-01-01")));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
