package lob;

import com.sequoiadb.base.*;
import com.sequoiadb.datasource.SequoiadbDatasource;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class LobUtil {

    /*make object exists in memory*/
    private static class InstanceHolder {
        public final static ConnUtil INSTANCE  = new ConnUtil();
    }

    public static ConnUtil getInstance() {
        return  InstanceHolder.INSTANCE;
    }


    public void removeLob(String csName, String clName) {
        //获取sdb连接
    //        ConnUtil conn = new ConnUtil();
        SequoiadbDatasource sequoiadbDatasource = getInstance().getSdbConnectPoll();
        Sequoiadb sequoiadb1 = null;
        try {
            sequoiadb1 = sequoiadbDatasource.getConnection();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    //Get the collection space object
        CollectionSpace cs = sequoiadb1.getCollectionSpace(csName);
    //Get the collection object
        DBCollection cl = cs.getCollection(clName);
    //Delete the specified Lob
        cl.removeLob(new ObjectId(""));
    }

    public void openLob(String csName, String clName) {
        //oid ""
        //获取sdb连接
//        ConnUtil conn = new ConnUtil();
        SequoiadbDatasource sequoiadbDatasource = getInstance().getSdbConnectPoll();
        Sequoiadb sequoiadb1 = null;
        try {
            sequoiadb1 = sequoiadbDatasource.getConnection();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //获取集合空间对象
        CollectionSpace cs = sequoiadb1.getCollectionSpace(csName);
        //获取集合对象
        DBCollection cl = cs.getCollection(clName);
    //Get the specified Lob object by oid
        DBLob dbLob = cl.openLob(new ObjectId(""));
        FileOutputStream fileOutputStream = null;
        try {
//Get the file output stream, and set the file path and file name
            fileOutputStream = new FileOutputStream(new File("/home/shiyanlou/Desktop/sequoiadb.txt"));
//Read the Lob and write to the local
            dbLob.read(fileOutputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            dbLob.close();
        }
    }

    public void listLob(String csName, String clName) {
        //获取sdb连接
        ConnUtil conn = new ConnUtil();
        SequoiadbDatasource sequoiadbDatasource = conn.getSdbConnectPoll();
        Sequoiadb sequoiadb1 = null;
        try {
            sequoiadb1 = sequoiadbDatasource.getConnection();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
//Get the collection space object
        CollectionSpace cs = sequoiadb1.getCollectionSpace(csName);
//Get collection object
        DBCollection cl = cs.getCollection(clName);
//Get the list of Lob in the collection
        DBCursor cursor = cl.listLobs();
        try {
//Traverse the Lob list and output Lob information
            while (cursor.hasNext()) {
                BSONObject record = cursor.getNext();
                System.out.println(record.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cursor.close();
        }
    }


    public void putLob(String csName, String clName, File file) throws FileNotFoundException {
        //获取sdb连接
        ConnUtil connUtil = new ConnUtil();
        SequoiadbDatasource sequoiadbDatasource = connUtil.getSdbConnectPoll();
        Sequoiadb sequoiadb1 = null;
        try {
            sequoiadb1 = sequoiadbDatasource.getConnection();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        FileInputStream fileInputStream = null;
        CollectionSpace cs = null;
        DBCollection cl = null;

        //Get the collection space object
        if (sequoiadb1.isCollectionSpaceExist(csName)) {
            cs = sequoiadb1.getCollectionSpace(csName);
        } else
            cs = sequoiadb1.createCollectionSpace(csName);
        //Get collection object
        if(cs.isCollectionExist(clName)){
            cl = cs.getCollection(clName);
        }else
            cl = cs.createCollection(clName);
        //Create the Lob
        DBLob lob = cl.createLob();
        ObjectId id = lob.getID();
        //Print oid, and a unique oid will be generated when the Lob object is created
        String s = id.toString();
        fileInputStream = new FileInputStream(file);
//Write data to the Lob
        lob.write(fileInputStream);
//Close the Lob
        lob.close();
        sequoiadb1.close();
        sequoiadbDatasource.close();
    }


    public void init(String csName) {
        ConnUtil conn = new ConnUtil();
        SequoiadbDatasource sequoiadbDatasource = conn.getSdbConnectPoll();
        Sequoiadb sequoiadb1 = null;
        try {
            sequoiadb1 = sequoiadbDatasource.getConnection();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        if (sequoiadb1.isCollectionSpaceExist(csName)) {
            sequoiadb1.dropCollectionSpace(csName);
        }
    }
}

