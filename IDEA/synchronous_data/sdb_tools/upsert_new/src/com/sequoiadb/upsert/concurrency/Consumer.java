package com.sequoiadb.upsert.concurrency;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.upsert.util.Utils;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicIntegerArray;

public class Consumer implements Runnable {

    private static Logger logger = Logger.getLogger(Consumer.class);

    private Industry ins;

    public Consumer(Industry industry) {
        this.ins = industry;
    }

    @Override
    public void run() {
        Sequoiadb sdb = Utils.getConnection();
        AtomicIntegerArray upsertCount = ins.getUpsertCount();
        while(!ins.isDone() || !ins.isEmpty()){ //当生产者读取完全部文件，并且容器中数据量为0，循环结束
            BsonParam bsonParam = null;
            try{
                bsonParam = ins.get();
                upsert(sdb, bsonParam, Utils.getRetriesWhenDupKey());
                upsertCount.getAndIncrement(1); //[1]   success count

            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
                break;
            } catch (BaseException e){
                upsertCount.getAndIncrement(2);     //[2]   fault count
                logger.error(e.getMessage());
                logger.info(bsonParam.getData());
            }
        }
        Utils.disconnect(sdb);
    }

    public void upsert(Sequoiadb sdb, BsonParam bsonParam, int count) throws BaseException{

        try {
            sdb.getCollectionSpace(Utils.getCollectionSpace())
                    .getCollection(Utils.getCollection())
                    .upsert(bsonParam.getCond(), bsonParam.getRule(), bsonParam.getHint());
        }catch (BaseException e){
            if (count > 0) {
                upsert(sdb, bsonParam, --count);
            } else {
                throw e;
            }
        }
    }
}