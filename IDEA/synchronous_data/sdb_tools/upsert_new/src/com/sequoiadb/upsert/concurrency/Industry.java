package com.sequoiadb.upsert.concurrency;

import com.sequoiadb.upsert.util.Utils;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class Industry {
	 private static Logger logger = Logger.getLogger(Industry.class);
    @SuppressWarnings("unchecked")
 //   private BsonParam[] dataArr = new BsonParam[Utils.getDataCache()];         //共享变量，数据容器

    private static ConcurrentLinkedQueue<BsonParam> queue = new ConcurrentLinkedQueue<BsonParam>();
    
    private int count, putPos, takePos;

  //  private Lock lock = new ReentrantLock();
    
    //通过已有的锁获取两组监视器，一组监视生产者，一组监视消费者。
  //  private Condition producer_con = lock.newCondition();
 //   private Condition consumer_con = lock.newCondition();
    
    public void put(BsonParam data){
      //      lock.lock();
    	queue.offer(data);
            /*try {
                while(count == Utils.getDataCache()){    //生产到最大数量就不再生产
                    try {
                    	logger.info(" zui da   l ");
        //                producer_con.await();               //生产者等待
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                dataArr[putPos] = data;
                if(++putPos == dataArr.length) putPos=0;
                count++;
                consumer_con.signal();//唤醒一个消费者
            } finally{
                lock.unlock();
            }*/
    }

    public BsonParam get() throws InterruptedException{
            /*try {
                    lock.lockInterruptibly();
            } catch (InterruptedException e) {
                    throw e;
//                    Thread.currentThread().interrupt();
//                    return null;
            }

            //lock.lock();
            BsonParam data = null;
            try {
                while(count == 0){                                          //全部消费完就等待
                    try {
                        consumer_con.await();                       //消费者等待
                    } catch (InterruptedException e) {
                            throw  e;
                            //e.printStackTrace();
//                            Thread.currentThread().interrupt();
//                            return null;
                    }
                }
                data = dataArr[takePos];
                if(++takePos == dataArr.length) takePos=0;
                count--;
                producer_con.signal();//唤醒生产者
            } finally{
                lock.unlock();
            }*/
    	BsonParam data = null;
    	data = queue.poll();
    		

            return data;
    }

    /**
     * 返回容器中剩余数据量
     * @return
     */
    public boolean isEmpty() {
            return queue.isEmpty();
    }

    private boolean isDone = false;         //生产者是否完成读取全部文件

    public boolean isDone() {
            return isDone;
    }

    public void setDone(boolean isDone) {
            this.isDone = isDone;
    }

    /**
     * [0]  all count
     * [1]  upsert success count
     * [2]  upsert fault count
     * [3]  encode error count
     */
    private volatile AtomicIntegerArray upsertCount = new AtomicIntegerArray(4);

    public AtomicIntegerArray getUpsertCount() {
        return this.upsertCount;
    }
}