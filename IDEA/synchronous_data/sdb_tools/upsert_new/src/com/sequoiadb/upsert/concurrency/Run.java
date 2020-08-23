package com.sequoiadb.upsert.concurrency;

import com.sequoiadb.upsert.util.EncodeUtils;
import com.sequoiadb.upsert.util.LogUtils;
import com.sequoiadb.upsert.util.Utils;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 一个生产者线程，负责读取数据文件，每次读取一行，然后放到数组中
 * 多个消费者线程，同时去数组中获取数据，然后执行upsert操作
 *
 * @author yang
 */
public class Run {

    private static Logger logger = null;

    /**
     * @param args [0] 配置文件  [1] 数据文件
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Please specify the configuration file and data file.");
            System.exit(1);
        }

        String propFile = args[0];
        String dataFile = args[1];

        Utils.init(propFile);

        //定义环境变量,用于动态指定日志文件名
        LogUtils.setEnv(Utils.getCollectionSpace() + "_"
                + Utils.getCollection() + "_"
                + new SimpleDateFormat("yyMMdd_HHmmssSSS").format(new Date()));

        // 因为日志文件中用到了环境变量，所以先定义好环境变量，再初始化日志
        logger = Logger.getLogger(Run.class);

        // 探测文件编码
        if (Utils.isCheckFileEncoding()) {
            String encode = EncodeUtils.guessFileEncoding(new File(dataFile));
            if (!encode.equals(Utils.getFileEncoding())) {
                String err = "Encode error: " + encode + "(file) " + Utils.getFileEncoding() + "(custom) " + Charset.defaultCharset() + "(default)";
                System.out.println(err);
                logger.warn(err);
                LogUtils.handleLogFile(true);
                System.exit(1);
            }
        }

        final Industry ins = new Industry();

        List<Thread> consumerList = new ArrayList<>();
        for (int i = 0; i < Utils.getThreadSize(); i++) {
            consumerList.add(new Thread(new Consumer(ins)));
        }

        for (Thread consumer : consumerList) {
            consumer.start();
        }

        /* 打印监控日志，监控已导入的数据量 */
        ScheduledExecutorService service = null;
        int sec = Utils.getPrintLogPerSecond();
        if (sec > 0) {
            service = Executors.newScheduledThreadPool(1);
            service.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    logger.warn("import records: " + ins.getUpsertCount().get(1));
                }
            }, sec, sec, TimeUnit.SECONDS);
        }

        long start = System.currentTimeMillis();

        //启动生产者线程，开始读文件
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Producer(ins, dataFile, latch, Utils.getFileEncoding())).start();
        latch.await();

        // 等待队列被消费完
        // 注：程序最初版本没有等待队列为0时，就执行循环中断线程了，
        //      导致最后有线程一直等待，程序不能退出
        while (!ins.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (Thread consumer : consumerList) {
            consumer.interrupt();
            consumer.join();
        }
        /*for (Thread consumer : consumerList) {
                while(consumer.isAlive()){
                        try{Thread.sleep(10);}
                        catch(InterruptedException e){e.printStackTrace();}
                }
        }*/

        long time = System.currentTimeMillis() - start;

        if (service != null) {
            service.shutdown();
            logger.warn("import records: " + ins.getUpsertCount().get(1) + "  done.");
        }

        int all = ins.getUpsertCount().get(0);
        int succ = ins.getUpsertCount().get(1);
        int fail = ins.getUpsertCount().get(2);
        int enerr = ins.getUpsertCount().get(3);

        System.out.println("Total Read: " + all);
        System.out.println("Success: " + succ);
        System.out.println("Fail: " + fail);
        System.out.println("Encode Error: " + enerr);
        System.out.println("Time: " + time + " ms");

        logger.warn("Total Read: " + all +
                ",Success: " + succ +
                ",Fail: " + fail +
                ",Encode Error: " + enerr +
                ",Time: " + time + " ms" +
                ",Avg: " + (time >= 1000 ? (succ / (time / 1000)):succ) + " r/s");

        // 关闭线程池
        Utils.closeDataSource();
        LogUtils.handleLogFile(all == succ);

    }


}