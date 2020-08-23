package com.sequoiadb.upsert.concurrency;

import com.sequoiadb.upsert.util.EncodeUtils;
import com.sequoiadb.upsert.util.Utils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class Producer implements Runnable {
    private static Logger logger = Logger.getLogger(Producer.class);

    Industry ins;
    String dataFile;
    CountDownLatch latch;
    String defaultEncode;

    public Producer(Industry industry, String dataFile, CountDownLatch latch, String defaultEncode) {
        this.ins = industry;
        this.dataFile = dataFile;
        this.latch = latch;
        this.defaultEncode = defaultEncode;
    }

    @Override
    public void run() {
        BufferedReader bufr = null;
        AtomicIntegerArray upsertCount = ins.getUpsertCount();
        try {
            bufr = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile), defaultEncode));
            logger.warn("Encoding: " + EncodeUtils.guessFileEncoding(new File(dataFile))
                    + "(FILE) " + defaultEncode + "(CUSTOM) " + Charset.defaultCharset() + "(JVM)");
            String line = null;
            while ((line = bufr.readLine()) != null) {
                upsertCount.getAndIncrement(0);        //read count
                /*if (line.indexOf(0xfffd) != -1) {
                    upsertCount.getAndIncrement(3); //encode error count
                    logger.error("Encode Error: " + line);
                    continue;
                }*/
                /* if (Utils.strToHexStr(line).contains("fffd")) {
                    upsertCount.getAndIncrement(3); //encode error count
                    logger.error("Encode Error: " + line);
                    continue;
                }*/
                BsonParam bsonParam = null;
                try {
                   bsonParam = BsonParam.newInstance(line, Utils.getCondFields());
                } catch (ClassCastException e) {
                   upsertCount.getAndIncrement(2);
                   logger.error("ClassCastException: " + line);
                    continue;
                }
                ins.put(bsonParam);
            }
            ins.setDone(true);

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (bufr != null) {
                try {
                    bufr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        latch.countDown();
    }

    public static void main(String[] args) throws Exception {
        //utf-8.txt 是GBK编码
        BufferedReader bufr = new BufferedReader(new InputStreamReader(new FileInputStream("d:/utf8.txt"), "utf-8"));
        String s = bufr.readLine();
        System.out.println(s);
        System.out.println(s.indexOf(0xefbfbd));
        System.out.println(s.indexOf(0xfffd));
        System.out.println("乱码16进制： " + Integer.toHexString(s.charAt(0)));
        System.out.println("0xefbfbd: " + 0xefbfbd + ", 0xfffd: "+ 0xfffd +", 乱码对应int值: "  + (int)s.charAt(0));
    }
}
