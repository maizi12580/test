package com.sequoiadb.upsert.util;


import org.mozilla.intl.chardet.nsDetector;
import org.mozilla.intl.chardet.nsICharsetDetectionObserver;

import java.io.*;
/**
 * Created by yang on 2017/4/26.
 */
public class EncodeUtils {
    private static boolean found = false;
    private static String encoding = null;

    public static void main(String[] argv) throws Exception {
        File file1 = new File("d:/utf8.txt");

        System.out.println("文件编码:" + new EncodeUtils().guessFileEncoding(file1));
    }

    /**
     * 传入一个文件(File)对象，检查文件编码
     *
     * @param file
     *            File对象实例
     * @return 文件编码，若无，则返回null
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static String guessFileEncoding(File file) throws FileNotFoundException, IOException {
        return guessFileEncoding(file, new nsDetector());
    }

    /**
     * <pre>
     * 获取文件的编码
     * @param file
     *            File对象实例
     * @param languageHint
     *            语言提示区域代码 @see #nsPSMDetector ,取值如下：
     *             1 : Japanese
     *             2 : Chinese
     *             3 : Simplified Chinese
     *             4 : Traditional Chinese
     *             5 : Korean
     *             6 : Dont know(default)
     * </pre>
     *
     * @return 文件编码，eg：UTF-8,GBK,GB2312形式(不确定的时候，返回可能的字符编码序列)；若无，则返回null
     * @throws FileNotFoundException
     * @throws IOException
     */
    public String guessFileEncoding(File file, int languageHint) throws FileNotFoundException, IOException {
        return guessFileEncoding(file, new nsDetector(languageHint));
    }

    /**
     * 获取文件的编码
     *
     * @param file
     * @param det
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static String guessFileEncoding(File file, nsDetector det) throws FileNotFoundException, IOException {
        // Set an observer...
        // The Notify() will be called when a matching charset is found.
        det.Init(new nsICharsetDetectionObserver() {
            public void Notify(String charset) {
                encoding = charset;
                found = true;
            }
        });

        BufferedInputStream imp = new BufferedInputStream(new FileInputStream(file));
        byte[] buf = new byte[1024];
        int len;
        boolean done = false;
        boolean isAscii = false;

        while ((len = imp.read(buf, 0, buf.length)) != -1) {
            // Check if the stream is only ascii.
            isAscii = det.isAscii(buf, len);
            if (isAscii) {
                break;
            }
            // DoIt if non-ascii and not done yet.
            done = det.DoIt(buf, len, false);
            if (done) {
                break;
            }
        }
        imp.close();
        det.DataEnd();

        if (isAscii) {
            encoding = "ASCII";
            found = true;
        }

        if (!found) {
            String[] prob = det.getProbableCharsets();
            //这里将可能的字符集组合起来返回
            for (int i = 0; i < prob.length; i++) {
                if (i == 0) {
                    encoding = prob[i];
                } else {
                    encoding += "," + prob[i];
                }
            }

            if (prob.length > 0) {
                // 在没有发现情况下,也可以只取第一个可能的编码,这里返回的是一个可能的序列
                return encoding;
            } else {
                return null;
            }
        }
        return encoding;
    }

    /**
     * 判断文件的编码格式
     * @param fileName :file
     * @return 文件编码格式
     * @throws Exception
     */
    public static String codeString(String fileName) throws Exception{
        BufferedInputStream bin = new BufferedInputStream(
                new FileInputStream(fileName));
        int p = (bin.read() << 8) + bin.read();
        String code = null;

        switch (p) {
            case 0xefbb:
                code = "UTF-8";
                break;
            case 0xfffe:
                code = "Unicode";
                break;
            case 0xfeff:
                code = "UTF-16BE";
                break;
            default:
                code = "GBK";
        }

        return code;
    }
}
