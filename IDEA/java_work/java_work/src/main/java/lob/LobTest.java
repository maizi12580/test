package lob;

import java.io.File;
import java.io.FileNotFoundException;

public class LobTest {

    public static void main(String[] args) throws FileNotFoundException {
        String csName = "image";
        String clName = "image_lob";

        LobUtil lobUtil = new LobUtil();
        if(args.length == 0){
            System.out.println("未传入 main() 方法参数！");
            System.exit(1);
        }else if ("createAndQuery".equals(args[0])) {
            //创建和查询
            //lobUtil.init(csName);
            System.out.println("上传LOB");
            //要上传的文件对象
            //File file = new File("/home/sdbadmin/1.txt");
            File file = new File("D:\\luke\\002201_official.apk");
            String fileName = file.getName();
            lobUtil.putLob(csName,clName,file);
            System.out.println("上传LOB结束");
            System.out.println("查询LOB");
            lobUtil.listLob(csName,clName);
            System.out.println("查询LOB结束");
        }else if ("get".equals(args[0])){
            //
            System.out.println("下载LOB");
            lobUtil.openLob(csName,clName);
            System.out.println("下载LOB结束");
        }else if ("deleteAndQuery".equals(args[0])) {
            //删除和查询
            System.out.println("删除LOB");
            lobUtil.removeLob(csName,clName);
            System.out.println("删除LOB结束");
            System.out.println("查询LOB");
            lobUtil.listLob(csName,clName);
            System.out.println("查询LOB结束");
        }else{
            System.out.println("传入 main() 方法的参数非法！非法参数为：" + args[0]);
            System.exit(1);
        }
    }
}
