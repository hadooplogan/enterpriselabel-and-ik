package IkAnalazyer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class FileUtil

{ /**
     * 读取txt文件的内容
     * @param file 想要读取的文件对象
     * @return 返回文件内容
     */
    public static Map getFileCfg(String file){
        Map<String,String> cfg = new HashMap<String,String>();

        try{
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                s.substring(0,s.indexOf(","));
                cfg.put(s.substring(0,s.indexOf(",")),s.substring( s.lastIndexOf(",")+1).replace("\\",""));
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return cfg;
    }

}
