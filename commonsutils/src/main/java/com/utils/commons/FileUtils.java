package com.utils.commons;

import jdk.nashorn.internal.runtime.linker.LinkerCallSite;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by WJ on 2018/6/6.
 */
public class FileUtils {

    protected static Log logger = LogFactory.getLog(HttpUtils.class);
    /**
     * 从文件中读取内容，返回list。每一个元素对应一行的数据
     * @param filename 文件名称
     * @return list的数据
     * @throws IOException
     */
    public static List<String> ReadFile(String filename) throws IOException {
        List<String> datas = new ArrayList<>();
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
        String str = null;
        while ((str = in.readLine()) != null) {
            datas.add(str);
        }
        in.close();
        return datas;
    }

    /**
     * 将数据存入到文本中
     * @param datas 要存入的数据，为一个list
     * @param filname 存入的文件名。
     * @param append 是否用追加的方式
     */
    public static void WriteFile(List<String> datas, String filname, boolean append) {

        FileWriter fw = null;
        try {
            //如果文件存在，则追加内容；如果文件不存在，则创建文件
            File f = new File(filname);
            fw = new FileWriter(f, append);
        } catch (IOException e) {
            logger.error("写入失败，请确定文件是否存在以及模式是否正确");
            e.printStackTrace();
        }
        PrintWriter pw = new PrintWriter(fw);
        for(String str:datas){
            pw.println(str);
        }
        pw.flush();
        try {
            fw.flush();
            pw.close();
            fw.close();
        } catch (IOException e) {
            logger.error("刷新失败！");
            e.printStackTrace();
        }
    }

    /**
     * 创建文件
     * @param fileName 要创建的文件名称
     * @return
     * @throws Exception
     */
    public static boolean createFile(String fileName)throws Exception{
        File file=new File(fileName);
        try{
            if(!file.exists()){
                file.mkdirs();
                file.createNewFile();
            }
        }catch(Exception e){
            logger.error("创建文件失败，请确认路径等是否正确");
            e.printStackTrace();
        }
        return true;
    }

    public static void main(String[] args) {
        List<String> datas=new ArrayList<>();
        for(int i=0;i<100;i++){
            datas.add("n"+i);
        }
        WriteFile(datas,"C:\\Users\\WJ\\Desktop\\test.txt",true);
    }
}
