package cn.edu.ruc.utils;

import java.io.*;

public class FileUtils {
    public static void writeLine(String relativePath,String data){
        File file = new File(relativePath);
        File parentFile = file.getParentFile();
        if(!parentFile.exists()){
            parentFile.mkdirs();
        }
        try {
            FileWriter fw = new FileWriter(relativePath, true);
            PrintWriter out = new PrintWriter(fw);
            out.write(data);
            out.println();
            fw.close();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static String readLines(String  relativePath,int linesNum){
        StringBuffer data = new StringBuffer();
        try {
            FileReader fr = new FileReader(relativePath);
            BufferedReader bf = new BufferedReader(fr);
            String str="";
            int count =0;
            // 按行读取字符串
            while ((str = bf.readLine()) != null&&count<linesNum) {
                data.append(str);
                data.append(System.getProperty("line.separator"));
                count++;
            }
            bf.close();
            fr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return data.toString();
    }
    public static String read(String  relativePath ){
        return readLines(relativePath,Integer.MAX_VALUE);
    }
    public static boolean existDir(String relativePath){
        File file = new File(relativePath);
        if(file.exists()&&file.isDirectory()){
            return true;
        }else{
            return false;
        }
    }
    public static boolean existFile(String relativePath){
        File file = new File(relativePath);
        if(file.exists()&&file.isFile()){
            return true;
        }else{
            return false;
        }
    }
    public static void mkdir(String relativePath){
        File file = new File(relativePath);
        if(!file.exists()){
           file.mkdirs();
        }
    }
    public static void main(String[] args) {
        String path = Class.class.getClass().getResource("/").getPath();
        String dirpath=path+"data/a/b/c/";
        System.out.println(dirpath);
        String filePath=dirpath+"d"+"/e/test.txt";
        File file = new File(filePath);
        File parentFile = file.getParentFile();
        if(!parentFile.exists()){
            parentFile.mkdirs();
        }
        FileUtils.writeLine(filePath,"eee");
        FileUtils.mkdir(dirpath);
        FileUtils.writeLine(dirpath+"test.txt","a");
        FileUtils.writeLine(dirpath+"test.txt","b");
        FileUtils.writeLine(dirpath+"test.txt","c");
//        FileUtils.writeLine("");
        System.out.println(FileUtils.readLines(filePath,3));
        System.out.println(FileUtils.read(filePath));
    }
}
