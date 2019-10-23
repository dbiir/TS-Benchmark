package cn.edu.ruc.utils;

public class StringUtils {
    public static boolean isBlank(String str){
        if(str==null){
            return true;
        }
        if(str.trim().isEmpty()){
            return true;
        }
        return false;
    }
    public static boolean isNotBlank(String str){
        return !isBlank(str);
    }
}
