package com.foo.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

/**
 * 这是一个flume日志过滤工具类
 */
public class LogUtils {

    public static boolean validateStart(String log) {
        if(log==null){
            return  false;
        }

        //检验json文件
        if(log.trim().startsWith("{")||log.trim().endsWith("}")){
            return false;
        }

        return true;
    }

    public static boolean validateEvent(String log) {

        //1.切割
        String[] logContents = log.split("\\|");
        //2.校验
        if(logContents.length!=2){
            return false;
        }
        //3.校验服务器的时间
        if(logContents[0].length()!=13 || NumberUtils.isDigits(logContents[0])){
            return false;
        }

        return false;
    }
}
