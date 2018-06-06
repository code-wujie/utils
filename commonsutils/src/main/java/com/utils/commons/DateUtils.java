package com.utils.commons;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by WJ on 2018/6/6.
 */
public class DateUtils {


    /**
     * 字符串转为日期格式
     * @param dateStr 字符串
     * @param format 格式
     * @return date
     */
    public static Date Str2Date(String dateStr, String format) {
        SimpleDateFormat simple = new SimpleDateFormat(format);
        try {
            simple.setLenient(false);
            return simple.parse(dateStr);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 日期转为字符串类型
     * @param date
     * @param format
     * @return
     */
    public static String Date2String(Date date, String format) {
        SimpleDateFormat formater = new SimpleDateFormat(format);
        try {
            return formater.format(date);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 获取当前系统的时间
     * @param format 返回个数
     * @return 字符串
     */
    public static String getCurrentDate(String format) {
        return new SimpleDateFormat(format).format(new Date());
    }

    /**
     * 获取当前系统的时间
     *
     * @return 时间戳
     */
    public static long getTimestamp() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * 字符串转时间戳
     * @param date_str
     * @param format
     * @return
     */
    public static long dateStr2TimeStamp(String date_str, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return sdf.parse(date_str).getTime() / 1000;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 时间戳转为字符串
     * @param seconds 时间戳
     * @param format 字符串的格式
     * @return 字符串
     */
    public static String timeStamp2DateStr(long seconds, String format) {
        if (seconds == 0 ) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds + "000")));
    }


    /**
     * 时间戳转为日期格式
     * @param seconds 时间戳
     * @param format 日期格式
     * @return
     */
    public static Date timeStamp2Date(long seconds,String format){
        String s = timeStamp2DateStr(seconds, format);
        return Str2Date(s,format);
    }

    /**
     * 日期转时间戳
     * @param date
     * @param format
     * @return
     */
    public static long Date2timeStamp(Date date,String format){
        String s = Date2String(date, format);
        return dateStr2TimeStamp(s,format);
    }


    public static void main(String[] args) {
        System.out.println(getTimestamp());
    }
}
