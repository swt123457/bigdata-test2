package com.kye.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtils {

    public static SimpleDateFormat fullTimeSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat hourSdf = new SimpleDateFormat("yyyy-MM-dd HH");
    public static SimpleDateFormat hourYMDHM = new SimpleDateFormat("yyyyMMddHHmm");
    public static SimpleDateFormat hourYMDH = new SimpleDateFormat("yyyyMMddHH");
    public static SimpleDateFormat hourMS = new SimpleDateFormat("HHmm");

    /**
     * 获取当天的 0时0分0秒，并按传入格式返回
     * @return
     */
    public synchronized static String CurrentDateBeginTime(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        Date time = c.getTime();
        String format = fullTimeSdf.format(time);
        return format;
    }

    public synchronized static String currentDateBeginTime(SimpleDateFormat sdf){
        Calendar c = Calendar.getInstance();
        /*c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);*/
        Date time = c.getTime();
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        String formatDateTimeStr = sdf.format(time);
        return formatDateTimeStr;
    }

    /**
     * 当前时间往前（offset小于0）或往后（offset大于0）推 offset天
     * */
    public synchronized static String dateBeginTime(SimpleDateFormat sdf, int offset){
        Calendar c = Calendar.getInstance();
        if (offset != 0) {
            c.add(Calendar.DATE, offset);
        }
        Date time = c.getTime();
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        String formatDateTimeStr = sdf.format(time);
        return formatDateTimeStr;
    }

    public synchronized static String CurrentDateMM(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        Date time = c.getTime();
        String format = hourYMDHM.format(time);
        return format;
    }

    public synchronized static String CurrentDateHH(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        Date time = c.getTime();
        String format = hourYMDH.format(time);
        return format;
    }

    public synchronized static String LastCurrentDateMM(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.add(Calendar.DATE,-1);
        Date time = c.getTime();
        String format = hourYMDHM.format(time);
        return format;
    }

    public synchronized static String LastCurrentDateHH(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.add(Calendar.DATE,-1);
        Date time = c.getTime();
        String format = hourYMDH.format(time);
        return format;
    }

    public synchronized static String CurrentDateMMFive(int minute ){
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MINUTE, minute);
        Date time = c.getTime();
        String format = hourMS.format(time);
        return format;
    }


    /**
     * 获取当天的 0时0分0秒，并按传入格式返回
     * @return
     */
    public synchronized static String CurrentDateBeginTime(String pattern){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        Date time = c.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        String format = sdf.format(time);
        return format;
    }

    /**
     * 转化时间戳为 str
     * @param millisecond
     * @param pattern 字符串时间格式 如 yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String transMillisecondTimeToStr(long millisecond,String pattern){
        Date date = new Date(millisecond);
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        String format = sdf.format(date);
        return format;
    }

    /**
     * 计算当前时间前 hours 小时范围，包括当前小时
     * @return
     */
    public synchronized static List<String> getPreHourRange(int hours){
        List<String> list = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        String hour = "";
        for(int i = 0; i < hours; i++){
            if(i == 0){
                Date time = c.getTime();
                hour = fullTimeSdf.format(time);
            }else {
                c.add(Calendar.HOUR_OF_DAY,-1);
                Date beginTime = c.getTime();
                hour = fullTimeSdf.format(beginTime);
            }
            list.add(hour);
        }
        return list;
    }

    public static int compare(String time, String time1) {
        int i = 0;
        try {
            Date parse = fullTimeSdf.parse(time);
            Date parse1 = fullTimeSdf.parse(time1);
            long l = parse.getTime() - parse1.getTime();
            if(l > 0){
                i = 1;
            }else if(l < 0){
                i = -1;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return i;
    }

    /**
     * 获取{seconds} 秒时间间隔的时间字符串 如 3600，-200
     * @param seconds
     * @return
     */
    public static String formatDateSecondDiff(int seconds) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.SECOND,seconds);
        String format = fullTimeSdf.format(c.getTime());
        return format;
    }

    /**
     * 计算当天已过小时 （不包含当前小时），加当前之前小时数, 时间列表["2020-07-10 10:00:00"]
     * @return
     */
    public synchronized static List<String> preCurrentDayHourRange(int addHours){
        List<String> list = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        int hoursOfDay = c.get(Calendar.HOUR_OF_DAY);
        int hours = hoursOfDay  + addHours;
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.add(Calendar.HOUR_OF_DAY,-1);
        String hour = "";
        for(int i = 0; i < hours; i++){
            if(i == 0){
                Date time = c.getTime();
                hour = fullTimeSdf.format(time);
            }else {
                c.add(Calendar.HOUR_OF_DAY,-1);
                Date beginTime = c.getTime();
                hour = fullTimeSdf.format(beginTime);
            }
            list.add(hour);
        }
        return list;
    }

    /**
     * 计算当天上个小时 结尾和 当天零时加上 addHouers小时 之前的两个时间点
     * @return
     */
    public synchronized static List<String> forcastStartAndEndTime(int addHours){
        List<String> list = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        int hoursOfDay = c.get(Calendar.HOUR_OF_DAY);
        int hours = hoursOfDay  + addHours;
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        c.add(Calendar.HOUR_OF_DAY,-1);
        String endTime = fullTimeSdf.format(c.getTime());
        c.set(Calendar.MINUTE, 00);
        c.set(Calendar.SECOND, 00);
        c.add(Calendar.HOUR_OF_DAY,-(hours-1));
        String startTime = fullTimeSdf.format(c.getTime());
        list.add(startTime);
        list.add(endTime);
        return list;
    }

    public static void main(String[] args) {
        List<String> strings = forcastStartAndEndTime(24);
        System.out.println(strings);
    }


    /*
    * 判断是否为 yyyy-MM-dd HH:mm:ss 的时间格式
    * */
    private static boolean isLegalDate(String sDate) {
        int legalLen = 19;
        if ((sDate == null) || (sDate.length() != legalLen)) {
            return false;
        }

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = formatter.parse(sDate);
            return sDate.equals(formatter.format(date));
        } catch (Exception e) {
            return false;
        }
    }


    /*
    * 时间戳转换为时间      2020-07-01 20:31:53
    * */
    public static String timeStamp2Date(String seconds,String format) {
        if(seconds == null || seconds.isEmpty() || seconds.equals("null")){
            return "";
        }
        if(format == null || format.isEmpty()){
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);

        if(isLegalDate(seconds) ){
            return seconds;
        }else if(seconds.length() == 13){
            return sdf.format(new Date(Long.valueOf(seconds)));
        }else if (seconds.length() <= 10){
            return sdf.format(new Date(Long.valueOf(seconds+"000")));
        }else {
            return "";
        }
    }

    /*
     * 将时间转换为时间戳 ( 毫秒: ms )    1608052912000
     */
    public static long dateToStamp(String s) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            Date date = simpleDateFormat.parse(s);
            return date.getTime();
        } catch (ParseException e) {
            return 0;
        }
    }

    public static Date strToDate(String s, SimpleDateFormat format) {
        Date date = null;
        try {
            date = format.parse(s);
        } catch (ParseException e) {
            return null;
        }

        return date;
    }


}
