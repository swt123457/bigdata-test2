package com.kye.utils;

import java.util.ResourceBundle;

public class KyeUtils {

    private KyeUtils(){

    }

    public static void main(String[] args){
        long p = 235;
        long t = 532;
        double percent = 0.00;
        percent = 100.00 * p / t;
        println(percent);
        println(KyeUtils.class.getName());
        //println(toString(9L));
        ResourceBundle prop = getProperties("kye-utils");
        String sql0 = prop.getString("hive.sql");
        String value1 = "CBA";
        sql0 = replaceTest(value1, sql0);

        println("------------------------------------------");
        System.out.println(sql0);
        println("------------------------------------------");
    }

    public static String replaceTest(String value1, String sql0){
        Double value2 = 25052.250;
        long value3 = 25052025052L;
        sql0 = sql0.replaceAll("\\$\\{\\s*value1\\s*}", value1)
                .replaceAll("\\$\\{\\s*value2\\s*}", String.valueOf(value2))
                .replaceAll("\\$\\{\\s*value3\\s*}", String.valueOf(value3));
        return sql0;
    }

    public static ResourceBundle getProperties(String res){
        return ResourceBundle.getBundle(res);
    }

    public static String toString(Object o) {
        return o == null ? "" : o.toString();
    }

    public static String[] splitToArray(String s, String split, int num) {
        return s.split(split, num);
    }

    public static void println(Object o) {
        System.out.println(o);
    }

}
