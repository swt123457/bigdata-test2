package com.swt.test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PhoenixJdbc {
    /**
     * 验证通过
     * */

    public static void main(String[] args) {

        String jdbcUrl = "jdbc:phoenix:10.83.192.6,10.83.192.7,10.83.192.8:2181:/hbase206";
        final String select = "select service_type\n" +
                "    ,sum(operation_weight) as total_weight\n" +
                "    ,avg(operation_weight) as avg_weight\n" +
                "    ,min(operation_weight) as min_weight\n" +
                "    ,max(operation_weight) as max_weight\n" +
                "from waybill_analysis_base_2\n" +
                "where shiping_time >= '2019-10-01'\n" +
                "and (service_type not in (190,200,290,300) or service_type is null)\n" +
                "and (scrap_flag != '10' or scrap_flag is null)\n" +
                "and enabled_flag = 1\n" +
                "and operation_weight >= 0\n" +
                "and coalesce(waybill_type, '-99') != '60'\n" +
                "and ((finance_audit_status != 10) or (other_time_1 is not null))\n" +
                "and signing_time is not null\n" +
                "group by service_type";

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            List<Connection> connPool = getConnectionPool(jdbcUrl, "", "",Integer.valueOf(args[0]) );

            println("------------------------- start to query ------------------------------");
            //long startTimeStamp = System.currentTimeMillis();
            final List<Thread> threadList = new ArrayList<Thread>();
            connPool.forEach(conn -> {
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        queryFromPhoenix(conn, select);
                    }
                });
                threadList.add(thread);
            });

            //List<Thread> threadList = threadStream.collect(Collectors.toList());

            long startTimeStamp = System.currentTimeMillis();
            threadList.forEach(thread -> {
                thread.start();
            });

            threadList.forEach(thread -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            long endTimeStamp = System.currentTimeMillis() - startTimeStamp;

            println("---------------------------- " + endTimeStamp + " ms --------------------------------");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }



//        try {
//            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//            println("------------------- jdbc connecting ------------------------");
//            Connection connection = DriverManager.getConnection(jdbcUrl, "", "");
//            println("----------------------- jdbc connect return, then create statement -------------------------");
//            Statement statement = connection.createStatement();
//            //statement.execute("upsert into PHOENIX_TEST (id, NAME, AGE, SEX, JOB) values ('36789','James', '68','M', 'Manager')");
//            println("=================== select executing ====================");
//            ResultSet resultSet = statement.executeQuery(select);
//            println("=================== select end ====================");
//            while(resultSet.next()){
//                println("-----------------------------------------------------------------------------");
//                println(resultSet.getInt(1) + "|" + resultSet.getBigDecimal(2));
//            }
//
//        }catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }catch (ClassNotFoundException e){
//            e.printStackTrace();
//        }
    }

    static void println(Object o) {
        System.out.println(o);
    }

    static List<Connection> getConnectionPool(String jdbcUrl, String user, String pw, int size) {
        ArrayList<Connection> connList = new ArrayList<Connection>(size);
        try {
            for (int i = 0; i < size; i++){
                connList.add(DriverManager.getConnection(jdbcUrl, user, pw));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connList;
    }

    static List<String> queryFromPhoenix(Connection connection, String sql) {
        ArrayList<String> result = new ArrayList<>();

        try {
            println("------------------------------------ a query task is respecting -----------------------------------------");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next()){
                result.add(resultSet.getInt(1) + "|" + resultSet.getBigDecimal(2));
            }
        }catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return result;
    }

}
