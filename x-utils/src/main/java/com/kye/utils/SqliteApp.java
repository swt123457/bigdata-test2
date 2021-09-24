package com.kye.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

public class SqliteApp {
    public static void main(String args[]) {
        println(SqliteApp.class.getName());
        ResourceBundle props = KyeUtils.getProperties("kye-utils");
        String dbName = props.getString("sqlite.db");
        int threadNum = Integer.valueOf(props.getString("sqlite.thread.num"));

        String jdbcUrl = "jdbc:sqlite:" + dbName;

        /*println(jdbcUrl);
        println(System.getProperty("user.dir"));
        println(SqlTest.sql1);
        if(!jdbcUrl.isEmpty()) return;*/

        try {
            Class.forName("org.sqlite.JDBC");
            /*Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbName);
            Statement statement = conn.createStatement();
            statement.execute("Pragma temp_store=2;");
            ResultSet resultSet0 = statement.executeQuery("Pragma temp_store;");
            resultSet0.next();
            println("temp_store: " + resultSet0.getObject(1));*/
            List<Connection> connPool = getConnectionPool(jdbcUrl, "", "", threadNum);

            println("------------------------- start to query ------------------------------");
            final List<Thread> threadList = new ArrayList<Thread>();
            connPool.forEach(conn -> {
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        queryFromSqlite(conn, SqlTest.sql1, 1);
                    }
                });
                threadList.add(thread);
            });

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

        } catch (Exception e){
            e.printStackTrace();
        }


    }

    static void println(Object o) {
        System.out.println(o);
    }

    static List<Connection> getConnectionPool(String jdbcUrl, String user, String pw, int size) {
        ArrayList<Connection> connList = new ArrayList<Connection>(size);
        try {
            for (int i = 0; i < size; i++){
                connList.add(DriverManager.getConnection(jdbcUrl));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connList;
    }

    static List<String> queryFromSqlite(Connection connection, String sql, int queryNum) {
        ArrayList<String> result = new ArrayList<>();

        try {
            //println("------------------------------------ a query task is respecting -----------------------------------------");
            Statement statement = connection.createStatement();
            for (int i = 0; i < queryNum; i++) {
                ResultSet resultSet = statement.executeQuery(sql);
                while(resultSet.next()){
                    result.add(resultSet.getLong(1) + "|" + resultSet.getInt(2) + "|" + resultSet.getInt(3));
                    //println(resultSet.getObject(1));
                }
            }
        }catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return result;
    }
}
