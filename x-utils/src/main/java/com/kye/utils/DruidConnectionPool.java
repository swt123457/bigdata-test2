package com.kye.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import ru.yandex.clickhouse.ClickHouseDriver;

public class DruidConnectionPool {
    private transient static DataSource dataSource = null;

    private transient static Properties props = new Properties();

    static {
        props.put("name", "DruidConnPool");
        props.put("driverClassName", "ru.yandex.clickhouse.ClickHouseDriver");
        props.put("url", "jdbc:clickhouse://10.83.192.9:19000");
        props.put("username", "writer");
        props.put("password", "123456");

        props.put("initialSize", 5);
        props.put("maxActive", 30);
        props.put("minIdle", 3);
        props.put("maxWait", 30000);
        props.put("timeBetweenEvictionRunsMillis", 60000);
        props.put("validationQuery", "SELECT 1");
        props.put("validationQueryTimeout", 30000);

        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DruidConnectionPool() {

    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
