package com.swt.test;

import java.sql.*;


public class HiveJdbc {
    /**
     * 验证通过
     * */

    public static void main(String[] args) {
        //String url = "jdbc:hive2://10.83.192.6:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2_uat";//10.83.192.6,10.83.192.7,
        String select = "select id, waybill_id, waybill_number, waybill_free_flag, waybill_label, waybill_type, shiping_company_id, shiping_company, shiping_time " +
                " from vdm_crm.v_o_waybill_analysis_base where etl_date = '20201116' limit 1"; //结尾不要加';'

        String url = "jdbc:mysql://10.83.193.24:13300/bigdata_app?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull&useSSL=false&allowMultiQueries=true";
        String user = "bdata";
        String pw = "b_data7890";
        String insertSql = "insert into mysql_stream_test (\n" +
                "    task_rule_id\n" +
                "  , assign_batch_no\n" +
                "  , month\n" +
                "  , assign_mode\n" +
                "  , assign_by\n" +
                "  , assign_by_id\n" +
                "  , status\n" +
                "  , update_time\n" +
                "  , enabled_flag\n" +
                ")\n" + //"values (?,?,?,?,?,?,?,?,?)";
                "values (111, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'swt', 668888, 0, now(), 1)," +
                "(112, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(113, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(114, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(115, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(116, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(117, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(118, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(119, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(110, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(211, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(212, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(213, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(214, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(215, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(216, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(217, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(218, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tws', 668999, 0, now(), 1)," +
                "(219, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'jill', 669999, 0, now(), 1)," +
                "(220, date_format(now(), '%Y%m%d%H%i%s'), date_format(now(), '%Y-%m'), 0, 'tony', 667999, 0, now(), 1)";
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            println("------------------- jdbc connecting ------------------------");
            Connection conn = DriverManager.getConnection(url, user, pw);
            println("----------------------- jdbc connect return, then create statement -------------------------");
            Statement statement = conn.createStatement();
            java.sql.PreparedStatement pstsm = conn.prepareStatement(insertSql);
            //statement.execute("upsert into PHOENIX_TEST (id, NAME, AGE, SEX, JOB) values ('36789','James', '68','M', 'Manager')");
            for(int i = 0; i < 10000; i++) {
                println("=================== select executing ====================");
                //ResultSet resultSet = statement.executeQuery(select);

//                for(int j = 0; j < 8; j++) {
//                    pstsm.setLong(1, System.currentTimeMillis() % 100000);
//                    pstsm.setString(2, );
//                }

                statement.execute(insertSql);
                Thread.sleep(1000);
                println("=================== select end ====================");
            }
//            while(resultSet.next()){
//                println("id: " + resultSet.getString("id"));
//                println("waybill_id:" + resultSet.getString("waybill_id"));
//            }
//            System.out.println(resultSet.toString());
//
//            resultSet.close();
            statement.close();
            conn.close();

        }catch (SQLException throwables) {
            throwables.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (java.lang.IllegalAccessException e){
            e.printStackTrace();
        }catch (InstantiationException e){
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static void println(Object o) {
        System.out.println(o);
    }
}
