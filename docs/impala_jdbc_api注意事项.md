1. 若写入汉字，sql中须使用 cast(? as string),  如下

```java
@Autowired
JdbcTemplate impalaTemplate;

public int[] batchInsertCarNodesDistances (List<Map<String, Object>> rows) {
        StringBuilder sqlSb = new StringBuilder("insert into rdm.r_tms_vehicle_type_milage_price ");
        sqlSb.append(" (id,car_number,start_node_id,start_node,end_node_id,end_node,distance1,distance2,enable_flag) ")
                .append("values(?,cast(? as string),?,cast(? as string),?,cast(? as string),?,?,?)");

        return impalaTemplate.batchUpdate(sqlSb.toString(), new BatchPreparedStatementSetter() {
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, MapUtil.getString(rows.get(i), "id"));
                ps.setString(2, MapUtil.getString(rows.get(i), "car_number"));
                ps.setLong(3, ParseUtil.strToLong(MapUtil.getString(rows.get(i), "start_node_id")));
                ps.setString(4, MapUtil.getString(rows.get(i), "start_node"));
                ps.setLong(5, ParseUtil.strToLong(MapUtil.getString(rows.get(i), "end_node_id")));
                ps.setString(6, MapUtil.getString(rows.get(i), "end_node"));
                ps.setBigDecimal(7, ParseUtil.strToDecimal(MapUtil.getString(rows.get(i), "distance1")));
                ps.setBigDecimal(8, ParseUtil.strToDecimal(MapUtil.getString(rows.get(i), "distance2")));
                ps.setInt(9, 1);
            }
            public int getBatchSize() {
                return rows.size();
            }
        });
    }

public List<Map<String, Object>> queryList(String sql){
        return impalaTemplate.queryForList(sql);
}
```

2. Java程序 sql 字符串中变量替换

```java
String sql = "SELECT * FROM tableName WHERE date = '${curDate}'";
String curDate = "2020-01-01";
sql = sql.replaceAll("\\$\\{\\s*curDate\\s*}", curDate);
```


