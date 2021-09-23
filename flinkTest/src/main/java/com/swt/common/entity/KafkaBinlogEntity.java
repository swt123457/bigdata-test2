package com.swt.common.entity;

import java.util.ArrayList;
import java.util.HashMap;

public class KafkaBinlogEntity {
    public ArrayList<HashMap<String, String>> data;

    public String database;

    //public long es;

    //public long id;

    //public boolean isDdl;

    //public String mysqlType;

    public ArrayList<String> pkNames;

    public String table;

    public long ts;

    public String type;

    public ArrayList<HashMap<String, String>> getData() {
        return data;
    }

    public void setData(ArrayList<HashMap<String, String>> data) {
        this.data = data;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public ArrayList<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(ArrayList<String> pkNames) {
        this.pkNames = pkNames;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
