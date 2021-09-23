package com.swt.test.flink

import com.kye.utils.KyeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}

object FlinkHighLevelTask {
    /**
     * MySQL: bigdata_app.dsp_mysql_stream_test
     * brokerList: 10.120.201.51:9092,10.120.201.52:9092,10.120.201.53:9092
     * zkquorum: 10.120.201.51:2181,10.120.201.52:2181,10.120.201.53:2181
     * topicName: bigdata_app-mysql_stream_test
     * */
    val zkquorum = "10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181"
    val kafkaBrokerList = "10.83.192.10:9092,10.83.192.11:9092,10.83.192.12:9092"
    //val hiveConfDir = "/opt/apache/hive/conf"

    //val topic = "flink-test"


    //val fieldNames = "name,sex,age,job"
    //val fieldTypes = "string,string,int,map"
    //val tempTable = "src_table"

    //val sqlQuery = s"select name, sex, age, job['job'], job['offer'] from ${tempTable} where age > 18"
    //val sinkTable = "result_tbl"

    def main(args: Array[String]): Unit = {

        val jobName = getClass.getSimpleName
        val metaConf = KyeUtils.getProperties("meta-conf")
        val srcTopic = metaConf.getString("00002.source.topic").trim
        val srcDdl = metaConf.getString("00002.soure.ddl").trim
        val sinkTopic = metaConf.getString("00002.sink.topic").trim
        val sinkDdl = metaConf.getString("00002.sink.ddl")
        val sqlDml = metaConf.getString("00002.etl.dml").trim

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val tblEnvSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()

        val tblEnv = StreamTableEnvironment.create(env,tblEnvSettings)

        val sqlDdlSrc =
            s"""
               |${srcDdl}
               |with (
               |'connector.type' = 'kafka',
               |'connector.version' = 'universal',
               |'connector.topic' = '${srcTopic}',
               |'connector.properties.zookeeper.connect' = '${zkquorum}',
               |'connector.properties.bootstrap.servers' = '${kafkaBrokerList}',
               |'connector.startup-mode' = 'latest-offset',
               |'format.type' = 'json',
               |'connector.properties.group.id' = '${jobName}'
               |)
               |
               |""".stripMargin

        val sqlDdlSink =
            s"""
               |${sinkDdl}
               |with (
               |'connector.type' = 'kafka',
               |'connector.version' = 'universal',
               |'connector.topic' = '${sinkTopic}',
               |'connector.properties.zookeeper.connect' = '${zkquorum}',
               |'connector.properties.bootstrap.servers' = '${kafkaBrokerList}',
               |'connector.startup-mode' = 'latest-offset',
               |'format.type' = 'json',
               |'connector.properties.group.id' = '${jobName}'
               |)
               |
               |""".stripMargin

        val ddlSql1 =
            s"""
              |create table src_table (
              |    id VARCHAR
              |   ,name VARCHAR
              |   ,timestamp_str VARCHAR
              |   ,type VARCHAR
              |   ,body map<varchar, varchar>
              |) with (
              |'connector.type' = 'kafka',
              |'connector.version' = 'universal',
              |'connector.topic' = 'events_topic',
              |'connector.properties.zookeeper.connect' = '${zkquorum}',
              |'connector.properties.bootstrap.servers' = '${kafkaBrokerList}',
              |'connector.startup-mode' = 'latest-offset',
              |'format.type' = 'json',
              |'connector.properties.group.id' = '${jobName}'
              |)
              |
              |""".stripMargin

        val ddlSql2 =
            s"""
                |create table sink_table (
                |    id  VARCHAR
                |   ,name  VARCHAR
                |   ,timestamp_str  VARCHAR
                |   ,type  VARCHAR
                |   ,appVersion  VARCHAR
                |   ,behavior  VARCHAR
                |   ,deviceId  VARCHAR
                |   ,userId  string
                |   ,userName  VARCHAR
                |) with (
                |'connector.type' = 'kafka',
                |'connector.version' = 'universal',
                |'connector.topic' = 'flink_topic',
                |'connector.properties.zookeeper.connect' = '${zkquorum}',
                |'connector.properties.bootstrap.servers' = '${kafkaBrokerList}',
                |'connector.startup-mode' = 'latest-offset',
                |'format.type' = 'json',
                |'connector.properties.group.id' = '${jobName}'
                |)
                |
                |""".stripMargin

        tblEnv.executeSql(sqlDdlSrc)
        tblEnv.executeSql(sqlDdlSink)

        val resultTbl = tblEnv.sqlQuery(sqlDml)
        //tblEnv.toRetractStream[Row](resultTbl).print()

        resultTbl.executeInsert("sink_table")

        env.execute(jobName)
    }

    def getFlinkSqlSchema(schemaStr: String) ={
        val schema = new Schema()
        if (StringUtils.isNotBlank(schemaStr)){
            val fieldArray = schemaStr.split(",", -1)

            fieldArray.foreach(field => {
                val nameAndType = field.split(":")
                if ("map".equals(nameAndType(1))){
                    schema.field(nameAndType(0), DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                }
                else {
                    schema.field(nameAndType(0), nameAndType(1))
                }
            })
        }
        schema
    }
}
