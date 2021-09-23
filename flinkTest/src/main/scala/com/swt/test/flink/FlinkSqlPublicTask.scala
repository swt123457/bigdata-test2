package com.swt.test.flink

import com.kye.utils.KyeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row

object FlinkSqlPublicTask {
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

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val tblEnvSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()

        val tblEnv = StreamTableEnvironment.create(env,tblEnvSettings)

        /*val hiveCatalog = new HiveCatalog("kyeHive", "default", hiveConfDir)
        tblEnv.registerCatalog("kyeHive", hiveCatalog)
        tblEnv.useCatalog("kyeHive")
        tblEnv.getConfig.setSqlDialect(SqlDialect.HIVE)*/


        val sourcTopic = metaConf.getString("00001.source.topic").trim
        val srcSchemaStr = metaConf.getString("00001.source.schema").trim

        val kafkaParam = new Kafka()
            .version("universal") //指定Kafka的版本，可选参数包含"0.8", "0.9", "0.10", "0.11", 和 "universal"，"universal"为通用版本
            .property("zookeeper.connect", zkquorum)
            .property("bootstrap.servers", kafkaBrokerList)
            .property("group.id", jobName)
            .topic(sourcTopic)
            .startFromLatest()
            .startFromGroupOffsets()

        val srcSchema = getFlinkSqlSchema(srcSchemaStr)

        tblEnv
            .connect(kafkaParam)
            .withFormat(new Json().failOnMissingField(true))
            .withSchema(srcSchema)
            .createTemporaryTable(sourcTopic)


        val sqlQuery = metaConf.getString("00001.etl.sql").trim

        val resultTbl = tblEnv.sqlQuery(sqlQuery)
        //tblEnv.toRetractStream[Row](resultTbl).print()

        val sinkTopic = metaConf.getString("00001.sink.topic").trim
        val sinkSchemaStr = metaConf.getString("00001.sink.schema").trim

        val sinkKafkaParam = new Kafka()
            .version("universal")
            .property("zookeeper.connect", zkquorum)
            .property("bootstrap.servers", kafkaBrokerList)
            .topic(sinkTopic)

        val sinkSchema = getFlinkSqlSchema(sinkSchemaStr)

        tblEnv.connect(sinkKafkaParam)
            .withFormat(new Json())
            .withSchema(sinkSchema)
            .inAppendMode()
            .createTemporaryTable(sinkTopic)


        resultTbl.executeInsert(sinkTopic)

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
