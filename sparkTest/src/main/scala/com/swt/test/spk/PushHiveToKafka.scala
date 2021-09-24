package com.swt.test.spk

import java.util

import com.kye.utils.{JsonUtil, KyeUtils}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{column, struct, to_json}

import scala.collection.JavaConverters.mapAsJavaMapConverter


object PushHiveToKafka extends Serializable {
    def main(args: Array[String]): Unit = {

        val hiveTable = args(0)
        val topicName = args(1)
        var fields = "*"
        if (args.length > 2) {
            fields = args(2)
        }

        val props = KyeUtils.getProperties("test-props")

        val kafkaBrokerList = props.getString("kafka.broker.list")

        val hiveSql = props.getString("test.hiveSql")
            .replaceAll("\\$\\{\\s*tableName\\s*}", hiveTable)
            .replace("*", fields)

        println("hive sql: " + hiveSql)

        val spkSessionBuilder = SparkSession
            .builder()
            .appName(getClass.getSimpleName)
            .enableHiveSupport()

        /*if ("local".equals(props.getString(envKey))) {
            spkSessionBuilder.master("local[*]")
        }*/

        val spkSession = spkSessionBuilder.getOrCreate()

        import spkSession.implicits._

        val hiveDf = spkSession
            //.sql(s"select * from ${hiveTable}")
            .sql(hiveSql)
            .rdd
            .map(row => {
                val map = row.getValuesMap[Object](row.schema.fieldNames).asJava
                /*val map = new util.HashMap[String, Object]()
                for (field <- row.schema.fieldNames) {
                    map.put(field, row.getAs[Object](field))
                }*/
                val value = JsonUtil.bean2Json(map)
                (null.asInstanceOf[String], value)
            })
            .toDF("key", "value")
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBrokerList)
            .option("topic", topicName)
            .save()


        ////// df.saveToPhoenix(Map("table" -> hTableName, "zkUrl" -> zkUrl))
        //df.saveToPhoenix(hTableName, hbaseConf, zkUrl = Option(zkUrl))

    }
}
