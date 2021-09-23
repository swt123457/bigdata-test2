package com.swt.test.flink

import java.util
import java.util.Properties

import com.kye.utils.KyeUtils
import com.swt.common.entity.KafkaBinlogEntity
import com.swt.common.utils.KafkaBinlogSchema
import com.swt.test.flink.caseclass.BinlogClass
import com.swt.test.flink.utils.CkSinkBuilder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters.asScalaBufferConverter

object SyncKfkToCK {
    /**
     * MySQL: bigdata_app.dsp_mysql_stream_test
     * brokerList: 10.120.201.51:9092,10.120.201.52:9092,10.120.201.53:9092
     * zkquorum: 10.120.201.51:2181,10.120.201.52:2181,10.120.201.53:2181
     * topicName: bigdata_app-mysql_stream_test
     * */

    //val zkquorum = "10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181"
    //val kafkaBrokerList = "10.83.192.10:9092,10.83.192.11:9092,10.83.192.12:9092"
    val zkquorum = "10.120.201.51:2181,10.120.201.52:2181,10.120.201.53:2181"
    val kafkaBrokerList = "10.120.201.51:9092,10.120.201.52:9092,10.120.201.53:9092"


    def main(args: Array[String]): Unit = {

        val execEnv = args(0)
        val topicName = args(1)  //metaConf.getString("test.topicName") //"bigdata_app-mysql_stream_test"
        val sinkTable = args(2)  //metaConf.getString("test.sinkTable") //"test.mysql_stream_test"
        val keysStr = args(3)    //metaConf.getString("test.keys").trim

        println("env: " + execEnv)
        println("topic: " + topicName)
        println("sink table: " + sinkTable)
        println("sink keys: " + keysStr)

        val metaConf = KyeUtils.getProperties("meta-conf")

        val parallelism = metaConf.getString(execEnv + ".flink.parallelism").toInt

        val ckUser = metaConf.getString(execEnv + ".ck.user")
        val ckPassword = metaConf.getString(execEnv + ".ck.password")
        val ckBatchSize= metaConf.getString(execEnv + ".ck.batchSize").toInt
        val ckBatchIntervalMs = metaConf.getString(execEnv + ".ck.batchIntervalMs").toInt

        val keys: Array[String] =
            keysStr
                .split(",")
                .map(key => key.trim)
                //.sortWith((a, b) => a.compareTo(b) < 0)

        val insertSql =
            s"""
               | insert into ${sinkTable} ( ${keys.addString(new StringBuilder, ",").toString()} , sync_time )
               |values
               |(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               |""".stripMargin


        /***********************************************************/
        val conf = new Configuration
        conf.setInteger(RestOptions.PORT, 28080)

        val jobName = getClass.getSimpleName + "test2000" //"_001"
        /***********************************************************/

        val kfkProps = new Properties()
        kfkProps.setProperty("bootstrap.servers", kafkaBrokerList)
        kfkProps.setProperty("group.id", jobName)
        kfkProps.setProperty("key.deserializer", classOf[StringDeserializer].getName)
        kfkProps.setProperty("value.deserializer", classOf[StringDeserializer].getName)
        kfkProps.setProperty("auto.offset.reset", "latest")

        //val kfkConsumer = new FlinkKafkaConsumer[String](topicName, new SimpleStringSchema(), kfkProps)
        val kfkConsumer = new FlinkKafkaConsumer[KafkaBinlogEntity](topicName, new KafkaBinlogSchema, kfkProps)

        val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        //val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(parallelism)
        env.enableCheckpointing(5000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

        /*
        val tblEnvSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()

        val tblEnv = StreamTableEnvironment.create(env,tblEnvSettings)
        */

        val stream = env.addSource(kfkConsumer)
            .rebalance
            .flatMap(e => {
                println("+++++++++++++++++++++++++++++++++++++") //todo ========================
                val database = e.getDatabase
                val table = e.getTable
                val pkNames = e.getPkNames.asScala.toArray
                val ts = String.valueOf(e.getTs)
                val data = e.getData.asScala.toArray
                data.map(e => {
                    val pk = pkNames
                        //.sortWith((a, b) => a.compareTo(b) < 0)
                        .map(name => e.get(name))
                        .addString(new StringBuilder, "_").toString()

                    e.put("ts", ts)
                    e.put("rowkey", pk)
                    println("rowkey: " + pk) //todo ==========================================
                    BinlogClass(database, table, pk, e)
                })
            })

        stream.print() //todo ==========================================================

        stream.addSink(JdbcSink.sink[BinlogClass](
            insertSql,
            new CkSinkBuilder(keys),
            new JdbcExecutionOptions
                .Builder()
                .withBatchSize(ckBatchSize)
                .withBatchIntervalMs(ckBatchIntervalMs)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                //.withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .withDriverName("com.github.housepower.jdbc.ClickHouseDriver")
                .withUrl("jdbc:clickhouse://10.83.192.9:19000")
                .withUsername(ckUser)
                .withPassword(ckPassword)
                .build()
        ))

        //tblEnv.fromDataStream(stream, )
        env.execute(jobName)
    }
}
