package com.swt.test.spk

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.phoenix.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkToPhoenix {

    def main(args: Array[String]): Unit = {
        val spkSession = SparkSession.builder()
            .master("local[*]")
            .appName("jdbc_test")
            //todo .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            //todo .enableHiveSupport()//todo
            .getOrCreate()

        spkSession.sparkContext.setLogLevel("WARN")

        val ssc = new StreamingContext(spkSession.sparkContext, Seconds(10))
        /*
                val kafkaParams = Map[String, Object](
                    "bootstrap.servers" -> "node1:9092,node2:9092,node3:9092",//todo
                    "key.deserializer" -> classOf[StringDeserializer],
                    "value.deserializer" -> classOf[StringDeserializer],
                    "group.id" -> "MyGroupId",//todo

                    /**
                     *
                     *  earliest ：当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始
                     *  latest：自动重置偏移量为最大偏移量【默认】*
                     *  none:没有找到以前的offset,抛出异常
                     */
                    "auto.offset.reset" -> "earliest",

                    /**
                     * 当设置 enable.auto.commit为false时，不会自动向kafka中保存消费者offset.需要异步的处理完数据之后手动提交
                     */
                    "enable.auto.commit" -> (false: java.lang.Boolean)//默认是true
                )

                val topics = Array[String]("topic-binlog")//todo

                val dataStream = KafkaUtils.createDirectStream(
                    ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Subscribe[String, String]()
                )*/

        //hbase 配制代码 ====================================================================
        val hTableName = "PHOENIX_TEST" //todo phoenix 表名注意大小写

        val zkQuorum = "10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181"
        val zkUrl = "10.83.192.6,10.83.192.7,10.83.192.8:2181"

        //hbase 配制代码 ====================================================================
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("zookeeper.znode.parent", "/hbase206")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, hTableName)

        /*val job = Job.getInstance(hbaseConf)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])*/
        //=================================================================================

        val dataStream = ssc.socketTextStream("localhost", 9999)

        dataStream
            .map(input => {
                val array = input.split(",", -1)
                (array(0), array(1), array(2), array(3), array(4))
            })
            .foreachRDD(rdd => {
                //todo 注意 phoenix 表中 rowkey 字段默认为大写，如果phoenix建表时，字段名设置为小写，以下必须为小写，否则字段名须为大写，不加列族名
                rdd.saveToPhoenix(hTableName, Seq("ID", "NAME", "AGE", "SEX", "JOB"), conf = hbaseConf, zkUrl = Some(zkUrl))
            })

        ssc.start()
        ssc.awaitTermination()
    }
}
