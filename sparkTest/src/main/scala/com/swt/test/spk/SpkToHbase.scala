package com.swt.test.spk

import java.nio.ByteBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.phoenix.schema.SaltingUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}



object SpkToHbase {
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
        val hTableName = "PHOENIX_TEST"
        val cf = "0"
        val zkQuorum = "10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181"

        //hbase 配制代码 ====================================================================
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("zookeeper.znode.parent", "/hbase206")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, hTableName)

        val job = Job.getInstance(hbaseConf)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        //=================================================================================

        val dataStream = ssc.socketTextStream("localhost", 9999)
        dataStream
            .map(input => {
                val array = input.split(",", -1)
                //todo val put = new Put(Bytes.toBytes(array(0)))

                val key = "151952072188449732"
                val b = SaltingUtil.getSaltingByte(key.getBytes(), 0, key.getBytes().length, 10)
                val buffer = ByteBuffer.allocateDirect(1 + "98989898".getBytes().length)
                buffer.put(b)
                buffer.put("98989898".getBytes())

                val put = new Put(Bytes.toBytes(buffer))

                if(StringUtils.isNotBlank(array(1)))
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("NAME"), Bytes.toBytes(array(1)))

                if(StringUtils.isNotBlank(array(2)))
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("AGE"), Bytes.toBytes(array(2)))

                if(StringUtils.isNotBlank(array(3)))
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("SEX"), Bytes.toBytes(array(3)))

                if(StringUtils.isNotBlank(array(4)))
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("JOB"), Bytes.toBytes(array(4)))

                (new ImmutableBytesWritable, put)
            })
            .foreachRDD(rdd => {
                rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
            })

        ssc.start()
        ssc.awaitTermination()
    }
}
