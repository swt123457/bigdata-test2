package com.swt.test.spk

import java.nio.ByteBuffer
import java.math.{BigDecimal => JDecimal}

import com.kye.utils.KyeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.phoenix.schema.SaltingUtil
import org.apache.phoenix.spark._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.Encoder

object LocalSpkToPhoenix extends Serializable {
    val envKey = "run.env"
    val phoenixColsKey="phoenix.columnNames"

    val zkQuorum = "10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181"
    val zkUrl = "10.83.192.6,10.83.192.7,10.83.192.8:2181"
    val tableName = "WAYBILL_PHOENIX"
    val cf = "0"
    val jdbc = "jdbc:phoenix:10.83.192.6,10.83.192.7,10.83.192.8:2181:/hbase206"//?useUnicode=true&characterEncoding=utf8

    val props = KyeUtils.getProperties("test-props")
    val colNames = props.getString(phoenixColsKey).split(",", -1)
    val schema = StructType(
        colNames.map(colName => {
            StructField(colName.toLowerCase, StringType, true)
        })
    )

    def main(args: Array[String]): Unit = {

        //hbase 配制代码 ====================================================================
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("zookeeper.znode.parent", "/hbase206")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

        val job = Job.getInstance(hbaseConf)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        //=================================================================================

        /*val spkConf = new SparkConf()
            .setAppName(getClass.getSimpleName)
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "6g")
            .setMaster("local[12]")

        val sc = new SparkContext(spkConf)*/
        val spkSessionBuilder = SparkSession
            .builder()
            .appName(getClass.getSimpleName)
            .enableHiveSupport()

        if ("local".equals(props.getString(envKey))) {
            spkSessionBuilder.master("local[8]")
        }

        val spkSession = spkSessionBuilder.getOrCreate()

        //val rdd = sc.textFile("file:\\D:\\data\\part-000" + args(0))
        //val rdd = spkSession.sparkContext.textFile("file:\\D:\\data\\part-*", 12)
        val rdd = spkSession.sparkContext.textFile("file:\\D:\\data\\part-000" + args(0))
            .map(line => {
                line.split("\\|", 27)
            })
            .filter(row => row.length == 27)
            .map(row => {
                /*val cols = line.split("\\|", 27)
                    .toSeq
                Row.fromSeq(cols)*/
                (
                    KyeUtils.toString(row(0)).trim,
                    KyeUtils.toString(row(1)).trim,
                    KyeUtils.toString(row(2)).trim,
                    KyeUtils.toString(row(3)).trim,
                    KyeUtils.toString(row(4)).trim,
                    KyeUtils.toString(row(5)).trim,
                    KyeUtils.toString(row(6)).trim,
                    KyeUtils.toString(row(7)).trim,
                    KyeUtils.toString(row(8)).trim,
                    KyeUtils.toString(row(9)).trim,
                    KyeUtils.toString(row(10)).trim,
                    KyeUtils.toString(row(11)).trim,
                    KyeUtils.toString(row(12)).trim,
                    KyeUtils.toString(row(13)).trim,
                    KyeUtils.toString(row(14)).trim,
                    KyeUtils.toString(row(15)).trim,
                    KyeUtils.toString(row(16)).trim,
                    KyeUtils.toString(row(17)).trim,
                    KyeUtils.toString(row(18)).trim,
                    //                    KyeUtils.toString(row(19)),
                    //                    KyeUtils.toString(row(20)),
                    //                    KyeUtils.toString(row(21)),
                    //                    KyeUtils.toString(row(22)),
                    //                    KyeUtils.toString(row(23)),
                    toBigDecimal(row(24)),
                    toBigDecimal(row(25)),
                    toBigDecimal(row(26))
                )
            })
        rdd.saveToPhoenix(tableName, colNames.toSeq, conf = hbaseConf, zkUrl = Some(zkUrl))

        //val df = spkSession.createDataFrame(rdd, schema)
        /*    /////////////////////////////////////////////////////////////////////////
            rdd
            .filter(row => { //row.getAs("waybill_id"), row.getAs("standard_amount"), row.getAs("driver_amount")
                StringUtils.isNotBlank(getColInRow(row, "waybill_id")) &&
                    StringUtils.isNotBlank(getColInRow(row, "standard_amount")) &&
                    StringUtils.isNotBlank(getColInRow(row, "driver_amount"))
            })
            .flatMap(row => {
                val wayBillId = getColInRow(row, "waybill_id") //row.getAs[String]("waybill_id")
                val putAndSalt = getSaltedPut(wayBillId)
                val put0 = putAndSalt._1
                colNames.foreach(col => {
                    if(!col.equals("WAYBILL_ID")){
                        put0.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(getColInRow(row, col.toLowerCase))) //row.getAs[String](col.toLowerCase)
                    }
                })
                val standard_amount = getColInRow(row, "standard_amount") //row.getAs[String]("standard_amount")
                val driver_amount = getColInRow(row, "driver_amount") //row.getAs[String]("driver_amount")

                val putIdx = getIndexPut(wayBillId, standard_amount, driver_amount, putAndSalt._2)
                putIdx.addColumn(Bytes.toBytes("L#0"), Bytes.toBytes("_0"), Bytes.toBytes("_0"))

                Array((new ImmutableBytesWritable, put0), (new ImmutableBytesWritable, putIdx))
            }).saveAsNewAPIHadoopDataset(job.getConfiguration)
        /////////////////////////////////////////////////////////////////////////   */

        //df.saveToPhoenix(tableName, hbaseConf, zkUrl = Option(zkUrl))
        /*df.write
            .format("org.apache.phoenix.spark")
            .mode(SaveMode.Overwrite)
            .option("table", tableName)
            .option("zkUrl", jdbc)
            .save()*/

        /*val rdd = sc.textFile("file:\\D:\\data\\part-000" + args(0))
        rdd.map(line => {
            val colValues = new String(line.getBytes("UTF-8"), "UTF-8").split(",", -1)
            colValues.toList
        }).filter(cols => {
            if("150545847553988768".equals(cols(0))){
                println("===================================================")
                println(cols)
                println(s"===================== ${cols.length} =============================")
                true
            }
            else false
        }).saveToPhoenix(tableName, phoenixColsArray.toSeq, conf = hbaseConf, zkUrl = Some(zkUrl))*/
        spkSession.stop()
    }

    def getSaltedPut(wayBillId: String): (Put, Byte) = {
        val saltedByte = SaltingUtil.getSaltingByte(wayBillId.getBytes(), 0, wayBillId.getBytes().length, 10)
        val buffer = ByteBuffer.allocateDirect(1 + wayBillId.getBytes().length)
        buffer.put(saltedByte)
        buffer.put(wayBillId.getBytes())
        (new Put(Bytes.toBytes(buffer)), saltedByte)
    }

    def getIndexPut(pk: String, standardAmount: String, driverAmount: String, saltedByte: Byte): Put = {
        val buffer = ByteBuffer.allocateDirect(5 + standardAmount.getBytes().length + driverAmount.getBytes().length + pk.getBytes().length)
        buffer.put(saltedByte)
        buffer.put(Integer.valueOf(0).byteValue())
        buffer.put(Integer.valueOf(0).byteValue())
        buffer.put(standardAmount.getBytes())
        buffer.put(Integer.valueOf(0).byteValue())
        buffer.put(driverAmount.getBytes())
        buffer.put(Integer.valueOf(0).byteValue())
        buffer.put(pk.getBytes())

        new Put(Bytes.toBytes(buffer))
    }

    def getColInRow(row: Row, colName: String): String = {
        try{
            KyeUtils.toString(row.get(schema.fieldIndex(colName)))
        }catch {
            case e: Exception => ""
        }
    }

    def toBigDecimal(num: String): JDecimal = {
        try{
            new JDecimal(num.trim).setScale(13, 4)
        }catch {
            case e: Exception => new JDecimal("0.00").setScale(13, 4)
        }
    }

}
