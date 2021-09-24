package com.swt.test.spk

import java.nio.ByteBuffer
import java.math.{BigDecimal => JDecimal}

import com.kye.utils.KyeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.Bytes
import org.apache.phoenix.schema.SaltingUtil
import org.apache.phoenix.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object HiveSpkToPhoenix {

    //val hTableName = "WAYBILL_ANALYSIS_BASE" //todo phoenix 表名注意大小写
    val zkQuorum = "10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181"
    val zkUrl = "10.83.192.6,10.83.192.7,10.83.192.8:2181"
    val cf = "0"

    val envKey = "run.env"
    val sqlKey = "hive.sql"
    val phoenixColsKey = "phoenix.columnNames"
    val tableNameKey = "phoenix.tableName"

    def main(args: Array[String]): Unit = {

        val spkSessionBuilder = SparkSession
            .builder()
            .appName(getClass.getSimpleName)
            .enableHiveSupport()

        val props = KyeUtils.getProperties("test-props")
        if ("local".equals(props.getString(envKey))) {
            spkSessionBuilder.master("local[*]")
        }

        val sql = props.getString(sqlKey)
        val colNames = props.getString(phoenixColsKey).split(",", -1)

        val hTableName = props.getString(tableNameKey)

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

        val spkSession = spkSessionBuilder.getOrCreate()

        val df = spkSession
            .sql(sql).toDF("WAYBILL_ID","ID","PAYMENT_CUSTOMER_ID","SHIPING_COMPANY_ID",
            "DISCOUNT_FREIGHT","GROSS_PROFIT","FREIGHT_WEIGHT","OPERATION_WEIGHT","SERVICE_TYPE",
            "SHIPING_TIME","SCRAP_FLAG","ENABLED_FLAG","WAYBILL_TYPE","FINANCE_AUDIT_STATUS","OTHER_TIME_1","SIGNING_TIME")

        //"select id, waybill_id, waybill_number,shiping_company from vdm_crm.v_o_waybill_analysis_base limit 1"

        ////// df.saveToPhoenix(Map("table" -> hTableName, "zkUrl" -> zkUrl))
        df.saveToPhoenix(hTableName, hbaseConf, zkUrl = Option(zkUrl))

        //////////////////////////////////////////////////////////////////////
        /*df.rdd
            .map(row => {
//                val cols = row.toSeq.map(col => KyeUtils.toString(col))
//                (cols(0), cols(1), cols(2), cols(3))
                (
                    KyeUtils.toString(row(0)),
                    KyeUtils.toString(row(1)),
                    KyeUtils.toString(row(2)),
                    KyeUtils.toString(row(3)),
                    KyeUtils.toString(row(4)),
                    KyeUtils.toString(row(5)),
                    KyeUtils.toString(row(6)),
                    KyeUtils.toString(row(7)),
                    KyeUtils.toString(row(8)),
                    KyeUtils.toString(row(9)),
                    KyeUtils.toString(row(10)),
                    KyeUtils.toString(row(11)),
                    KyeUtils.toString(row(12)),
                    KyeUtils.toString(row(13)),
                    KyeUtils.toString(row(14)),
                    KyeUtils.toString(row(15)),
                    KyeUtils.toString(row(16)),
                    KyeUtils.toString(row(17)),
                    KyeUtils.toString(row(18)),
//                    KyeUtils.toString(row(19)),
//                    KyeUtils.toString(row(20)),
//                    KyeUtils.toString(row(21)),
//                    KyeUtils.toString(row(22)),
//                    KyeUtils.toString(row(23)),
                    row.getDecimal(24),
                    row.getDecimal(25),
                    row.getDecimal(26)
                )
            })
            .saveToPhoenix(hTableName, colNames, conf = hbaseConf, zkUrl = Option(zkUrl))*/
        /////////////////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////////////
        /*
        df.rdd
            .map(row => {
                row.toSeq
                    .map(col => KyeUtils.toString(col))
                    .addString(new StringBuilder, "|")
                    .toString()
            }).coalesce(6).saveAsTextFile("hdfs:////tmp/waybill_phoenix_text.txt")
        */
        /////////////////////////////////////////////////////////////////////////

        /*df.rdd
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
                        if (col.equals("STANDARD_AMOUNT") || col.equals("DRIVER_AMOUNT")){
                            put0.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(new JDecimal("23.32")))
                        }
                        else
                            put0.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(getColInRow(row, col.toLowerCase))) //row.getAs[String](col.toLowerCase)
                    }
                })
                val standard_amount =  (row.getDecimal(row.fieldIndex("standard_amount"))) //row.getAs[String]("standard_amount")
                val driver_amount = row.getDecimal(row.fieldIndex("driver_amount"))//getColInRow(row, "driver_amount") //row.getAs[String]("driver_amount")

                //val putIdx = getIndexPut(wayBillId, standard_amount, driver_amount, putAndSalt._2)
                //putIdx.addColumn(Bytes.toBytes("L#0"), Bytes.toBytes("_0"), Bytes.toBytes("_0"))

                Array((new ImmutableBytesWritable, put0)/*, (new ImmutableBytesWritable, putIdx)*/)
            }).saveAsNewAPIHadoopDataset(job.getConfiguration)*/
        /**
        val conf = new SparkConf()
            .setAppName(getClass.getSimpleName)
            //.setMaster("local[*]")

        val sc = new SparkContext(conf)
        val hc = new HiveContext(sc)
        hc.sql(
            "select id, waybill_id, waybill_number,shiping_company from vdm_crm.v_o_waybill_analysis_base limit 1"
        )
            .show()
        sc.stop()
        **/
    }

    def getSaltedPut(wayBillId: String): (Put, Byte) = {
        val saltedByte = SaltingUtil.getSaltingByte(wayBillId.getBytes(), 0, wayBillId.getBytes().length, 10)
        val buffer = ByteBuffer.allocateDirect(1 + wayBillId.getBytes().length)
        buffer.put(saltedByte)
        buffer.put(wayBillId.getBytes())
        (new Put(Bytes.toBytes(buffer)), saltedByte)
    }

    def getIndexPut(pk: String, standardAmount: String, driverAmount: String, saltedByte: Byte): Put = {
        val buffer = ByteBuffer.allocateDirect(5 + standardAmount.getBytes.length + driverAmount.getBytes().length + pk.getBytes().length)
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
            KyeUtils.toString(row.get(row.fieldIndex(colName)))
        }catch {
            case e: Exception => ""
        }
    }
}
