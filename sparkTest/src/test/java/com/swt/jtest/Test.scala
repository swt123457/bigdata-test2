package com.swt.jtest

import java.util

import com.kye.utils.KyeUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.phoenix.schema.SaltingUtil

import scala.collection.JavaConversions.asScalaBuffer


object Test {
    def main(args: Array[String]): Unit = {

        val list = new util.ArrayList[Int]()
        val value = list.iterator()
        list.add(1)
        list.add(2)
        list.add(3)
        list.add(4)
        for (i <- 1 to list.length) {
            if (i % 2 == 0) {
                list.remove(i - 1)
            }
        }
        list.foreach(i => {
            list.remove()
        })

        list.foreach(i => {
            println(i)
        })

        //hbaseTest()
        return

        val key = "150545847553988768"
        //val b = SaltingUtil.getSaltedKey(new ImmutableBytesWritable("151952072188449732".getBytes()), 10)
        val b = SaltingUtil.getSaltingByte(key.getBytes(), 0, key.getBytes().length, 10)
        println(b.toHexString)
    }

    def hbaseTest(): Unit ={
        val zkQuorum = "10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181"
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("zookeeper.znode.parent", "/hbase206")

        val put1 = new Put(Bytes.toBytes("77777"))
        put1.addColumn(Bytes.toBytes("0"), Bytes.toBytes("NAME"), Bytes.toBytes("Biden"))
        put1.addColumn(Bytes.toBytes("0"), Bytes.toBytes("AGE"), Bytes.toBytes("76"))
        put1.addColumn(Bytes.toBytes("0"), Bytes.toBytes("SEX"), Bytes.toBytes("M"))
        put1.addColumn(Bytes.toBytes("0"), Bytes.toBytes("JOB"), Bytes.toBytes("ShaBi"))
        put1.addColumn(Bytes.toBytes("0"), Bytes.toBytes("OFFER"), Bytes.toBytes(23.3200))

        val put2 = new Put(Bytes.toBytes("66666"))
        put2.addColumn(Bytes.toBytes("0"), Bytes.toBytes("NAME"), Bytes.toBytes("Pelosi"))
        put2.addColumn(Bytes.toBytes("0"), Bytes.toBytes("AGE"), Bytes.toBytes("74"))
        put2.addColumn(Bytes.toBytes("0"), Bytes.toBytes("SEX"), Bytes.toBytes("F"))
        put2.addColumn(Bytes.toBytes("0"), Bytes.toBytes("JOB"), Bytes.toBytes("NiggerFuck"))
        put2.addColumn(Bytes.toBytes("0"), Bytes.toBytes("OFFER"), Bytes.toBytes("23.3200"))

        val put3 = new Put(Bytes.toBytes("55555"))
        put3.addColumn(Bytes.toBytes("0"), Bytes.toBytes("NAME"), Bytes.toBytes("Hillary"))
        put3.addColumn(Bytes.toBytes("0"), Bytes.toBytes("AGE"), Bytes.toBytes("74"))
        put3.addColumn(Bytes.toBytes("0"), Bytes.toBytes("SEX"), Bytes.toBytes("F"))
        put3.addColumn(Bytes.toBytes("0"), Bytes.toBytes("JOB"), Bytes.toBytes("ErBi"))
        put3.addColumn(Bytes.toBytes("0"), Bytes.toBytes("OFFER"), Bytes.toBytes(new java.math.BigDecimal("23.32").setScale(13,4)))

        val conn = ConnectionFactory.createConnection(hbaseConf)
        val table = conn.getTable(TableName.valueOf("decimal_test"))

        table.put(put1)
        table.put(put2)
        table.put(put3)
        table.close()
        conn.close()

    }

    def tupleTest = {
        val strArray = Array("1", "2")
        toTuple(strArray)
    }

    def toTuple(elements: Array[String]) = {
        elements match {
            case Array(a, b, c, _*) => (a, b, c)
        }
    }

    def seqToString(): Unit ={
        val seq = Seq("111", "", 222, 666, "aaa", "", "").addString(new StringBuilder, ",")
        println(seq.toString())
    }

    def strTest(): Unit ={
        /*val str =
            """
               |select *
               |from table
               |where id = 111
               """*/

        val key = "hive.sql"
        val str = KyeUtils.getProperties("test-props").getString(key)
        println(str)
        println("------------------")
        println(str.stripMargin)
        println("======================")

    }

    def strSplitTest(): Unit ={
        val str = ",#,##,#"
        val arr = str.split("#", -1)
        println(arr.length)
        arr.foreach(e =>
            println("----------:" + e + ":----------")
        )
    }

    def tuple = {
        val row = Seq("")
        val value = (KyeUtils.toString(row(0)),
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
            KyeUtils.toString(row(19)),
            //KyeUtils.toString(row(20)),
            //KyeUtils.toString(row(21)))
            //KyeUtils.toString(row(22))
            KyeUtils.toString(row(23)),KyeUtils.toString(row(24)))
    }
}
