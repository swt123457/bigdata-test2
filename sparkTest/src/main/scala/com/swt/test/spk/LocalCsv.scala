package com.swt.test.spk

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

object LocalCsv {
    def main(args: Array[String]): Unit = {
        System.setProperty("hadoop.home.dir", "D:\\apps\\hadoop")
        if (StringUtils.isBlank(args(0))) {
            println("-----------------   no param for text file name.   --------------------")
        }
        val fileName = args(0)
        val spkSessionBuilder = SparkSession
            .builder()
            .appName(getClass.getSimpleName)
            .master("local[8]")

        val spkSession = spkSessionBuilder.getOrCreate()

        val rdd = spkSession.sparkContext.textFile("file:\\D:\\data\\" + fileName)
            .map(line => {
                var rst = line.replaceAll("\"\t\"", " , ")
                if (rst.startsWith("\"")) {
                    rst = rst.drop(1)
                }
                if (rst.endsWith("\"")) {
                    rst = rst.dropRight(1)
                }
                rst
            })
        rdd.coalesce(1).saveAsTextFile("file:\\D:\\data\\" + fileName + ".new")

        spkSession.stop()
    }
}
