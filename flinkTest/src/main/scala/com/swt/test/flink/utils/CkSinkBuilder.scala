package com.swt.test.flink.utils

import java.sql.{PreparedStatement, Timestamp, Types}

import com.kye.utils.DateUtils
import com.swt.test.flink.caseclass.BinlogClass
import org.apache.flink.connector.jdbc.JdbcStatementBuilder

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConverters.asScalaSetConverter

class CkSinkBuilder(keys: Array[String]) extends JdbcStatementBuilder[BinlogClass] {
    override def accept(ps: PreparedStatement, v: BinlogClass): Unit = {
        //val keys = v.data.keySet().asScala.toArray.sortWith((a, b) => a.compareTo(b) < 0)
        /*for(i <- 0 until keys.length) {
            ps.setString(i+1, v.data.get(keys.apply(i)))
        }
        ps.setLong(keys.length+1, System.currentTimeMillis() / 1000)
        */

        /*ps.setObject(1, v.data.get(keys(0)), Types.BIGINT)
        ps.setObject(2, v.data.get(keys(1)), Types.VARCHAR)
        ps.setObject(3, v.data.get(keys(2)), Types.VARCHAR)
        ps.setObject(4, v.data.get(keys(3)), Types.INTEGER)
        ps.setObject(5, v.data.get(keys(4)), Types.VARCHAR)
        ps.setObject(6, v.data.get(keys(5)), Types.BIGINT)
        ps.setObject(7, v.data.get(keys(6)), Types.INTEGER)
        ps.setObject(8, v.data.get(keys(7)), Types.DATE)
        ps.setObject(9, v.data.get(keys(8)), Types.INTEGER)
        ps.setObject(10, v.data.get(keys(9)), Types.VARCHAR)
        ps.setObject(11, v.data.get(keys(10)), Types.BIGINT)*/

        ps.setObject(1, v.data.get(keys(0)).toLong)
        ps.setObject(2, v.data.get(keys(1)))
        ps.setObject(3, v.data.get(keys(2)))
        ps.setObject(4, v.data.get(keys(3)).toInt)
        ps.setObject(5, v.data.get(keys(4)))
        ps.setObject(6, v.data.get(keys(5)).toLong)
        ps.setObject(7, v.data.get(keys(6)).toInt)
        ps.setTimestamp(8, new Timestamp(DateUtils.dateToStamp(v.data.get(keys(7)))))
        ps.setObject(9, v.data.get(keys(8)).toInt)
        ps.setObject(10, v.data.get(keys(9)))
        ps.setObject(11, v.data.get(keys(10)).toLong)

        ps.setTimestamp(12, new Timestamp(System.currentTimeMillis()))

    }
}
