package com.swt.test.flink

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.windowing.time.Time


case class Record(id: String, eventTime: Long, value: Long)

object FlinkTableStream {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(4)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream: DataStream[String] = env.socketTextStream("localhost", 9999)

        val timeStream =
            stream
                .map(text => {
                    val fields = text.split(",", -1)
                    Record(fields(0), fields(1).toLong, fields(2).toLong)
                })
                .filter(recorder => {
                    StringUtils.isNotBlank(recorder.id) &&
                        recorder.eventTime > 0
                })
                .assignTimestampsAndWatermarks(
                    new BoundedOutOfOrdernessTimestampExtractor[Record](Time.seconds(5)) {
                        override def extractTimestamp(t: Record): Long = {
                            t.eventTime * 1000
                        }
                    }
                )

        val envSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build() //todo useAnyPlanner()需设置
        // val tblEnv = BatchTableEnvironment.create(env, envSettings)   // 批量计算
        val tblEnv = StreamTableEnvironment.create(env, envSettings)     // 流式计算

        /*    .registerDataStream("recorders", timeStream, 'id, 'eventTime.rowtime, 'value)
        val table = tblEnv.sqlQuery(
            """
              |select id, sum(value) as cnt
              | from recorders where value > 0
              | group by id
              """.stripMargin)
        table.toRetractStream[(String, Int)]
            .print()*/

//        val exp = Expressions.$("name")
//        val expres = Array[ApiExpression](exp,exp)
        val colNames = Array("id", "value", "eventTime")
        val colExpressions = colNames.map(col => {
            if ("eventTime".equals(col))
                Expressions.$(col).rowtime()
            else
                Expressions.$(col)
        })

//        val table = tblEnv.fromDataStream(timeStream, colExpressions)

//        val sqlQuery = tblEnv.fromDataStream(timeStream, 'id, 'value, 'eventTime.rowtime)
        val sqlQuery = tblEnv.fromDataStream(timeStream, colExpressions:_*)
            .window(Tumble over 15.seconds on 'eventTime as 'ts)
            .groupBy('id, 'ts)
            .select('id, 'value.sum)

        //tblEnv.toAppendStream[(String, Long)](sqlQuery).print()
        tblEnv.toAppendStream[Row](sqlQuery).print()

        env.execute("text stream")
    }
}
