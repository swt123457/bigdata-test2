package com.swt.test.flink

import org.apache.flink.api.scala._     //为了 Flink API使用Scala的隐式转换
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class Recorder(id: String, eventTime: String, value: Int)

object HelloFlink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(4)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream: DataStream[String] = env.socketTextStream("localhost", 9999)

        stream
            .map(text => {
                val fields = text.split(",", -1)
                Recorder(fields(0), fields(1), fields(2).toInt)
            })
            .filter(recorder => {
                StringUtils.isNotBlank(recorder.id) &&
                    StringUtils.isNotBlank(recorder.eventTime)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Recorder](Time.seconds(5)) {
                override def extractTimestamp(t: Recorder): Long = {
                    t.eventTime.toLong * 1000
                }
            })
            //.map((_, 1)) //.map(str => (str, 1))
            //.map(str => (str, 1))
            .keyBy(rec => rec.id)//keyBy("id")
            .window(TumblingEventTimeWindows.of(Time.seconds(15)))
            //.sum(2)//sum("value")
            .reduce((a, b) => {
                if (a.value > b.value) b
                else a
            })
            .print()
        env.execute("text stream")
    }
}
