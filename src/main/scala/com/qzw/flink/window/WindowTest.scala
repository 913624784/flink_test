package com.qzw.flink.window

import com.qzw.flink.source.SourceTest.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter

import java.time.Duration

/**
 * @author : qizhiwei
 * @date : 2021/2/8
 * @Description : ${Description}
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lateTag = new OutputTag[SensorReading]("late")
    //时间语义 默认 ProcessTime 在 Flink 1.12 中，默认的流时间特性已更改为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)
    val socketDs = env.socketTextStream("localhost", 9999)
    val ds: DataStream[SensorReading] = socketDs
      .map(line => {
        val sensor = line.split(",")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble)
      })
      //      .assignAscendingTimestamps(_.timeStamp * 1000L) //数据中时间戳是升序的时候可以用这个
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(3)) //分布式数据乱序用这个
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timeStamp * 1000L
        }).withIdleness(Duration.ofMinutes(10)) //处理空闲数据源
      )


    //每 15s 统计一次窗口内温度的最小值
    val result = ds
      .keyBy(_.id)
      //      .window(TumblingEventTimeWindows.of(Time.seconds(5))) //滚动窗口
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(3)) //滑动窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(5))) //会话窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(15))) // 取决于setStreamTimeCharacteristic 设置
      .allowedLateness(Time.minutes(1)) //允许数据迟到的最大时间
      .sideOutputLateData(lateTag) //将迟到的数据输出到侧输出流
      //      .countWindow(10)
      //      .minBy(3)
      .reduce((x1, x2) => SensorReading(x1.id, x2.timeStamp, x1.temperature.min(x2.temperature)))

    val lateResult = result.getSideOutput(lateTag)
    lateResult.print("late")
    result.print("result")
    env.execute("WindowTest")

  }
}

class MyReduce extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t1.timeStamp, t.temperature.min(t1.temperature))
  }
}