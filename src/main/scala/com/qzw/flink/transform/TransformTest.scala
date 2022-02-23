package com.qzw.flink.transform

import com.qzw.flink.SplitProcess
import com.qzw.flink.source.SourceTest.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "src/main/resources/sensors.txt"
    val ds = env.readTextFile(inputPath)
    val wd: DataStream[SensorReading] = ds
      .map(line => {
        val sensor = line.split(",")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble)
      })

    val agg = wd.keyBy(_.id)
      //      .sum(2)
      //输出最新温度，时间戳是上一次的时间加 10
      .reduce((x, y) => SensorReading(y.id, x.timeStamp + 10, y.temperature))


    //split and select
    val splitStream = wd.process(new SplitProcess(37.0))

    val highStream = splitStream.getSideOutput(new OutputTag[SensorReading]("high"))
    val lowStream = splitStream.getSideOutput(new OutputTag[SensorReading]("low"))

    //connect(两条流的数据类型可以不同，只能两条流合并) and coMap/coFlatMap
    val warning = highStream.map(sensor => (sensor.id, sensor.temperature))
    val connectedStreams = warning.connect(lowStream)

    val coMapStreams = connectedStreams.map(
      warnData => (warnData._1, warnData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    //union 可以合并多条流，但是多条流的数据结构必须一样
    val unionStream = highStream.union(lowStream)

    wd.filter(new MyFilter).print()
    env.execute("TransformTest")
  }
}

//自定义 Filter
class MyFilter extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

//富函数，有一些可以操作生命周期的方法
class MyMapper extends RichMapFunction[SensorReading, String] {
  override def map(in: SensorReading): String = {
    "flink"
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)
}
