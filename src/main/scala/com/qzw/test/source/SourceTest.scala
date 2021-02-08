package com.qzw.test.source

import org.apache.flink.streaming.api.scala._

/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object SourceTest {

  case class SensorReading(id: String, timeStamp: Long, temperature: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sensors = env.fromCollection(Seq(
      SensorReading("sensor_1", 1612523073, 35.6),
      SensorReading("sensor_2", 1612523091, 36.1),
      SensorReading("sensor_3", 1612523129, 37.6),
      SensorReading("sensor_4", 1612523135, 38.3)
    ))

    sensors.print("sensors")
    env.execute("SourceTest")
  }
}
