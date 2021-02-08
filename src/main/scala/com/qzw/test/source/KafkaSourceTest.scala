package com.qzw.test.source

import com.qzw.test.source.SourceTest.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import java.util.Properties
import scala.util.Random

/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object KafkaSourceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "KafkaSourceTest")
    properties.setProperty("auto.offset.reset", "latest")

    //    val kafka = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))
    val kafka = env.addSource(new SensorSource)

    kafka.print()
    env.execute("KafkaSourceTest")
  }
}

/**
 * 自定义 source
 */
class SensorSource extends SourceFunction[SensorReading] {

  var flag = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    var temp = 1.to(10).map(
      //正太分布 高斯随机数 [-2,+2] 概率 99%
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )
    while (flag) {
      temp = temp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      val curTime = System.currentTimeMillis()
      temp.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    flag = false
  }
}
