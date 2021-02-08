package com.qzw.test.SinkTest

import com.qzw.test.SourceTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "KafkaSourceTest")
    properties.setProperty("auto.offset.reset", "latest")

    //Source 操作
    val kafka = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))

    //Transform 操作
    val dataStream = kafka
      .map(line => {
        val sensor = line.split(", ")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble).toString
      })

    //Sink 操作
    dataStream.addSink(new FlinkKafkaProducer[String]("localhost:9092", "test", new SimpleStringSchema()))
    env.execute("KafkaSinkTest")
  }
}
