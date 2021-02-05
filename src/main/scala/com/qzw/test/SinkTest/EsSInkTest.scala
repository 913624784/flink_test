package com.qzw.test.SinkTest

import com.qzw.test.SourceTest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object EsSInkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //Source 操作
    val inputPath = "src/main/resources/sensors.txt"
    val ds = env.readTextFile(inputPath)

    //Transform 操作
    val dataStream = ds
      .map(line => {
        val sensor = line.split(", ")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble)
      })

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](httpHosts, new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //包装成 map 或者 json
        val json = new util.HashMap[String, String]()
        json.put("sensor_id", t.id)
        json.put("temperature", t.temperature.toString)
        json.put("ts", t.timeStamp.toString)

        //创建 index request，准备发送数据
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("read_data")
          .source(json)

        //利用 RequestIndexer 发送请求，写数据
        requestIndexer.add(indexRequest)
      }
    })

    //Sink 操作
    dataStream.addSink(esSinkBuilder.build())
    env.execute("EsSInkTest")
  }
}
