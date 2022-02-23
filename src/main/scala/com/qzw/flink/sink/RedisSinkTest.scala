package com.qzw.flink.sink

import com.qzw.flink.source.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper._


/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //Source 操作
    val inputPath = "src/main/resources/sensors.txt"
    val ds = env.readTextFile(inputPath)

    //Transform 操作
    val dataStream = ds
      .map(line => {
        val sensor = line.split(",")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble)
      })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    //Sink 操作
    dataStream.addSink(new RedisSink(conf, new MyRedisMapper))
    env.execute("RedisSinkTest")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {

  //定义保存数据到 redis 的命令
  override def getCommandDescription: RedisCommandDescription = {
    //把传感器 id 和温度保存成 hash
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  //定义保存到 redis 的 Key
  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  //定义保存到 redis 的 value
  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}
