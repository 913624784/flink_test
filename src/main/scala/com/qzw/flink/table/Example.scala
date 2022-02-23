package com.qzw.flink.table

import com.qzw.flink.source.SourceTest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @author : qizhiwei
 * @date : 2021/2/18
 * @Description : ${Description}
 */
object Example {
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
    //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //创建表
    val dataTb = tableEnv.fromDataStream(wd)
    tableEnv.createTemporaryView("dataTb", dataTb)

    val sql = "select id,temperature from dataTb where id = 'sensor_1'"
    val resultSql = tableEnv.sqlQuery(sql)

    //调用 table api
    val result = dataTb
      .select($"id", $"temperature")
      .filter($"id" === "sensor_1")

    result.toAppendStream[(String, Double)].print("result")
    resultSql.toAppendStream[(String, Double)].print("resultSql")
    env.execute("Example")
  }
}
