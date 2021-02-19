package com.qzw.test.table

import com.qzw.test.source.SourceTest.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

import java.time.Duration

/**
 * @author : qizhiwei
 * @date : 2021/2/19
 * @Description : ${Description}
 */
object TimeAndWinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val blinkSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, blinkSettings)

    val inputPath = "src/main/resources/sensors.txt"
    val ds = env.readTextFile(inputPath)
    val dataStream: DataStream[SensorReading] = ds
      .map(line => {
        val sensor = line.split(",")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(3)) //分布式数据乱序用这个
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timeStamp * 1000L
        })
      )

    val sensorTable = tableEnv.fromDataStream(dataStream, $"id", $"timeStamp", $"temperature", $"ts".rowtime())

    // group table
    val groupTable = sensorTable
      .window(Tumble over 10.seconds on $"ts" as $"w") //每 10s 统计一次
      .groupBy($"id", $"w")
      .select($"id", $"id".count(), $"temperature".avg(), $"w".end())

    tableEnv.createTemporaryView("sensor", sensorTable)

    val groupSql =
      """
        |select
        | id,
        | count(id),
        | avg(temperature),
        | tumble_end(ts,interval '10' second)
        |from sensor
        | group by
        |  id,
        |  tumble(ts,interval '10' second)
        |""".stripMargin
    val resultGroupSqlTable = tableEnv.sqlQuery(groupSql)

    groupTable.toAppendStream[Row].print("groupTable")
    resultGroupSqlTable.toRetractStream[Row].print("resultSqlTable")

    // over table 统计当前行与前两行的平均温度
    val overTable = sensorTable
      .window(Over partitionBy $"id" orderBy $"ts" preceding 2.rows as $"ow")
      .select($"id", $"ts", $"id".count() over $"ow", $"temperature".avg() over $"ow")

    val overSql =
      """
        |select
        | id,
        | ts,
        | count(id) over ow,
        | avg(temperature) over ow
        |from sensor
        | window ow as (
        |  partition by id
        |  order by ts
        |  rows between 2 preceding and current row
        |)
        |""".stripMargin
    val resultOverSqlTable = tableEnv.sqlQuery(overSql)

    overTable.toAppendStream[Row].print("overTable")
    resultOverSqlTable.toRetractStream[Row].print("resultOverSqlTable")

    //    sensorTable.printSchema()
    //    sensorTable.toAppendStream[Row].print()

    env.execute("TimeAndWinTest")
  }
}
