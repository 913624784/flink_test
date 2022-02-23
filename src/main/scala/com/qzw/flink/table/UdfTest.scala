package com.qzw.flink.table

import com.qzw.flink.source.SourceTest.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableAggregateFunction, TableFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * @author : qizhiwei
 * @date : 2021/2/19
 * @Description : ${Description}
 */
object UdfTest {
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

    //自定义udf
    val hashCode = new HashCode(2)
    val split = new MySplitUDTF("_")
    val avgTemp = new AvgTempUDAF
    val Top2 = new Top2Temp

    //注册表
    tableEnv.createTemporaryView("sensor", sensorTable)
    //注册 udf
    tableEnv.createTemporarySystemFunction("hashCode", hashCode)
    tableEnv.createTemporarySystemFunction("split", split)
    tableEnv.registerFunction("avgTemp", avgTemp)
    tableEnv.registerFunction("Top2", Top2)

    sensorTable.printSchema()

    val result = sensorTable
      .select($"id", $"ts", hashCode($"id"))
    val sql =
      """
        |select
        | id,
        | ts,
        | hashCode(id)
        |from sensor
        |""".stripMargin
    val sqlResult = tableEnv.sqlQuery(sql)

    //    result.toAppendStream[Row].print("result")
    //    sqlResult.toAppendStream[Row].print("sql")


    val splitTable = sensorTable
      .joinLateral(split($"id") as("word", "length"))
      .select($"id", $"ts", $"word", $"length")
    val splitSql =
      """
        |select
        | id,
        | ts,
        | word,
        | length
        |from
        | sensor,
        | lateral table(split(id)) as splitId(word,length)
        |""".stripMargin
    val splitSqlResult = tableEnv.sqlQuery(splitSql)

    //    splitTable.toAppendStream[Row].print("splitTable")
    //    splitSqlResult.toAppendStream[Row].print("splitSqlResult")


    val avgTable = sensorTable
      .groupBy($"id")
      .aggregate(avgTemp($"temperature") as "avgTemp")
      .select($"id", $"avgTemp")
    val avgSql =
      """
        |select
        | id,
        | avgTemp(temperature) as avgTemp
        |from
        | sensor
        |group by
        | id
        |""".stripMargin
    val avgSqlResult = tableEnv.sqlQuery(avgSql)

    //    avgTable.toRetractStream[Row].print("avgTable")
    //    avgSqlResult.toRetractStream[Row].print("avgSqlResult")


    val tableAvgTable = sensorTable
      .groupBy($"id")
      .flatAggregate(Top2($"temperature") as("temp", "rank"))
      .select($"id", $"temp", $"rank")

    tableAvgTable.toRetractStream[Row].print("tableAvgTable")

    env.execute("UdfTest")
  }
}

//自定义标量函数，一对一
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode * factor - 1000
  }
}

//自定义表函数,一对多
class MySplitUDTF(separator: String) extends TableFunction[Word] {
  def eval(str: String): Unit = {
    str.split(separator).foreach(
      word => collect(Word(word, word.length))
    )
  }
}

//定义一个 class 专门用于表示聚合的状态
class AvgTempAcc {
  var sum: Double = _
  var count: Int = _
}

//自定义聚合函数,求每个传感器的平均温度,保存状态（温度和，传感器数量），多对一
class AvgTempUDAF extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(acc: AvgTempAcc): Double = acc.sum / acc.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  //还需要实现一个处理函数，函数名必须叫 accumulate
  def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
    acc.sum += temp
    acc.count += 1
  }
}

//定义一个 class 专门用于表示表聚合的状态
class Top2TempAcc {
  var highestTemp: Double = _
  var secondHighestTemp: Double = _
}

//自定义表聚合函数，提取 top2 温度值,输出二元组温度和排名
class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc

  //实现计算逻辑
  def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
    if (temp > acc.highestTemp) {
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if (temp > acc.secondHighestTemp) {
      acc.secondHighestTemp = temp
    }
  }

  //输出结果,函数名必须叫 emitValue
  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp, 2))
  }
}

case class Word(word: String, length: Int)
