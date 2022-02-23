package com.qzw.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @author : qizhiwei
 * @date : 2021/2/18
 * @Description : ${Description}
 */
object FileOutputTest {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val blinkSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, blinkSettings)

    val inputPath = "src/main/resources/sensors.txt"
    tableEnv.connect(new FileSystem().path(inputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timeStamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val inputTable = tableEnv.from("inputTable")

    val result = inputTable
      .select($"id", $"temperature")
      .filter($"id" === "sensor_1")

    val agg = inputTable
      .groupBy($"id")
      .select($"id", $"id".count() as "cnt")

    val outputPath = "src/main/resources/output_" + System.currentTimeMillis() + ".txt"
    tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("outputTable")

    result.executeInsert("outputTable")

    agg.toRetractStream[(String, Long)].print("agg")

    /**
     * Blink Planner:从 1.11 版本开始，sqlUpdate 方法 和 insertInto 方法被废弃，从这两个方法构建的 Table 程序必须通过 StreamTableEnvironment.execute() 方法执行，而不能通过 StreamExecutionEnvironment.execute() 方法来执行。
     * Old Planner:从 1.11 版本开始，sqlUpdate 方法 和 insertInto 方法被废弃。对于 Streaming 而言，如果一个 Table 程序是从这两个方法构建出来的，必须通过 StreamTableEnvironment.execute() 方法执行，而不能通过 StreamExecutionEnvironment.execute() 方法执行；对于 Batch 而言，如果一个 Table 程序是从这两个方法构建出来的，必须通过 BatchTableEnvironment.execute() 方法执行，而不能通过 ExecutionEnvironment.execute() 方法执行。
     */
    env.execute("FileOutputTest")
  }
}
