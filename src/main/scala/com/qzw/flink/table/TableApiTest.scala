package com.qzw.flink.table

import com.qzw.flink.source.SourceTest.SensorReading
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

/**
 * @author : qizhiwei
 * @date : 2021/2/18
 * @Description : ${Description}
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /*
        // 1.1 基于老版本planner 的流处理
        val oldSettings = EnvironmentSettings.newInstance()
          .useOldPlanner()
          .inStreamingMode()
          .build()
        val oldStreamEnv = StreamTableEnvironment.create(env, oldSettings)

        // 1.2 基于老版本planner 的批处理
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchEnv = BatchTableEnvironment.create(batchEnv)

        // 1.3 基于 blink planner 的流处理
        val blinkSettings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()
        val blinkStreamEnv = StreamTableEnvironment.create(env, blinkSettings)

        // 1.4 基于 blink planner 的批处理
        val blinkBatchSettings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inBatchMode()
          .build()
        val blinkBatchEnv = TableEnvironment.create(blinkBatchSettings)
    */

    //2.1 创建表执行环境,默认调用老版本的planner
    val tableEnv = StreamTableEnvironment.create(env)

    val inputPath = "src/main/resources/sensors.txt"
    tableEnv.connect(new FileSystem().path(inputPath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timeStamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    //2.2 从 kafka 读取数据
    tableEnv.connect(new Kafka()
      .version("2.3.0")
      .topic("test")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timeStamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaInputTable")

    // 3.查询转换
    // 3.1 table api
    val inputTable = tableEnv.from("inputTable")
    val result = inputTable
      .select($"id", $"temperature")
      .filter($"id" === "sensor_1")

    // 3.2 table sql
    val sql =
      """
        |select id,temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin
    val sqlResult = tableEnv.sqlQuery(sql)

    inputTable.toAppendStream[(String, Long, Double)].print("inputTable")
    sqlResult.toAppendStream[(String, Double)].print("sqlResult")

    env.execute("TableApiTest")
  }
}
