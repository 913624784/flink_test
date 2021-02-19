package com.qzw.test.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @author : qizhiwei
 * @date : 2021/2/19
 * @Description : ${Description}
 */
object KafkaPipeLineTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val blinkSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, blinkSettings)

    tableEnv.connect(new Kafka()
      .version("2.3.0")
      .topic("input_test")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timeStamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaInputTable")

    val sql =
      """
        |select id,temperature
        |from kafkaInputTable
        |where id = 'sensor_1'
        |""".stripMargin
    val sqlResult = tableEnv.sqlQuery(sql)

    tableEnv.connect(new Kafka()
      .version("2.3.0")
      .topic("sink_test")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutputTable")

    sqlResult.executeInsert("kafkaOutputTable")

    env.execute("KafkaPipeLineTest")

  }
}
