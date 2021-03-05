package com.qzw.test.table

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @author : qizhiwei
 * @date : 2021/3/5
 * @Description : ${Description}
 */
object FlinkSqlDDL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val blinkSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, blinkSettings)
    // 创建kafka源表
    val createTable =
      """
        |CREATE TABLE test_tb (
        |    impression_id string,
        |    uaa_user_id string,
        |    album_id string,
        |    episode_id bigint,
        |    bidding_price double,
        |    request_time string,
        |    ts as TO_TIMESTAMP(request_time),
        |    pt as PROCTIME(),
        |    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        |)
        |WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'test',
        |    'scan.startup.mode' = 'latest-offset',
        |    'properties.bootstrap.servers' = 'localhost:9092',
        |    'format' = 'json'
        |)
            """.stripMargin
    tEnv.executeSql(createTable)

    //执行 sql
    val query =
      """
        | SELECT
        |   album_id,
        |   sum(bidding_price) as price,
        |   TUMBLE_START(ts, INTERVAL '5' second) as start_time,
        |   TUMBLE_END(ts, INTERVAL '5' second) as end_time
        | FROM test_tb
        |  WHERE album_id is not null
        |  GROUP BY
        |     album_id,
        |     TUMBLE(ts, INTERVAL '5' second)
            """.stripMargin

    val result = tEnv.sqlQuery(query)
    result.toRetractStream[Row].print()

    val ddlMysql = "CREATE TABLE album_sink (" +
      "    album_id varchar," +
      "    price double," +
      "    start_time TIMESTAMP(3)," +
      "    end_time TIMESTAMP(3)" +
      ") WITH (" +
      "    'connector' = 'jdbc'," +
      "    'url' = 'jdbc:mysql://localhost:3306/dbgirl'," +
      "    'driver' = 'com.mysql.cj.jdbc.Driver'," +
      "    'table-name' = 'album_sink'," +
      "    'username' = 'root'," +
      "    'password' = 'root'" +
      ")"
    tEnv.executeSql(ddlMysql)

    //写 mysql
    tEnv.executeSql("insert into album_sink " + query)

    env.execute("FlinkSqlDDL")
  }
}
