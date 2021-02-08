package com.qzw.test.sink

import com.qzw.test.source.SourceTest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object JdbcSInkTest {
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

    //Sink 操作
    dataStream.addSink(new MyJdbcSink)
    env.execute("JdbcSInkTest")
  }
}

class MyJdbcSink extends RichSinkFunction[SensorReading] {
  //定义 sql 连接
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor,temp) values (?,?)")
    updateStmt = conn.prepareStatement("update temperatures set temp=? where sensor = ?")
  }

  //执行 sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //如果 update 没有查到数据 insert
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(1, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
