package com.qzw.test.state

import com.qzw.test.source.SourceTest.SensorReading
import com.qzw.test.window.MyReduce
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

import java.util

/**
 * @author : qizhiwei
 * @date : 2021/2/8
 * @Description : ${Description}
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val socketDs = env.socketTextStream("localhost", 9999)
    val ds: DataStream[SensorReading] = socketDs
      .map(line => {
        val sensor = line.split(", ")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble)
      })

    env.execute("StateTest")
  }
}

class MyRichMapper extends RichMapFunction[SensorReading, String] {

  lazy val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduceState", new MyReduce, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {

  }

  override def map(value: SensorReading): String = {
    val myV = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(1)
    listState.addAll(list)
    listState.update(list)
    val rslist = listState.get()
    mapState.contains("xxxx")
    mapState.get("xxxx")
    mapState.put("xxxx", 0.0)
    //获取聚合完的值
    reduceState.get()
    //做聚合
    reduceState.add(value)
    value.id
  }
}