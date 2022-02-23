package com.qzw.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object StreamWordCount {

  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    val socketDs = env.socketTextStream(host, port)
    val wd = socketDs
      .filter(_.nonEmpty)
      .flatMap(_.split(" "))
      .map(WordWithCount(_, 1))
      .keyBy(_.word)
      .sum(1)
    wd.print()
    env.execute("StreamWordCount")
  }
}
