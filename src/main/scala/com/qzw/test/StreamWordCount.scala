package com.qzw.test

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
    val socketDs = env.socketTextStream("localhost", 9999)
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
