package com.qzw.test

import org.apache.flink.api.scala._

/**
 * @author : qizhiwei 
 * @date : 2021/2/5
 * @Description : ${Description}
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputPath = "src/main/resources/people.csv"
    val ds = env.readTextFile(inputPath)
    val wd = ds
      .flatMap(_.split(";"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wd.print()
  }
}
