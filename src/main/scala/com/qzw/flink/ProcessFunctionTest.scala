package com.qzw.flink

import com.qzw.flink.source.SourceTest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author : qizhiwei
 * @date : 2021/2/9
 * @Description : ${Description}
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * 默认情况下，状态是保持在 TaskManagers 的内存中，checkpoint 保存在 JobManager 的内存中。为了合适地持久化大体量状态， Flink 支持各种各样的途径去存储 checkpoint 状态到其他的 state backends 上。通过 StreamExecutionEnvironment.setStateBackend(…) 来配置所选的 state backends。
     * 是否增量的存储 checkpoint
     */
    env.setStateBackend(new RocksDBStateBackend("file:///Users/qizhiwei/Downloads/test/checkpoint/ProcessFunctionTest", true).asInstanceOf[StateBackend])

    /**
     * 默认情况下 checkpoint 是禁用的。通过调用 StreamExecutionEnvironment 的 enableCheckpointing(n) 来启用 checkpoint，里面的 n 是进行 checkpoint 的间隔，单位毫秒。
     * 设置模式为精确一次 (这是默认值)
     * Flink 现在为没有迭代（iterations）的作业提供一致性的处理保证。在迭代作业上开启 checkpoint 会导致异常。为了在迭代程序中强制进行 checkpoint，用户需要在开启 checkpoint 时设置一个特殊的标志： env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE, force = true)。
     */
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)

    /**
     * checkpoints 之间的最小时间：该属性定义在 checkpoint 之间需要多久的时间，以确保流应用在 checkpoint 之间有足够的进展。如果值设置为了 5000， 无论 checkpoint 持续时间与间隔是多久，在前一个 checkpoint 完成时的至少五秒后会才开始下一个 checkpoint。
     * 往往使用“checkpoints 之间的最小时间”来配置应用会比 checkpoint 间隔容易很多，因为“checkpoints 之间的最小时间”在 checkpoint 的执行时间超过平均值时不会受到影响（例如如果目标的存储系统忽然变得很慢）。
     * 注意这个值也意味着并发 checkpoint 的数目是一。
     * 确认 checkpoints 之间的时间会进行 500 ms
     */
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    /**
     * checkpoint 超时：如果 checkpoint 执行的时间超过了该配置的阈值，还在进行中的 checkpoint 操作就会被抛弃。
     * Checkpoint 必须在一分钟内完成，否则就会被抛弃
     */
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    /**
     * 设置可容忍的检查点失败次数，默认值为0，表示我们不容忍任何检查点失败。
     * 如果 task 的 checkpoint 发生错误，会阻止 task 失败，checkpoint 仅仅会被抛弃
     */
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(Int.MaxValue)

    /**
     * externalized checkpoints: 你可以配置周期存储 checkpoint 到外部系统中。Externalized checkpoints 将他们的元数据写到持久化存储上并且在 job 失败的时候不会被自动删除。 这种方式下，如果你的 job 失败，你将会有一个现有的 checkpoint 去恢复。
     * 更多的细节请看: https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/ops/state/checkpoints.html#externalized-checkpoints
     * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：当作业取消时，保留作业的 checkpoint。注意，这种情况下，需要手动清除该作业保留的 checkpoint。
     * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：当作业取消时，删除作业的 checkpoint。仅当作业失败时，作业的 checkpoint 才会被保留。
     */
    env.getCheckpointConfig.enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION)

    /**
     * 并发 checkpoint 的数目: 默认情况下，在上一个 checkpoint 未完成（失败或者成功）的情况下，系统不会触发另一个 checkpoint。这确保了拓扑不会在 checkpoint 上花费太多时间，从而影响正常的处理流程。 不过允许多个 checkpoint 并行进行是可行的，对于有确定的处理延迟（例如某方法所调用比较耗时的外部服务），但是仍然想进行频繁的 checkpoint 去最小化故障后重跑的 pipelines 来说，是有意义的。
     * 该选项不能和 “checkpoints 间的最小时间（setMinPauseBetweenCheckpoints）”同时使用。
     * 同一时间只允许一个 checkpoint 进行
     */
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setParallelism(1)

    val socketDs = env.socketTextStream("localhost", 9999)
    val ds: DataStream[SensorReading] = socketDs
      .map(line => {
        val sensor = line.split(",")
        SensorReading(sensor(0).trim, sensor(1).trim.toLong, sensor(2).trim.toDouble)
      })

    //连续 10s 温度上升，报警
    val warnings = ds
      .keyBy(_.id)
      .process(new TempIncWarning(10 * 1000L))
    warnings.print("warnings")

    //按照阈值切分流
    val high = ds
      .process(new SplitProcess(30.0))

    high.print("high")
    high.getSideOutput(new OutputTag[SensorReading]("low")).print("low")

    env.execute("ProcessFunctionTest")
  }
}

class SplitProcess(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    //按照阈值做分流
    if (value.temperature > threshold) {
      out.collect(value)
      ctx.output(new OutputTag[SensorReading]("high"), value)
    } else {
      //输出到侧输出流
      ctx.output(new OutputTag[SensorReading]("low"), value)
    }
  }
}

/**
 * 当温度在{interval}内连续上升的时候，报警
 * KeyedProcessFunction<K, I, O>
 * K: keyBy 的key 类型
 * I: input 输入的数据类型
 * O: output 输出的数据类型，也可以选择不输出，当一个 filter 的功能
 *
 * @param interval 毫秒时间戳
 */
class TempIncWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {

  //保存上一次的温度,每次都比较是否连续上升
  lazy val temperatureState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //保存上一次定时器的时间戳,用于删除
  lazy val lastTimeStamp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTimeStamp", classOf[Long]))


  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先拿到状态和 timerService
    val lastTemperature = temperatureState.value()
    val timerTs = lastTimeStamp.value()
    val timerService = context.timerService()

    //当前温度大于上一次温度，并且当前没有设置定时器，因为lastTimeStamp是 Long 型 所以默认为 0
    if (value.temperature > lastTemperature && timerTs == 0) {
      //注册当前时间开始 10s 之后的定时器
      val ts = timerService.currentProcessingTime() + interval
      timerService.registerProcessingTimeTimer(ts)
      //更新定时器和上一次温度
      lastTimeStamp.update(ts)
    } else if (value.temperature < lastTemperature) { //当前温度小于上一次温度的时候，删除定时器，清空定时器状态
      //出现温度下降，删除定时器(注意定时器类型和注册时候保持一致)
      timerService.deleteProcessingTimeTimer(timerTs)
      lastTimeStamp.clear()
    }
    temperatureState.update(value.temperature)
  }

  /**
   * 触发定时器
   *
   * @param timestamp 当前时间戳触发的时间，不同的定时器可以根据这个触发的时间戳判断
   * @param ctx       KeyedProcessFunction
   * @param out       Collector
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度" + interval / 1000 + "秒内连续上升！！！当前温度为：" + temperatureState.value())
    lastTimeStamp.clear()
  }
}
