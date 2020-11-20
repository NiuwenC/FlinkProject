package com.jackniu.flink.stateinfo

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration

object ListStateDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(List(
      (1L, 3L),
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L),
      (2L, 10L),
      (2L, 20L),
      (2L, 10L)
    )).keyBy(_._1)
      .flatMap(new ListStateFunction())
      .print()

    env.execute("ExampleKeyedState")
  }

}

// 通过状态来判断是否有重复数据，如果有则取消输出
class ListStateFunction extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  private var listState: ListState[Long] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(
      new ListStateDescriptor[Long]("listState", createTypeInformation[Long])
    )
  }

  //随便写的逻辑：
  override def flatMap(in: (Long, Long), collector: Collector[(Long, Long)]): Unit = {
    listState.add(in._2)
    println("内部数据状态",listState.get())
    collector.collect((in._1,in._2))
  }

}


