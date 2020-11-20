package com.jackniu.flink.stateinfo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode

object StateCheckpointDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //每隔1000ms进行一个检查点
    env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new FsStateBackend("file:///D:/logs"));


    //id name 品类 数量 单价 订单项
    //1 zhangsan 水果 2 4.5

    val inputs = env.socketTextStream("localhost",9999)
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
    inputs.print()

    env.execute("wc")




  }
}
