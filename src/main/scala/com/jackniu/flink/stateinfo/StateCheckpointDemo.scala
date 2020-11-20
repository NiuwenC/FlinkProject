package com.jackniu.flink.stateinfo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig

case class WC(word:String,count:Int)
object StateCheckpointDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //每隔1000ms进行一个检查点
    env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new FsStateBackend("hdfs:///sxw/test_exam_nwc/flink_checkpoint/f1"));
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    //id name 品类 数量 单价 订单项
    //1 zhangsan 水果 2 4.5
    val properties = new Properties();
    properties.setProperty("bootstrap.servers","bdp.slave001:6667,bdp.slave002:6667,bdp.slave003:6667");
    properties.setProperty("group.id", "first-group");
    properties.setProperty( "deserializer.encoding","UTF8");

    val stream:DataStream[String] = env
      .addSource(new FlinkKafkaConsumer010("test", new SimpleStringSchema(), properties));

    val inputs =stream
      .flatMap(_.split("\\W+"))
      .map(word=>WC(word,1))
      .keyBy(_.word)
      .sum(1)
    inputs.print()

    env.execute("wc")




  }
}
