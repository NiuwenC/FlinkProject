package com.jackniu.flink.stateinfo

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration

object StateTTLConfigDemo {



  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sources:DataStream[String] = env.socketTextStream("localhost",9999)

    val wordAndOne = sources.map(word=>(word,1))
    val keyedStream = wordAndOne.keyBy(_._1)
    //使用KeyedState 通过中间状态求和
    val summed:DataStream[(String,Int)] = keyedStream.mapWithState((in:(String,Int),count:Option[Int]) =>
      count match {
        case Some(c) =>((in._1,c),Some(c + in._2))
        case None =>((in._1,0),Some(in._2))
      })
    //使用KeyedState 中间状态求和结束

    summed.print()

    env.execute("xxx")
  }

}
class WordCountMapFunction extends RichMapFunction[(String, Int), (String, Int)]{
  //状态数据不参与序列化，添加transient修饰
  private var sum: ValueState[Int] = _
  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ValueStateDescriptor[Int]("sum-key-state",createTypeInformation[Int])
    stateDescriptor.enableTimeToLive(StateTtlConfig
      .newBuilder(Time.seconds(5))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build)
    sum = getRuntimeContext.getState(stateDescriptor)

  }


  override def map(in: (String, Int)): (String, Int) = {
    //输入的额单词
    val word = in._1
    val count = in._2
    //根据State获取中间数据
    var historyVal = sum.value()
    //根据State进行累加
    if(historyVal!= null){
      historyVal += count
      sum.update(historyVal)
    }else{
      sum.update(count)
    }
    (word,sum.value())

  }
}
