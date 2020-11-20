package com.jackniu.flink.stateinfo

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkBroadcastNonKeyStream  extends  App{
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //吞吐量高
  val inputs = env.socketTextStream("localhost",9999)
  //定义需要的广播流，吞吐量低 (name,key,value)
  val bcsd = new MapStateDescriptor[String,String]("bcsd",createTypeInformation[String], createTypeInformation[String])

  val broadcaststream = env.socketTextStream("localhost", 8888)
    .broadcast(bcsd)

  val tag = new OutputTag[String]("notmatch")

  val dataStream = inputs.connect(broadcaststream)
    .process(new UserDefinedBroadcastProcessFunction(tag,bcsd))

  dataStream.print("满足条件")
  dataStream.getSideOutput(tag).print("不满足")

  env.execute("Window")




}

class UserDefinedBroadcastProcessFunction(tag:OutputTag[String],msd:MapStateDescriptor[String,String]) extends BroadcastProcessFunction[String,String,String]{
  //处理广播流，通常需要在这里修改广播的状态，低吞吐
  override def processBroadcastElement(in2: String, context: BroadcastProcessFunction[String, String, String]#Context, collector: Collector[String]): Unit = {
    val mapState =context.getBroadcastState(msd)
    mapState.put("rule",in2)
  }

  //处理正常流  高吞吐，通常在该方法读取广播状态
  override def processElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, collector: Collector[String]): Unit = {
    //获取状态，只读
    val readOnleMapstate = ctx.getBroadcastState(msd)
    if(readOnleMapstate.contains("rule")){
      val rule = readOnleMapstate.get("rule")
      if(value.contains(rule)){
        collector.collect(rule+"\t"+value)
      }else{
        ctx.output(tag,value)
      }
    }else{//侧输出
      ctx.output(tag,value)
    }
  }


}
