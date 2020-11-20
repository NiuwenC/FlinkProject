package com.jackniu.flink.stateinfo

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{MapStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
case class OrderItem(id:String,name:String,category:String,count:Int,price:Double)
case class Rule(category:String,threshold:Double)
case class User(id:String,name:String)
object FlinkBroadcastKeyStream  extends App{
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //每隔1000ms进行一个检查点
  env.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)
  env.setStateBackend(new FsStateBackend("/sxw/test_exam_nwc/flink_checkpoint/f1"));


  //id name 品类 数量 单价 订单项
  //1 zhangsan 水果 2 4.5

  val inputs = env.socketTextStream("localhost",9999)
    .map(line=>line.split(" "))
    .map(ts=>OrderItem(ts(0),ts(1),ts(2),ts(3).toInt,ts(4).toDouble))
    .keyBy(orderItem=>orderItem.category+":"+orderItem.id)



  //品类 阈值 水果 8.0  --奖励
  //定义需要的广播流 吞吐量低
  val bcsd = new MapStateDescriptor[String,Double]("bcsd",createTypeInformation[String],createTypeInformation[Double])
  val broadcastKeyStream=env.socketTextStream("localhost",8888)
    .map(line=>line.split(" "))
    .map(ts=>Rule(ts(0),ts(1).toDouble))
    .broadcast(bcsd)


  val datastream = inputs.connect(broadcastKeyStream)
    .process(new UserDefineKeyBroadcastProcessFunction(bcsd))
    .print("奖励")

  env.execute("Window")


}
//String(The key type of the input keyed stream.), 第一个输入，第二个输入，输出
class UserDefineKeyBroadcastProcessFunction(msd:MapStateDescriptor[String,Double]) extends  KeyedBroadcastProcessFunction[String,OrderItem,Rule,User]{

  var userTotalcCost:ReducingState[Double] = _

  override def open(parameters: Configuration): Unit = {
    val rsd = new ReducingStateDescriptor[Double]("userTitalCost",new ReduceFunction[Double] {
      override def reduce(t: Double, t1: Double): Double = {
        t + t1
      }
    },createTypeInformation[Double])
    userTotalcCost = getRuntimeContext.getReducingState(rsd);
  }

  //处理高吞吐 orderItem
  override def processElement(in1: OrderItem, ctx: KeyedBroadcastProcessFunction[String, OrderItem, Rule, User]#ReadOnlyContext, collector: Collector[User]): Unit = {
    //计算当前类别下用户的总消费
    userTotalcCost.add(in1.count * in1.price)
    val ruleState = ctx.getBroadcastState(msd)
    var user = User(in1.id,in1.name)

    //设定奖励规则
    if(ruleState != null && ruleState.contains(in1.category)){
      if(userTotalcCost.get() >= ruleState.get(in1.category)) //达到了奖励标准
      {
        collector.collect(user)  // 用户被奖励了
        userTotalcCost.clear()
      }
      else{
        println(s"不满足条件: $user 当前总消费: ${userTotalcCost.get()}, 阈值: ${ruleState.get(in1.category)}")
      }
    }else{
      println("该类别不执行奖励策略")
    }

  }

  //设定某个策略的执行奖励策略
  override def processBroadcastElement(in2: Rule, context: KeyedBroadcastProcessFunction[String, OrderItem, Rule, User]#Context, collector: Collector[User]): Unit = {
    val broadcast= context.getBroadcastState(msd)
    broadcast.put(in2.category,in2.threshold)
  }
}