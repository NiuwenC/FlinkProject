package com.jackniu.flink.timestamp

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer

object FlinkTimeStampDemo {
  val sdf: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss:SSS")
  def main(args:Array[String]): Unit ={
    val hostname="localhost"
    val port=9999
    val delimiter='\n'
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val streams:DataStream[String] = env.socketTextStream(hostname,port,delimiter)

    val data = streams.map(data=>{
      try{
        val items = data.split(":")
        (items(0),items(1).toLong)
      }catch {
        case _:Exception =>println("输入数据不符合格式: "+ data)
          ("0",0L)
      }
    }).filter(data => !data._1.equals("0") && data._2 !=0L)

    val waterSteam: DataStream[(String,Long)] =data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      //事件时间
      var currentMaxTImestamp = 0L
      val maxOutoutOrderness = 10000L
      var lastEmittedWatermark: Long = Long.MinValue

      //返回当前的水印
      override def getCurrentWatermark: Watermark = {
        //允许延迟3秒
        val allowDelay = currentMaxTImestamp - maxOutoutOrderness
        // 保证水印依次递增
        if(allowDelay >= lastEmittedWatermark){
          lastEmittedWatermark = allowDelay
        }
        new Watermark(lastEmittedWatermark)
      }

      //分配一个时间戳给一个元素
      override def extractTimestamp(element: (String, Long), previousElementTimeStamp: Long): Long = {
        val time = element._2
        if(time > currentMaxTImestamp){
          currentMaxTImestamp = time
        }
        val outData = String.format("key: %s EventTime:%s waterMark:%s",
          element._1,sdf.format(time),sdf.format(getCurrentWatermark.getTimestamp))
        println(outData)
        time
      }
    })


//    val waterSteam: DataStream[(String,Long)] =data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
//      //事件时间
//      var currentMaxTImestamp = 0L
//      val maxOutoutOrderness = 0L
//      var lastEmittedWatermark: Long = Long.MinValue
//
//      //返回当前的水印
//      override def getCurrentWatermark: Watermark = {
//        //允许延迟3秒
//        val allowDelay = currentMaxTImestamp - maxOutoutOrderness
//        // 保证水印依次递增
//        if(allowDelay >= lastEmittedWatermark){
//          lastEmittedWatermark = allowDelay
//        }
//        new Watermark(lastEmittedWatermark)
//      }
//
//      //分配一个时间戳给一个元素
//      override def extractTimestamp(element: (String, Long), previousElementTimeStamp: Long): Long = {
//        val time = element._2
//        if(time > currentMaxTImestamp){
//          currentMaxTImestamp = time
//        }
//        val outData = String.format("key: %s EventTime:%s waterMark:%s",
//          element._1,sdf.format(time),sdf.format(getCurrentWatermark.getTimestamp))
//        println(outData)
//        time
//      }
//    })



    val result:DataStream[String] = waterSteam.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5L))) //5s宽度的基于事件时间的翻转窗口
      .apply(new WindowFunction[(String,Long),String,Tuple,TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          val timeArr = ArrayBuffer[String]()
          val iterator = input.iterator
          while(iterator.hasNext){
            val tup2 = iterator.next()
            timeArr.append(sdf.format(tup2._2))
          }

          val outData = String.format("key:%s  data:%s  startTime:%s   endTime:%s",
            key.toString,timeArr.mkString("-"),sdf.format(window.getStart),sdf.format(window.getEnd))

          out.collect(outData)
        }

      })

    result.print("最终计算结果")
    env.execute("xxxxxx")


  }


}
