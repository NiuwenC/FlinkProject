package com.jackniu.flink.timestamp

import org.apache.flink.api.common.eventtime.Watermark


object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val timestamp= System.currentTimeMillis()
    val waterMark = new Watermark(timestamp)
    println(waterMark)
  }
}
