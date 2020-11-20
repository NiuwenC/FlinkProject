package com.jackniu.flink.timestamp;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class WatermarkGeneratorImpl implements WatermarkGenerator<String> {
    public static void main(String[] args) throws InterruptedException {
        long now = System.currentTimeMillis();
        WatermarkOutputImpl output = new WatermarkOutputImpl(0);
        WatermarkGeneratorImpl impl = new WatermarkGeneratorImpl();
        for (int i=0; i<=10; i++) {
//            impl.onEvent(i+"",now,new WatermarkOutputImpl());
            impl.onPeriodicEmit(output);
            Thread.sleep(100);
        }
    }

    // 如果每个event都发送一个水印
    @Override
    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
        long timestamp = System.currentTimeMillis();
        output.emitWatermark(new Watermark(timestamp));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        long timestamp = System.currentTimeMillis();
        output.emitWatermark(new Watermark(timestamp));
    }
}
