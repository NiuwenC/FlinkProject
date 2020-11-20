package com.jackniu.flink.timestamp;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class WatermarkOutputImpl implements WatermarkOutput {
    private int count;
    public WatermarkOutputImpl(int count){
        this.count = count;
    }
    @Override
    public void emitWatermark(Watermark watermark) {
        count ++;
        if (count %2 ==0){
            this.markIdle();
        }else{
            System.out.println("输出水印:"+ watermark);
        }

    }

    @Override
    public void markIdle() {
        System.out.println("设置为Idle，下游的操作不需要等待这次输出的水印");
    }
}
