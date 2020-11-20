package com.jackniu.flink.timestamp;


import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;

import java.time.Duration;

public class BoundedOutOfOrdernessWatermarksTest {
    public static void main(String[] args) {
        BoundedOutOfOrdernessWatermarks watermarkGenerator =
                new BoundedOutOfOrdernessWatermarks(Duration.ofMillis(1L));

        watermarkGenerator.onPeriodicEmit(new WatermarkOutputImpl(1));

    }
}
