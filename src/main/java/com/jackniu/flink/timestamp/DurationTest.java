package com.jackniu.flink.timestamp;

import java.time.Duration;

public class DurationTest {
    public static void main(String[] args) {
        for(int i=0;i<10;i++){
            System.out.println(Duration.ofMillis(0).toMillis());
        }
    }
}
