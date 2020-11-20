package com.jackniu.flink.timestamp;

import org.apache.flink.api.common.eventtime.TimestampAssigner;

public class TimestampAssignerImpl implements TimestampAssigner<String> {
    public static void main(String[] args) {
        TimestampAssignerImpl impl = new TimestampAssignerImpl();
        long nowtimeStamp = System.currentTimeMillis();
        impl.extractTimestamp("event string",nowtimeStamp);
    }

    /**
     * 为元素指定时间戳，以纪元后的毫秒为单位。这与任何特定时区或日历无关。
     * @param element 将为其分配时间戳的元素。元素的当前内部时间戳
     * @param recordTimestamp
     * @return
     */
    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        long elementTimeStamp = System.currentTimeMillis();
        System.out.println("当前元素分配的时间戳 "+ elementTimeStamp);
        return elementTimeStamp;
    }
}
