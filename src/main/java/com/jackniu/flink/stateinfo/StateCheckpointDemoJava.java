package com.jackniu.flink.stateinfo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class StateCheckpointDemoJava {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.setStateBackend(new FsStateBackend("hdfs:///sxw/test_exam_nwc/flink_checkpoint/f1"));
        executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);



        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","bdp.slave001:6667,bdp.slave002:6667,bdp.slave003:6667");
        properties.setProperty("group.id", "first-group");
        properties.setProperty( "deserializer.encoding","UTF8");

        DataStream<String> stream = executionEnvironment
                .addSource(new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties));

        DataStream<Tuple2<String, Integer>> counts =stream.flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);

        counts.print();
        executionEnvironment.execute("run");

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
