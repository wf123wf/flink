package com.atguigu.flink.day3;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test5_union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 9999);;
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);;
        //使用union连接两条流
        source1.union(source2).print();
        env.execute();
    }
}
