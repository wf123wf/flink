package com.atguigu.flink.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test5 {
    public static void main(String[] args) throws Exception {
        //1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);

// 把多个流union在一起成为一个流, 这些流中存储的数据类型必须一样: 水乳交融
        stream1.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer integer) throws Exception {
                return integer + "map";
            }
        }).print();
        stream1.print();
        System.out.println("++++++++++++++++++++");
        stream1
                .union(stream2)
                .union(stream3)
                .print();
        //7.开始执行
        env.execute();

    }
}
