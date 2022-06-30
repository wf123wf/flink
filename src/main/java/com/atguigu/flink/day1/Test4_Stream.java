package com.atguigu.flink.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test4_Stream {
    //无界流处理
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);
        //2.接收数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.数据打散转化格式（word,1）
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //4.按照word分组
        KeyedStream<Tuple2<String, Long>, Tuple> group = flatMap.keyBy(0);
        //5.组内聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = group.sum(1);
        //6.打印数据
        sum.print();
        //7.开始执行
        env.execute();


    }
}
