package com.atguigu.flink.day1;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test3_Stream {
    //有界流处理，采用用flatmap先打散，然后用map转化格式
    public static void main(String[] args) throws Exception {
        //1.搭建流处理环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //可以访问web ui
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //设置环境并行度为1
        env.setParallelism(1);
        //2.读取数据
        //DataStreamSource<String> Dlines = env.readTextFile("input/words.txt");
        //无界流测试
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3.数据处理
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }

            }
        });
        //4.进行map格式转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        //5.进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);
        //6.进行组内聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);
        //7.打印数据
        sum.print();
        //8.执行操作
        env.execute();



    }



}
