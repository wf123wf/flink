package com.atguigu.flink.day3;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置运行环境并行度
        env.setParallelism(1);
        //3.数据获取
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> map = streamSource.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return Tuple2.of(Integer.parseInt(value) % 2, Integer.parseInt(value));
            }
        });
        //4.keyBy处理
        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = map.keyBy(0);

        //5.sum求和处理
        keyedStream.sum(1).print();

        //6.数据打印




        //6.执行
        env.execute();


    }
}
