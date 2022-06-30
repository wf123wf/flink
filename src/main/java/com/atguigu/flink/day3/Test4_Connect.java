package com.atguigu.flink.day3;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Test4_Connect {
    public static void main(String[] args) throws Exception {
        //connect连接两个流，但是各自仍有自己的形态和计算逻辑
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取数据
        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);
        //3.数据处理：连接两条流
        ConnectedStreams<String, String> connect = source1.connect(source2);
        //4.数据无法直接作为一个整体打印，必须借助其他算子重写两个方法来进行每一支流的
        connect.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s + "9999";
            }

            @Override
            public String map2(String s) throws Exception {
                return s + "8888";
            }
        }).print();

        env.execute();


    }
}
