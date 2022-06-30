package com.atguigu.flink.day3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test3_filter {
    public static void main(String[] args) throws Exception {
        //过滤掉奇数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //利用filter过滤掉奇数
        SingleOutputStreamOperator<String> filter = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {

                return Integer.parseInt(s) % 2 == 0;
            }
        });
        filter.print();
        env.execute();


    }
}
