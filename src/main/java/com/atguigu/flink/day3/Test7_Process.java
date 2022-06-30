package com.atguigu.flink.day3;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Test7_Process {
    public static void main(String[] args) throws Exception {
        //利用process实现map 和 sum的功能
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //2.利用process实现map的功能word => (word,1)
        SingleOutputStreamOperator<Tuple2<String,Integer>> map = streamSource.process(new ProcessFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void processElement(String s, Context context, Collector<Tuple2<String,Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        //3.对word进行分组，泛型中的第一个tuple就是自动识别到的keyBy的key
        KeyedStream<Tuple2<String,Integer>, Tuple> keyBy = map.keyBy(0);
        //4.利用process实现sum
        keyBy.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            //利用map来区分key,因为process无法区别key
            //此处方法每个算子只会执行一次，可以做缓存
            private HashMap<String, Integer> map = new HashMap<>();

            @Override
            public void processElement(Tuple2<String, Integer> tuple2, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //判断单词是否是第一次出现，只有这个方法里的逻辑是针对于每一个元素执行一次
                if (!map.containsKey(tuple2.f0)) {
                    map.put(tuple2.f0, tuple2.f1);
                } else {
                    //单词在map里存在就得重新赋值

                    map.put(tuple2.f0, map.get(tuple2.f0) + tuple2.f1);
                }
                out.collect(Tuple2.of(tuple2.f0, map.get(tuple2.f0)));
            }
        }).print();

        //执行程序
        env.execute();



    }
}
