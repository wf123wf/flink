package com.atguigu.flink.day3;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1_keyBy {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置运行环境并行度
        env.setParallelism(4);
        //3.数据获取
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //4.数据map处理
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).setParallelism(2);
        //5.keyBy处理
       //map.keyBy("id");
        //keyBy处理,用keySelector
        KeyedStream<WaterSensor, String> keyBy = map.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });
        //5.数据打印
        map.print("原始分区").setParallelism(2);
        keyBy.print("keyBy");

        //6.执行
        env.execute();


    }
}
