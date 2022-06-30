package com.atguigu.flink.day6;
//了解算子状态：广播状态，是一个map格式，可以将一个流的状态广播到另一条流，通过读取这条流的状态来决定另一条流的逻辑，

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_OperaterState_Broadcast {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       //2.获取两条流
        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> hadoop103 = env.socketTextStream("hadoop103", 9999);
        //3.定义一个广播状态
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
        //4.将这个状态广播给算子
        BroadcastStream<String> broadcast = hadoop102.broadcast(mapStateDescriptor);
        //5.连接广播流和普通流
        BroadcastConnectedStream<String, String> connect = hadoop103.connect(broadcast);
        //6.对两条流的数据进行对比处理
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            //因为是两条流所以有两个方法
            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                //读取广播状态的值，并根据值的不同选择不同的操作逻辑
                String aSwitch = broadcastState.get("switch");
                if("1".equals( aSwitch)){
                    collector.collect("执行逻辑1");
                } else if ("2".equals(aSwitch)){
                    collector.collect("执行逻辑2");
                } else {
                    collector.collect("执行逻辑3");
                }
            }

            @Override
            public void processBroadcastElement(String s, Context context, Collector<String> collector) throws Exception {
                //获取广播状态
                BroadcastState<String, String> broadcastState = context.getBroadcastState(mapStateDescriptor);
                //将数据保存
                broadcastState.put("switch",s);

            }
        }).print();

        //执行
        env.execute();
    }
}
