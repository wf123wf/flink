package com.atguigu.flink.day6;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
//创建一个ReducingState	计算每个传感器的水位和
public class Flink03_KeyedState_ReducingState {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3.将数据转化成Java Bean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //4.将数据keyby，表示相同的key去同一个分区,为了方便同一个传感器的前后水位对比
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");
        //5.进行状态的保存提取和操作
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, Tuple2<String,Integer>>() {
            //TODO 设置一个状态
            private ReducingState<Integer> reduceState;
            //TODO open是周期性函数，一个并行度只执行一次，对状态进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                reduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reduce-state", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                },Integer.class));
            }

            //TODO 主要执行逻辑，用reduce来统计水位和，先存入，再输出
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<Tuple2<String,Integer>> collector) throws Exception {
               //1.先将数据状态存入reduce状态中做累加计算
                reduceState.add(waterSensor.getVc());
                //2.取出数据打印
                collector.collect(Tuple2.of(waterSensor.getId(),reduceState.get()));
            }
        }).print();

        //执行
        env.execute();
    }
}
