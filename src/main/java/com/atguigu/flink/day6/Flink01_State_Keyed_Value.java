package com.atguigu.flink.day6;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//键控状态的使用，一个key存一个状态，类似于redis的string
//案例一：valueState, 如果前后两次的水位差超过10m就报警
public class Flink01_State_Keyed_Value {
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
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 设置一个状态
            private ValueState<Integer> valueState;
            //TODO open是周期性函数，一个并行度只执行一次，对状态进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState",Integer.class));
            }

            //TODO 主要执行逻辑，用来比较此次的水位和前一次水位（说明一个数据只保存一个状态）
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
              //判断当前水位与状态水位相差是否大于10，
                // 注意要考虑第一次状态水位的值可能是空，用一个新变量来装这个状态
                int lastVC = valueState.value() == null  ? waterSensor.getVc() : valueState.value();
                //前水位与状态水位相差是否大于10，是就报警
                if(Math.abs(lastVC - waterSensor.getVc()) > 10){
                    collector.collect("水位超过10报警！！！");

                }
                //每一次都要更新状态
                valueState.update(waterSensor.getVc());


            }
        }).print();

        //执行
        env.execute();
    }
}
