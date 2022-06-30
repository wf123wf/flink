package com.atguigu.flink.day6;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//去重: 去掉重复的水位值.
// 思路: 把水位值作为MapState的key来实现去重, value随意
public class Flink05_KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3.将数据转化成JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //4.对相同的key的数据做聚合,由于是去重，想到MapState,键值对集合，每一个元素都是由键值对组成,键不能重复，值可以重复
        KeyedStream<WaterSensor, Integer> waterSensorIntegerKeyedStream = map.keyBy(new KeySelector<WaterSensor, Integer>() {
            @Override
            public Integer getKey(WaterSensor value) throws Exception {
                return value.getVc();
            }
        });
        //5.去掉重复水位
        waterSensorIntegerKeyedStream.process(new KeyedProcessFunction<Integer, WaterSensor, String>() {
            //1.创建Map State状态,规定了这个状态的key；和 value的泛型
            private MapState<Integer,WaterSensor> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState= getRuntimeContext().getMapState(new MapStateDescriptor<Integer,WaterSensor>("map-state",Integer.class,WaterSensor.class));
            }
            //2.利用状态进行去掉重复水位处理
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                //todo 将数据存入状态，如果key已经存在会覆盖,如果不存在会存进去
                mapState.put(waterSensor.getVc(),waterSensor);
                //todo 输出结果，即去重的waterSensor
                collector.collect(mapState.get(waterSensor.getVc()).toString());


            }
        }).print();
        env.execute();
    }
}
