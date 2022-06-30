package com.atguigu.flink.day3;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test3_Reduce {
    public static void main(String[] args) throws Exception {
        //对一个对象的id进行分组，并且对某个属性进行聚合
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //进行map,将String转化成java bean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //对jave bean对象进行分组,泛型表示分组对象是WaterSensor，分组的key是tuple(String)，
        // 因为这里是用filed字段或者index表示都会自动识别成tuple
        KeyedStream<WaterSensor, Tuple> keyBy = map.keyBy("id");
        //用id来进行聚合，聚合的属性是vc,聚合后返回新的WaterSensor类型
        keyBy.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                //waterSensor:表示当上次聚合后的数据；t1:当前数据
                return new WaterSensor(t1.getId(),waterSensor.getTs(),t1.getVc()+waterSensor.getVc());
            }
        }).print();


        //执行程序
        env.execute();


    }
}
