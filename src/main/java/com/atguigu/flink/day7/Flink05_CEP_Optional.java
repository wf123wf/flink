package com.atguigu.flink.day7;


import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;
//模式组的可选择性
public class Flink05_CEP_Optional {
    public static void main(String[] args) throws Exception {

            //1.获取流的执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(1);

            //2.从文件中获取数据
            SingleOutputStreamOperator<WaterSensor> inputDS = env.readTextFile("input/sensor.txt")
                    .map(new MapFunction<String, WaterSensor>() {
                        @Override
                        public WaterSensor map(String value) throws Exception {
                            String[] split = value.split(",");
                            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                        }
                    })
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                        @Override
                                        public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                            return element.getTs() * 1000;
                                        }
                                    })
                    );

            //TODO 1.定义规则
            Pattern<WaterSensor, WaterSensor> pattern = Pattern
                    .<WaterSensor>begin("start")
                    //迭代条件
                    .where(new IterativeCondition<WaterSensor>() {
                        @Override
                        public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                            return "sensor_1".equals(waterSensor.getId());
                        }
                    })
                    //默认是松散连续
                    .times(2)
                    //可选,表示上一个模式可以是循环两次，也可以是忽略
                    .optional()
                    .next("next")
                    .where(new IterativeCondition<WaterSensor>() {
                        @Override
                        public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                            return "sensor_2".equals(value.getId());
                        }
                    })
                    ;

            //TODO 2.将规则作用于流上
            PatternStream<WaterSensor> patternStream = CEP.pattern(inputDS, pattern);


            //TODO 3.获取符合规则的数据
            patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
                @Override
                public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                    return pattern.toString();
                }
            }).print();

            env.execute();
    }
}
