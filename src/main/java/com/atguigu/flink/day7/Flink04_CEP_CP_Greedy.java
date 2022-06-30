package com.atguigu.flink.day7;

import com.atguigu.flink.WaterSensor;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

//贪婪模式：在组合模式情况下, 对次数的处理尽快能获取最多个的那个次数, 就是贪婪!当一个事件同时满足两个模式的时候起作用.
public class Flink04_CEP_CP_Greedy {
    public static void main(String[] args) throws Exception {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor2.txt");
        //3.数据类型转化及添加waterMark
        SingleOutputStreamOperator<WaterSensor> inputDS = streamSource.map(new MapFunction<String, WaterSensor>() {

            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                })
        );
        //TODO 1.定义规则

        Pattern<WaterSensor, WaterSensor> pattern =
                Pattern.begin(
                Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                //默认是松散连续的
                .times(2, 3)
                //循环模式的贪婪性
                //.greedy()
                .next("next")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return waterSensor.getVc() == 30;
                    }
                })
        );
        //TODO 2.将规则作用在流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(inputDS, pattern);
        //TODO 3.对流进行筛选
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();

        env.execute();


    }
}
