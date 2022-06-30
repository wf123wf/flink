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
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;
//模式组的循环

public class Flink06_CEP_PatternGroup {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件中获取数据
//        SingleOutputStreamOperator<WaterSensor> map = env.readTextFile("input/sensor2.txt")
//                .map(new MapFunction<String, WaterSensor>() {
//                    @Override
//                    public WaterSensor map(String value) throws Exception {
//                        String[] split = value.split(",");
//                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
//                    }
//                });

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3.对数据进行格式转化
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //4.设置waterMark
        SingleOutputStreamOperator<WaterSensor> watermarks = map.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                })
        );
        //todo CEP编程
        //TODO 1.定义规则
        Pattern<WaterSensor, WaterSensor> pattern =
                Pattern.begin(
                        Pattern
                                .<WaterSensor>begin("start")
                                //迭代条件
/*                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }

                })*/
                                //简单条件，与迭代条件功能类似，但是没有上下文对象，功能不如迭代条件齐全;现在都不直接提供
/*
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() > 30;
                    }
                })*/
                                //组合条件：组合条件，把多个条件使用where结合起来使用
                                /*.where(new IterativeCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                                        return "sensor_1".equals(waterSensor.getId());
                                    }
                                })
                                .where(new IterativeCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                                        return waterSensor.getVc() > 30;
                                    }
                                })
                                .or(new IterativeCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                                        return waterSensor.getTs() > 3;
                                    }
                                });*/
                                //循环模式表示一次处理的事件个数，事件之间是默认的松散连续
//                .times(2);
                                //停止条件，oneOrMore/timesOrMore用在无界流的时候需要有停止条件，否则一直在等待后面的数据
                                //此处表示碰到vc > 40的时候就是这个处理事件个数增长停止的时候
                                .where(new IterativeCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                                        return "sensor_1".equals(waterSensor.getId());
                                    }
                                })
//                .times(2)
                                //模式可选性，不管是什么模式，添加后表示模式可以存在也可以不存在
                                //.optional()
                                .next("next")
                                .where(new IterativeCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                                        return waterSensor.getVc() > 10;
                                    }
                                })
                )
                        .times(2)
                .consecutive()
                ;

        //TODO 2.将规则应用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(watermarks, pattern);
        //todo 3.获取符合规则的数据
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();

        env.execute();

    }
}
