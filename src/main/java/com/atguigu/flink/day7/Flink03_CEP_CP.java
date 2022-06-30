package com.atguigu.flink.day7;

import com.atguigu.flink.WaterSensor;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;
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

//把多个单个模式组合在一起就是组合模式.  组合模式由一个初始化模式(.begin(...))开头
public class Flink03_CEP_CP {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");
        //3.改变数据格式并设置waterMark
        SingleOutputStreamOperator<WaterSensor> inputDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
                );

        //4.CEP编程
        // TODO 1.定义规则
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                //默认是松散模式,而且是同一个判断条件重复两次
          .times(2)

        //单循环的严格连续
        .consecutive();
                //严格连续
                //.next("next")
                //松散模式
                //.followedBy("follow")
                //非确定松散连续
//               .next("followAny")
//                .where(new IterativeCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
//                        //return "sensor_2".equals(waterSensor.getId());
//                        return waterSensor.getTs() == 4;
//                    }
//                });
//                .where(new IterativeCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
//                        return  1== (waterSensor.getTs());
//                    }
//                });

        //TODO 2.将规则作用在流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(inputDS, pattern);
        //TODO 3.获取符合规则的数据
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {

            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();

        env.execute();

    }
}
