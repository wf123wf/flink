package com.atguigu.flink.day7;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

import static jdk.nashorn.internal.objects.Global.print;

public class Flink01_CEP_BasicUse {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");
        //3.转化数据类型并给它添加waterMark
        SingleOutputStreamOperator<WaterSensor> streamOperator = streamSource.map(new MapFunction<String, WaterSensor>() {


            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                System.out.println(element.getTs() * 1000);
                return element.getTs() * 1000;
            }
        })
        );
        streamOperator.process(new ProcessFunction<WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                long l = ctx.timerService().currentWatermark();
                Long timestamp = ctx.timestamp();
                System.out.println(timestamp + "timestamp");

                System.out.println(l + "waterMark");
            }
        });
        //TODO CEP编程
        //1.定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                //模式名
                .<WaterSensor>begin("start")
                //过滤
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                });
        //2.将规则作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(streamOperator, pattern);

        //3.获取符合规则的数据
        patternStream.select(new PatternSelectFunction<WaterSensor,String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();


        env.execute();


    }
}
