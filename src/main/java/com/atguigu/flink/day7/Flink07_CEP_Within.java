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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink07_CEP_Within {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102",9999);
        //3.转化数据类型并给它添加waterMark
        SingleOutputStreamOperator<WaterSensor> streamOperator = streamSource.map(new MapFunction<String, WaterSensor>() {


            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
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
                })
                .next("next")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                //表示两个紧密相连的事件的时间间隔不能超过三秒
                .within(Time.seconds(3));
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
