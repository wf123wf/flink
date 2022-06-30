package com.atguigu.flink.day5;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

//迟到+将迟到的数据放入侧输出流
public class Flink07_EventTime_WaterMark_Bounded_Lateness_SideOutput {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
//        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4.设置WaterMark(允许固定延迟)乱序程度或者说叫固定延迟为3秒
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        //指定哪个字段作为事件时间字段
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );

        //将相同key的数据聚和到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorSingleOutputStreamOperator.keyBy("id");

        //TODO 创造一个侧输出流叫late，注意此处要求是匿名内部类，这种带{}的写法表示new 一个匿名内部类对象
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("late") {
        };
        //TODO 开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //窗口允许迟到时间是3s
                .allowedLateness(Time.seconds(3))
                //TODO 将迟到的数据写入侧输出流
                .sideOutputLateData(outputTag);


        //窗口打印
        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
                                                                        @Override
                                                                        public void process(Tuple key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                                                            String msg = "当前key: " + key
                                                                                    + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                                                                    + elements.spliterator().estimateSize() + "条数据 ";
                                                                            out.collect(msg);
                                                                        }
                                                                    }
        );
        process.print("主流");

        //TODO 打印侧输出流
        process.getSideOutput(outputTag).print("迟到的数据（侧输出）");


        env.execute();
    }
}
