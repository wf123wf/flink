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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//允许数据迟到：也就是waterMark即使触发了关闭窗口，但是允许迟到，不会立马关闭窗口，只要是窗口的数据都可以进入
// 而是增量计算，从达到关闭窗口的条件开始每来一条数据计算一次，
// 直到迟到时间到了，才会关闭窗口，后面的值也就无法进入了
public class Flink06_EventTime_WaterMark_Bounded_Lateness {
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

        //TODO 开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //TODO 设置允许迟到的时间为3s
                .allowedLateness(Time.seconds(3));


        //窗口打印
        window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
                           @Override
                           public void process(Tuple key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                               String msg = "当前key: " + key
                                       + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                       + elements.spliterator().estimateSize() + "条数据 ";
                               out.collect(msg);
                           }
                       }
        ).print();

        env.execute();
    }
}
