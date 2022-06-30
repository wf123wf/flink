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

import java.time.Duration;

public class Flink02_EventTime_WaterMark_Bounded {
    //主要用来看有乱序度的waterMark的作用，当添加了waterMark后，waterMark是评判事件是否发生的标准，表示在waterMark之前的事件都发生完了，用来决定是否关闭窗口
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3.将数据转为JaveBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

            }
        });
        //4.设置WaterMark(允许固定延迟)乱序程度或者说叫固定延迟为3秒
        //通过设置我们可以感受到，有了waterMark乱序度后,窗口关闭时间由waterMark决定了，本来事件时间是7s,但是watermark的值为：7-3-1ms,也就是只有4s前的事件已经处理完毕，达不到关窗的要求
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
        //5.将相同的key的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorSingleOutputStreamOperator.keyBy("id");
        //6.开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        //7.打印窗口相关信息
        window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            //等一个窗口的数据传完之后才执行
            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                String msg = "当前key:" + tuple
                        //获得窗口的开窗时间和关闭时间,以及一个窗口的数据个数
                        + "窗口：[" + context.window().getStart()/1000 + "," + context.window().getEnd()/1000 + ")一共有"
                        + iterable.spliterator().estimateSize() + "条数据";
                collector.collect(msg);
            }
        }).print();

        env.execute();
    }
}
