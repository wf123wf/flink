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

public class Flink01_EventTime_WaterMark_Monotonous {
    //主要用来看乱序度0的waterMark的作用，当添加了waterMark后，waterMark是时间戳，用来决定是否关闭窗口
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
        //4.设置WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(
                //Watermark策略
                WatermarkStrategy
                        //单调增长，也就是乱序度为0，指定泛型
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            //指定哪个字段是事件的时间字段,
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs() * 1000;
                            }
                        })

        );
        //5.将相同的key的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorSingleOutputStreamOperator.keyBy("id");
        //6.开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
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
