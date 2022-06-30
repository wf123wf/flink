package com.atguigu.flink.day5;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.shaded.zookeeper3.org.apache.jute.CsvOutputArchive;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.w3c.dom.ls.LSOutput;

//基于处理时间的定时器,处理时间是不涉及到waterMark，此处为了简化也没有加入窗口
public class Flink09_ProcessingTime_Timer {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口读数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3.将数据转化成JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.将相同key的数据聚合在一起
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //5.通过process来注册一个定时器
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                //TODO 利用环境上下文context来注册一个基于处理时间的定时器
                context.timerService().registerProcessingTimeTimer(
                        //定时器的时间是当前事件的处理时间过5S
                        context.timerService().currentProcessingTime() + 5000);
                //打印一下当前的处理时间
                System.out.println("生成定时器" + context.timerService().currentProcessingTime() / 1000);
                collector.collect(waterSensor);
            }

            @Override
            //TODO 表示定时器时间达到后触发的事件
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("5S已过，定时器触发" + ctx.timerService().currentProcessingTime() / 1000);
                out.collect(new WaterSensor("555", 5L, 5));
            }
        });
        process.print("主流：");
        env.execute();


    }
}
