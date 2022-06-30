package com.atguigu.flink.day6;
//监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流，用处理时间即可
//分析:由于要分析5秒钟之内的连续上升(实质是一个上升状态发生后能否继续保持上升5S)，所以决定用键控状态，
// 只用前后对比状态对比即可判断是否上升，所以用valueState;同时又要判断5s的连续上升，而且还要报警，所以用定时器，
//如果这5s内有水位变化，不是上升的我们就取消报警器，如果5s过了，得重置报警器判断下一个5s

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink06_Timer_Exec_WithKeyedState {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3.将数据转化成Java Bean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //4.将数据keyby，表示相同的key去同一个分区,为了方便同一个传感器的前后水位对比
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");
        //定义一个侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("报警"){};
        //5.进行状态的保存提取和操作
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            //TODO 为水位设置一个状态
            private ValueState<Integer> stateVC;
            //TODO 为定时器时间设置一个状态
            private ValueState<Long> stateTimer;

            //TODO open是周期性函数，一个并行度只执行一次，对状态进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                stateVC = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("stateVC", Integer.class, Integer.MIN_VALUE));
                stateTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("stateTimer", Long.class, Long.MIN_VALUE));
            }


            //TODO 主要执行逻辑，用来比较此次的水位和前一次水位（说明一个数据只保存一个状态）
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {

                //1.判断水位是否上升
                if (waterSensor.getVc() > stateVC.value()) {
                    //2.如果没有定时器，那么注册定时器，因为如果有定时器重新注册定时器就改变了状态，前一个计时器就失效了

                    if (stateTimer.value() == Long.MIN_VALUE) {
                        //注册定时器
                        System.out.println("注册定时器：" + context.getCurrentKey());
                        //设置定时器时间
                        stateTimer.update(context.timerService().currentProcessingTime() + 5000);
                        //设置定时器
                        context.timerService().registerProcessingTimeTimer(stateTimer.value());
                    }
                } else {
                    //3.如果水位没有上升，取消定时器
                    System.out.println("删除定时器：" + context.getCurrentKey());
                    context.timerService().deleteProcessingTimeTimer(stateTimer.value());
                    //4.恢复定时器时间的状态
                    stateTimer.clear();

                }
                //5.更新水位状态
                stateVC.update(waterSensor.getVc());
                collector.collect(waterSensor);

            }

            //此处表示5S到了触发报警器
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                //获取侧输出流，将报警信息打印到侧输出流
                ctx.output(outputTag, "报警!!!!!");
                stateTimer.clear();

            }
        });

        process.print("主流");
        //侧输出流打印
        process.getSideOutput(outputTag).print("侧输出流");

        //执行
        env.execute();
    }
}
