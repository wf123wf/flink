package com.atguigu.flink.day6;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
public class Test1 {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.对相同key的数据做聚合
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(r -> r.getId());

        //5.检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            //定义一个状态用来保存上一次水位
            private ValueState<Integer> valueState;

            //初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
//                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Types.INT,0));
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.拿当前的水位和上一次的水位对比，如果大于10就报警
                int lastVc = valueState.value() == null ? value.getVc() : valueState.value();
//                Integer lastVc = valueState.value();
//                if (lastVc==null){
//                    lastVc = 0;
//                }

                if (Math.abs(value.getVc() - lastVc) > 10) {
                    out.collect("水位超过10报警！！！！！");
                }

                //2.将当前水位保存到状态中
                valueState.update(value.getVc());
            }
        }).print();

        env.execute();
    }
}
