package com.atguigu.flink.day6;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;



// 利用AggregatingState计算每个传感器的平均水位
//AggregatingState与reduceState的区别，AggregatingState要重写四个方法，也就是其累加器与状态最后的输出结果是可以类型不一致的
public class Flink04_KeyedState_AggState {
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
        //4.将数据keyby，表示相同的key去同一个分区,为了方便同一个传感器的前后水位对比；且键控状态必须要keyby,因为他是一个key对应一个key State
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");
       //TODO 计算每个传感器的平均水位
         keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, Double>() {
            //1.定义状态用来aggrateState中的累积器来存储水位总和及传输的次数,integer表示输入的是vc,Double表示输出即vc的平均值
             private AggregatingState<Integer,Double>  aggregatingState;
             //2.状态初始化

             @Override
             public void open(Configuration parameters) throws Exception {
                 aggregatingState = getRuntimeContext()
                         .getAggregatingState(new AggregatingStateDescriptor<Integer,Tuple2<Integer,Integer> , Double>(
                                 "agg-state",
                                 new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                     //初始化累加器,第一个作为水位和，第二个作为统计次数
                                     @Override
                                     public Tuple2<Integer, Integer> createAccumulator() {
                                         return Tuple2.of(0,0);
                                     }
                                     //当前的输入数据和上一次状态进行累加
                                     @Override
                                     public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                         return Tuple2.of(accumulator.f0 + value,accumulator.f1 + 1 );
                                     }
                                    //整个累积状态的返回值
                                     @Override
                                     public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                         return accumulator.f0 * 1D / accumulator.f1;
                                     }

                                     @Override
                                     public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                         return null;
                                     }
                                 },  Types.TUPLE(Types.INT, Types.INT
                         )));
             }

             @Override
             public void processElement(WaterSensor waterSensor, Context context, Collector<Double> collector) throws Exception {
                //todo 根据状态来计算平均水位，只需要将新的值存入状态，再取出状态值即可
                 //todo 是当使用add(T)的时候AggregatingState会使用指定的AggregatingFunction进行聚合,ReduceState的使用方法与之类似
                 //1.将数据存入状态中
                 aggregatingState.add(waterSensor.getVc());
                 //2.从状态中取出数据
                 Double aDouble = aggregatingState.get();
                 //3.将状态平均水位输出
                 collector.collect(aDouble);
             }
         }).print();


        env.execute();
    }
}
