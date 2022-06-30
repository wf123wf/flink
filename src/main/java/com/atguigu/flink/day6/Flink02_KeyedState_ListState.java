package com.atguigu.flink.day6;

import com.atguigu.flink.WaterSensor;
import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.Comparator;

//利用list输出每个传感器最高的3个水位值
public class Flink02_KeyedState_ListState {
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
        //5.进行状态的保存提取和操作
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 设置一个状态，由于是保存多个状态，所以用listState
            private ListState<Integer> listState;
            //TODO open是周期性函数，一个并行度只执行一次，对状态进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                listState=getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state",Types.INT));
            }

            //TODO 主要执行逻辑，用来比较此次的水位和前一次水位（说明一个数据只保存一个状态）
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
             //先存入元素，然后作比较，取出最大的三个元素更新状态
                //1.将当前的水位保存到状态
                listState.add( waterSensor.getVc());
                //2.将状态中的数据取出来排序
                Iterable<Integer> integers = listState.get();
                //3. 迭代器需要转化成list排序，又java里迭代器没有tolist方法，遍历迭代器
                ArrayList<Integer> listVC = new ArrayList<>();
                for (Integer integer : integers) {
                    listVC.add(integer);
                }
                //4.对list集合的元素进行排序
                listVC.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer a1, Integer a2) {
                        //正序a1 -a2，倒序a2-a1
                        return a2- a1;
                    }
                });
                //todo 5.判断集合个数并更新状态的值:说明状态值都是通过代码逻辑来选择是否更新的，
                //listState,valueState是靠update（）更新，aggratingState 和reduceState是靠add()自动掉对应的Function更新
                if(listVC.size() > 3){
                    listVC.remove(3);
                }
                listState.update(listVC);
                //输出最后的结果
                System.out.println(listVC.toString());

            }
        }).print();

        //执行
        env.execute();

    }
}
