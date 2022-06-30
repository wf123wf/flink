package com.atguigu.flink.day9;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

//自定义一个表聚合函数，多进多出，求vc最大的两个值
public class Flink06_UDF_TableAggFun {
    public static void main(String[] args) {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3.获取端口中的数据,并进行格式转化
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        //4.将流转化成表
        Table table = tableEnv.fromDataStream(waterSensorStream);
        //TODO 5.用table API查询数据,注意这种函数没有sql查询
        table
                .groupBy($("id"))
                .flatAggregate(call(MyUDAF.class,$("vc")))
                .select($("id"),$("f0"),$("f1")).execute().print();



    }
    //自定义一个累加器类
    public static class Myacc {
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;


    }
    //TODO 自定义一个表聚合函数，多进多出，求vc最大的两个值
    public static class MyUDAF extends TableAggregateFunction<Tuple2<Integer,String>,Myacc>{
        //创建累加器
        @Override
        public Myacc createAccumulator() {
            return new Myacc();
        }
        //累加器逻辑
        public void accumulate(Myacc acc, Integer value){
            if(acc.first < value){
                acc.second = acc.first;
                acc.first = value;
            } else if(acc.second < value) {
                acc.second =value;
            }
        }
        //累加器输出,有数据才通过采集输出
        public void emitValue(Myacc acc, Collector<Tuple2<Integer,String>> out){
            if(acc.first != null){
                out.collect(Tuple2.of(acc.first,"first"));
            }
            if(acc.second != null){
                out.collect(Tuple2.of(acc.second,"second"));
            }
        }
    }

}
