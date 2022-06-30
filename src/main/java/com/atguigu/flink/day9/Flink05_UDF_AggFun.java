package com.atguigu.flink.day9;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import javax.sound.sampled.spi.AudioFileReader;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

//自定义一个聚合函数，多进一出，求vc平均值
public class Flink05_UDF_AggFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取端口中的数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream);
        //TODO 5..用table API写数据查询
//        table
//                .groupBy($("id"))
//                .select($("id"),call(MyUDAF.class,$("vc"))).execute().print();
        //TODO 6.用SQL API写数据查询
        //1)创建函数名
        tableEnv.createTemporaryFunction("avg",MyUDAF.class);
        tableEnv.executeSql("select id, avg(vc) from "+table+" group by id").print();

    }
    //TODO 创建一个类作为累加器
    public static class Myacc{
        public Integer sum ;
        public Integer count;
    }

    // TODO 自定义一个聚合函数，多进一出，求vc平均值
    public static class MyUDAF extends AggregateFunction<Double,Myacc>{
        //初始化累加器
        @Override
        public Myacc createAccumulator() {
            Myacc myacc = new Myacc();
            myacc.sum =0;
            myacc.count=0;
            return myacc;
        }
        //累加器执行逻辑,注意这是固有的写法，除了里面的逻辑
        public void accumulate(Myacc myacc,Integer value){
            myacc.count ++;
            myacc.sum += value;
        }
        @Override
        public Double getValue(Myacc myacc) {
            if (myacc.count == 0){
                return null;
            } else{
                return myacc.sum * 1D/myacc.count;
            }

        }


    }
}
