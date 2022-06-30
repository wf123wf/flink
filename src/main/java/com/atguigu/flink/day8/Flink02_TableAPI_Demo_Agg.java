package com.atguigu.flink.day8;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
//用table API进行聚合，由于聚合，所以将结果表转化的时候只能转为撤回流
public class Flink02_TableAPI_Demo_Agg {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        //TODO 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 将流转化成表
        Table table = tableEnv.fromDataStream(waterSensorStream);
        //todo 通过连续查询出数据并生成动态结果表
        Table result = table
               .groupBy($("id"))
                .select($("id"),$("vc").sum().as("vcSum"));
        //todo 将结果转化成流（撤回流）
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(result, Row.class);


        retractStream.print();
        env.execute();
    }
}
