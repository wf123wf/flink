package com.atguigu.flink.day8;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink01_CEP_BasicUse {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取数据
        DataStreamSource<WaterSensor> elements = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        //TODO 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 将流转化成表
        Table table = tableEnv.fromDataStream(elements);
        //todo 通过连续查询出数据并生成动态结果表
        Table result = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));
        //todo 将结果转化为流（因为table是无法直接打印的）,两个参数一个是table对象，一个是输出类型
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(result, Row.class);

        rowDataStream.print();
        env.execute();



    }
}
