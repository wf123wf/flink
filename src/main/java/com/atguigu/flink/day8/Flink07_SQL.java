package com.atguigu.flink.day8;
//从流中获取数据：（1）流——未注册表——可视视图表
                //(2) 流——可视视图表
import com.atguigu.flink.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;
public class Flink07_SQL {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));


        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //将流转化成表（未注册）
        Table table = tableEnv.fromDataStream(waterSensorStream);
        //todo 将未注册表转化成临时视图（已注册表）
        tableEnv.createTemporaryView("sensor",table);
        //todo 直接将流转化成可视视图(已注册表)
//        tableEnv.createTemporaryView("sensor",waterSensorStream);
        //todo 通过可视视图读取表中的数据
        tableEnv.executeSql("select * from sensor").print();
    }
}
