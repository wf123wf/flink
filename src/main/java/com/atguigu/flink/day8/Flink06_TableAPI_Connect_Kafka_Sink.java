package com.atguigu.flink.day8;


import com.atguigu.flink.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

//将数据写出外部系统Kafka
public class Flink06_TableAPI_Connect_Kafka_Sink {
    public static void main(String[] args) {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        //2.获取表环境,注意此处导java的相关依赖
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //获取数据，将流转换成表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);
        Table selectResultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //创建表格式
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

//        //todo 1.创建临时表和kafka的映射
        tableEnv
                .connect(new Kafka()
                .version("universal")
                        .topic("sensor")

                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092"))
                .withFormat(new Json())
                .withSchema(schema)

                .createTemporaryTable("sensor");
        //todo 2.将数据插入临时表
        selectResultTable
                .executeInsert("sensor");

//        tableEnv.connect(new Kafka()
//                .version("universal")
//                .topic("sensor")
//                .sinkPartitionerRoundRobin()
//                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
//        )
//                .withFormat(new Json())
//                .withSchema(schema)
//                .createTemporaryTable("sensor");
////
////        //将数据插入到临时表中
//        selectResultTable.executeInsert("sensor");


    }
}
