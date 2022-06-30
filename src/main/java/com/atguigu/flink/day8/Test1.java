package com.atguigu.flink.day8;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;
public class Test1 {
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

        //将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        Table selectResultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //TODO 3.连接外部文件系统，获取数据，并将其映射为临时表

        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .sinkPartitionerRoundRobin()
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将数据插入到临时表中
        selectResultTable.executeInsert("sensor");
    }
}
