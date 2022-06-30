package com.atguigu.flink.day8;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

//从外部系统kafka写入数据
public class Flink04_TableAPI_Connect_Kafka {
    public static void main(String[] args) {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3.TODO 从kafka读取数据并转化为临时表
        //创建表结构
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv.connect(new Kafka()
        .version("universal")
                .topic("sensor")
                .startFromLatest()
                .property(ConsumerConfig.GROUP_ID_CONFIG,"0726")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"HADOOP102:9092")

        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //用table API输出结果
        Table table = tableEnv.from("sensor");
        Table table1 = table
                .select($("id"), $("ts"), $("vc"));
        table1.execute().print();


    }
}
