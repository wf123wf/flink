package com.atguigu.flink.day8;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

//将数据写出外部文件系统
public class Flink05_TableAPI_Connect_File_Sink {
    public static void main(String[] args) {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       //准备表中的数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //2.获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3.TODO 将符合id是sensor_1的数据写入文件系统
        //将流转成表
        Table table1 = tableEnv.fromDataStream(waterSensorDataStreamSource);
//        Table table1 = table.where($("id").isEqual("sensor_1"))
//                .select($("id"), $("ts"), $("vc"));
       //规定表的格式
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());
        //将表的数据输出文件系统
        tableEnv.connect(new FileSystem().path("output/sensor-sql2.txt"))
                //由于是table默认的是一条数据一行，所以规定不用换行
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor");
        //将数据插入临时表
        TableResult result = table1.executeInsert("sensor");



    }
}
