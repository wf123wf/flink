package com.atguigu.flink.day8;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

//用table API连接外部文件系统，从而输入数据
public class Flink03_TableAPI_Connect_File {
    public static void main(String[] args) {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 3.连接外部文件系统，获取数据，并将其映射为临时表
        //创建表的格式：字段
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        //连接并映射表结构 + 表名
        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().lineDelimiter("\n").fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("sensor");
        //查询临时表中的数据,注意只有table Result才可以直接打印
        //方式1
//        TableResult tableResult = tableEnv.executeSql("select * from sensor");
//        tableResult.print();
        //方式2
//        TableResult execute = tableEnv.sqlQuery("select * from sensor").execute();
//        execute.print();
        //需求升级，从临时表中用table API进行id的分组，并聚合vc，并打印到控制台
        //TODO  将临时表转化成table对象，然后调用api
        Table table = tableEnv.from("sensor");
        Table table1 = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());
        table1.execute().print();




    }
}
