package com.atguigu.flink.day9;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//开窗，over窗
public class Flink02_SQL_OverWindow {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //建表
        tableEnv.executeSql("create table \n" +
                "\tsensor(\n" +
                "\t\tid string,\n" +
                "\t\tts bigint,\n" +
                "\t\tvc int,\n" +
                "\t\tt as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "\t\twatermark for t as t - interval '5' second)\n" +
                "\t\twith (\n" +
                "\t\t\t'connector' = 'filesystem',\n" +
                "\t\t\t'path' = 'input/sensor-sql.txt',\n" +
                "\t\t\t'format' = 'csv'\n" +
                "\n" +
                "\t\t)\n");
        // todo 3.使用Over window;
        tableEnv.executeSql("select\n" +
                "\tid,\n" +
                "\tts,\n" +
                "\tvc,\n" +
                "\tcount(vc) over w,\n" +
                "\tsum(vc) over w\n" +
                "from \n" +
                "\tsensor\n" +
                "window w as (partition by id order by t rows between 1 preceding and current row)").print();

    }
}
