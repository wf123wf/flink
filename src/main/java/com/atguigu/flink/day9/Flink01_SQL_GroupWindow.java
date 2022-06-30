package com.atguigu.flink.day9;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//sql 的group window使用
public class Flink01_SQL_GroupWindow {
    public static void main(String[] args) {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3.创建一个表sensor
        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")");
        //todo 4.开启一个滚动窗口
//        tableEnv.executeSql("select\n" +
//                "\tid,\n" +
//                "\ttumble_end(t, interval '2' second) as wEnd,\n" +
//                "\ttumble_start(t, interval '2' second) as  wStart,\n" +
//                "\tsum(vc) sum_vc\n" +
//                "from\n" +
//                "\tsensor\n" +
//                "group by \n" +
//                "\ttumble(t, interval '2' second),id\n").print();
        //开启一个滑动窗口
//            tableEnv.executeSql("select\n" +
//                    "\tid,\n" +
//                    "\thop_start(t,interval '2' second,interval '3' second) as wStart,\n" +
//                    "\thop_end(t,interval '2' second,interval '3' second) as wEnd,\n" +
//                    "\tsum(vc) sum_vc\n" +
//                    "\n" +
//                    "from \n" +
//                    "\tsensor\n" +
//                    "group by\n" +
//                    "\tid,\n" +
//                    "\thop(t,interval '2' second,interval '3' second)").print();
        //开启一个会话窗口
        tableEnv.executeSql("select\n" +
                "\tid,\n" +
                "\tsession_start(t,interval '2' second) as wStart,\n" +
                "\tsession_end(t,interval '2' second) as wEnd,\n" +
                "\tsum(vc) sum_vc\n" +
                "from \n" +
                "\tsensor\n" +
                "group by\n" +
                "\tid,\n" +
                "\tsession(t,interval '2' second)").print();
    }
}
