package com.atguigu.flink.day9;


import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

//自定义一个表函数，一进多出，根据id按照下划线切分出多个数据,一进多出相当于炸裂，需要进行侧写表join
public class Flink04_UDF_TableFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取端口中的数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 5.用table API写数据查询
        //1)不注册直接使用
//        table
//             .joinLateral(call(MyUDTF.class,$("id")).as("newWord"))
//                .select($("id"),$("newWord")).execute().print();
        //2)先注册再使用
        tableEnv.createTemporaryFunction("myExplor",MyUDTF.class);
//        table
//                .joinLateral(call("myExplor",$("id")))
//                .select($("id"),$("word")).execute().print();
        //TODO 6.用SQL API写数据查询
        //1)SQL语法1：注意使用是需要 ， 而且后面必须写 lateral table()
//        tableEnv.executeSql("select id, word from " + table + " , lateral table(myExplor(id))").print();
        //2）SQLy语法2
            tableEnv.executeSql("select id,word from "+table+ " join lateral table(myExplor(id)) on true" ).print();
//            tableEnv.executeSql("select id,word from "+table+ " join lateral table(myExplor(id)) on true" ).print();


    }
    //TODO 自定义表函数，一进多出，id按照下划线分出多个数据,炸裂出来的字段需要暗示指定字段名
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class MyUDTF extends TableFunction<Row>{
        //仍要声明方法名为eval,由于是多出，结果不确定所以没有返回值，最后用collect收集结果
        public void eval(String value){
            String[] split = value.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }

}
