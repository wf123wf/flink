package com.atguigu.flink.day9;

import com.atguigu.flink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

//自定义一个标量函数，一进一出，获取id的字符串长度
public class Flink03_UDF_SalarFun {
    public static void main(String[] args) {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3.获取端口中的数据,并进行格式转化
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        //4.将流转化成表
        Table table = tableEnv.fromDataStream(waterSensorStream);
        //TODO 5.用table API查询数据
        //1)不注册直接使用(传类名)
//        table
//                .select($("id"),call(MyUDF.class,$("id")).as("long"))
//                .execute()
//                .print();
        //2)不注册直接使用(传对象)
//        table
//                .select($("id"),call(new MyUDF(),$("id")).as("long"))
//                .execute()
//                .print();
        //3)先注册再使用
//        tableEnv.createTemporaryFunction("strLen",MyUDF.class);
//        table
//                .select($("id"),call("strLen",$("id")))
//                .execute()
//                .print();
        //TODO 6.用SQL API查询数据
        //1)注册函数
        tableEnv.createTemporaryFunction("strLen",MyUDF.class);
        //2)sql进行查询，此处table没有名字，用变量表示，注意空格
        tableEnv.executeSql("select id , strLen(id) from " + table).print();




    }
    //TODO 自定义一个标量函数，一进一出，获取id的字符串长度,只需要将方法名声明为eval即可
    public static class MyUDF extends ScalarFunction{
        public Integer eval(String value){
            return value.length();
        }
    }
}
