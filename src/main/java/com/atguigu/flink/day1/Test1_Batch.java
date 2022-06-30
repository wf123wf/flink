package com.atguigu.flink.day1;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Test1_Batch {
    public static void main(String[] args) throws Exception {
        //创建类用来批处理word.txt文件做wordcount，一行一行处理，类似于批
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.利用执行环境来读取数据,一行行读
        DataSource<String> linDS = env.readTextFile("input/words.txt");
        //3.一行内容的格式转换
        //将一行数据打散然后转化成（单词，1）,
        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = linDS.flatMap(new MyFlatMap());
        //按照元组的索引来group by
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);
        //组内聚合,按照元组索引聚合
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);
        sum.print();


    }
    //通过flatMap实现先打散，然后转换成二元组(单词，1)，string指的是输入数据泛型，tuple指的是输出数据的泛型
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{
        /**
         *
         * @param s 输入数据
         * @param collector 采集器，将数据采集起来发送到下游
         * @throws Exception
         */
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                        collector.collect( new Tuple2<>(word,1));
            }

        }
    }
}
