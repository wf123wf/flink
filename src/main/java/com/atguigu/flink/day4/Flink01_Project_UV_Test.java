package com.atguigu.flink.day4;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink01_Project_UV_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //此处也是每个并行度执行一次
        HashSet<Long> userIds = new HashSet<>();
        //当有多个并行度时，结果会分散到不同的slot的print,所以需要用法国keyby放到同一个分区
        env
                .readTextFile("input/UserBehavior.csv")
              .flatMap(new FlatMapFunction<String, Integer>() {
                  @Override
                  public void flatMap(String s, Collector<Integer> collector) throws Exception {
                      String[] split = s.split(",");
                      UserBehavior behavior = new UserBehavior(
                              Long.valueOf(split[0]),
                              Long.valueOf(split[1]),
                              Integer.valueOf(split[2]),
                              split[3],
                              Long.valueOf(split[4]));
                      if ("pv".equals(behavior.getBehavior())) {
                          userIds.add(behavior.getUserId());

                      }
                      collector.collect(userIds.size());
                  }
              }).print();

        env.execute();
    }
}
