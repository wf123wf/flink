package com.atguigu.flink.day4;


import com.atguigu.flink.bean.MarketingUserBehavior;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Flink06_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) {
        //不同渠道的app市场统计
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.通过自定义数据源获取数据
        DataStreamSource<MarketingUserBehavior> source = env.addSource(new AppMarketingDataSource());
        //3.将渠道和行为组成key，value为1,因为要统计不同渠道不同行为的个数
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new FlatMapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(MarketingUserBehavior marketingUserBehavior, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String key = marketingUserBehavior.getChannel() + marketingUserBehavior.getBehavior();
                collector.collect(Tuple2.of(key, 1));
            }
        });
        //4.对不同渠道不同行为进行分组并sum
        flatMap.keyBy(0).sum(1).print();
    }
    //自定义输出的数据类型，实现RichSourceFunction,泛型表示输出类型
    //主要完成生产数据并传输的功能
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior>{
        boolean canRun = true;
        //随机数备用来产生数据
        Random random = new Random();
        //造部分数据
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");
        //用来产生数据或者将数据发出去，用while(true)体现流
        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            while (canRun){
                MarketingUserBehavior marketingUserBehavior=new MarketingUserBehavior(
                        (long)random.nextInt(100000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                sourceContext.collect(marketingUserBehavior);
                Thread.sleep(200);

            }
        }


       //用来取消数据发送，由系统调用
        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
