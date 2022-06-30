package com.atguigu.flink.day4;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Project_Order {
    public static void main(String[] args) {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从文件获取数据并转化成JavaBean
        //读取订单相关数据
        DataStreamSource<String> source = env.readTextFile("input/OrderLog.csv");
        //从String转化成Java Bean
        SingleOutputStreamOperator<OrderEvent> ordeDS = source.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        });
        //读取交易相关数据
        SingleOutputStreamOperator<TxEvent> txDS = env.readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new TxEvent(
                                split[0],
                                split[1],
                                Long.parseLong(split[2])
                        );
                    }
                });

        //3.connect连接两个流
        ConnectedStreams<OrderEvent, TxEvent> connect = ordeDS.connect(txDS);
        //4.将相同的key聚合到一起，因为是按照分区来进行操作的，如果不在一个分区会影响匹配
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = connect.keyBy("txId", "txId");
        //5.实时对账
    }
}
