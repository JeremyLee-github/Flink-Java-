package com.it.orderpay_detect;

import com.it.orderpay_detect.beans.OrderEvent;
import com.it.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.concurrent.TimeUnit;

public class TxPayMatchByJoin {
    public static void main(String[] args) throws Exception{

        /*
        這種方法的缺陷，只能獲得正常匹配的結果，不能獲得未匹配成功的記錄。!!!!
         */

        // 創建環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 讀取數據並轉換成POJO類型
        // 讀取訂單支付事件數據
        URL orderResource = TxPayMatchByJoin.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.of(200, TimeUnit.MICROSECONDS)) {
                            @Override
                            public long extractTimestamp(OrderEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ))
                .filter(data -> !"".equals(data.getTxId()));  // 交易id不為空，必須是pay事件

        // 讀取到賬事件數據
        URL receiptResource = TxPayMatchByJoin.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.of(200, TimeUnit.MICROSECONDS)) {
                            @Override
                            public long extractTimestamp(ReceiptEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));

        // 區間連接兩條流，得到匹配的數據
        SingleOutputStreamOperator<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                // -3，5 區間範圍
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxPayMatchDetectByJoin());

        resultStream.print();

        env.execute("tx pay match join job");
    }

    // 實現自定義ProcessJoinFunction
    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {
        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent, receiptEvent));
        }
    }
}
