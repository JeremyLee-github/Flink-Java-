package com.it.orderpay_detect;

import com.it.orderpay_detect.beans.OrderEvent;
import com.it.orderpay_detect.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.OutputTag;

import java.awt.image.LookupOp;
import java.net.URL;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OrderPayTimeout {
    public static void main(String[] args) throws Exception{

        // 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 讀取數據並轉換成POJO類型
        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventDataStream = env.readTextFile(resource.getPath())
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
                ));

        // 1. 定義一個待時間限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 2. 定義側輸出流標籤，用來表示超時事件
        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("order-timeout") {};

        // 3. 將pattern應用到輸入數據上，得到pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventDataStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 4. 調用select方法，實現對匹配複雜事件和超時復雜事件的提取和處理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(outputTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(outputTag).print("timeout");

        env.execute("order timeout detect job");

    }

    // 實現自定義的超時事件處理函數
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult>{

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long timeoutTimestamp) throws Exception {
            Long orderId = map.get("create").get(0).getOrderId();
            String s = new Timestamp(timeoutTimestamp).toString();
            return new OrderResult(orderId, "timeout "+s);
        }
    }

    // 實現自定義的超時事件處理函數
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long orderId = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(orderId, "payed");
        }
    }
}
