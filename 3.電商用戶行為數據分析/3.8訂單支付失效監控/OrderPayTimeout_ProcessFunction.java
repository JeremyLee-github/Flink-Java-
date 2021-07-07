package com.it.orderpay_detect;


import com.it.orderpay_detect.beans.OrderEvent;
import com.it.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.concurrent.TimeUnit;


public class OrderPayTimeout_ProcessFunction {

    // 定義超時事件的側輸出流標籤(全局變量)
    private final static OutputTag<OrderResult> orderTimeoutTag =  new OutputTag<OrderResult>("order-timeout"){};

    public static void main(String[] args) throws Exception{

        // 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 讀取數據並轉換成POJO類型
        URL resource = OrderPayTimeout_ProcessFunction.class.getResource("/OrderLog.csv");
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

        // 定義自定義處理函數，主流輸出正常匹配訂單事件，側輸出流輸出超時報警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventDataStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print("pay normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");
    }

    // 實現自定義的超時事件處理函數
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        // 定義狀態，保存之前點單是否已經來過create、pay的事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;
        // 定義狀態，保存定時器時間戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
            isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {

            // 先獲取當前狀態
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timerTs = timerTsState.value();

            // 判斷當前事件類型("crate", "pay" or "修改訂單"=>這個現在不討論!!)
            if ("create".equals(value.getEventType())) {
                // 1. 如果來的是create，要判斷是否支付過
                if (isPayed) {
                    // 1.1 如果已經正常支付，輸出正常匹配結果
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    isPayedState.clear();
                    isCreatedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                }else {
                    // 1.2 如果沒有支付過，註冊15分鐘後的定時器，開始等待支付事件
                    long ts = (value.getTimestamp() + 15 * 60) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    // 更新狀態
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }

            }else if ("pay".equals(value.getEventType())){
                // 2. 如果來的是pay，要判斷是否有下單事件來過
                if (isCreated) {
                    // 2.1 已經有過下單事件，要繼續判斷支付的時間戳是否超過15分鐘
                    if (value.getTimestamp() *1000L < timerTs) {
                        // 2.1.1 在15分鐘內，沒有超時，正常匹配輸出
                        out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    }else {
                        // 2.1.2 已經超時，輸出側輸出流報警
                        ctx.output(orderTimeoutTag, new OrderResult(value.getOrderId(), "payed but already timeout"));
                    }

                    // 統一清空狀態
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);

                }else {
                    // 2.2 沒有下單事件，亂序，註冊一個定時器，等待下單事件(時間點直接使用當下時間!!)
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp()*1000L);
                    // 更新狀態
                    isPayedState.update(true);
                    timerTsState.update(value.getTimestamp()*1000L);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定時器觸發，說明一定有一個事件沒來
            if (isPayedState.value()) {
                // 如果pay來了，說明create沒來
                out.collect(new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
            }else {
                // 如果pay沒來，支付超時
                out.collect(new OrderResult(ctx.getCurrentKey(), "timeout"));
            }
            // 清空狀態
            timerTsState.clear();
            isPayedState.clear();
            isCreatedState.clear();
        }
    }

}
