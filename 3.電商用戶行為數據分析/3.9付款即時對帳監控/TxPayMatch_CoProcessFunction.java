package com.it.orderpay_detect;

import com.it.orderpay_detect.beans.OrderEvent;
import com.it.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.concurrent.TimeUnit;

public class TxPayMatch_CoProcessFunction {

    // 定義側輸出流標籤
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts"){};

    public static void main(String[] args) throws Exception{

        // 創建環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 讀取數據並轉換成POJO類型
        // 讀取訂單支付事件數據
        URL orderResource = TxPayMatch_CoProcessFunction.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.of(200, TimeUnit.MICROSECONDS)) {
                            @Override
                            public long extractTimestamp(OrderEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ))
                .filter(data -> !"".equals(data.getTxId()));  // 交易id不為空，必須是pay事件

        // 讀取到賬事件數據
        URL receiptResource = TxPayMatch_CoProcessFunction.class.getResource("/ReceiptLog.csv");
        DataStream<ReceiptEvent> receiptEventStream = env.readTextFile(receiptResource.getPath())
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

        // 將兩條流進行連接合併，進行匹配處理，不匹配的事件輸出到側輸出流
        SingleOutputStreamOperator<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute("tx match detect job");
    }

    // 實現自定義CoProcessFunction
    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {

        // 定義狀態，保存當前已經到來的訂單支付事件和到賬時間
        ValueState <OrderEvent> payState;
        ValueState <ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 訂單支付事件來了，判斷是否已經有對應的到賬事件
            ReceiptEvent receipt = receiptState.value();

            if (receipt != null) {
                // 如果receipt不為空，說明到賬事件已經來過，輸出匹配事件，清空狀態
                out.collect(new Tuple2<>(pay,receipt));
                payState.clear();
                receiptState.clear();
            }else {
                // 如果receipt沒來，註冊一個定時器，開始等待
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp()+5)*1000L);  // 等待5秒鐘，具體要看數據
                // 更新狀態
                payState.update(pay);
            }

        }

        @Override
        public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 到賬事件來了，判斷是否已經有對應的支付事件
            OrderEvent pay = payState.value();

            if (pay != null) {
                // 如果pay不為空，說明支付事件已經來過，輸出匹配事件，清空狀態
                out.collect(new Tuple2<>(pay,receipt));
                payState.clear();
                receiptState.clear();
            }else {
                // 如果pay沒來，註冊一個定時器，開始等待
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp()+3)*1000L);  // 等待3秒鐘，具體要看數據
                // 更新狀態
                receiptState.update(receipt);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 定時器觸發，有可能是有一個事件沒來，不匹配，也有可能是都來過了，已經輸出並清空狀態
            // 判斷哪個不為空，那麼另一個就沒來
            if (payState.value() != null) {
                ctx.output(unmatchedPays, payState.value());
            }
            if (receiptState.value() != null) {
                ctx.output(unmatchedReceipts, receiptState.value());
            }

            // 清空狀態
            payState.clear();
            receiptState.clear();
        }
    }
}
