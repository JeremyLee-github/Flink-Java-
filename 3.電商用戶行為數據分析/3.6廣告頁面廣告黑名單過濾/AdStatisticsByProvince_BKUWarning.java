package com.it.market_analysis;


import com.it.market_analysis.beans.AdClickEvent;
import com.it.market_analysis.beans.AdCountViewByProvince;
import com.it.market_analysis.beans.BlackListUserWarning;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.security.AccessControlContext;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class AdStatisticsByProvince_BKUWarning {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 從自定義數據源中讀取數據,轉為AdClickEvent類型
        URL resource = AdStatisticsByProvince_BKUWarning.class.getResource("/AdClickLog.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<AdClickEvent> adClickEventDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
        })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.of(200, TimeUnit.MICROSECONDS)) {
                            @Override
                            public long extractTimestamp(AdClickEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));

        // 3. 對同一個用戶點擊同一個廣告的行為進行檢測報警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventDataStream
                .keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
                        return new Tuple2<>(adClickEvent.getUserId(), adClickEvent.getAdId());
                    }
                }).process(new FilterBlackListUser(100));

        // 4. 基於省份分組，開窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream
                .keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();
        // 將側輸出流打印出來!!(顯示黑名單資料)
        filterAdClickStream
                .getSideOutput(new OutputTag<BlackListUserWarning>("blacklist"){})
                .print("blacklist-user");

        env.execute("ad count by province job");
    }

    // 實現自定義的增量聚合函數
    public static class AdCountAgg implements AggregateFunction<AdClickEvent,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    // 實現自定義的全窗口函數
    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountViewByProvince> collector) throws Exception {
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new AdCountViewByProvince(s,windowEnd,count));
        }
    }

    // 實現自定義處理函數
    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long,Long>,AdClickEvent, AdClickEvent>{
        // 定義屬性：點擊次數上線
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        // 定義狀態，保存當前用戶對某一廣告的點擊次數
        ValueState<Long> countState;
        // 定義一個標誌狀態，保存當前用戶是否已經被發送到了黑名單裡
        ValueState<Boolean> isSentState;

        // 使用open方法註冊countState跟isSentState
        @Override
        public void open(Configuration parameters) throws Exception {
            //  ValueState<Long> countState => 這裡前面不要給狀態類型 countState(這樣就可以了!!!)
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class));
        }

        // 重寫繼承KeyedProcessFunction的方法
        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            // 判斷當前用戶對同一廣告的點擊次數，如果不夠上限，該count加1正常輸出；
            // 如果到達上限，直接過濾掉，並側輸出流輸出黑名單報警

            // 首先獲取當前count值
            Long curCount = countState.value();
            Boolean isSent = isSentState.value();

            // 設定初始值
            if (curCount == null) curCount = 0L;
            if (isSent == null) isSent = false;

            // 1. 判斷是否是第一個數據，如果是的話，註冊一個第二天0點的定時器
            if (curCount == 0) {
                long ts = ctx.timerService().currentProcessingTime();
                long fixedTime = DateUtils.addDays(new Date(ts), 1).getTime();
                ctx.timerService().registerProcessingTimeTimer(fixedTime);
            }

            // 2. 判斷是否報警
            if (curCount >= countUpperBound) {
                // 判斷是否輸出到黑名單過，如果沒有的話就輸出到側輸出流
                if (!isSent) {
                    isSentState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist"){},  // 記得new OutputTag<>(){} => 一定要加{}
                            new BlackListUserWarning(value.getUserId(), value.getAdId(), " click over " + countUpperBound + "times."));
                }
                // 不再進行下面操作(已經是黑名單了!!!) => 直接return跳出!!!
                return;
            }

            // 如果沒有返回，點擊次數加1，更新狀態，正常輸出當前數據到主流
            countState.update(curCount+1);
            out.collect(value);

        }

        // 使用定時器將狀態清空(重新計算)
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 清空所有狀態
            countState.clear();
            isSentState.clear();
        }
    }
}
