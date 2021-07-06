package com.it.market_analysis;

import com.it.market_analysis.beans.ChannelPromotionCount;
import com.it.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AppMarketingStatistics {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 從自定義數據源中讀取數據
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new AppMarketingByChannel())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<MarketingUserBehavior>(Time.of(200, TimeUnit.MICROSECONDS)) {
                            @Override
                            public long extractTimestamp(MarketingUserBehavior element) {
                                return element.getTimestamp();
                            }
                        }
                ));

        // 3. 分渠道開窗統計
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                    // 這裡KeyBy後已經轉為String
                }).keyBy(data -> data.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        resultStream.print();

        env.execute("app marketing by channel job");
    }

    // 實現自定義的模擬市場用戶行為數據源
    public static class AppMarketingByChannel implements SourceFunction<MarketingUserBehavior> {

        Boolean running = true;
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "Line", "facebook");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (running) {
                long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                long timestamp = System.currentTimeMillis();  // 這裡為毫秒

                ctx.collect(new MarketingUserBehavior(id,behavior,channel,timestamp));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // 實現自定義的增量聚合函數
    public static class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String,Long>,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
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
    public static class MarketingStatisticsResult implements WindowFunction<Long,ChannelPromotionCount, String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new ChannelPromotionCount("total","total",windowEnd,count));
        }
    }
}
