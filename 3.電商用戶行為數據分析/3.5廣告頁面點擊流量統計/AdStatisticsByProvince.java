package com.it.market_analysis;

import com.it.market_analysis.beans.AdClickEvent;
import com.it.market_analysis.beans.AdCountViewByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 從自定義數據源中讀取數據,轉為AdClickEvent類型
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<AdClickEvent> adClickEventDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
        })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.of(200, TimeUnit.MICROSECONDS)) {
                            @Override
                            public long extractTimestamp(AdClickEvent element) {
                                return element.getTimestamp() * 1000L; // 數據資料庫裏面是以秒為單位,所以這裡要做轉換為毫秒!!
                            }
                        }
                ));

        // 3. 基於省份分組，開窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountStream = adClickEventDataStream
                .keyBy(AdClickEvent::getProvince) // 這裡已經轉為String
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))  // // 定義滑窗,5min輸出一次
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountStream.print();

        env.execute("ad count by province job");
    }

    // 實現自定義的增量聚合函數
    public static class AdCountAgg implements AggregateFunction<AdClickEvent,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    // 實現自定義的全窗口函數
    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow>{
        @Override
        public void apply(String province, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountViewByProvince> collector) throws Exception {
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new AdCountViewByProvince(province, windowEnd, count));
        }
    }
}
