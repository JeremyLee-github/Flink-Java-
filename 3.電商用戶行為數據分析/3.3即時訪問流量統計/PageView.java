package com.it.networkflow_analysis;

import com.it.networkflow_analysis.beans.PageViewCount;
import com.it.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.windowing.time.Time.hours;

public class PageView {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 讀取文件，轉換成POJO
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<UserBehavior> userBehaviorDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MICROSECONDS)) {
                    // 這裡資料庫時間為亂序排列(實務也都是亂序時間),所以使用new BoundedOutOfOrdernessTimestampExtractor<>()處理
                    // Time.of(200, TimeUnit.MICROSECONDS) => 符合實務操作(這裡選擇延遲200毫秒)
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;  // 轉換成毫秒
                    }
                }
        ));

        // 4. 分組開窗聚合，得到每個窗口內各個商品的count值
        DataStream<Tuple2<String, Long>> pvResultStream1 = userBehaviorDataStream
                // 過濾只保留pv行為
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                // 按照商品ID分組
                .keyBy(item -> item.f0)
                // 1小時滾動窗口
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum(1);

        // 並行任務改進，設計隨機key，解決數據傾斜問題
        SingleOutputStreamOperator<PageViewCount> pvStream = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior element) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(hours(1)))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 將各分區數據匯總起來
        DataStream<PageViewCount> pvResultStream = pvStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());

        pvResultStream.print();

        env.execute("pv count job");
    }
    // 自定義預聚合函數
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    // 自定義預聚合函數
    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow>{

        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
        }
    }

    // 實現自定義處理函數，把相同窗口分組統計的count值疊加
    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount>{
        // 定義狀態，保存當前的總count值
        ValueState<Long> totalCountState;

        // 使用open方法取得資料,存入totalCountState
        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class));
        }

        // 重寫繼承KeyedProcessFunction的方法
        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            Long totalCount = totalCountState.value();
            if (totalCount == null) {
                totalCount = 0L;
                totalCountState.update(totalCount);
            }
            totalCountState.update(totalCount + value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定時器觸發，所有分組count值都到齊，直接輸出當前的總count數量
            Long value = totalCountState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), value));
            // 清空狀態
            totalCountState.clear();
        }
    }
}
