package com.it.networkflow_analysis;

import com.it.networkflow_analysis.beans.ApacheLogEvent;
import com.it.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class HotPages_final {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 讀取文件，轉換成POJO
        //URL resource = HotPages_final.class.getResource("/apache.log");  // 指定resources裡面的檔案位子
        //DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 方便測試，使用本地Socket輸入數據
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long timestamp = simpleDateFormat.parse(fields[3]).getTime();  // 這裡將原本時間格式轉成時間戳(毫秒)
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                // 這裡資料庫時間為亂序排列(實務也都是亂序時間),所以使用new BoundedOutOfOrdernessTimestampExtractor<>()處理
                // Time.of(1, TimeUnit.SECONDS) => 符合實務操作(這裡選擇延遲一秒)
                new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.of(1, TimeUnit.SECONDS)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();  // 上面timestamp已經轉為毫秒,所以這裡不用進行轉換
                    }
                }
        ));

        dataStream.print();

        // 定義一個側輸出流標籤
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
        };

        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                // 過濾get請求
                .filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    // 使用正則方法把css,js,png,ico等結尾的資源文件頁面去除!!!
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                // 按照url分組
                .keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))  // 設定allowedLateness => 1分鐘
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        // sideOutputLateData輸出
        windowAggStream.print("agg");
        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count數據，排序輸出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print();
        env.execute("hot pages job");
    }
    // 自定義預聚合函數
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    // 實現自定義的窗口函數
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow>{

        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    // 實現自定義的窗口函數
    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String>{

        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定義狀態，保存當前所有PageViewCount到Map中(使用Map函數才可以更新去重)
        MapState<String, Long> pageViewCountMapState;

        // 使用open方法取得資料,存入pageViewCountMapState
        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountMapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
        }

        // 重寫繼承KeyedProcessFunction的方法
        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每來一條數據添加到pageViewCountMapState裡面
            pageViewCountMapState.put(value.getUrl(), value.getCount());
            // 另外在註冊一個定時器(使用當前windowEnd加1毫秒)
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
            // 註冊一個1分鐘之後的定時器，用來清空狀態(根據allowedLateness(Time.minutes(1))所設定的一分鐘時間)
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+60*1000L);
        }

        // 設定定時器觸發
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判斷是否到了窗口關閉清理時間(上面設定一分鐘)，如果是，直接清空狀態返回 => 可以從ctx.getCurrentKey()調出windowEnd!!!
            if (timestamp == ctx.getCurrentKey()+60*1000L){
                pageViewCountMapState.clear();
                return;
            }

            // 利用Lists.newArrayList()的方式,將pageViewCountMapState裡面的數據透過entries().iterator()的方式取出
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            //進行排序
            pageViewCounts.sort((a,b)-> -Long.compare(a.getValue(), b.getValue()));

            // 格式化成String輸出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("======================================").append(System.lineSeparator());
            resultBuilder.append("窗口結束時間 : ").append(new Timestamp(timestamp-1)).append(System.lineSeparator());

            // 遍歷列表，取top n輸出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> pageViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 頁面URL = ").append(pageViewCount.getKey())
                        .append(" 瀏覽量 = ").append(pageViewCount.getValue())
                        .append(System.lineSeparator());
            }
            resultBuilder.append("======================================").append(System.lineSeparator());

            // 控制輸出頻率(隔一秒輸出一次)
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());

        }
    }

}
