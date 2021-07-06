package com.it.hotitems_analysis;

import com.it.hotitems_analysis.beans.ItemViewCount;
import com.it.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class HotItems {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //設定事件時間(在Flink 1.12 已經預設是EventTime)

        // 2. 從csv文件中獲取數據
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Jeremy\\Desktop\\Java\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        // 3. 轉換成POJO,分配時間戳和watermark
        DataStream<UserBehavior> userBehaviorDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MICROSECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp()*1000L; //記得轉換成毫秒
                    }
                }
        ));

        // 4. 分組開窗聚合，得到每個窗口內各個商品的count值
        // DataStream<ItemViewCount> windowAggStream = userBehaviorDataStream
        DataStream<ItemViewCount> windowAggStream = userBehaviorDataStream
                // 過濾只保留pv行為
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                // 按照商品ID分組
                .keyBy(UserBehavior::getItemId)
                // 滑動窗口
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 5. 收集同一窗口的所有商品的count數據，排序輸出top n
        DataStream<String> resultStream = windowAggStream
                // 按照窗口分組
                .keyBy(ItemViewCount::getWindowEnd)
                // 用自定義處理函數排序取前5
                .process(new TopNHotItems(5));

        resultStream.print();
        env.execute("hot items analysis");
    }
    // 實現自定義增量聚合函數
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    // 自定義全窗口函數
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    // 實現自定義KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
        // 定義屬性， TopN的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定義狀態列表，保存當前窗口內所有輸出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class)
            );
        }


        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception{
            // 每來一條數據，存入List中，並註冊定時器
            itemViewCountListState.add(value);
            // 模擬等待，所以這裡時間設的比較短(1ms)
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定時器觸發，當前已收集到所有數據，排序輸出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            // 從多到少(越熱門越前面)
            itemViewCounts.sort((a,b)-> -Long.compare(a.getCount(), b.getCount()));  //這個比較好用!!!
            //itemViewCounts.sort(new Comparator<ItemViewCount>() {
            //    @Override
            //    public int compare(ItemViewCount o1, ItemViewCount o2) {
            //        return o2.getCount().intValue() - o1.getCount().intValue();
            //    }
            //});

            StringBuilder resultBuilder = new StringBuilder();

            resultBuilder.append("===================================").append(System.lineSeparator()); //System.lineSeparator()=>換行
            resultBuilder.append("窗口結束時間 : ").append(new Timestamp(timestamp-1)).append(System.lineSeparator()); //這裡的new Timestamp()要從SQL裡面去調~~~

            // 遍歷列表，取top n 輸出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO_").append(i+1).append(" :")
                        .append(" 商品ID =  ").append(currentItemViewCount.getItemId())
                        .append(", 熱門度 = ").append(currentItemViewCount.getCount())
                        .append(System.lineSeparator());
            }
            resultBuilder.append("===================================").append(System.lineSeparator());

            // 控制輸出頻率(隔一秒輸出一次)
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }

}
