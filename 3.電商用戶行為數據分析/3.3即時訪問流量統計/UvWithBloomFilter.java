package com.it.networkflow_analysis;

import akka.routing.MurmurHash;
import com.it.networkflow_analysis.beans.PageViewCount;
import com.it.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;
import java.util.concurrent.TimeUnit;

public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 從csv文件中獲取數據
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 3. 轉換成POJO,分配時間戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MICROSECONDS)) {
                            @Override
                            public long extractTimestamp(UserBehavior element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));

        // 開窗統計uv值
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFliter());  // 將數據儲存在redis,在進去redis進行確認資料是否存在!!!

        uvStream.print();

        env.execute("uv count with bloom filter job");
    }
    // 自定義觸發器
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每一條數據來到，直接觸發窗口計算，並且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // 這裡主要清除的是自定義狀態!!!如果沒有自定義設定,不用理他!!!
        }
    }
    // 自定義一個布隆過濾器
    public static class MyBloomFilter {
        // 定義位圖的大小，一般需要定義為2的整數次方
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 實現一個hash函數(一般使用MurmurHash處理)
        public Long hashCode(String value, Integer seed) {
            long result = 0L;
            // 為了避免相同的字段產生相同的值(EX:abc,bca),所以先將每個單字拆出做處理
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);  // 將每個單字對應的哈希瑪值+一個隨機數
            }
            return result & (cap-1);
        }
    }

    // 實現自定義的處理函數
    public static class UvCountResultWithBloomFliter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>{
        // 定義jedis連接和布隆過濾器
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        // 使用open方法開啟redis跟代入myBloomFilter
        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            myBloomFilter = new MyBloomFilter(1 << 29);  // 要處理1億個數據，用64MB大小的位圖(1 << 29 => 1左移29個位子)
        }

        // 重寫繼承ProcessAllWindowFunction的子方法
        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            // 將位圖和窗口count值全部存入redis，用windowEnd作為key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            // 把count值存成一張hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1. 取當前的userId
            Long userId = elements.iterator().next().getUserId();

            // 2. 計算位圖中的offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 3. 用redis的getbit命令，判斷對應位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist){ // 因為是針對使用者ID進行去重,所以只針對存在ID進行標記1,然後加總做count取值!!!
                // 如果不存在，對應位圖位置置1
                jedis.setbit(bitmapKey, offset, true);
                // 更新redis中保存的count值
                Long uvCount = 0L;  // 初始count值
                String uvCountString = jedis.hget(countHashName, countKey);

                if (uvCountString != null && !"".equals(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);  // 將uvCountString轉成Long類型!!
                }
                jedis.hset(countHashName, countKey, String.valueOf(uvCount+1));

                out.collect(new PageViewCount("uv", windowEnd, uvCount+1)); // 在控制台顯示輸出
            }
        }

        // 使用close方法關閉redis
        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

}
