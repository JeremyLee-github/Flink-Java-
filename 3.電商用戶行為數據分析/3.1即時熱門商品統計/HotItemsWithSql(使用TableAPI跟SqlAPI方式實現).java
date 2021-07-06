package com.it.hotitems_analysis;

import com.it.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class HotItemsWithSql {
    public static void main(String[] args) throws Exception{
        // 1. 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  //設定事件時間(在Flink 1.12 已經預設是EventTime)

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
                        return element.getTimestamp() * 1000;  // 需要轉換成毫秒
                    }
                }
        ));

        // 4. 創建表執行環境,使用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 5. 將流轉換成表
        Table dataTable = tableEnv.fromDataStream(userBehaviorDataStream,
                $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));

        // 6. 分組開窗
        // Table API (Table API們辦法做Top5排序!!所以需要用下面SQL方式處理!!!)
        Table windowAggTable = dataTable
                .filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("ts")).as("w"))
                .groupBy($("itemId"), $("w"))
                .select($("itemId"), $("w").end().as("windowEnd"), $("itemId").count().as("cnt"));

        // 7. 利用開創函數，對count值進行排序，並獲取Row number，得到Top N
        // SQL
        // 需先將上面 Table API 轉換成流才能讓下面tableEnv.createTemporaryView()進行接收轉換(Table API 轉 SQL 操作!!!!)
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        // 建立表格
        tableEnv.createTemporaryView("agg", aggStream, $("itemId"), $("windowEnd"), $("cnt"));

        Table resultTable = tableEnv.sqlQuery("select * from " +
                "(select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from agg) " +
                "where row_num <= 5");

        // 純SQL實現
        tableEnv.createTemporaryView("data_table", userBehaviorDataStream,
                $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "    from data_table " +
                "    where behavior = 'pv' " +
                "    group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ");

        //tableEnv.toRetractStream(resultTable, Row.class).print();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute("hot items with sql job");
    }
}
