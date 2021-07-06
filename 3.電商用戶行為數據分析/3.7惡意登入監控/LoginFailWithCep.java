package com.it.loginfail_detect;

import com.it.loginfail_detect.beans.LoginEvent;
import com.it.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LoginFailWithCep {
    public static void main(String[] args) throws Exception{
        // 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 從文件中讀取數據
        URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");
        DataStream<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.of(3, TimeUnit.SECONDS)) {
                            @Override
                            public long extractTimestamp(LoginEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));

        // 1. 定義一個匹配模式
        // firstFail -> secondFail, within 2s
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                .next("secondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                .within(Time.seconds(2));

        // 2. 將匹配模式應用到數據流上，得到一個pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 3. 檢出符合匹配條件的複雜事件，進行轉換處理，得到報警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning());

        warningStream.print();

        env.execute("login fail detect with cep job");
    }

    // 實現自定義的PatternSelectFunction
    public static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning>{
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            // pattern.get("").iterator().next() = pattern.get("").get(0) => 兩個方式都可以取道資料(裡面也只有一筆對應的資料!!!)
            LoginEvent firstFail = pattern.get("firstFail").iterator().next();
            LoginEvent secondFail = pattern.get("secondFail").get(0);

            return new LoginFailWarning(
                    firstFail.getUserId(),
                    firstFail.getTimestamp(),
                    secondFail.getTimestamp(),
                    "login fail 2 times"
            );
        }
    }
}
