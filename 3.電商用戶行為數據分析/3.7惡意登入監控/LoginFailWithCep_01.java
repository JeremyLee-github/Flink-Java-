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
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LoginFailWithCep_01 {
    public static void main(String[] args) throws Exception{

        // 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 從文件中讀取數據
        URL resource = LoginFailWithCep_01.class.getResource("/LoginLog.csv");
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
                .<LoginEvent>begin("failEvents")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                .times(3).consecutive()  // .times(3) => 需要實現三次 .consecutive() => 此條件為必須連續三次,中間不能有插入其他數據!!!
                .within(Time.seconds(5));

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

            // 按照Pattern裡面的定義事件,取得資料!!!
            List<LoginEvent> failEvents = pattern.get("failEvents");

            return new LoginFailWarning(
                    failEvents.get(0).getUserId(),
                    failEvents.get(0).getTimestamp(),
                    failEvents.get(failEvents.size()-1).getTimestamp(),
                    "login fail " + pattern.get("failEvents").size() + " times"
            );
        }
    }
}
