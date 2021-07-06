package com.it.loginfail_detect;

import com.it.loginfail_detect.beans.LoginEvent;
import com.it.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class LoginFail_01 {
    // 改進 :　有可能在兩秒內發動超過2次以上攻擊!!破解成功!!
    // 缺點 :  針對亂序的資料源,會沒辦法偵測出來!!!
    public static void main(String[] args) throws Exception{

        // 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 從文件中讀取數據
        URL resource = LoginFail_01.class.getResource("/LoginLog.csv");
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

        // 自定義處理函數檢測連續登錄失敗事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute("login fail detect job");
    }

    // 實現自定義KeyedProcessFunction
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>{

        // 定義屬性，最大連續登錄失敗次數
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定義狀態：保存2秒內所有的登錄失敗事件
        ListState<LoginEvent> loginEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 以登錄事件作為判斷報警的觸發條件，不再註冊定時器

            // 判斷當前事件登錄狀態
            if ("fail".equals(value.getLoginState())) {
                // 1. 如果是登錄失敗，獲取狀態中之前的登錄失敗事件，繼續判斷是否已有失敗事件
                Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
                if (iterator.hasNext()){
                    // 1.1 如果已經有登錄失敗事件，繼續判斷時間戳是否在2秒之內
                    // 獲取已有的登錄失敗事件
                    LoginEvent firstFailEvent = iterator.next();
                    if (value.getTimestamp() - firstFailEvent.getTimestamp() <= 2 ){
                        // 1.1.1 如果在2秒之內，輸出報警
                        out.collect(new LoginFailWarning(
                                value.getUserId(),
                                firstFailEvent.getTimestamp(),
                                value.getTimestamp(),
                                "login fail 2 times in 2s"
                        ));
                    }

                    // 不管報不報警，這次都已處理完畢，直接更新狀態
                    loginEventListState.clear();
                    loginEventListState.add(value);

                }else {
                    // 1.2 如果沒有登錄失敗，直接將當前事件存入ListState
                    loginEventListState.add(value);
                }

            }else {
                // 2. 如果是登錄成功，直接清空狀態
                loginEventListState.clear();
            }

        }
    }

}
