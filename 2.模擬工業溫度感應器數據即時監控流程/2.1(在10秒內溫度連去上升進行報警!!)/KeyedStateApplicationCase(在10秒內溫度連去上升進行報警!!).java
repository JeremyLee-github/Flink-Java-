package com.it.apitest.state;

import akka.japi.tuple.Tuple3;
import com.it.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        //假設做一個溫度報警，如果一個傳感器前後溫差超過10度就報警。這裡使用鍵控狀態Keyed State + flatMap來實現
        //創建環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        //讀取數據
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 轉換為SensorReading類型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple3<String,Double,Double>> resultStream = dataStream.keyBy("id").flatMap(new MyFlatMapper(10.0));

        resultStream.print();
        env.execute();
    }
    // 如果 傳感器溫度 前後差距超過指定溫度(這裡指定10.0),就報警
    public static class MyFlatMapper extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{
        // 報警的溫差閾值
        private final Double threshold;
        // 記錄上一次的溫度
        ValueState<Double> lastTemperature;

        public MyFlatMapper(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 從運行時上下文中獲取keyedState
            ValueState<Double> lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));

        }

        @Override
        public void close() throws Exception {
            // 手動釋放資源
            lastTemperature.clear();
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTemperature.value();
            Double curTemp = value.getTemperature();
            // 如果不為空，判斷是否溫差超過閾值，超過則報警
            if(lastTemp != null){
                if (Math.abs(curTemp-lastTemp) > threshold){
                    out.collect(new Tuple3<>(value.getId(),lastTemp,curTemp));
                }
            }
            // 更新保存的"上一次溫度"
            lastTemperature.update(curTemp);
        }
    }
}
