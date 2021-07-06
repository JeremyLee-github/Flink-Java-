package com.it.apitest.processfunction;

import com.it.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {
    public static void main(String[] args) throws Exception{
        // 創建環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 讀去數據資料
        DataStream<String> inputDataStream1 = env.readTextFile("src/main/resources/sensor.txt");

        // 從socket文本流獲取數據
        DataStream<String> inputDataStream2 = env.socketTextStream("localhost", 7777);

        //轉換成SensorReading類型
        DataStream<SensorReading> dataStream = inputDataStream2.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定義一個OutputTag，用來表示側輸出流低溫流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp"){ }; //注意:後面建議加一個大括號{}!!

        // 測試ProcessFunction，自定義側輸出流實現分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                // 判斷溫度，大於30度，高溫流輸出到主流；小於低溫流輸出到側輸出流
                if (value.getTimestamp() > 30) out.collect(value);
                else ctx.output(lowTempTag,value);
            }
        });

        //輸出數據
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }
}
