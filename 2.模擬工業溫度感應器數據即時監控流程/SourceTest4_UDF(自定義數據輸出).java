package com.it.apitest.source;


import com.it.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
//創造隨機溫度數據!!!!
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception{
        //創建環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度1
        env.setParallelism(1);
        //從創造數據()
        DataStream<SensorReading> DataStream = env.addSource(new MySensorSource());
        //打印輸出
        DataStream.print();
        //執行
        env.execute();
    }
    //自訂義MySensorSource()隨機生成溫度數據提供給Kafka
    public static class MySensorSource implements SourceFunction<SensorReading> {
        // 標示位，控制數據產生
        private Boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //模擬生產實際溫度數據(使用new Random()隨機生成數據)
            Random random = new Random();
            //使用HashMap創造前10個sensorID的初始溫度數據
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 1; i < 11; i++) {
                sensorTempMap.put("sensor_"+i, 45+random.nextGaussian()*15);
            }

            while (running=true){
                for (String sensorID : sensorTempMap.keySet()) {
                    //在當前溫度數值隨機波動(模擬真實溫度數據場景)
                    double newtemp = sensorTempMap.get(sensorID) + random.nextGaussian();
                    sensorTempMap.put(sensorID, newtemp);
                    ctx.collect(new SensorReading(sensorID, System.currentTimeMillis(),newtemp));
                }
                //控制輸出頻率
                Thread.sleep(1000); //時間間隔1秒!!
            }
        }

        @Override
        public void cancel() {
            running = false;  //結束數據輸出!!
        }
    }
}
