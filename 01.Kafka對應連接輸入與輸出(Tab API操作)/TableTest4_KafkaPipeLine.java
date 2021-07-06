package com.it.com.it.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception{
        // 創建執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 連接Kafka，讀取數據
          /*
        Kafka作為消息隊列，和文件系統類似的，只能往裡追加數據，不能修改數據。
       （我用的新版Flink和新版kafka連接器，所以version指定"universal"）
         */
        tableEnv.connect(
                new Kafka()
                .version("universal")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table sensorTable = tableEnv.from("inputTable");

        // 3. 查詢轉換
        // 簡單轉換
        Table resultTable = sensorTable.select("id, temp").filter("id === 'sensor_6'");
        // 聚合統計
        Table aggTable = sensorTable.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        // 4. 建立kafka連接，輸出到不同的topic下
        tableEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("sinktest")
                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                //.field("timestamp", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        tableEnv.execute("");
    }
}
