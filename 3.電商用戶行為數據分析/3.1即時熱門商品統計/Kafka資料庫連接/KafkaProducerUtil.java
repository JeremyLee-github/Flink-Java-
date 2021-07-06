package com.it.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception{
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws Exception{
        // Kafka配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 這裡是Producer所以使用"key.serializer"&"value.serializer"
        //Consumer使用"key.deserializer" and "value.deserializer"
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定義一個Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 用緩衝方式來讀取文本
        BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\Jeremy\\Desktop\\Java\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line;
        while ((line = bufferedReader.readLine())!=null) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, line);
            // 用producer發送數據
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }
}
