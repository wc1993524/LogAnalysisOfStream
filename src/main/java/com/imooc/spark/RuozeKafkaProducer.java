package com.imooc.spark;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * https://www.bilibili.com/video/av36387945?from=search&seid=8946375676581864316
 * Spark Streaming整合Kafka offset管理
 */
public class RuozeKafkaProducer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");

        ProducerConfig config = new ProducerConfig(properties);
        Producer<String,String> producer = new Producer<String, String>(config);

        String topic = "kafka_offset_manager_topic";

        for (int i = 0; i < 100; i++) {
            producer.send(new KeyedMessage<String, String>(topic, i+"kafka生产消息"));
        }
    }
}
