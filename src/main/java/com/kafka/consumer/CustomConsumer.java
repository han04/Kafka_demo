package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomConsumer {
    public static void main(String[] args) {
        String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

        //创建Kafka生产者的配置对象
        Properties properties = new Properties();
        //给Kafka配置对象添加信息 BOOTSTRAP_SERVERS
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //key,value 序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //配置Consumer group
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        //创建Consumer对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //注册消费topic
        ArrayList<String> topics = new ArrayList<>();
        //添加
        topics.add("first");
        //订阅
        kafkaConsumer.subscribe(topics);

        while (true) {
            //1s消费一批数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            //打印消费获得
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
