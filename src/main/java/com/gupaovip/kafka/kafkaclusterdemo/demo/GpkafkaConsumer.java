package com.gupaovip.kafka.kafkaclusterdemo.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * ClassName:GpkafkaConsumer
 * Package:com.gupaovip.kafka.kafkaclusterdemo.demo
 * description
 * Created by zhangbin on 2019/8/19.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/8/19 20:58
 */
public class GpkafkaConsumer extends Thread{
    // producer api
    KafkaConsumer<Integer, String> kafkaConsumer;
    // 主题
    String topic;

    public GpkafkaConsumer(String topic) {

        Properties properties = new Properties();
        // kafka bootstrap 的 连接地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.241:9092,192.168.5.173:9092,192.168.5.178:9092,192.168.5.179:9092");
        // key 的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        // value 的反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // client ID
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "gp-consumer");
        // group ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "gp-gid");
        // session 超时时间
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // 自动提交的间隔时间(批量确认)
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //一个新的group的消费者去消费一个topic 重新消费  ->  更换groupid  就会重新消费这个topic下的全部消息(包含历史)
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// 这个属性,他能够消费昨天发布的数据

        kafkaConsumer = new KafkaConsumer<Integer, String>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        kafkaConsumer.subscribe(Collections.singleton(this.topic));
        while (true){

            ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            consumerRecords.forEach(record -> {
                System.out.println(record.key() + "->" + record.value() + "->" + record.offset());
            });
        }
    }

    public static void main(String[] args) {
        new GpkafkaConsumer("test").start();
    }
}
