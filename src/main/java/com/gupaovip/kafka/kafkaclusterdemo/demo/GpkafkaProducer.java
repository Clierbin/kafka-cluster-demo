package com.gupaovip.kafka.kafkaclusterdemo.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * ClassName:GpkafkaProducer
 * Package:com.gupaovip.kafka.kafkaclusterdemo.demo
 * description
 * Created by zhangbin on 2019/8/19.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/8/19 20:36
 */
public class GpkafkaProducer extends Thread {
    // producer api
    KafkaProducer<Integer, String> kafkaProducer;
    // 主题
    String topic;

    public GpkafkaProducer(String topic) {
        Properties properties = new Properties();
        // kafka bootstrap 的 连接地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.241:9092,192.168.5.173:9092,192.168.5.178:9092,192.168.5.179:9092");
        // key 的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        // value 的序列化
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // client ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "gp-producer");
        // 频繁网络通信 -> 批量发送
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "5");
        // 两次发送的间隔时间
        // partition 发送的分区  前提需要保证topic在创建时指定了多个分区,否则会报空指针
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.gupaovip.kafka.kafkaclusterdemo.demo.MyPartition");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "3000");
        this.topic = topic;
        kafkaProducer = new KafkaProducer<Integer, String>(properties);
    }

    @Override
    public void run() {
        int num = 0;
        String msg = "";
        while (num < 20) {
            try {
                msg = "gp kafka practice msg" + num;
                // 同步  get() ->Future()
               /* RecordMetadata recordMetadata = kafkaProducer.send(new ProducerRecord<>(topic, msg)).get();
                System.out.println(recordMetadata.offset() + "->" + recordMetadata.topic() + "->" + recordMetadata.partition());*/
                // 回调通知
                kafkaProducer.send(new ProducerRecord<>(topic, msg), (metadata, exception) -> {
                    System.out.println(metadata.offset() + "->" + metadata.topic() + "->" + metadata.partition());
                });
                TimeUnit.SECONDS.sleep(2);
                ++num;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        new GpkafkaProducer("test_partition").start();
    }
}
