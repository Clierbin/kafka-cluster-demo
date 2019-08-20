package com.gupaovip.kafka.kafkaclusterdemo.springdemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * ClassName:GpSpringConsumerKafka
 * Package:com.gupaovip.kafka.kafkaclusterdemo.springdemo
 * description
 * Created by zhangbin on 2019/8/20.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/8/20 10:34
 */
@Component
public class GpSpringConsumerKafka {
    @KafkaListener(topics = {"test"})
    public void listen(ConsumerRecord record){
        Optional msg = Optional.ofNullable(record.value());
        if (msg.isPresent()){
            System.out.println(msg.get());
        }
    }
}
