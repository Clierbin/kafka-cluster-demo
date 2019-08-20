package com.gupaovip.kafka.kafkaclusterdemo.springdemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * ClassName:GpSpringProducerKafka
 * Package:com.gupaovip.kafka.kafkaclusterdemo.springdemo
 * description
 * Created by zhangbin on 2019/8/20.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/8/20 10:24
 */
@Component
public class GpSpringProducerKafka {
    @Autowired
    private KafkaTemplate<Integer,String> kafkaTemplate;

    public void send(){
        kafkaTemplate.send("test",1,"msgData");
    }
}
