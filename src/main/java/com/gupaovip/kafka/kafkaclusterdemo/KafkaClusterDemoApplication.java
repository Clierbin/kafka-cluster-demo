package com.gupaovip.kafka.kafkaclusterdemo;

import com.gupaovip.kafka.kafkaclusterdemo.springdemo.GpSpringProducerKafka;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaClusterDemoApplication {

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = SpringApplication.run(KafkaClusterDemoApplication.class, args);
        GpSpringProducerKafka producerKafka = context.getBean(GpSpringProducerKafka.class);
        for (int i = 0; i < 20; i++) {
            producerKafka.send();
            TimeUnit.SECONDS.sleep(2);
        }
    }

}
