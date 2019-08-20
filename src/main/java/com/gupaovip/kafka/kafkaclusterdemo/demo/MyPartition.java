package com.gupaovip.kafka.kafkaclusterdemo.demo;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * ClassName:MyPartition
 * Package:com.gupaovip.kafka.kafkaclusterdemo.demo
 * description
 * Created by zhangbin on 2019/8/20.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/8/20 11:31
 */
public class MyPartition implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("enter");
        List<PartitionInfo> list = cluster.partitionsForTopic("test_partition");
        int length = list.size();
        if (length>1){
            Random random = new Random();
            return random.nextInt(length);
        }
        return Math.abs(key.hashCode())%length;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
