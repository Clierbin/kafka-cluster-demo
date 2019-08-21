package com.gupaovip.kafka.kafkaclusterdemo.demo;

import java.util.Map;

/**
 * ClassName:Methhash
 * Package:com.gupaovip.kafka.kafkaclusterdemo.demo
 * description
 * Created by zhangbin on 2019/8/21.
 *
 * @author: zhangbin q243132465@163.com
 * @Version 1.0.0
 * @CreateTime： 2019/8/21 11:16
 */
public class Methhash {
    public static void main(String[] args) {
        // 17
        int abs = Math.abs("gp-gid3".hashCode() % 50);
        System.out.println(abs);
    }
}
