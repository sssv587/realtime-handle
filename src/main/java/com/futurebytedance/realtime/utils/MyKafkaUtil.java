package com.futurebytedance.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/12/21 - 0:03
 * @Description 操作kafka的工具类
 */
public class MyKafkaUtil {
    private static String kafkaServer = "localhost:9092";

    /**
     * 获取FlinkKafkaConsumer
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        // Kafka连接的一些属性配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    //封装kafkaProducer
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(kafkaServer, topic, new SimpleStringSchema());
    }
}
