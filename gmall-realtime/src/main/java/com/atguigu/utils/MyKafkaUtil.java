package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * flink连接kafka工具类
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/8 21:46
 */
public class MyKafkaUtil {
    private static Properties properties = new Properties();
    private static final String BOOTSTRAP_SERVERS = "hadoop102:9092";

    static {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }


    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 自定义实现kafka的反序列化
        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {

            //产生的数据类型或输入格式
            @Override
            public TypeInformation getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }

            @Override
            public boolean isEndOfStream(String o) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record == null || record.value() == null) {
                    return "";
                } else {
                    return new String(record.value());
                }
            }
        }, properties);
    }

    /**
     * 获取flink Kafka生产者
     *
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

}
