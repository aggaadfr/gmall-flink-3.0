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
        // SimpleStringSchema主题中的数据不能为null
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

    /**
     * flinkSQL-kafka读取topic_db主题数据
     * 在SQL中流失概念-时间属性中  -->  PROCTIME()：声明一个额外的列作为处理时间属性
     * <p>
     * 一般来说在spark和flink中不会出现复杂的数据结构，都会在前面处理完后在给到计算，Map的数据结构已经算复杂的了
     *
     * @param groupId
     * @return
     */
    public static String getTopicDbDDL(String groupId) {
        return "CREATE TABLE topic_db ( " +
                "  `database` String, " +
                "  `table` String, " +
                "  `type` String, " +
                "  `data` Map<String,String>, " +
                "  `old` Map<String,String>, " +
                "  `pt` AS PROCTIME() " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", groupId);
    }

    /**
     * Kafka-Source DDL 语句
     *
     * @param topic
     * @param groupId
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset')";
    }

    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return
     */
    public static String getUpsertKafkaDDL(String topic) {
        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}
