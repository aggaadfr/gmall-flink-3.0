package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * TODO 4
 * 流量域用户跳出事务事实表
 *  1、读取kafka dwd_traffic_page_log 主题数据
 *  2、 将数据转换成JSON对象，提取事件时间生成watermark
 *  3、 按照mid进行分组
 *  4、 定义CEP模式序列
 *              判断为进入页面：last_page_id is null
 *             判断为跳出页面：下一条数据的last_page_id is null 或者 超过10秒没有数据(定时器，TTL)
 *             逻辑：
 *                 来了一条数据，取出上一跳页面信息判断是否为null
 *                     1.last_page_id == null ->
 *                         取出状态数据如果不为null则输出
 *                         将当前数据更新至状态中来等待第三条数据
 *                         删除上一次定时器
 *                         重新注册10秒的定时器
 *                     2、last_page_id != null  ->
 *                         清空状态数据
 *                         删除上一次定时器
 *                     定时器方法：
 *                         输出状态数据
 *  5、 将模式序列作用到流上
 *  6、 提取匹配上的事件以及超时事件
 *  7、 合并两个事件流
 *  8、 将数据写出到kafka
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dwd.log
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/24 23:48
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx//xx");

        // 读取kafka dwd_traffic_page_log 主题数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_page_log_jump";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        // 将数据转换成JSON对象，提取事件时间生成watermark
        // 允许的乱序数据为2秒，默认小于确认ts2秒前的数据全部到齐
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 按照mid进行分组
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjWithWmDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        // 定义CEP模式序列
        /*
            判断为进入页面：last_page_id is null
            判断为跳出页面：下一条数据的last_page_id is null 或者 超过10秒没有数据(定时器，TTL)
            逻辑：
                来了一条数据，取出上一跳页面信息判断是否为null
                    1.last_page_id == null ->
                        取出状态数据如果不为null则输出
                        将当前数据更新至状态中来等待第三条数据
                        删除上一次定时器
                        重新注册10秒的定时器
                    2、last_page_id != null  ->
                        清空状态数据
                        删除上一次定时器
                    定时器方法：
                        输出状态数据
         */
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("second").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10));

        Pattern<JSONObject, JSONObject> p2 = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .times(2)           //默认为宽松近邻
                .consecutive()      //指定为严格近邻
                .within(Time.seconds(10));


        // 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedByMidStream, pattern);

        // 提取匹配上的事件以及超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time-out") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                // 超时后的选择
                return map.get("first").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                // 匹配中的选择
                return map.get("first").get(0);
            }
        });

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        // 合并两个事件流
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        // 将数据写出到kafka
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(targetTopic));


        env.execute("DwdTrafficUserJumpDetail");
    }
}
