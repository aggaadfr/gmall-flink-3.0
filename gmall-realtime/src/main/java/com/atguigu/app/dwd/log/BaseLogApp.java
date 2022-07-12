package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO 2 未经加工的log数据表
 * 1、数据清洗（脏数据清洗，过滤掉非JSON数据）
 * 2、使用keyby聚合mid数据，做新老用户的检验
 * 3、分流（将各个流的数据分别写出到kafka对应的主题中）
 *
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dwd.log
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/11 22:07
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        // 读取kafka topic_log 主题的数据创建流 包含所有业务数据
        String topic = "topic_log";
        String groupId = "topic_log_BaseLogApp";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        // 将数据转换为json格式，并过滤掉非json格式的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // 使用状态编程做新老用户检验,根据mid做聚合
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedByMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 获取"is_new"标记&获取状态数据
                String isNew = value.getJSONObject("common").getString("is_new");
                String lastVisitDt = lastVisitDtState.value();
                Long ts = value.getLong("ts");

                // 判断是否为"1"
                if ("1".equals(isNew)) {
                    // 获取当前数据的时间
                    String curDt = DateFormatUtil.toDate(ts);

                    if (lastVisitDt == null) {
                        lastVisitDtState.update(curDt);
                    } else if (!lastVisitDt.equals(curDt)) {
                        // 最后的登陆时间和当前时间不相等，更新时间
                        value.getJSONObject("common").put("is_new", "0");
                    }

                } else if (lastVisitDt == null) {
                    // 前台检验已经不是新用户了，但是状态中没有存储，则更新状态 ts 为昨天的时间
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    lastVisitDtState.update(yesterday);
                }

                return value;
            }
        });

        // 使用侧输出流对数据进行分流处理，将各个流的数据分别写出到kafka对应的主题中
        OutputTag<String> startTag = new OutputTag<String>("start") {
            //实现了一个匿名函数，为了放置范型的擦出
            // startTag<String> extend OutputTag<String>
            // new OutputTag<String>    --> 编译的时候会被范型擦除
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                String jsonString = value.toString();

                //尝试取出数据中的Error字段
                String error = value.getString("err");
                if (error != null) {
                    //输出数据到错误日志
                    ctx.output(errorTag, jsonString);
                }

                //尝试获取启动字段
                String start = value.getString("start");
                if (start != null) {
                    //输出数据到启动日志
                    ctx.output(startTag, jsonString);
                } else {
                    //取出页面id与时间戳
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    String common = value.getString("common");

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //曝光数据一条条输出
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            display.put("common", common);

                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    //尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page_id", pageId);
                            action.put("ts", ts);
                            action.put("common", common);

                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    //输出数据到页面浏览日志
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });


        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        // 页面浏览：主流
        pageDS.addSink(MyKafkaUtil.getKafkaProducer(page_topic));

        // 启动日志
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        startDS.addSink(MyKafkaUtil.getKafkaProducer(start_topic));

        // 曝光日志
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        displayDS.addSink(MyKafkaUtil.getKafkaProducer(display_topic));

        // 动作日志
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        actionDS.addSink(MyKafkaUtil.getKafkaProducer(action_topic));

        // 错误日志
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        errorDS.addSink(MyKafkaUtil.getKafkaProducer(error_topic));


        env.execute("BaseLogApp");

    }
}
