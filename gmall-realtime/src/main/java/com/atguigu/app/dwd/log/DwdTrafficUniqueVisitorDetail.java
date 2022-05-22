package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 3
 * dwd层对页面浏览主题，做过滤并统计日活明细uv
 * 1、读取页面浏览主题数据 dwd_traffic_page_log
 * 2、把数据转成json格式，并过滤上一跳id不等于null的数据
 * 3、把数据keyby后，使用状态算子(带有过期时间1 day)，过滤掉重复的mid数据
 * 4、将数据写入到kafka dwd_traffic_unique_visitor_detail 主题
 *
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dwd.log
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/15 22:03
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx//xx");

        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_page_log_uv";

        // 读取 kafka dwd_traffic_page_log 主题数据
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        // 将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);


        // 过滤掉上一跳页面id不等于null的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(json -> json.getJSONObject("page").getString("last_page_id") == null);

        // 按照mid分组
        KeyedStream<JSONObject, String> keyedByMidStream = filterDS.keyBy(str -> str.getJSONObject("common").getString("mid"));

        // 使用状态编程进行每日登陆数据去重
        SingleOutputStreamOperator<JSONObject> uvDetailDS = keyedByMidStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> visitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-dt", String.class);

                StateTtlConfig build = new StateTtlConfig.Builder(Time.days(1))
                        // 在状态创建时初始化，并在每次写操作时更新
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                // 配置状态过期策略
                // 状态一直保留会浪费资源
                valueStateDescriptor.enableTimeToLive(build);

                visitDtState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                String dt = visitDtState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                // 如果数据在凌晨出现乱序，则使用 curDt > dt
                if (dt == null || !dt.equals(curDt)) {
                    visitDtState.update(curDt);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // 将数据写入到kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDetailDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("targetTopic"));


        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
