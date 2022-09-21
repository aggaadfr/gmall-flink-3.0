package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 封装flatMap
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.func
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/21 21:23
 */
public class OrderDetailFilterFunction {
    public static SingleOutputStreamOperator<JSONObject> getDwdOrderDetail(StreamExecutionEnvironment env, String groupId) {

        String topic = "dwd_trade_order_detail";
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.过滤(""不需要,保留"insert")&转换数据为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = orderDetailStrDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (!"".equals(value)) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    if ("insert".equals(jsonObject.getString("type"))) {
                        out.collect(jsonObject);
                    }
                }
            }
        });

        //TODO 4.按照order_detail_id分组
        KeyedStream<JSONObject, String> keyedByOrderDetailIdStream = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));

        //TODO 5.去重并返回返回结果
        return keyedByOrderDetailIdStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> orderDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderDetailState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("order-detail", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                //获取状态数据并判断是否有数据
                JSONObject orderDetail = orderDetailState.value();

                if (orderDetail == null) {
                    //把当前数据设置进状态并且注册定时器
                    orderDetailState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
                } else {

                    //2022-04-01 11:10:55.042Z
                    String stateTs = orderDetail.getString("ts");
                    //2022-04-01 11:10:55.9Z
                    String curTs = value.getString("ts");

                    int compare = TimestampLtz3CompareUtil.compare(stateTs, curTs);
                    if (compare != 1) {  //表示后到的数据时间大
                        //更新状态
                        orderDetailState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //提取状态数据并输出
                JSONObject orderDetail = orderDetailState.value();
                out.collect(orderDetail);
            }
        });
    }
}
